//! Table catalog management: table lifecycle, declarations, and runtime handles.

mod model;
mod runtime;

use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

use parking_lot::RwLock;
use zendb_storage::{DurableStorage, ReadBackend, Table, TableConfig};
use zendb_types::{Blob, Cell, Event, EventStamp, Op, Path as CrdtPath, PrimaryKey, Value};

pub(crate) use model::{is_system_table, DEVICES_NAME, TABLE_CATALOG_NAME};
pub use model::{TableInfo, UpdateOutcome};
pub(crate) use runtime::TableEntry;
pub use runtime::{TableConsumer, TableHandle, TableReadGuard};

use crate::{devices::Devices, Error, Result};

/// Public handler for table catalog management.
///
/// Exposes lifecycle operations (create/update/delete/list/contains) and
/// `open` for obtaining a [`TableHandle`]. Table operations themselves are
/// performed through the handle, not on this handler.
#[derive(Clone)]
pub struct Tables {
    core: Arc<TablesCore>,
    devices: Arc<Devices>,
}

impl Tables {
    pub(crate) fn new(core: Arc<TablesCore>, devices: Arc<Devices>) -> Self {
        Self { core, devices }
    }

    pub fn contains(&self, name: &str) -> bool {
        !is_system_table(name) && self.core.contains_table(name)
    }

    pub fn list(&self) -> Vec<TableInfo> {
        self.core.list_tables()
    }

    pub fn create(&self, name: &str, config: TableConfig) -> Result<TableHandle> {
        self.devices.authorize(zendb_types::Roles::Operator)?;
        let stamp = self.devices.mint()?;
        let entry = self.core.create_table(name, config, stamp)?;
        self.devices.observe(stamp)?;
        Ok(TableHandle::new(
            name.to_owned(),
            entry,
            self.devices.clone(),
        ))
    }

    pub fn open(&self, name: &str) -> Result<TableHandle> {
        if is_system_table(name) {
            return Err(Error::NotFound(name.to_owned()));
        }
        Ok(TableHandle::new(
            name.to_owned(),
            self.core.table_entry(name)?,
            self.devices.clone(),
        ))
    }

    pub fn update(&self, name: &str, config: TableConfig) -> Result<UpdateOutcome> {
        self.devices.authorize(zendb_types::Roles::Operator)?;
        let stamp = self.devices.mint()?;
        let outcome = self.core.update_table(name, config, stamp)?;
        if outcome == UpdateOutcome::Updated {
            self.devices.observe(stamp)?;
        }
        Ok(outcome)
    }

    pub fn delete(&self, name: &str) -> Result<bool> {
        self.devices.authorize(zendb_types::Roles::Operator)?;
        let stamp = self.devices.mint()?;
        let deleted = self.core.delete_table(name, stamp)?;
        if deleted {
            self.devices.observe(stamp)?;
        }
        Ok(deleted)
    }
}

/// Internal storage boundary for tables: owns the `_table_catalog` Table and
/// the in-memory map of opened table handles.
///
/// The `_table_catalog` Table is the source of truth for table declarations.
/// This struct does not cache declarations in memory; `contains`/`list`/`update`
/// read the catalog Table directly. Only the opened [`TableEntry`] handles are
/// cached, keyed by table name.
pub(crate) struct TablesCore {
    root: PathBuf,
    catalog: Arc<TableEntry>,
    tables: RwLock<HashMap<String, Arc<TableEntry>>>,
}

impl TablesCore {
    pub(crate) fn create(
        root: &Path,
        catalog_stamp: EventStamp,
        devices_stamp: EventStamp,
    ) -> Result<Arc<Self>> {
        let tables_root = root.join("tables");
        fs::create_dir_all(&tables_root)?;

        let catalog_config = TableConfig::default();
        let catalog = TableEntry::new(Table::create(
            &tables_root.join(TABLE_CATALOG_NAME),
            catalog_config.clone(),
        )?)?;
        let result = Arc::new(Self {
            root: root.to_path_buf(),
            catalog: catalog.clone(),
            tables: RwLock::new(HashMap::new()),
        });
        result
            .tables
            .write()
            .insert(TABLE_CATALOG_NAME.to_owned(), catalog);
        result.write_entry(TABLE_CATALOG_NAME, &catalog_config, catalog_stamp)?;

        let devices_config = TableConfig::default();
        let devices = TableEntry::new(Table::create(
            &tables_root.join(DEVICES_NAME),
            devices_config.clone(),
        )?)?;
        result.write_entry(DEVICES_NAME, &devices_config, devices_stamp)?;
        result
            .tables
            .write()
            .insert(DEVICES_NAME.to_owned(), devices);
        Ok(result)
    }

    pub(crate) fn open(root: &Path) -> Result<Arc<Self>> {
        let tables_root = root.join("tables");
        let catalog_config = TableConfig::default();
        let catalog = TableEntry::new(Table::open(
            &tables_root.join(TABLE_CATALOG_NAME),
            catalog_config,
        )?)?;
        let result = Arc::new(Self {
            root: root.to_path_buf(),
            catalog: catalog.clone(),
            tables: RwLock::new(HashMap::new()),
        });

        // Read all declarations from the catalog Table, then open each one.
        // The catalog row for `_table_catalog` itself resolves to the already
        // opened catalog handle; all others are opened from their physical
        // directory.
        let declarations: Vec<_> = catalog
            .table
            .read()
            .entries()
            .map(|(key, cell)| decode_catalog_row(key.into_owned(), cell.into_owned()))
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect();
        {
            let mut tables = result.tables.write();
            for (name, config) in declarations {
                let table = if name == TABLE_CATALOG_NAME {
                    catalog.clone()
                } else {
                    TableEntry::new(Table::open(&tables_root.join(&name), config.clone())?)?
                };
                tables.insert(name, table);
            }
        }
        Ok(result)
    }

    pub(crate) fn table_entry(&self, name: &str) -> Result<Arc<TableEntry>> {
        self.tables
            .read()
            .get(name)
            .cloned()
            .ok_or_else(|| Error::NotFound(name.to_owned()))
    }

    pub(crate) fn replay_receipts(&self, devices: &Devices) -> Result<()> {
        let entries: Vec<_> = self.tables.read().values().cloned().collect();
        for entry in entries {
            let mut receipt = entry.receipt.lock();
            for change in receipt.by_ref() {
                devices.observe(change?.event.stamp)?;
            }
            receipt.commit()?;
        }
        Ok(())
    }

    fn create_table(
        &self,
        name: &str,
        config: TableConfig,
        stamp: EventStamp,
    ) -> Result<Arc<TableEntry>> {
        if is_system_table(name) || self.tables.read().contains_key(name) {
            return Err(Error::AlreadyExists(name.to_owned()));
        }

        let path = self.root.join("tables").join(name);
        if path.exists() {
            fs::remove_dir_all(&path)?;
        }
        let table = TableEntry::new(Table::create(&path, config.clone())?)?;
        self.write_entry(name, &config, stamp)?;
        self.tables.write().insert(name.to_owned(), table.clone());
        Ok(table)
    }

    fn contains_table(&self, name: &str) -> bool {
        self.tables.read().contains_key(name)
    }

    fn list_tables(&self) -> Vec<TableInfo> {
        // Read declarations from the catalog Table (the source of truth),
        // not from the in-memory handle map, so that tables declared but not
        // currently open are still listed.
        let mut tables: Vec<_> = self
            .catalog
            .table
            .read()
            .entries()
            .filter_map(|(key, cell)| {
                let name = match key.into_owned() {
                    PrimaryKey::String(name) => name,
                    _ => return None,
                };
                if is_system_table(&name) {
                    return None;
                }
                let (_, config) =
                    decode_catalog_row(PrimaryKey::String(name.clone()), cell.into_owned())
                        .ok()??;
                Some(TableInfo { name, config })
            })
            .collect();
        tables.sort_by(|left, right| left.name.cmp(&right.name));
        tables
    }

    fn update_table(
        &self,
        name: &str,
        config: TableConfig,
        stamp: EventStamp,
    ) -> Result<UpdateOutcome> {
        if is_system_table(name) {
            return Err(Error::ResourceBusy(name.to_owned()));
        }
        let current = self.catalog_entry(name)?;
        if current == config {
            return Ok(UpdateOutcome::Unchanged);
        }

        self.write_entry(name, &config, stamp)?;
        Ok(UpdateOutcome::Updated)
    }

    fn delete_table(&self, name: &str, stamp: EventStamp) -> Result<bool> {
        if is_system_table(name) {
            return Err(Error::ResourceBusy(name.to_owned()));
        }

        let mut tables = self.tables.write();
        let Some(entry) = tables.get(name) else {
            return Ok(false);
        };
        if Arc::strong_count(entry) > 1 {
            return Err(Error::ResourceBusy(name.to_owned()));
        }

        {
            let mut catalog = self.catalog.table.write();
            catalog.insert(Event {
                primary_key: PrimaryKey::String(name.to_owned()),
                path: CrdtPath::new(),
                op: Op::Delete,
                stamp,
            })?;
            catalog.sync()?;
        }
        // Drop the handle before removing the physical directory so the Table
        // closes its files first. The `tables` lock is still held, which is
        // fine: directory removal does not touch the index.
        let entry = tables.remove(name);
        drop(entry);

        let path = self.root.join("tables").join(name);
        if path.exists() {
            fs::remove_dir_all(path)?;
        }
        Ok(true)
    }

    /// Read one declaration from the catalog Table.
    fn catalog_entry(&self, name: &str) -> Result<TableConfig> {
        let key = PrimaryKey::String(name.to_owned());
        let catalog = self.catalog.table.read();
        let cell = catalog
            .get(&key)
            .ok_or_else(|| Error::NotFound(name.to_owned()))?;
        let blob = match cell.into_owned().value {
            Some(Value::Blob(blob)) => blob,
            None => return Err(Error::NotFound(name.to_owned())),
            Some(_) => {
                return Err(Error::CorruptCatalog(format!(
                    "table catalog row {name:?} is not a Blob"
                )));
            }
        };
        blob.decode().map_err(|error| {
            Error::CorruptCatalog(format!(
                "table catalog row {name:?} cannot be decoded: {error}"
            ))
        })
    }

    fn write_entry(&self, name: &str, config: &TableConfig, stamp: EventStamp) -> Result<()> {
        let blob = Blob::encode(config)?;
        let mut catalog = self.catalog.table.write();
        catalog.insert(Event {
            primary_key: PrimaryKey::String(name.to_owned()),
            path: CrdtPath::new(),
            op: Op::Upsert {
                value: Value::Blob(blob),
            },
            stamp,
        })?;
        catalog.sync()?;
        Ok(())
    }
}

fn decode_catalog_row(key: PrimaryKey, cell: Cell) -> Result<Option<(String, TableConfig)>> {
    let PrimaryKey::String(name) = key else {
        return Err(Error::CorruptCatalog(
            "table catalog key is not a String".to_owned(),
        ));
    };
    let blob = match cell.value {
        Some(Value::Blob(blob)) => blob,
        None => return Ok(None),
        Some(_) => {
            return Err(Error::CorruptCatalog(format!(
                "table catalog row {name:?} is not a Blob"
            )));
        }
    };
    let config = blob.decode().map_err(|error| {
        Error::CorruptCatalog(format!(
            "table catalog row {name:?} cannot be decoded: {error}"
        ))
    })?;
    Ok(Some((name, config)))
}
