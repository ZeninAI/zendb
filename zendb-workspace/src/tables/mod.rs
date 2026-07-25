//! Table catalog management: table lifecycle, declarations, and runtime handles.

pub(crate) mod listeners;
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

pub use model::TableInfo;
pub(crate) use model::{is_system_table, DEVICES_NAME, TABLE_CATALOG_NAME};
pub(crate) use runtime::TableEntry;
pub use runtime::{ChangeListener, TableConsumer, TableHandle, TableReadGuard};

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

    /// Declare a new table. Void-returning: the caller obtains a handle
    /// separately via [`Tables::open`]. The table is opened synchronously by
    /// the `CatalogSync` callback during the catalog write.
    pub fn create(&self, name: &str, config: TableConfig) -> Result<()> {
        self.devices.authorize(zendb_types::Roles::Operator)?;
        let stamp = self.devices.mint()?;
        self.core.create_table(name, config, stamp)
    }

    /// Obtain a handle to an existing table. Returns handles to system tables
    /// too; system handles refuse `insert` (see [`TableHandle::is_system`]).
    pub fn open(&self, name: &str) -> Result<TableHandle> {
        Ok(TableHandle::new(
            name.to_owned(),
            self.core.table_entry(name)?,
            self.devices.clone(),
            is_system_table(name),
        ))
    }

    /// Update a table's config. Returns `true` if the config changed, `false`
    /// if it was already the same. Works on system tables (config updates are
    /// allowed); the `CatalogSync` callback no-ops since they're already open.
    pub fn update(&self, name: &str, config: TableConfig) -> Result<bool> {
        self.devices.authorize(zendb_types::Roles::Operator)?;
        let stamp = self.devices.mint()?;
        self.core.update_table(name, config, stamp)
    }

    /// Delete a table. Returns `true` if a row was removed, `false` if the
    /// table was not declared. Refuses system tables with `ResourceBusy`.
    pub fn delete(&self, name: &str) -> Result<bool> {
        self.devices.authorize(zendb_types::Roles::Operator)?;
        let stamp = self.devices.mint()?;
        self.core.delete_table(name, stamp)
    }
}

/// Internal storage boundary for tables: owns the `_table_catalog` Table and
/// the in-memory map of opened table handles.
///
/// The `_table_catalog` Table is the source of truth for table declarations.
/// This struct does not cache declarations in memory; `contains`/`list`/`update`
/// read the catalog Table directly. Only the opened [`TableEntry`] handles are
/// cached, keyed by table name. Handle-map mutation after bootstrap is owned
/// by the `CatalogSync` listener; the CRUD methods here only validate and
/// publish catalog events.
pub(crate) struct TablesCore {
    pub(crate) root: PathBuf,
    pub(crate) catalog: Arc<TableEntry>,
    pub(crate) tables: RwLock<HashMap<String, Arc<TableEntry>>>,
}

impl TablesCore {
    /// Open the system tables (`_table_catalog`, `_devices`) and insert them
    /// into the handle map. Writes NO catalog rows — the bootstrap caller
    /// writes those after listeners are registered, via
    /// [`Self::write_bootstrap_entry`].
    pub(crate) fn create(root: &Path) -> Result<Arc<Self>> {
        let tables_root = root.join("tables");
        fs::create_dir_all(&tables_root)?;

        let catalog = TableEntry::build(Table::create(
            &tables_root.join(TABLE_CATALOG_NAME),
            TableConfig::default(),
        )?)?;
        let devices = TableEntry::build(Table::create(
            &tables_root.join(DEVICES_NAME),
            TableConfig::default(),
        )?)?;
        let result = Arc::new(Self {
            root: root.to_path_buf(),
            catalog: catalog.clone(),
            tables: RwLock::new(HashMap::new()),
        });
        {
            let mut tables = result.tables.write();
            tables.insert(TABLE_CATALOG_NAME.to_owned(), catalog);
            tables.insert(DEVICES_NAME.to_owned(), devices);
        }
        Ok(result)
    }

    pub(crate) fn open(root: &Path) -> Result<Arc<Self>> {
        let tables_root = root.join("tables");
        let catalog = TableEntry::build(Table::open(
            &tables_root.join(TABLE_CATALOG_NAME),
            TableConfig::default(),
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
                    TableEntry::build(Table::open(&tables_root.join(&name), config.clone())?)?
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

    /// Write a bootstrap catalog row directly. Used only during workspace
    /// bootstrap, after listeners are registered, so the `CatalogSync`
    /// callback fires (and no-ops since the system tables are pre-opened) and
    /// the `Receipt` callback observes the stamp.
    pub(crate) fn write_bootstrap_entry(
        &self,
        name: &str,
        config: &TableConfig,
        stamp: EventStamp,
    ) -> Result<()> {
        self.write_entry(name, config, stamp)
    }

    fn create_table(&self, name: &str, config: TableConfig, stamp: EventStamp) -> Result<()> {
        if is_system_table(name) || self.catalog_entry(name).is_ok() {
            return Err(Error::AlreadyExists(name.to_owned()));
        }
        self.write_entry(name, &config, stamp)
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

    fn update_table(&self, name: &str, config: TableConfig, stamp: EventStamp) -> Result<bool> {
        let current = self.catalog_entry(name)?;
        if current == config {
            return Ok(false);
        }
        self.write_entry(name, &config, stamp)?;
        Ok(true)
    }

    fn delete_table(&self, name: &str, stamp: EventStamp) -> Result<bool> {
        if is_system_table(name) {
            return Err(Error::ResourceBusy(name.to_owned()));
        }
        if let Some(entry) = self.tables.read().get(name) {
            if Arc::strong_count(entry) > 1 {
                return Err(Error::ResourceBusy(name.to_owned()));
            }
        } else {
            return Ok(false);
        }
        self.write_delete(name, stamp)?;
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
        self.catalog.insert(Event {
            primary_key: PrimaryKey::String(name.to_owned()),
            path: CrdtPath::new(),
            op: Op::Upsert {
                value: Value::Blob(blob),
            },
            stamp,
        })?;
        self.catalog.table.write().sync()?;
        Ok(())
    }

    fn write_delete(&self, name: &str, stamp: EventStamp) -> Result<()> {
        self.catalog.insert(Event {
            primary_key: PrimaryKey::String(name.to_owned()),
            path: CrdtPath::new(),
            op: Op::Delete,
            stamp,
        })?;
        self.catalog.table.write().sync()?;
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
