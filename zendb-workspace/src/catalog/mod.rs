//! Table-backed workspace catalog and Catalog-owned typed State lifecycle.

mod model;
mod runtime;
mod states;

use std::{
    collections::HashMap,
    fs,
    hash::Hash,
    path::{Path, PathBuf},
    sync::Arc,
};

use bincode::{Decode, Encode};
use parking_lot::RwLock;
use zendb_storage::{DurableStorage, KeyDirConfig, ReadBackend, StateConfig, Table, TableConfig};
use zendb_types::{Blob, Cell, Event, EventStamp, Op, Path as CrdtPath, PrimaryKey, Value};

pub use model::{CatalogEntry, TableInfo, UpdateOutcome};
pub use runtime::{TableConsumer, TableHandle, TableReadGuard};
pub use states::StateHandle;

pub(crate) use model::{
    is_system_state, is_system_table, DEVICES_NAME, PEER_STATE_NAME, TABLE_CATALOG_NAME,
};
pub(crate) use runtime::TableEntry;

use self::states::StateCatalog;
use crate::{devices::Devices, Error, Result};

pub(crate) struct Catalog {
    root: PathBuf,
    catalog: Arc<TableEntry>,
    index: RwLock<CatalogIndex>,
    states: StateCatalog,
}

#[derive(Default)]
struct CatalogIndex {
    entries: HashMap<String, TableConfig>,
    tables: HashMap<String, Arc<TableEntry>>,
}

impl Catalog {
    pub(crate) fn create(
        root: &Path,
        catalog_stamp: EventStamp,
        devices_stamp: EventStamp,
    ) -> Result<Self> {
        let tables_root = root.join("tables");
        fs::create_dir_all(&tables_root)?;
        let states = StateCatalog::create(root)?;

        let catalog_config = TableConfig::default();
        let catalog = TableEntry::new(Table::create(
            &tables_root.join(TABLE_CATALOG_NAME),
            catalog_config.clone(),
        )?)?;
        let result = Self {
            root: root.to_path_buf(),
            catalog: catalog.clone(),
            index: RwLock::new(CatalogIndex::default()),
            states,
        };
        {
            let mut index = result.index.write();
            index
                .entries
                .insert(TABLE_CATALOG_NAME.to_owned(), catalog_config.clone());
            index.tables.insert(TABLE_CATALOG_NAME.to_owned(), catalog);
        }
        result.write_entry(
            TABLE_CATALOG_NAME,
            &CatalogEntry::new(catalog_config),
            catalog_stamp,
        )?;

        let devices_config = TableConfig::default();
        let devices = TableEntry::new(Table::create(
            &tables_root.join(DEVICES_NAME),
            devices_config.clone(),
        )?)?;
        result.write_entry(
            DEVICES_NAME,
            &CatalogEntry::new(devices_config.clone()),
            devices_stamp,
        )?;
        {
            let mut index = result.index.write();
            index
                .entries
                .insert(DEVICES_NAME.to_owned(), devices_config);
            index.tables.insert(DEVICES_NAME.to_owned(), devices);
        }
        Ok(result)
    }

    pub(crate) fn open(root: &Path) -> Result<Self> {
        let tables_root = root.join("tables");
        let catalog_config = TableConfig::default();
        let catalog = TableEntry::new(Table::open(
            &tables_root.join(TABLE_CATALOG_NAME),
            catalog_config,
        )?)?;
        let result = Self {
            root: root.to_path_buf(),
            catalog: catalog.clone(),
            index: RwLock::new(CatalogIndex::default()),
            states: StateCatalog::open(root)?,
        };

        let declarations: Vec<_> = catalog
            .table
            .read()
            .entries()
            .map(|(key, cell)| decode_catalog_row(key.into_owned(), cell.into_owned()))
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect();
        for (name, entry) in declarations {
            let table = if name == TABLE_CATALOG_NAME {
                catalog.clone()
            } else {
                TableEntry::new(Table::open(&tables_root.join(&name), entry.config.clone())?)?
            };
            let mut index = result.index.write();
            index.entries.insert(name.clone(), entry.config);
            index.tables.insert(name, table);
        }
        Ok(result)
    }

    pub(crate) fn table_entry(&self, name: &str) -> Result<Arc<TableEntry>> {
        self.index
            .read()
            .tables
            .get(name)
            .cloned()
            .ok_or_else(|| Error::NotFound(name.to_owned()))
    }

    pub(crate) fn replay_receipts(&self, devices: &Devices) -> Result<()> {
        let entries: Vec<_> = self.index.read().tables.values().cloned().collect();
        for entry in entries {
            let mut receipt = entry.receipt.lock();
            for change in receipt.by_ref() {
                devices.observe(change?.event.stamp)?;
            }
            receipt.commit()?;
        }
        Ok(())
    }

    pub(crate) fn create_table(
        &self,
        name: &str,
        config: TableConfig,
        stamp: EventStamp,
    ) -> Result<Arc<TableEntry>> {
        if is_system_table(name) || self.index.read().entries.contains_key(name) {
            return Err(Error::AlreadyExists(name.to_owned()));
        }

        let path = self.root.join("tables").join(name);
        if path.exists() {
            fs::remove_dir_all(&path)?;
        }
        let table = TableEntry::new(Table::create(&path, config.clone())?)?;
        self.write_entry(name, &CatalogEntry::new(config.clone()), stamp)?;
        let mut index = self.index.write();
        index.entries.insert(name.to_owned(), config);
        index.tables.insert(name.to_owned(), table.clone());
        Ok(table)
    }

    pub(crate) fn contains_table(&self, name: &str) -> bool {
        self.index.read().entries.contains_key(name)
    }

    pub(crate) fn list_tables(&self) -> Vec<TableInfo> {
        let mut tables: Vec<_> = self
            .index
            .read()
            .entries
            .iter()
            .filter(|(name, _)| !is_system_table(name))
            .map(|(name, config)| TableInfo {
                name: name.clone(),
                config: config.clone(),
            })
            .collect();
        tables.sort_by(|left, right| left.name.cmp(&right.name));
        tables
    }

    pub(crate) fn update_table(
        &self,
        name: &str,
        config: TableConfig,
        stamp: EventStamp,
    ) -> Result<UpdateOutcome> {
        if is_system_table(name) {
            return Err(Error::ResourceBusy(name.to_owned()));
        }
        let current = self
            .index
            .read()
            .entries
            .get(name)
            .cloned()
            .ok_or_else(|| Error::NotFound(name.to_owned()))?;
        if current == config {
            return Ok(UpdateOutcome::Unchanged);
        }

        self.write_entry(name, &CatalogEntry::new(config.clone()), stamp)?;
        self.index.write().entries.insert(name.to_owned(), config);
        Ok(UpdateOutcome::Updated)
    }

    pub(crate) fn delete_table(&self, name: &str, stamp: EventStamp) -> Result<bool> {
        if is_system_table(name) {
            return Err(Error::ResourceBusy(name.to_owned()));
        }

        let mut index = self.index.write();
        let Some(entry) = index.tables.get(name) else {
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
        let entry = index.tables.remove(name);
        index.entries.remove(name);
        drop(index);
        drop(entry);

        let path = self.root.join("tables").join(name);
        if path.exists() {
            fs::remove_dir_all(path)?;
        }
        Ok(true)
    }

    pub(crate) fn state<K, V>(
        &self,
        name: &str,
        config: Option<StateConfig>,
    ) -> Result<StateHandle<K, V>>
    where
        K: Encode + Decode<()> + Hash + Eq + Clone + Ord + Send + Sync + 'static,
        V: Encode + Decode<()> + Clone + Send + Sync + 'static,
    {
        self.states.state(name, config)
    }

    pub(crate) fn peer_state<K, V>(&self) -> Result<StateHandle<K, V>>
    where
        K: Encode + Decode<()> + Hash + Eq + Clone + Ord + Send + Sync + 'static,
        V: Encode + Decode<()> + Clone + Send + Sync + 'static,
    {
        self.states.state(
            PEER_STATE_NAME,
            Some(StateConfig::Unordered(KeyDirConfig::default())),
        )
    }

    pub(crate) fn contains_state(&self, name: &str) -> bool {
        self.states.contains(name)
    }

    pub(crate) fn list_states(&self) -> Vec<String> {
        self.states
            .list()
            .into_iter()
            .filter(|name| !is_system_state(name))
            .collect()
    }

    pub(crate) fn list_open_states(&self) -> Vec<String> {
        self.states
            .list_open()
            .into_iter()
            .filter(|name| !is_system_state(name))
            .collect()
    }

    pub(crate) fn state_config(&self, name: &str) -> Option<StateConfig> {
        self.states.config(name)
    }

    pub(crate) fn close_state(&self, name: &str) -> bool {
        !is_system_state(name) && self.states.close(name)
    }

    pub(crate) fn delete_state(&self, name: &str) -> Result<bool> {
        if is_system_state(name) {
            return Err(Error::ResourceBusy(name.to_owned()));
        }
        self.states.delete(name)
    }

    fn write_entry(&self, name: &str, entry: &CatalogEntry, stamp: EventStamp) -> Result<()> {
        let blob = Blob::encode(entry)?;
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

fn decode_catalog_row(key: PrimaryKey, cell: Cell) -> Result<Option<(String, CatalogEntry)>> {
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
    let entry = blob.decode().map_err(|error| {
        Error::CorruptCatalog(format!(
            "table catalog row {name:?} cannot be decoded: {error}"
        ))
    })?;
    Ok(Some((name, entry)))
}
