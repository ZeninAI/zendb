//! Table catalog management: table lifecycle, declarations, and runtime handles.

mod listeners;
mod runtime;

use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

use parking_lot::RwLock;
use zendb_storage::{DurableStorage, ReadBackend, Table, TableConfig};
use zendb_types::{
    Blob, Cell, Event, EventStamp, InstallationId, Op, Path as CrdtPath, PrimaryKey, PublicKey,
    Role, Value,
};

pub use runtime::{ChangeListener, TableHandle};

use crate::{
    Error, Result,
    consts::{
        INSTALLATIONS_TABLE_NAME, SYSTEM_TABLE_CONFIG, TABLE_CATALOG_NAME, TABLES_DIR,
        is_system_table,
    },
    installations::{
        Installations, PeerState,
        listeners::{InstallationRegistryListener, ReceiptListener},
    },
    states::StateHandle,
};

/// Table catalog management: owns the `_catalog` handle and the
/// in-memory map of opened [`TableHandle`]s.
///
/// The `_catalog` Table is the source of truth for table declarations.
/// Every declared table is eagerly opened and stored in the handle map.
/// Handle-map mutation after bootstrap is owned by the table catalog listener;
/// the lifecycle methods here only validate and publish catalog events.
///
/// Exposes lifecycle operations (upsert/get/delete/list/contains) and
/// durability barriers. Table operations themselves are performed through the
/// handle, not on this handler.
pub struct Tables {
    pub(crate) root: PathBuf,
    catalog: Arc<TableHandle>,
    pub(crate) tables: RwLock<HashMap<String, Arc<TableHandle>>>,
    pub(crate) installations: Arc<Installations>,
}

impl Tables {
    pub(crate) fn create(
        root: &Path,
        peer_state: Arc<StateHandle<InstallationId, PeerState>>,
        local_installation_id: InstallationId,
        display_name: String,
        public_key: PublicKey,
    ) -> Result<Arc<Self>> {
        let root = root.join(TABLES_DIR);
        fs::create_dir_all(&root)?;

        let installations = Installations::create(
            Table::create(
                &root.join(INSTALLATIONS_TABLE_NAME),
                SYSTEM_TABLE_CONFIG.clone(),
            )?,
            peer_state,
            local_installation_id,
            display_name,
            public_key,
        )?;
        let catalog = TableHandle::new(
            TABLE_CATALOG_NAME.to_owned(),
            Table::create(&root.join(TABLE_CATALOG_NAME), SYSTEM_TABLE_CONFIG.clone())?,
            Arc::downgrade(&installations),
            true,
        );
        let tables = HashMap::from([
            (TABLE_CATALOG_NAME.to_owned(), catalog.clone()),
            (
                INSTALLATIONS_TABLE_NAME.to_owned(),
                installations.registry.clone(),
            ),
        ]);
        let tables = Arc::new(Self {
            root,
            catalog,
            tables: RwLock::new(tables),
            installations: installations.clone(),
        });
        tables.register_listeners();

        tables.write_entry(
            TABLE_CATALOG_NAME,
            &SYSTEM_TABLE_CONFIG,
            installations.mint()?,
        )?;
        tables.write_entry(
            INSTALLATIONS_TABLE_NAME,
            &SYSTEM_TABLE_CONFIG,
            installations.mint()?,
        )?;

        Ok(tables)
    }

    pub(crate) fn open(
        root: &Path,
        peer_state: Arc<StateHandle<InstallationId, PeerState>>,
        local_installation_id: InstallationId,
        expected_public_key: &PublicKey,
    ) -> Result<Arc<Self>> {
        let tables_dir = root.join(TABLES_DIR);
        let installations = Installations::open(
            Table::open(
                &tables_dir.join(INSTALLATIONS_TABLE_NAME),
                SYSTEM_TABLE_CONFIG.clone(),
            )?,
            peer_state,
            local_installation_id,
            expected_public_key,
        )?;
        let installations_weak = Arc::downgrade(&installations);
        let catalog = TableHandle::new(
            TABLE_CATALOG_NAME.to_owned(),
            Table::open(
                &tables_dir.join(TABLE_CATALOG_NAME),
                SYSTEM_TABLE_CONFIG.clone(),
            )?,
            installations_weak.clone(),
            true,
        );

        // Read all declarations from the catalog Table, then open each one.
        // The system tables are already represented by their bootstrapped
        // handles; all application tables are opened from their directories.
        let catalog_rows: Vec<_> = catalog
            .read()
            .entries()
            .map(|(key, cell)| decode_catalog_row(key.into_owned(), cell.into_owned()))
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect();
        let mut tables = HashMap::from([
            (TABLE_CATALOG_NAME.to_owned(), catalog.clone()),
            (
                INSTALLATIONS_TABLE_NAME.to_owned(),
                installations.registry.clone(),
            ),
        ]);
        for (name, config) in catalog_rows {
            if !is_system_table(&name) {
                let path = tables_dir.join(&name);
                tables.insert(
                    name.clone(),
                    TableHandle::new(
                        name,
                        Table::open(&path, config)?,
                        installations_weak.clone(),
                        false,
                    ),
                );
            }
        }
        let tables = Arc::new(Self {
            root: tables_dir,
            catalog,
            tables: RwLock::new(tables),
            installations,
        });
        tables.register_listeners();
        Ok(tables)
    }

    pub(crate) fn join(
        root: &Path,
        peer_state: Arc<StateHandle<InstallationId, PeerState>>,
        local_installation_id: InstallationId,
    ) -> Result<Arc<Self>> {
        let root = root.join(TABLES_DIR);
        fs::create_dir_all(&root)?;
        let installations = Installations::join(
            Table::create(
                &root.join(INSTALLATIONS_TABLE_NAME),
                SYSTEM_TABLE_CONFIG.clone(),
            )?,
            peer_state,
            local_installation_id,
        )?;
        let catalog = TableHandle::new(
            TABLE_CATALOG_NAME.to_owned(),
            Table::create(&root.join(TABLE_CATALOG_NAME), SYSTEM_TABLE_CONFIG.clone())?,
            Arc::downgrade(&installations),
            true,
        );
        let tables = HashMap::from([
            (TABLE_CATALOG_NAME.to_owned(), catalog.clone()),
            (
                INSTALLATIONS_TABLE_NAME.to_owned(),
                installations.registry.clone(),
            ),
        ]);
        let tables = Arc::new(Self {
            root,
            catalog,
            tables: RwLock::new(tables),
            installations,
        });
        tables.register_listeners();
        Ok(tables)
    }

    pub fn contains(&self, name: &str) -> bool {
        self.tables.read().contains_key(name)
    }

    pub fn list(&self) -> Vec<String> {
        self.tables.read().keys().cloned().collect()
    }

    pub fn flush(&self) -> Result<()> {
        for table in self.tables.read().values() {
            table.table.write().flush()?;
        }
        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        for table in self.tables.read().values() {
            table.table.write().sync()?;
        }
        Ok(())
    }

    /// Declare a new table or update its config. Creates the table declaration
    /// if it does not exist; if it already exists, updates the config only
    /// when it differs. Returns `true` if a change was made (created or config
    /// changed), `false` if the declaration was already present with the same
    /// config. The table is opened synchronously by the table catalog listener
    /// during the catalog write. The caller obtains a handle separately via
    /// [`Tables::get`]. Refuses system tables.
    pub fn upsert(&self, name: &str, config: TableConfig) -> Result<bool> {
        if is_system_table(name) {
            return Err(Error::SystemTableReadOnly(name.to_owned()));
        }
        self.installations
            .require_access(&self.installations.local_installation_id(), Role::Admin)?;
        let key = PrimaryKey::String(name.to_owned());
        let current = {
            let catalog = self.catalog.read();
            catalog.get(&key).map(|cell| cell.into_owned())
        }
        .map(|cell| decode_catalog_row(key, cell))
        .transpose()?
        .flatten()
        .map(|(_, config)| config);
        if current.as_ref() == Some(&config) {
            return Ok(false);
        }
        let stamp = self.installations.mint()?;
        self.write_entry(name, &config, stamp)?;
        Ok(true)
    }

    /// Obtain a handle to an existing table. Returns handles to system tables
    /// too; system handles refuse `insert` (see [`TableHandle::is_system`]).
    pub fn get(&self, name: &str) -> Result<Arc<TableHandle>> {
        self.tables
            .read()
            .get(name)
            .cloned()
            .ok_or_else(|| Error::NotFound(name.to_owned()))
    }

    /// Delete a table. Returns `true` if a row was removed, `false` if the
    /// table was not declared. Refuses system tables with `ResourceBusy`.
    pub fn delete(&self, name: &str) -> Result<bool> {
        if is_system_table(name) {
            return Err(Error::ResourceBusy(name.to_owned()));
        }
        self.installations
            .require_access(&self.installations.local_installation_id(), Role::Admin)?;
        if let Some(entry) = self.tables.read().get(name) {
            if Arc::strong_count(entry) > 1 {
                return Err(Error::ResourceBusy(name.to_owned()));
            }
        } else {
            return Ok(false);
        }
        let stamp = self.installations.mint()?;
        self.catalog.insert_internal(Event {
            primary_key: PrimaryKey::String(name.to_owned()),
            path: CrdtPath::new(),
            op: Op::Delete,
            stamp,
        })?;
        Ok(true)
    }

    fn write_entry(&self, name: &str, config: &TableConfig, stamp: EventStamp) -> Result<()> {
        let blob = Blob::encode(config)?;
        self.catalog.insert_internal(Event {
            primary_key: PrimaryKey::String(name.to_owned()),
            path: CrdtPath::new(),
            op: Op::Upsert {
                value: Value::Blob(blob),
            },
            stamp,
        })?;
        Ok(())
    }

    fn register_listeners(self: &Arc<Self>) {
        let receipt_listener = ReceiptListener::build(Arc::downgrade(&self.installations));
        for table in self.tables.read().values() {
            table.add_internal_listener(receipt_listener.clone());
        }
        self.installations
            .registry
            .add_internal_listener(InstallationRegistryListener::build(Arc::downgrade(
                &self.installations,
            )));
        let catalog_listener =
            listeners::CatalogListener::build(Arc::downgrade(self), receipt_listener);
        self.catalog.add_internal_listener(catalog_listener);
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
