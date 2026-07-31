//! Table catalog management: table lifecycle, declarations, and runtime handles.

pub(crate) mod listeners;
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

pub(crate) use runtime::ChangeListenerFactory;
pub use runtime::{ChangeListener, TableHandle};

use crate::{
    Error, Result,
    consts::{
        DEVICES_TABLE_NAME, SYSTEM_TABLE_CONFIG, TABLE_CATALOG_NAME, TABLES_DIR, is_system_table,
    },
    devices::{Devices, PeerState},
    states::StateHandle,
};

/// Table catalog management: owns the `_catalog` handle and the
/// in-memory map of opened [`TableHandle`]s.
///
/// The `_catalog` Table is the source of truth for table declarations.
/// Every declared table is eagerly opened and stored in the handle map.
/// Handle-map mutation after bootstrap is owned by `TableCatalogListener`;
/// the lifecycle methods here only validate and publish catalog events.
///
/// Exposes lifecycle operations (upsert/get/delete/list/contains) and
/// durability barriers. Table operations themselves are performed through the
/// handle, not on this handler.
pub struct Tables {
    pub(crate) root: PathBuf,
    catalog: Arc<TableHandle>,
    pub(crate) tables: RwLock<HashMap<String, Arc<TableHandle>>>,
    pub(crate) devices: Arc<Devices>,
    table_listeners: Arc<RwLock<Vec<Arc<dyn ChangeListener>>>>,
    listener_factories: Arc<RwLock<Vec<Arc<dyn ChangeListenerFactory>>>>,
    device_listener: Arc<listeners::DeviceRegistryListener>,
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

        let devices = Devices::create(
            Table::create(&root.join(DEVICES_TABLE_NAME), SYSTEM_TABLE_CONFIG.clone())?,
            peer_state,
            local_installation_id,
            display_name,
            public_key,
        )?;
        let catalog = TableHandle::new(
            TABLE_CATALOG_NAME.to_owned(),
            Table::create(&root.join(TABLE_CATALOG_NAME), SYSTEM_TABLE_CONFIG.clone())?,
            Arc::downgrade(&devices),
            true,
        );
        let tables = HashMap::from([
            (TABLE_CATALOG_NAME.to_owned(), catalog.clone()),
            (DEVICES_TABLE_NAME.to_owned(), devices.registry.clone()),
        ]);
        let receipt_listener = listeners::ReceiptListener::build(Arc::downgrade(&devices));
        let table_listeners = Arc::new(RwLock::new(vec![receipt_listener]));
        let device_listener = listeners::DeviceRegistryListener::build(Arc::downgrade(&devices));
        let tables = Arc::new(Self {
            root,
            catalog,
            tables: RwLock::new(tables),
            devices: devices.clone(),
            table_listeners,
            listener_factories: Arc::new(RwLock::new(Vec::new())),
            device_listener,
        });
        tables.register_listeners();

        tables.write_entry(TABLE_CATALOG_NAME, &SYSTEM_TABLE_CONFIG, devices.mint()?)?;
        tables.write_entry(DEVICES_TABLE_NAME, &SYSTEM_TABLE_CONFIG, devices.mint()?)?;

        Ok(tables)
    }

    pub(crate) fn open(
        root: &Path,
        peer_state: Arc<StateHandle<InstallationId, PeerState>>,
        local_installation_id: InstallationId,
        expected_public_key: &PublicKey,
    ) -> Result<Arc<Self>> {
        let tables_dir = root.join(TABLES_DIR);
        let devices = Devices::open(
            Table::open(
                &tables_dir.join(DEVICES_TABLE_NAME),
                SYSTEM_TABLE_CONFIG.clone(),
            )?,
            peer_state,
            local_installation_id,
            expected_public_key,
        )?;
        let devices_weak = Arc::downgrade(&devices);
        let catalog = TableHandle::new(
            TABLE_CATALOG_NAME.to_owned(),
            Table::open(
                &tables_dir.join(TABLE_CATALOG_NAME),
                SYSTEM_TABLE_CONFIG.clone(),
            )?,
            devices_weak.clone(),
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
            (DEVICES_TABLE_NAME.to_owned(), devices.registry.clone()),
        ]);
        for (name, config) in catalog_rows {
            if !is_system_table(&name) {
                let path = tables_dir.join(&name);
                tables.insert(
                    name.clone(),
                    TableHandle::new(
                        name,
                        Table::open(&path, config)?,
                        devices_weak.clone(),
                        false,
                    ),
                );
            }
        }
        let receipt_listener = listeners::ReceiptListener::build(Arc::downgrade(&devices));
        let table_listeners = Arc::new(RwLock::new(vec![receipt_listener]));
        let device_listener = listeners::DeviceRegistryListener::build(Arc::downgrade(&devices));
        let tables = Arc::new(Self {
            root: tables_dir,
            catalog,
            tables: RwLock::new(tables),
            devices: devices.clone(),
            table_listeners,
            listener_factories: Arc::new(RwLock::new(Vec::new())),
            device_listener,
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
        let devices = Devices::join(
            Table::create(&root.join(DEVICES_TABLE_NAME), SYSTEM_TABLE_CONFIG.clone())?,
            peer_state,
            local_installation_id,
        )?;
        let catalog = TableHandle::new(
            TABLE_CATALOG_NAME.to_owned(),
            Table::create(&root.join(TABLE_CATALOG_NAME), SYSTEM_TABLE_CONFIG.clone())?,
            Arc::downgrade(&devices),
            true,
        );
        let tables = HashMap::from([
            (TABLE_CATALOG_NAME.to_owned(), catalog.clone()),
            (DEVICES_TABLE_NAME.to_owned(), devices.registry.clone()),
        ]);
        let receipt_listener = listeners::ReceiptListener::build(Arc::downgrade(&devices));
        let table_listeners = Arc::new(RwLock::new(vec![receipt_listener]));
        let device_listener = listeners::DeviceRegistryListener::build(Arc::downgrade(&devices));
        let tables = Arc::new(Self {
            root,
            catalog,
            tables: RwLock::new(tables),
            devices,
            table_listeners,
            listener_factories: Arc::new(RwLock::new(Vec::new())),
            device_listener,
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
        let tables = self.tables.read().values().cloned().collect::<Vec<_>>();
        for table in tables {
            table.table.write().flush()?;
        }
        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        let tables = self.tables.read().values().cloned().collect::<Vec<_>>();
        for table in tables {
            table.table.write().sync()?;
        }
        Ok(())
    }

    /// Declare a new table or update its config. Creates the table declaration
    /// if it does not exist; if it already exists, updates the config only
    /// when it differs. Returns `true` if a change was made (created or config
    /// changed), `false` if the declaration was already present with the same
    /// config. The table is opened synchronously by `TableCatalogListener`
    /// during the catalog write. The caller obtains a handle separately via
    /// [`Tables::get`]. Refuses system tables.
    pub fn upsert(&self, name: &str, config: TableConfig) -> Result<bool> {
        if is_system_table(name) {
            return Err(Error::SystemTableReadOnly(name.to_owned()));
        }
        self.devices
            .require_access(&self.devices.local_installation_id(), Role::Admin)?;
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
        let stamp = self.devices.mint()?;
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
        self.devices
            .require_access(&self.devices.local_installation_id(), Role::Admin)?;
        if let Some(entry) = self.tables.read().get(name) {
            if Arc::strong_count(entry) > 1 {
                return Err(Error::ResourceBusy(name.to_owned()));
            }
        } else {
            return Ok(false);
        }
        let stamp = self.devices.mint()?;
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
        for table in self.tables.read().values() {
            for listener in self.table_listeners.read().iter() {
                table.add_listener(listener.clone());
            }
        }
        let catalog_listener = listeners::TableCatalogListener::build(
            Arc::downgrade(self),
            self.table_listeners.clone(),
            self.listener_factories.clone(),
        );
        self.catalog.add_listener(catalog_listener);
        self.devices
            .registry
            .add_listener(self.device_listener.clone());
    }

    pub(crate) fn add_internal_listener_factory(&self, factory: Arc<dyn ChangeListenerFactory>) {
        for (name, table) in self.tables.read().iter() {
            table.add_listener(factory.build(name));
        }
        self.listener_factories.write().push(factory);
    }

    pub(crate) fn set_device_observer(
        &self,
        observer: std::sync::Weak<dyn listeners::DeviceChangeObserver>,
    ) {
        self.device_listener.set_observer(observer);
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
