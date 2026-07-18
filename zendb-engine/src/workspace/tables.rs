//! Table lookup and operator subscription maintenance.

use std::{borrow::Cow, fs, io, sync::Arc};

use log::{info, trace};
use parking_lot::RwLock;
use zendb_replication::{Table, TableConfig};
use zendb_storage::backend::_traits::{ReadBackend, Storage};
use zendb_types::{
    Cell, ContainerType, EventIdentity, Op, Path as ValuePath, PrimaryKey, SyncPolicy, SyncScope,
    Value,
};

use super::{TableHandle, TableLifecycleEvent, Workspace, TABLES_DIR};

/// Fluent table lookup/creation command.
pub struct TableRequest {
    workspace: Arc<Workspace>,
    name: String,
    config: Option<TableConfig>,
    policy: Option<SyncPolicy>,
}

/// Fluent row/path command backed by Workspace authorization and routing.
pub struct RowRequest {
    table: TableRequest,
    key: PrimaryKey,
    path: ValuePath,
}

impl TableRequest {
    pub(crate) fn new(workspace: &Arc<Workspace>, name: &str) -> Self {
        Self {
            workspace: Arc::clone(workspace),
            name: name.into(),
            config: None,
            policy: None,
        }
    }

    pub fn config(mut self, config: TableConfig) -> Self {
        self.config = Some(config);
        self
    }

    pub fn local(mut self) -> Self {
        self.policy = Some(SyncPolicy::Local);
        self
    }

    pub fn shared(mut self) -> Self {
        self.policy = Some(SyncPolicy::Inherit);
        self
    }

    pub fn row(self, key: PrimaryKey) -> RowRequest {
        RowRequest {
            table: self,
            key,
            path: ValuePath::new(),
        }
    }

    /// Tombstone a shared declaration or remove a local table and its files.
    pub fn delete(self) -> io::Result<bool> {
        if !self.workspace.contains_table(&self.name) {
            return Ok(false);
        }
        if self.workspace.is_shared_table(&self.name)? {
            self.workspace.delete_shared_table(&self.name)
        } else {
            self.workspace.delete_table(&self.name)
        }
    }

    /// Open a cataloged table without implicitly creating a new declaration.
    pub fn open(self) -> io::Result<TableHandle> {
        if !self.workspace.contains_table(&self.name) {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("table {:?} is absent from _catalog", self.name),
            ));
        }
        if let Some(policy) = self.policy {
            let shared = self.workspace.is_shared_table(&self.name)?;
            if shared != policy.resolve(SyncScope::Shared).is_shared() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "requested table policy conflicts with the local catalog Cell",
                ));
            }
        }
        self.workspace.table_impl(&self.name, self.config)
    }

    /// Create the declaration when absent, otherwise open the existing table.
    pub fn create(self) -> io::Result<TableHandle> {
        if self.workspace.contains_table(&self.name) {
            // Validate a physical override before changing catalog policy.
            let handle = self.workspace.table_impl(&self.name, self.config)?;
            if let Some(policy) = self.policy {
                let is_shared = self.workspace.is_shared_table(&self.name)?;
                if is_shared != policy.resolve(SyncScope::Shared).is_shared() {
                    self.workspace.set_table_sync_policy(&self.name, policy)?;
                }
            }
            return Ok(handle);
        }
        let policy = self.policy.unwrap_or(SyncPolicy::Local);
        match policy {
            SyncPolicy::Inherit => self
                .workspace
                .create_shared_table(&self.name, self.config.unwrap_or_default()),
            SyncPolicy::Local => self.workspace.table_impl(&self.name, self.config),
        }
    }
}

impl RowRequest {
    pub fn at(mut self, path: ValuePath) -> Self {
        self.path = path;
        self
    }

    pub fn get(self) -> io::Result<Option<Cell>> {
        let RowRequest { table, key, path } = self;
        let handle = table.open()?;
        let table = handle.get()?;
        let value = ReadBackend::get(&*table.read(), &key).map(Cow::into_owned);
        Ok(value.and_then(|cell| cell.cell_at_path(&path).cloned()))
    }

    pub fn replace(self, value: Value) -> io::Result<Option<EventIdentity>> {
        self.apply(Op::Replace { value })
    }

    pub fn delete(self) -> io::Result<Option<EventIdentity>> {
        self.apply(Op::Delete)
    }

    pub fn apply(self, op: Op) -> io::Result<Option<EventIdentity>> {
        // Opening first makes a typo an error instead of implicitly creating a
        // local table through the mutation path.
        let _ = self.table.workspace.table(&self.table.name).open()?;
        self.table
            .workspace
            .mutate(&self.table.name, self.key, self.path, op)
    }

    pub fn local(self) -> io::Result<bool> {
        self.set_policy(SyncPolicy::Local)
    }

    pub fn inherit(self) -> io::Result<bool> {
        self.set_policy(SyncPolicy::Inherit)
    }

    fn set_policy(self, policy: SyncPolicy) -> io::Result<bool> {
        let RowRequest { table, key, path } = self;
        // Verify the table exists and honor any requested physical override
        // before routing the policy transition through Workspace replication.
        let _ = table.workspace.table(&table.name).open()?;
        table
            .workspace
            .set_path_sync_policy(&table.name, key, path, policy)
    }
}

impl Workspace {
    pub fn table(self: &Arc<Self>, name: &str) -> TableRequest {
        TableRequest::new(self, name)
    }

    /// Return `true` if a table exists in the durable table catalog.
    pub fn contains_table(&self, name: &str) -> bool {
        self.control
            .lock()
            .table_config(name)
            .ok()
            .flatten()
            .is_some()
    }

    /// Return `true` if a table is currently loaded in memory.
    pub fn is_table_open(&self, name: &str) -> bool {
        self.tables.read().contains_key(name)
    }

    /// List every table known to the durable table catalog.
    pub fn list_tables(&self) -> Vec<String> {
        self.control
            .lock()
            .catalog_names()
            .into_iter()
            .filter(|name| !super::system::is_system_table(name))
            .collect()
    }

    /// List application and reserved system entries from `_catalog`.
    pub fn list_catalog_tables(&self) -> Vec<String> {
        self.control.lock().catalog_names()
    }

    /// List every table currently loaded in memory.
    pub fn list_open_tables(&self) -> Vec<String> {
        self.tables.read().keys().cloned().collect()
    }

    /// Return the persisted config for a table, if the catalog contains one.
    pub fn table_config(&self, name: &str) -> Option<TableConfig> {
        self.control.lock().table_config(name).ok().flatten()
    }

    /// Remove an open table from the in-memory cache and notify observers that
    /// the input closed. The durable table remains in the catalog and can
    /// be reopened later with [`Workspace::table`].
    pub fn close_table(&self, name: &str) -> bool {
        let removed = self.tables.write().remove(name).is_some();
        if !removed {
            return false;
        }

        info!("closing table {name:?}");
        self.notify_table_observers(TableLifecycleEvent::Closed(name.to_owned()));
        true
    }

    /// Delete a table entirely: remove from the catalog, evict from memory,
    /// notify operators, and delete its on-disk directory.
    ///
    /// The catalog entry is removed first so that operators receiving the
    /// `InputClosed` event can distinguish a deletion from a temporary close
    /// by checking [`Workspace::contains_table`].
    ///
    /// Running operators that subscribe to this table receive an `InputClosed`
    /// event. If that was their only input, they will suspend (remain active in
    /// the catalog but not running). The operator's consumer offsets for this
    /// table are destroyed along with the table files.
    ///
    /// Returns `Ok(true)` if the table existed, `Ok(false)` if it was not in
    /// the catalog.
    pub fn delete_table(&self, name: &str) -> io::Result<bool> {
        if self
            .control
            .lock()
            .shared_table_state(name)?
            .is_some_and(|(live, _)| live)
        {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "use delete_shared_table for replicated table membership",
            ));
        }
        let was_open = self.tables.write().remove(name).is_some();

        let at = self.device_profile.next_hlc(super::now_ms())?;
        if !self.control.lock().delete_local_table(name, at)? {
            return Ok(false);
        }

        if was_open {
            self.notify_table_observers(TableLifecycleEvent::Closed(name.to_owned()));
        }

        // 4. Delete on-disk files (includes all consumer offsets).
        let path = self.path.join(TABLES_DIR).join(name);
        if path.exists() {
            fs::remove_dir_all(&path)?;
        }
        info!("deleted table {name:?}");
        Ok(true)
    }

    /// Return an open table, opening it lazily from the catalog or creating it
    /// with `config`. For an existing table, a supplied config is a local
    /// physical-storage override; the replicated catalog baseline is unchanged.
    /// Automatically starts catalog operators that subscribe to this table.
    pub(crate) fn table_impl(
        self: &Arc<Self>,
        name: &str,
        config: Option<TableConfig>,
    ) -> io::Result<TableHandle> {
        if matches!(
            name,
            super::system::CATALOG_TABLE
                | super::system::DEVICES_TABLE
                | super::system::TICKETS_TABLE
        ) {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "system tables are managed by Workspace APIs",
            ));
        }
        // Fast path: already open
        if let Some(table) = self.tables.read().get(name).cloned() {
            if config
                .as_ref()
                .is_some_and(|requested| requested != &Storage::config(&*table.read()))
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "a table configuration override cannot migrate existing storage",
                ));
            }
            trace!("table {name:?} already open, returning cached handle");
            return Ok(TableHandle::new(name, &table));
        }

        // Slow path: open/create under table catalog lock, then spawn operators outside it.
        let table = {
            let mut table_catalog = self.control.lock();
            // Double-check under catalog lock
            if let Some(table) = self.tables.read().get(name).cloned() {
                if config
                    .as_ref()
                    .is_some_and(|requested| requested != &Storage::config(&*table.read()))
                {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "a table configuration override cannot migrate existing storage",
                    ));
                }
                return Ok(TableHandle::new(name, &table));
            }

            let table = match table_catalog.table_config(name)? {
                Some(saved_config) => {
                    let sync_policy = if table_catalog
                        .shared_table_state(name)?
                        .is_some_and(|(live, _)| live)
                    {
                        zendb_types::SyncPolicy::Inherit
                    } else {
                        zendb_types::SyncPolicy::Local
                    };
                    let path = self.path.join(TABLES_DIR).join(name);
                    if path.exists() {
                        let persisted = Table::persisted_config(&path)?.ok_or_else(|| {
                            io::Error::new(
                                io::ErrorKind::InvalidData,
                                format!("table {name:?} has no physical configuration"),
                            )
                        })?;
                        if config
                            .as_ref()
                            .is_some_and(|requested| requested != &persisted)
                        {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidInput,
                                "a table configuration override cannot migrate existing storage",
                            ));
                        }
                        info!("opening existing table {name:?}");
                        Arc::new(RwLock::new(Table::open_with_policy(
                            &path,
                            persisted,
                            self.config.device_id,
                            sync_policy,
                        )?))
                    } else {
                        let effective_config = config.unwrap_or(saved_config);
                        info!("materializing cataloged table {name:?}");
                        Arc::new(RwLock::new(Table::create_with_policy(
                            &path,
                            effective_config,
                            self.config.device_id,
                            sync_policy,
                        )?))
                    }
                }
                None => {
                    let config = config.unwrap_or_default();
                    let path = self.path.join(TABLES_DIR).join(name);
                    if let Some(parent) = path.parent() {
                        fs::create_dir_all(parent)?;
                    }
                    info!("creating new table {name:?}");
                    let raw =
                        Table::create_with_device(&path, config.clone(), self.config.device_id)?;
                    table_catalog.put_local_table(
                        name,
                        config,
                        self.device_profile.next_hlc(super::now_ms())?,
                    )?;
                    Arc::new(RwLock::new(raw))
                }
            };
            // Publish the cache entry before releasing the serialized open path.
            self.tables
                .write()
                .insert(name.to_owned(), Arc::clone(&table));
            table
        };
        let handle = TableHandle::new(name, &table);
        self.notify_table_observers(TableLifecycleEvent::Opened(handle.clone()));
        Ok(handle)
    }
}
