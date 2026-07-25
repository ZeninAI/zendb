//! Workspace identity, bootstrap lifecycle, and synchronous orchestration API.

use std::{
    fs::{self, File, OpenOptions},
    hash::Hash,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use bincode::{Decode, Encode};
use zendb_storage::{StateConfig, TableConfig};
use zendb_types::{
    utils::{deserialize_from, serialize_to_vec},
    EventId, EventStamp, EventTime, PeerId, Roles, WorkspaceId,
};

use crate::{
    catalog::{
        is_system_state, is_system_table, Catalog, StateHandle, TableHandle, TableInfo,
        UpdateOutcome, DEVICES_NAME,
    },
    devices::{Devices, PeerRecord},
    Error, Result,
};

const IDENTITY_FILE: &str = "_identity";
const LOCK_FILE: &str = "_lock";

#[derive(Debug, Clone)]
pub struct WorkspaceConfig {
    pub local_peer_name: String,
}

impl Default for WorkspaceConfig {
    fn default() -> Self {
        Self {
            local_peer_name: "local".to_owned(),
        }
    }
}

#[derive(Debug, Clone, Encode, Decode)]
struct WorkspaceIdentity {
    workspace_id: WorkspaceId,
    local_peer_id: PeerId,
}

struct WorkspaceLock {
    _file: File,
}

impl WorkspaceLock {
    fn acquire(root: &Path) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(root.join(LOCK_FILE))?;
        file.try_lock()
            .map_err(|_| Error::AlreadyExists("workspace is already open".to_owned()))?;
        Ok(Self { _file: file })
    }
}

pub struct Workspace {
    root: PathBuf,
    identity: WorkspaceIdentity,
    catalog: Catalog,
    devices: Devices,
    _lock: WorkspaceLock,
}

impl Workspace {
    pub fn create(path: impl AsRef<Path>, config: WorkspaceConfig) -> Result<Self> {
        let root = path.as_ref().to_path_buf();
        fs::create_dir_all(&root)?;
        let lock = WorkspaceLock::acquire(&root)?;
        let identity_path = root.join(IDENTITY_FILE);
        if identity_path.exists() {
            return Err(Error::AlreadyExists("workspace identity".to_owned()));
        }

        let identity = WorkspaceIdentity {
            workspace_id: WorkspaceId::generate(),
            local_peer_id: PeerId::generate(),
        };
        fs::write(&identity_path, serialize_to_vec(&identity)?)?;

        let physical_ms: u64 = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| Error::Io(std::io::Error::other(error.to_string())))?
            .as_millis()
            .try_into()
            .map_err(|_| Error::ClockExhausted)?;
        let catalog_stamp = EventStamp::new(
            EventId::new(identity.local_peer_id, 1),
            EventTime::new(physical_ms, 0),
        );
        let devices_stamp = EventStamp::new(
            EventId::new(identity.local_peer_id, 2),
            EventTime::new(physical_ms, 1),
        );
        let catalog = Catalog::create(&root, catalog_stamp, devices_stamp)?;
        let devices = Devices::create(
            catalog.table_entry(DEVICES_NAME)?,
            catalog.peer_state::<PeerId, PeerRecord>()?,
            identity.local_peer_id,
        )?;
        catalog.replay_receipts(&devices)?;
        devices.bootstrap_local(config.local_peer_name)?;

        Ok(Self {
            root,
            identity,
            catalog,
            devices,
            _lock: lock,
        })
    }

    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let root = path.as_ref().to_path_buf();
        let lock = WorkspaceLock::acquire(&root)?;
        let bytes = fs::read(root.join(IDENTITY_FILE))?;
        let identity: WorkspaceIdentity = deserialize_from(&bytes)?;
        let catalog = Catalog::open(&root)?;
        let devices = Devices::open(
            catalog.table_entry(DEVICES_NAME)?,
            catalog.peer_state::<PeerId, PeerRecord>()?,
            identity.local_peer_id,
        )?;
        catalog.replay_receipts(&devices)?;

        Ok(Self {
            root,
            identity,
            catalog,
            devices,
            _lock: lock,
        })
    }

    pub fn id(&self) -> WorkspaceId {
        self.identity.workspace_id
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn devices(&self) -> &Devices {
        &self.devices
    }

    pub fn create_table(&self, name: &str, config: TableConfig) -> Result<TableHandle> {
        self.devices.authorize(Roles::Operator)?;
        let stamp = self.devices.mint()?;
        let entry = self.catalog.create_table(name, config, stamp)?;
        self.devices.observe(stamp)?;
        Ok(TableHandle::new(
            name.to_owned(),
            entry,
            self.devices.clone(),
        ))
    }

    pub fn table(&self, name: &str) -> Result<TableHandle> {
        if is_system_table(name) {
            return Err(Error::NotFound(name.to_owned()));
        }
        Ok(TableHandle::new(
            name.to_owned(),
            self.catalog.table_entry(name)?,
            self.devices.clone(),
        ))
    }

    pub fn contains_table(&self, name: &str) -> bool {
        !is_system_table(name) && self.catalog.contains_table(name)
    }

    pub fn list_tables(&self) -> Vec<TableInfo> {
        self.catalog.list_tables()
    }

    pub fn update_table(&self, name: &str, config: TableConfig) -> Result<UpdateOutcome> {
        self.devices.authorize(Roles::Operator)?;
        let stamp = self.devices.mint()?;
        let outcome = self.catalog.update_table(name, config, stamp)?;
        if outcome == UpdateOutcome::Updated {
            self.devices.observe(stamp)?;
        }
        Ok(outcome)
    }

    pub fn delete_table(&self, name: &str) -> Result<bool> {
        self.devices.authorize(Roles::Operator)?;
        let stamp = self.devices.mint()?;
        let deleted = self.catalog.delete_table(name, stamp)?;
        if deleted {
            self.devices.observe(stamp)?;
        }
        Ok(deleted)
    }

    pub fn state<K, V>(&self, name: &str, config: Option<StateConfig>) -> Result<StateHandle<K, V>>
    where
        K: Encode + Decode<()> + Hash + Eq + Clone + Ord + Send + Sync + 'static,
        V: Encode + Decode<()> + Clone + Send + Sync + 'static,
    {
        if is_system_state(name) {
            return Err(Error::NotFound(name.to_owned()));
        }
        self.catalog.state(name, config)
    }

    pub fn contains_state(&self, name: &str) -> bool {
        !is_system_state(name) && self.catalog.contains_state(name)
    }

    pub fn list_states(&self) -> Vec<String> {
        self.catalog.list_states()
    }

    pub fn list_open_states(&self) -> Vec<String> {
        self.catalog.list_open_states()
    }

    pub fn state_config(&self, name: &str) -> Option<StateConfig> {
        (!is_system_state(name))
            .then(|| self.catalog.state_config(name))
            .flatten()
    }

    pub fn close_state(&self, name: &str) -> bool {
        self.catalog.close_state(name)
    }

    pub fn delete_state(&self, name: &str) -> Result<bool> {
        self.catalog.delete_state(name)
    }

    pub fn flush(&self) -> Result<()> {
        self.devices.flush()
    }
}

impl Drop for Workspace {
    fn drop(&mut self) {
        let _ = self.flush();
    }
}
