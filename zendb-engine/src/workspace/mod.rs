//! Eager workspace lifecycle and resource ownership.

mod cluster;
mod network;
mod onboarding;
mod replication;
mod rotation;
mod snapshot;
mod system;
mod tables;

use std::{
    fs, io,
    io::Write,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU32, AtomicU64},
        Arc, Weak,
    },
    time::{SystemTime, UNIX_EPOCH},
};

use bincode::{Decode, Encode};
use hashbrown::HashMap;
use parking_lot::{Mutex, RwLock};
use zendb_replication::SharedJournal;
use zendb_replication::Table;
use zendb_transport::DeviceProfile;
use zendb_types::{
    ContainerType, DeviceId, Event, Op, Path as ValuePath, PrimaryKey, SyncPolicy, SyncScope,
    WorkspaceAction, WorkspaceId,
};

use log::info;

use crate::TableConfig;

pub use cluster::{ClusterConfig, ClusterRuntime};
pub use network::SyncReport;
pub use onboarding::OnboardingResult;
pub use tables::{RowRequest, TableRequest};

pub(crate) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as u64)
        .unwrap_or(0)
}

pub(crate) const TABLES_DIR: &str = "tables";
const WORKSPACE_ID_FILE: &str = "_workspace_id";
const DEVICE_PROFILE_FILE: &str = "_device_profile";
const SHARED_EVENTS_FILE: &str = "_shared_events";
const FRONTIER_FILE: &str = "_replication_frontier";
const SNAPSHOT_FILE: &str = "_snapshot";
const RECONCILIATION_FILE: &str = "_state_reconciliation_required";

#[derive(Debug, Clone, Encode, Decode)]
pub struct WorkspaceConfig {
    /// Stable identity of the workspace represented by this local replica.
    pub workspace_id: WorkspaceId,
    /// Stable identity for this workspace installation/profile. It is persisted
    /// at the workspace root and reused when reopening the workspace.
    pub device_id: DeviceId,
}

impl Default for WorkspaceConfig {
    fn default() -> Self {
        let device_id = DeviceId::generate().expect("failed to generate workspace device id");
        Self {
            workspace_id: WorkspaceId::generate().expect("failed to generate workspace id"),
            device_id,
        }
    }
}

pub type ConcurrentTable = Arc<RwLock<Table>>;

/// A durable, weak reference to a workspace table.
///
/// The workspace is the single owner of every table, so handles never keep a
/// table (or the workspace) alive. Call [`TableHandle::get`] to obtain a strong
/// guard for an operation; once the owning workspace is dropped, `get` fails
/// instead of resurrecting a detached table.
#[derive(Clone)]
pub struct TableHandle {
    name: String,
    inner: Weak<RwLock<Table>>,
}

impl TableHandle {
    pub(crate) fn new(name: &str, table: &ConcurrentTable) -> Self {
        Self {
            name: name.to_owned(),
            inner: Arc::downgrade(table),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// Upgrade to a strong handle for a single operation, or fail if the owning
    /// workspace has been dropped.
    pub fn get(&self) -> io::Result<ConcurrentTable> {
        self.inner.upgrade().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotConnected,
                format!(
                    "table {:?} is unavailable because its workspace was dropped",
                    self.name
                ),
            )
        })
    }
}

/// Notification emitted after the concrete table cache changes.
#[derive(Clone)]
pub enum TableLifecycleEvent {
    Opened(TableHandle),
    Closed(String),
}

type TableObserver = Arc<dyn Fn(TableLifecycleEvent) + Send + Sync>;

/// RAII registration for a table lifecycle observer.
pub struct TableObservation {
    workspace: Weak<Workspace>,
    id: u64,
}

impl Drop for TableObservation {
    fn drop(&mut self) {
        if let Some(workspace) = self.workspace.upgrade() {
            workspace.table_observers.write().remove(&self.id);
        }
    }
}

/// Lifecycle root for one local replica of a Workspace.
pub struct Workspace {
    path: PathBuf,
    config: WorkspaceConfig,
    tables: RwLock<HashMap<String, ConcurrentTable>>,
    table_observers: RwLock<HashMap<u64, TableObserver>>,
    next_table_observer_id: AtomicU64,
    device_profile: Arc<DeviceProfile>,
    control: Mutex<system::WorkspaceControl>,
    shared_journal: Mutex<SharedJournal>,
    /// Serializes shared sequence allocation, journal append, application,
    /// snapshot capture, and snapshot installation.
    shared_mutation: Mutex<()>,
    state_reconciliation_required: AtomicBool,
    presence: Mutex<HashMap<DeviceId, zendb_transport::PresenceTracker>>,
    presence_idle_ms: AtomicU64,
    presence_grace_multiplier: AtomicU32,
}

impl Workspace {
    /// Create a new workspace at `path`. Fails if the directory already contains a
    /// workspace; use [`Workspace::open`] to reopen an existing one.
    pub fn create(path: &Path, config: WorkspaceConfig) -> io::Result<Arc<Self>> {
        Self::create_with_initial_device(
            path,
            config,
            None,
            std::collections::BTreeSet::new(),
            std::collections::BTreeSet::from([
                zendb_types::WorkspaceRole::Contributor,
                zendb_types::WorkspaceRole::Dispatcher,
                zendb_types::WorkspaceRole::Manager,
            ]),
        )
    }

    /// Create a local bootstrap candidate. Its provisional control state grants
    /// no roles and is replaced by a verified peer snapshot during onboarding.
    pub fn create_joining(
        path: &Path,
        config: WorkspaceConfig,
        requested_name: String,
        capabilities: std::collections::BTreeSet<zendb_types::CapabilityId>,
    ) -> io::Result<Arc<Self>> {
        Self::create_with_initial_device(
            path,
            config,
            Some(requested_name),
            capabilities,
            std::collections::BTreeSet::new(),
        )
    }

    fn create_with_initial_device(
        path: &Path,
        config: WorkspaceConfig,
        requested_name: Option<String>,
        capabilities: std::collections::BTreeSet<zendb_types::CapabilityId>,
        roles: std::collections::BTreeSet<zendb_types::WorkspaceRole>,
    ) -> io::Result<Arc<Self>> {
        fs::create_dir_all(path)?;
        let device_profile = Arc::new(DeviceProfile::create_with_device_id(
            &path.join(DEVICE_PROFILE_FILE),
            config.device_id,
        )?);
        persist_workspace_id(&path.join(WORKSPACE_ID_FILE), &config.workspace_id)?;
        let initial_hlc = device_profile.next_hlc(now_ms())?;
        let initial_device = zendb_types::DeviceRecord {
            name: requested_name.unwrap_or_else(|| format!("device-{}", config.device_id)),
            key_ring: device_profile.initial_key_ring(),
            roles,
            capabilities,
            replication_frontier: zendb_types::ContiguousFrontier::default(),
        };
        let control = system::WorkspaceControl::create(
            &path.join(TABLES_DIR),
            config.device_id,
            initial_device,
            initial_hlc,
        )?;
        let shared_journal =
            SharedJournal::create(&path.join(SHARED_EVENTS_FILE), &path.join(FRONTIER_FILE))?;
        info!("creating workspace at {:?}", path);
        let workspace = Self::from_parts(path, config, device_profile, control, shared_journal)?;
        workspace.recover_shared_journal()?;
        Ok(workspace)
    }

    /// Open an existing workspace at `path`. Fails if the directory does not
    /// contain a valid workspace.
    pub fn open(path: &Path, config: WorkspaceConfig) -> io::Result<Arc<Self>> {
        let mut config = config;
        let device_profile = Arc::new(DeviceProfile::open(&path.join(DEVICE_PROFILE_FILE))?);
        config.device_id = device_profile.device_id();
        config.workspace_id =
            load_or_persist_workspace_id(&path.join(WORKSPACE_ID_FILE), config.workspace_id)?;
        let control = system::WorkspaceControl::open(&path.join(TABLES_DIR))?;
        let shared_journal =
            SharedJournal::open(&path.join(SHARED_EVENTS_FILE), &path.join(FRONTIER_FILE))?;
        info!("opening workspace at {:?}", path);
        let workspace = Self::from_parts(path, config, device_profile, control, shared_journal)?;
        workspace.recover_shared_journal()?;
        Ok(workspace)
    }

    /// Assemble a Workspace from its durable core resources.
    fn from_parts(
        path: &Path,
        config: WorkspaceConfig,
        device_profile: Arc<DeviceProfile>,
        control: system::WorkspaceControl,
        shared_journal: SharedJournal,
    ) -> io::Result<Arc<Self>> {
        let workspace = Arc::new(Self {
            path: path.to_path_buf(),
            config,
            tables: RwLock::new(HashMap::new()),
            table_observers: RwLock::new(HashMap::new()),
            next_table_observer_id: AtomicU64::new(1),
            device_profile,
            control: Mutex::new(control),
            shared_journal: Mutex::new(shared_journal),
            shared_mutation: Mutex::new(()),
            state_reconciliation_required: AtomicBool::new(path.join(RECONCILIATION_FILE).exists()),
            presence: Mutex::new(HashMap::new()),
            presence_idle_ms: AtomicU64::new(30_000),
            presence_grace_multiplier: AtomicU32::new(3),
        });
        let highest_local_sequence = workspace
            .shared_journal
            .lock()
            .highest_sequence(workspace.device_id());
        workspace
            .device_profile
            .advance_origin_seq_past(highest_local_sequence)?;
        if let Some(local_device) = workspace.device(workspace.device_id())? {
            workspace
                .device_profile
                .reconcile_key_ring(&local_device.key_ring)?;
        }
        Ok(workspace)
    }

    /// Filesystem root used by optional local subsystems such as operators.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Subscribe to table open/close events. Callbacks must return quickly.
    pub fn observe_tables(
        self: &Arc<Self>,
        observer: impl Fn(TableLifecycleEvent) + Send + Sync + 'static,
    ) -> TableObservation {
        let id = self
            .next_table_observer_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.table_observers.write().insert(id, Arc::new(observer));
        TableObservation {
            workspace: Arc::downgrade(self),
            id,
        }
    }

    pub(crate) fn notify_table_observers(&self, event: TableLifecycleEvent) {
        let observers: Vec<_> = self.table_observers.read().values().cloned().collect();
        for observer in observers {
            observer(event.clone());
        }
    }

    /// Return a reference to the workspace configuration.
    pub fn config(&self) -> &WorkspaceConfig {
        &self.config
    }

    /// Return the workspace identity represented by this local replica.
    pub fn workspace_id(&self) -> &WorkspaceId {
        &self.config.workspace_id
    }

    /// Return the stable device identity of this local replica.
    pub fn device_id(&self) -> DeviceId {
        self.config.device_id
    }

    /// Internal access to durable local signing and sequence state.
    pub(crate) fn device_profile(&self) -> &DeviceProfile {
        &self.device_profile
    }

    /// Read one live replicated Device record.
    pub fn device(&self, device_id: DeviceId) -> io::Result<Option<zendb_types::DeviceRecord>> {
        self.control.lock().device(device_id)
    }

    /// List every currently admitted Device record.
    pub fn devices(&self) -> io::Result<Vec<(DeviceId, zendb_types::DeviceRecord)>> {
        self.control.lock().devices()
    }

    /// List the live shared-table declarations from replicated control state.
    pub fn list_shared_tables(&self) -> io::Result<Vec<String>> {
        self.control.lock().shared_tables()
    }

    /// Return whether replicated control currently declares this table live.
    pub fn is_shared_table(&self, name: &str) -> io::Result<bool> {
        Ok(self
            .control
            .lock()
            .shared_table_state(name)?
            .is_some_and(|(live, _)| live))
    }

    /// Idempotently create a Contributor-authorized shared-table declaration
    /// and open its local physical storage.
    pub fn create_shared_table(
        self: &Arc<Self>,
        name: &str,
        config: TableConfig,
    ) -> io::Result<TableHandle> {
        if self.is_shared_table(name)? {
            return self.table_impl(name, None);
        }
        if self.table_config(name).is_some() {
            // Validate the requested physical override before making the
            // catalog row shared; a failed migration must not change policy.
            self.table_impl(name, Some(config))?;
            self.set_table_sync_policy(name, SyncPolicy::Inherit)?;
            return self.table_impl(name, None);
        }
        let at = self.device_profile.next_hlc(now_ms())?;
        self.commit_shared_event(system::catalog_event(at, name, Some(config.clone()))?)?;
        self.table_impl(name, Some(config))
    }

    /// Change this device's table-wide synchronization boundary.
    ///
    /// Local-to-inherited promotion publishes CRDT merge state with original
    /// row clocks. Inherited-to-local is replica-local and emits no data event.
    pub fn set_table_sync_policy(
        self: &Arc<Self>,
        name: &str,
        policy: SyncPolicy,
    ) -> io::Result<bool> {
        if self.table_config(name).is_none() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("table {name:?} is absent from _catalog"),
            ));
        }
        if policy == SyncPolicy::Inherit {
            let local = self.device(self.device_id())?.ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "local Device is not admitted",
                )
            })?;
            if !local.allows(WorkspaceAction::Contribute) {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "Contributor is required to publish a local table",
                ));
            }
        }

        let catalog_cell = {
            let mut control = self.control.lock();
            if !control.set_catalog_policy(name, policy)? {
                return Ok(false);
            }
            control
                .catalog_cell(name)
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "catalog row disappeared"))?
        };
        let table = self.table_impl(name, None)?;
        table.get()?.write().set_table_sync_policy(policy);

        if policy == SyncPolicy::Local {
            return Ok(true);
        }

        self.mark_state_reconciliation_required()?;

        let at = self.device_profile.next_hlc(now_ms())?;
        self.commit_shared_event(Event {
            table_id: system::CATALOG_TABLE.into(),
            primary_key: PrimaryKey::String(name.into()),
            path: ValuePath::new(),
            op: Op::Merge {
                cell: catalog_cell
                    .shared_clone(SyncScope::Shared)
                    .ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            "promoted catalog row still resolves to local policy",
                        )
                    })?,
            },
            hlc: at,
        })?;

        let rows = table.get()?.read().shared_rows();
        for (primary_key, cell) in rows {
            let at = self.device_profile.next_hlc(now_ms())?;
            self.commit_shared_event(Event {
                table_id: name.into(),
                primary_key,
                path: ValuePath::new(),
                op: Op::Merge { cell },
                hlc: at,
            })?;
        }
        Ok(true)
    }

    /// Tombstone a live shared-table declaration. Physical files are retained
    /// for recovery and explicit later cleanup.
    pub fn delete_shared_table(self: &Arc<Self>, name: &str) -> io::Result<bool> {
        if !self.is_shared_table(name)? {
            return Ok(false);
        }
        let at = self.device_profile.next_hlc(now_ms())?;
        self.commit_shared_event(system::catalog_event(at, name, None)?)?;
        self.close_table(name);
        Ok(true)
    }

    /// Directly pre-admit a Device as an implicit Reader. Non-empty roles,
    /// staged keys, and pre-populated frontiers are rejected; a Manager grants
    /// roles in separate visible events.
    pub fn admit_device(
        self: &Arc<Self>,
        device_id: DeviceId,
        record: zendb_types::DeviceRecord,
    ) -> io::Result<bool> {
        system::validate_initial_device(&record)?;
        let at = self.device_profile.next_hlc(now_ms())?;
        self.commit_shared_event(system::admit_event(at, device_id, record))?;
        Ok(true)
    }

    /// Tombstone a Device membership Cell. Requires the Manager role.
    pub fn remove_device(self: &Arc<Self>, device_id: DeviceId) -> io::Result<bool> {
        let at = self.device_profile.next_hlc(now_ms())?;
        self.commit_shared_event(system::remove_device_event(at, device_id))?;
        Ok(true)
    }

    /// Change a Device alias. A Device may rename itself; changing another
    /// Device requires Manager.
    pub fn rename_device(self: &Arc<Self>, device_id: DeviceId, name: String) -> io::Result<bool> {
        let at = self.device_profile.next_hlc(now_ms())?;
        self.commit_shared_event(system::replace_field_event(
            at,
            device_id,
            "name",
            zendb_types::Value::String(name),
        ))?;
        Ok(true)
    }

    /// Add or remove one fixed workspace role. Requires Manager.
    pub fn set_device_role(
        self: &Arc<Self>,
        device_id: DeviceId,
        role: zendb_types::WorkspaceRole,
        enabled: bool,
    ) -> io::Result<bool> {
        let at = self.device_profile.next_hlc(now_ms())?;
        self.commit_shared_event(system::role_event(at, device_id, role, enabled))?;
        Ok(true)
    }

    /// Advertise or withdraw one scheduler capability for the local Device.
    pub fn set_local_capability(
        self: &Arc<Self>,
        capability: zendb_types::CapabilityId,
        enabled: bool,
    ) -> io::Result<bool> {
        let at = self.device_profile.next_hlc(now_ms())?;
        let control = self.control.lock();
        let event = control.capability_event(at, self.device_id(), capability, enabled)?;
        drop(control);
        self.commit_shared_event(event)?;
        Ok(true)
    }

    pub(crate) fn publish_local_frontier(
        self: &Arc<Self>,
        frontier: &zendb_types::ContiguousFrontier,
    ) -> io::Result<bool> {
        self.commit_frontier_checkpoint(frontier.clone())?;
        Ok(true)
    }

    /// Publish progress only when the replicated Device checkpoint is behind.
    pub fn checkpoint_local_frontier(self: &Arc<Self>) -> io::Result<bool> {
        let frontier = self.shared_frontier();
        let device = self.device(self.device_id())?.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::PermissionDenied,
                "local Device is not admitted",
            )
        })?;
        if device.replication_frontier == frontier {
            return Ok(false);
        }
        self.publish_local_frontier(&frontier)
    }
}

fn persist_workspace_id(path: &Path, workspace_id: &WorkspaceId) -> io::Result<()> {
    let bytes = bincode::encode_to_vec(workspace_id, bincode::config::standard())
        .map_err(|error| io::Error::other(error.to_string()))?;
    let mut file = fs::File::create(path)?;
    file.write_all(&bytes)?;
    file.sync_all()
}

fn load_or_persist_workspace_id(path: &Path, configured: WorkspaceId) -> io::Result<WorkspaceId> {
    match fs::read(path) {
        Ok(bytes) => {
            let (workspace_id, consumed): (WorkspaceId, usize) =
                bincode::decode_from_slice(&bytes, bincode::config::standard())
                    .map_err(|error| io::Error::other(error.to_string()))?;
            if consumed != bytes.len() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "workspace identity contains trailing bytes",
                ));
            }
            Ok(workspace_id)
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            persist_workspace_id(path, &configured)?;
            Ok(configured)
        }
        Err(error) => Err(error),
    }
}

impl Drop for Workspace {
    fn drop(&mut self) {
        self.tables.write().clear();
    }
}
