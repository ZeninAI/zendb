//! Eager workspace lifecycle and resource ownership.

mod cluster;
mod control;
mod journal;
mod network;
mod onboarding;
mod operators;
mod replication;
mod rotation;
mod snapshot;
mod states;
mod tables;
mod timers;

use std::{
    any::Any,
    fs, io,
    io::Write,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU32, AtomicU64},
        Arc, Weak,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use bincode::{Decode, Encode};
use hashbrown::HashMap;
use parking_lot::{Condvar, Mutex, RwLock};
use zendb_storage::core::{
    btree::{BPlusTree, BPlusTreeConfig},
    keydir::{KeyDir, KeyDirConfig},
    traits::DurableStorage,
};
use zendb_storage::frontend::{
    state::{State, StateConfig},
    table::Table,
};
use zendb_transport::DeviceProfile;
use zendb_types::{DeviceId, WorkspaceId};

use log::{debug, info};

use crate::{
    operator::worker::OperatorWorker, runtime::Executor, DispatchOperator, OperatorPhase,
    TableConfig,
};

pub use cluster::{ClusterConfig, ClusterRuntime};
pub use network::SyncReport;
pub use onboarding::OnboardingResult;
use timers::run_scheduler;

/// Ordering key: earliest `fire_at_ms` first; within the same millisecond,
/// lexicographic by operator name.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Encode, Decode)]
pub(crate) struct TimerKey {
    pub(crate) fire_at_ms: u64,
    pub(crate) operator: String,
}

/// Opaque payload stored with each timer.
#[derive(Debug, Clone, Encode, Decode)]
pub(crate) struct TimerEntry {
    pub(crate) payload: Vec<u8>,
}

pub(crate) type TimerStore = BPlusTree<TimerKey, TimerEntry>;

pub(crate) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as u64)
        .unwrap_or(0)
}

#[derive(Debug, Clone, Encode, Decode)]
pub(super) struct OperatorEntry<Config> {
    pub(super) config: Config,
    pub(super) phase: OperatorPhase,
}

pub(super) type TableCatalog = KeyDir<String, TableConfig>;
pub(super) type StateCatalog = KeyDir<String, StateConfig>;
pub(super) type OperatorCatalog<Config> = KeyDir<String, OperatorEntry<Config>>;

const TABLE_CATALOG_FILE: &str = "_tables";
const STATE_CATALOG_FILE: &str = "_states";
const OPERATOR_CATALOG_FILE: &str = "_operators";
pub(crate) const TABLES_DIR: &str = "tables";
pub(crate) const STATES_DIR: &str = "states";
const TIMERS_FILE: &str = "_timers";
const WORKSPACE_ID_FILE: &str = "_workspace_id";
const DEVICE_PROFILE_FILE: &str = "_device_profile";
const CONTROL_FILE: &str = "_control";
const SHARED_EVENTS_FILE: &str = "_shared_events";
const FRONTIER_FILE: &str = "_replication_frontier";
const SNAPSHOT_FILE: &str = "_snapshot";

#[derive(Debug, Clone, Encode, Decode)]
pub struct WorkspaceConfig {
    /// Stable identity of the workspace represented by this local replica.
    pub workspace_id: WorkspaceId,
    /// Stable identity for this workspace installation/profile. It is persisted
    /// at the workspace root and reused when reopening the workspace.
    pub device_id: DeviceId,
    pub graceful_shutdown_max_duration: Duration,
}

impl Default for WorkspaceConfig {
    fn default() -> Self {
        let device_id = DeviceId::generate().expect("failed to generate workspace device id");
        Self {
            workspace_id: WorkspaceId::from(format!("workspace-{device_id}")),
            device_id,
            graceful_shutdown_max_duration: Duration::from_secs(7),
        }
    }
}

pub type ConcurrentTable = Arc<RwLock<Table>>;
pub type ConcurrentState<K, V> = Arc<RwLock<State<K, V>>>;
pub(super) type ErasedStateHandle = Arc<dyn Any + Send + Sync>;

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

/// A durable, weak reference to a typed workspace state, mirroring
/// [`TableHandle`]. The workspace owns the state; the handle never keeps it (or
/// the workspace) alive.
#[derive(Clone)]
pub struct StateHandle<K, V>
where
    K: Encode + Decode<()> + std::hash::Hash + Eq + Clone + Ord + Send + Sync + 'static,
    V: Encode + Decode<()> + Clone + Send + Sync + 'static,
{
    name: String,
    inner: Weak<RwLock<State<K, V>>>,
}

impl<K, V> StateHandle<K, V>
where
    K: Encode + Decode<()> + std::hash::Hash + Eq + Clone + Ord + Send + Sync + 'static,
    V: Encode + Decode<()> + Clone + Send + Sync + 'static,
{
    pub(crate) fn new(name: &str, state: &ConcurrentState<K, V>) -> Self {
        Self {
            name: name.to_owned(),
            inner: Arc::downgrade(state),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// Upgrade to a strong handle for a single operation, or fail if the owning
    /// workspace has been dropped.
    pub fn get(&self) -> io::Result<ConcurrentState<K, V>> {
        self.inner.upgrade().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotConnected,
                format!(
                    "state {:?} is unavailable because its workspace was dropped",
                    self.name
                ),
            )
        })
    }
}

/// The workspace is the single lifecycle root. It holds the only strong
/// references to its tables, states, and operator workers; everything it owns
/// is torn down deterministically when the last `Arc<Workspace>` is dropped.
pub struct Workspace<D>
where
    D: DispatchOperator,
{
    path: PathBuf,
    config: WorkspaceConfig,
    executor: Arc<dyn Executor>,
    table_catalog: Mutex<TableCatalog>,
    state_catalog: Mutex<StateCatalog>,
    operator_catalog: Mutex<OperatorCatalog<D::Config>>,
    tables: RwLock<HashMap<String, ConcurrentTable>>,
    states: RwLock<HashMap<String, ErasedStateHandle>>,
    operators: RwLock<HashMap<String, Arc<OperatorWorker<D>>>>,
    timers: Arc<RwLock<TimerStore>>,
    device_profile: Arc<DeviceProfile>,
    control: Mutex<control::WorkspaceControl>,
    shared_journal: Mutex<journal::SharedJournal>,
    /// Serializes shared sequence allocation, journal append, application,
    /// snapshot capture, and snapshot installation.
    shared_mutation: Mutex<()>,
    presence: Mutex<HashMap<DeviceId, zendb_transport::PresenceTracker>>,
    presence_idle_ms: AtomicU64,
    presence_grace_multiplier: AtomicU32,
    /// Notified by `register_timer` to wake the scheduler early.
    timer_notify: Arc<(Mutex<()>, Condvar)>,
}

impl<D> Workspace<D>
where
    D: DispatchOperator,
{
    /// Create a new workspace at `path`. Fails if the directory already contains a
    /// workspace; use [`Workspace::open`] to reopen an existing one.
    pub fn create(
        path: &Path,
        executor: Arc<dyn Executor>,
        config: WorkspaceConfig,
    ) -> io::Result<Arc<Self>> {
        Self::create_with_initial_device(
            path,
            executor,
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
        executor: Arc<dyn Executor>,
        config: WorkspaceConfig,
        requested_name: String,
        capabilities: std::collections::BTreeSet<zendb_types::CapabilityId>,
    ) -> io::Result<Arc<Self>> {
        Self::create_with_initial_device(
            path,
            executor,
            config,
            Some(requested_name),
            capabilities,
            std::collections::BTreeSet::new(),
        )
    }

    fn create_with_initial_device(
        path: &Path,
        executor: Arc<dyn Executor>,
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
        let table_catalog =
            TableCatalog::create(&path.join(TABLE_CATALOG_FILE), KeyDirConfig::default())?;
        let state_catalog =
            StateCatalog::create(&path.join(STATE_CATALOG_FILE), KeyDirConfig::default())?;
        let operator_catalog = OperatorCatalog::<D::Config>::create(
            &path.join(OPERATOR_CATALOG_FILE),
            KeyDirConfig::default(),
        )?;
        let timers = TimerStore::create(&path.join(TIMERS_FILE), BPlusTreeConfig::default())?;
        let initial_hlc = device_profile.next_hlc(now_ms())?;
        let initial_device = zendb_types::DeviceRecord {
            name: requested_name.unwrap_or_else(|| format!("device-{}", config.device_id)),
            key_ring: device_profile.initial_key_ring(),
            roles,
            capabilities,
            replication_frontier: zendb_types::ContiguousFrontier::default(),
        };
        let control = control::WorkspaceControl::create(
            &path.join(CONTROL_FILE),
            config.device_id,
            initial_device,
            initial_hlc,
        )?;
        let shared_journal = journal::SharedJournal::create(
            &path.join(SHARED_EVENTS_FILE),
            &path.join(FRONTIER_FILE),
        )?;
        info!("creating workspace at {:?}", path);
        let workspace = Self::from_parts(
            path,
            table_catalog,
            state_catalog,
            operator_catalog,
            timers,
            executor,
            config,
            device_profile,
            control,
            shared_journal,
        )?;
        workspace.recover_shared_journal()?;
        Ok(workspace)
    }

    /// Open an existing workspace at `path`. Fails if the directory does not
    /// contain a valid workspace.
    pub fn open(
        path: &Path,
        executor: Arc<dyn Executor>,
        config: WorkspaceConfig,
    ) -> io::Result<Arc<Self>> {
        let mut config = config;
        let device_profile = Arc::new(DeviceProfile::open(&path.join(DEVICE_PROFILE_FILE))?);
        config.device_id = device_profile.device_id();
        config.workspace_id =
            load_or_persist_workspace_id(&path.join(WORKSPACE_ID_FILE), config.workspace_id)?;
        let table_catalog =
            TableCatalog::open(&path.join(TABLE_CATALOG_FILE), KeyDirConfig::default())?;
        let state_catalog =
            StateCatalog::open(&path.join(STATE_CATALOG_FILE), KeyDirConfig::default())?;
        let operator_catalog = OperatorCatalog::<D::Config>::open(
            &path.join(OPERATOR_CATALOG_FILE),
            KeyDirConfig::default(),
        )?;
        let timers = TimerStore::open(&path.join(TIMERS_FILE), BPlusTreeConfig::default())?;
        let control = control::WorkspaceControl::open(&path.join(CONTROL_FILE))?;
        let shared_journal = journal::SharedJournal::open(
            &path.join(SHARED_EVENTS_FILE),
            &path.join(FRONTIER_FILE),
        )?;
        info!("opening workspace at {:?}", path);
        let workspace = Self::from_parts(
            path,
            table_catalog,
            state_catalog,
            operator_catalog,
            timers,
            executor,
            config,
            device_profile,
            control,
            shared_journal,
        )?;
        workspace.recover_shared_journal()?;
        Ok(workspace)
    }

    /// Assemble a `Workspace` from its constituent parts and spawn the background timer scheduler.
    fn from_parts(
        path: &Path,
        table_catalog: TableCatalog,
        state_catalog: StateCatalog,
        operator_catalog: OperatorCatalog<D::Config>,
        timers: TimerStore,
        executor: Arc<dyn Executor>,
        config: WorkspaceConfig,
        device_profile: Arc<DeviceProfile>,
        control: control::WorkspaceControl,
        shared_journal: journal::SharedJournal,
    ) -> io::Result<Arc<Self>> {
        let timer_notify = Arc::new((Mutex::new(()), Condvar::new()));
        let workspace = Arc::new(Self {
            path: path.to_path_buf(),
            config,
            executor,
            table_catalog: Mutex::new(table_catalog),
            state_catalog: Mutex::new(state_catalog),
            operator_catalog: Mutex::new(operator_catalog),
            tables: RwLock::new(HashMap::new()),
            states: RwLock::new(HashMap::new()),
            operators: RwLock::new(HashMap::new()),
            timers: Arc::new(RwLock::new(timers)),
            device_profile,
            control: Mutex::new(control),
            shared_journal: Mutex::new(shared_journal),
            shared_mutation: Mutex::new(()),
            presence: Mutex::new(HashMap::new()),
            presence_idle_ms: AtomicU64::new(30_000),
            presence_grace_multiplier: AtomicU32::new(3),
            timer_notify: Arc::clone(&timer_notify),
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
        let workspace_weak = Arc::downgrade(&workspace);
        debug!("spawning background timer scheduler");
        workspace
            .executor
            .spawn(Box::pin(run_scheduler(workspace_weak, timer_notify)));
        Ok(workspace)
    }

    pub(crate) fn executor(&self) -> Arc<dyn Executor> {
        Arc::clone(&self.executor)
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

    /// Access the durable local signing and sequence profile.
    pub fn device_profile(&self) -> &DeviceProfile {
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
        mut config: TableConfig,
    ) -> io::Result<TableHandle> {
        if self.is_shared_table(name)? {
            return self.table(name, None);
        }
        if self
            .table_config(name)
            .is_some_and(|existing| !existing.sync)
        {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "a local-only table already uses this name",
            ));
        }
        config.sync = true;
        let at = self.device_profile.next_hlc(now_ms())?;
        self.commit_shared_event(control::shared_table_event(at, name, true))?;
        self.table(name, Some(config))
    }

    /// Tombstone a live shared-table declaration. Physical files are retained
    /// for recovery, overlays, and later compaction.
    pub fn delete_shared_table(self: &Arc<Self>, name: &str) -> io::Result<bool> {
        if !self.is_shared_table(name)? {
            return Ok(false);
        }
        let at = self.device_profile.next_hlc(now_ms())?;
        self.commit_shared_event(control::shared_table_event(at, name, false))?;
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
        control::validate_initial_device(&record)?;
        let at = self.device_profile.next_hlc(now_ms())?;
        self.commit_shared_event(control::admit_event(at, device_id, record))?;
        Ok(true)
    }

    /// Tombstone a Device membership Cell. Requires the Manager role.
    pub fn remove_device(self: &Arc<Self>, device_id: DeviceId) -> io::Result<bool> {
        let at = self.device_profile.next_hlc(now_ms())?;
        self.commit_shared_event(control::remove_device_event(at, device_id))?;
        Ok(true)
    }

    /// Change a Device alias. A Device may rename itself; changing another
    /// Device requires Manager.
    pub fn rename_device(self: &Arc<Self>, device_id: DeviceId, name: String) -> io::Result<bool> {
        let at = self.device_profile.next_hlc(now_ms())?;
        self.commit_shared_event(control::replace_field_event(
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
        self.commit_shared_event(control::role_event(at, device_id, role, enabled))?;
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

// Close table loop
impl<D> Drop for Workspace<D>
where
    D: DispatchOperator,
{
    fn drop(&mut self) {
        let deadline = Instant::now() + self.config.graceful_shutdown_max_duration;

        loop {
            let tables_to_close: Vec<String> = {
                let mut tables = self.tables.write();
                let tables_to_close = tables.keys().cloned().collect();
                tables.clear();
                tables_to_close
            };

            if !tables_to_close.is_empty() {
                let workers: Vec<_> = self.operators.read().values().cloned().collect();
                for table in &tables_to_close {
                    for worker in &workers {
                        worker.detach_input(table);
                    }
                }
            }

            if self.tables.read().is_empty() && self.operators.read().is_empty() {
                return;
            }

            if Instant::now() >= deadline {
                return;
            }

            std::thread::sleep(Duration::from_millis(10));
        }
    }
}
