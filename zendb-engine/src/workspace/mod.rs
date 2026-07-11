//! Eager workspace lifecycle and resource ownership.

mod operators;
mod states;
mod tables;
mod timers;

use std::{
    any::Any,
    fmt, fs, io,
    io::Write,
    path::{Path, PathBuf},
    sync::{Arc, Weak},
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
use zendb_types::{DeviceId, PrincipalId, WorkspaceId};

use log::{debug, info};

use crate::{
    operator::worker::OperatorWorker, runtime::Executor, DispatchOperator, OperatorPhase,
    TableConfig,
};

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
const DEVICE_ID_FILE: &str = "_device_id";
const WORKSPACE_ID_FILE: &str = "_workspace_id";

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
        fs::create_dir_all(path)?;
        persist_device_id(&path.join(DEVICE_ID_FILE), config.device_id)?;
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
        info!("creating workspace at {:?}", path);
        Self::from_parts(
            path,
            table_catalog,
            state_catalog,
            operator_catalog,
            timers,
            executor,
            config,
        )
    }

    /// Open an existing workspace at `path`. Fails if the directory does not
    /// contain a valid workspace.
    pub fn open(
        path: &Path,
        executor: Arc<dyn Executor>,
        config: WorkspaceConfig,
    ) -> io::Result<Arc<Self>> {
        let mut config = config;
        config.device_id = load_or_persist_device_id(&path.join(DEVICE_ID_FILE), config.device_id)?;
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
        info!("opening workspace at {:?}", path);
        Self::from_parts(
            path,
            table_catalog,
            state_catalog,
            operator_catalog,
            timers,
            executor,
            config,
        )
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
            timer_notify: Arc::clone(&timer_notify),
        });
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
}

fn persist_device_id(path: &Path, device_id: DeviceId) -> io::Result<()> {
    let bytes = bincode::encode_to_vec(device_id, bincode::config::standard())
        .map_err(|error| io::Error::other(error.to_string()))?;
    let mut file = fs::File::create(path)?;
    file.write_all(&bytes)?;
    file.sync_all()
}

fn load_or_persist_device_id(path: &Path, configured: DeviceId) -> io::Result<DeviceId> {
    match fs::read(path) {
        Ok(bytes) => {
            let (device_id, consumed): (DeviceId, usize) =
                bincode::decode_from_slice(&bytes, bincode::config::standard())
                    .map_err(|error| io::Error::other(error.to_string()))?;
            if consumed != bytes.len() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "workspace device identity contains trailing bytes",
                ));
            }
            Ok(device_id)
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            persist_device_id(path, configured)?;
            Ok(configured)
        }
        Err(error) => Err(error),
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

/// Request to join or authorize this local replica for a workspace.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceJoinRequest {
    pub workspace_id: WorkspaceId,
    pub principal: PrincipalId,
    pub device_id: DeviceId,
}

/// Result placeholder for the future join state machine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceJoinPlan {
    pub workspace_id: WorkspaceId,
    pub device_id: DeviceId,
    pub principal: PrincipalId,
}

/// Request to synchronize with one authenticated peer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceSyncRequest {
    pub workspace_id: WorkspaceId,
    pub peer_device_id: DeviceId,
}

/// Input to the local declarative operator reconciler.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkspaceReconcileRequest {
    pub now_ms: u64,
}

/// Workspace operations are part of the concrete root. They return explicit
/// errors until their coordinators are implemented; no separate Workspace
/// trait is needed because this type is the sole lifecycle owner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkspaceError {
    InvalidWorkspace,
    InvalidDevice,
    Unsupported(&'static str),
}

impl fmt::Display for WorkspaceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidWorkspace => formatter.write_str("workspace identity does not match"),
            Self::InvalidDevice => formatter.write_str("device identity does not match"),
            Self::Unsupported(operation) => write!(formatter, "{operation} is not implemented"),
        }
    }
}

impl std::error::Error for WorkspaceError {}

impl<D> Workspace<D>
where
    D: DispatchOperator,
{
    /// Begin the workspace join state machine for this local replica.
    pub fn join_workspace(
        &self,
        request: WorkspaceJoinRequest,
    ) -> Result<WorkspaceJoinPlan, WorkspaceError> {
        if request.workspace_id != *self.workspace_id() {
            return Err(WorkspaceError::InvalidWorkspace);
        }
        if request.device_id != self.device_id() {
            return Err(WorkspaceError::InvalidDevice);
        }
        Err(WorkspaceError::Unsupported("workspace join"))
    }

    /// Leave the current workspace after revocation and local cleanup are
    /// implemented by the workspace coordinator.
    pub fn leave_workspace(&self) -> Result<(), WorkspaceError> {
        Err(WorkspaceError::Unsupported("workspace leave"))
    }

    /// Run one authenticated anti-entropy cycle with a peer.
    pub fn synchronize_workspace(
        &self,
        request: WorkspaceSyncRequest,
    ) -> Result<(), WorkspaceError> {
        if request.workspace_id != *self.workspace_id() {
            return Err(WorkspaceError::InvalidWorkspace);
        }
        Err(WorkspaceError::Unsupported("workspace synchronization"))
    }

    /// Run one local desired-versus-observed operator reconciliation cycle.
    pub fn reconcile_workspace(
        &self,
        _request: WorkspaceReconcileRequest,
    ) -> Result<(), WorkspaceError> {
        Err(WorkspaceError::Unsupported("workspace reconciliation"))
    }
}
