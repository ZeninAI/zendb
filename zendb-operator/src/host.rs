use std::{
    any::Any,
    fs, io,
    ops::Deref,
    path::{Path, PathBuf},
    sync::{Arc, Weak},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use bincode::{Decode, Encode};
use hashbrown::HashMap;
use parking_lot::{Condvar, Mutex, RwLock};
use zendb_engine::{
    TableLifecycleEvent, TableObservation, TableRequest, Workspace, WorkspaceConfig,
};
use zendb_storage::{
    BPlusTree, BPlusTreeConfig, DurableStorage, KeyDir, KeyDirConfig, State, StateConfig,
};

use crate::{
    operator::{worker::OperatorWorker, DispatchOperator, OperatorPhase},
    runtime::Executor,
    timers::run_scheduler,
};

pub(crate) const STATES_DIR: &str = "states";
pub(crate) const TABLES_DIR: &str = "tables";
const OPERATOR_DIR: &str = "_operator";
const STATE_CATALOG_FILE: &str = "_states";
const OPERATOR_CATALOG_FILE: &str = "_operators";
const TIMERS_FILE: &str = "_timers";

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Encode, Decode)]
pub(crate) struct TimerKey {
    pub(crate) fire_at_ms: u64,
    pub(crate) operator: String,
}

#[derive(Debug, Clone, Encode, Decode)]
pub(crate) struct TimerEntry {
    pub(crate) payload: Vec<u8>,
}

#[derive(Debug, Clone, Encode, Decode)]
pub(crate) struct OperatorEntry<Config> {
    pub(crate) config: Config,
    pub(crate) phase: OperatorPhase,
}

pub(crate) type TimerStore = BPlusTree<TimerKey, TimerEntry>;
pub(crate) type StateCatalog = KeyDir<String, StateConfig>;
pub(crate) type OperatorCatalog<Config> = KeyDir<String, OperatorEntry<Config>>;
pub type ConcurrentState<K, V> = Arc<RwLock<State<K, V>>>;
pub(crate) type ErasedStateHandle = Arc<dyn Any + Send + Sync>;

pub(crate) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as u64)
        .unwrap_or(0)
}

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

    pub fn get(&self) -> io::Result<ConcurrentState<K, V>> {
        self.inner.upgrade().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotConnected,
                format!(
                    "state {:?} is unavailable because its host was dropped",
                    self.name
                ),
            )
        })
    }
}

/// Owns local operator state and execution for one core Workspace.
pub struct OperatorHost<D>
where
    D: DispatchOperator,
{
    pub(crate) workspace: Arc<Workspace>,
    pub(crate) path: PathBuf,
    pub(crate) executor: Arc<dyn Executor>,
    pub(crate) state_catalog: Mutex<StateCatalog>,
    pub(crate) operator_catalog: Mutex<OperatorCatalog<D::Config>>,
    pub(crate) states: RwLock<HashMap<String, ErasedStateHandle>>,
    pub(crate) operators: RwLock<HashMap<String, Arc<OperatorWorker<D>>>>,
    pub(crate) timers: Arc<RwLock<TimerStore>>,
    pub(crate) timer_notify: Arc<(Mutex<()>, Condvar)>,
    table_observation: Mutex<Option<TableObservation>>,
}

impl<D> OperatorHost<D>
where
    D: DispatchOperator,
{
    pub fn create(
        path: &Path,
        executor: Arc<dyn Executor>,
        config: WorkspaceConfig,
    ) -> io::Result<Arc<Self>> {
        let workspace = Workspace::create(path, config)?;
        Self::attach(workspace, executor)
    }

    pub fn open(
        path: &Path,
        executor: Arc<dyn Executor>,
        config: WorkspaceConfig,
    ) -> io::Result<Arc<Self>> {
        let workspace = Workspace::open(path, config)?;
        Self::attach(workspace, executor)
    }

    pub fn attach(workspace: Arc<Workspace>, executor: Arc<dyn Executor>) -> io::Result<Arc<Self>> {
        let path = workspace.path().join(OPERATOR_DIR);
        fs::create_dir_all(path.join(STATES_DIR))?;
        let state_catalog = open_or_create_keydir(&path.join(STATE_CATALOG_FILE))?;
        let operator_catalog =
            open_or_create_operator_catalog::<D>(&path.join(OPERATOR_CATALOG_FILE))?;
        let timers = open_or_create_timers(&path.join(TIMERS_FILE))?;
        let timer_notify = Arc::new((Mutex::new(()), Condvar::new()));
        let host = Arc::new(Self {
            workspace: Arc::clone(&workspace),
            path,
            executor,
            state_catalog: Mutex::new(state_catalog),
            operator_catalog: Mutex::new(operator_catalog),
            states: RwLock::new(HashMap::new()),
            operators: RwLock::new(HashMap::new()),
            timers: Arc::new(RwLock::new(timers)),
            timer_notify: Arc::clone(&timer_notify),
            table_observation: Mutex::new(None),
        });

        let weak = Arc::downgrade(&host);
        let observation = workspace.observe_tables(move |event| {
            if let Some(host) = weak.upgrade() {
                if let Err(error) = host.handle_table_event(event) {
                    log::error!("operator table lifecycle failed: {error}");
                }
            }
        });
        *host.table_observation.lock() = Some(observation);
        host.executor
            .spawn(Box::pin(run_scheduler(Arc::downgrade(&host), timer_notify)));
        Ok(host)
    }

    pub fn workspace(&self) -> &Arc<Workspace> {
        &self.workspace
    }

    /// Start a fluent table request against the wrapped Workspace.
    pub fn table(self: &Arc<Self>, name: &str) -> TableRequest {
        self.workspace.table(name)
    }

    pub(crate) fn executor(&self) -> Arc<dyn Executor> {
        Arc::clone(&self.executor)
    }

    /// Ask every worker to tear down and wait up to `timeout` while the host is
    /// still strongly reachable by their run loops.
    pub fn shutdown(self: &Arc<Self>, timeout: Duration) -> bool {
        for worker in self.operators.read().values() {
            worker.begin_shutdown(OperatorPhase::Cancelled);
        }
        let deadline = std::time::Instant::now() + timeout;
        while !self.operators.read().is_empty() && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        self.operators.read().is_empty()
    }

    fn handle_table_event(self: &Arc<Self>, event: TableLifecycleEvent) -> io::Result<()> {
        match event {
            TableLifecycleEvent::Opened(handle) => self.activate_table_subscribers(handle.name()),
            TableLifecycleEvent::Closed(name) => {
                for worker in self.operators.read().values() {
                    worker.detach_input(&name);
                }
                Ok(())
            }
        }
    }
}

impl<D> Deref for OperatorHost<D>
where
    D: DispatchOperator,
{
    type Target = Workspace;

    fn deref(&self) -> &Self::Target {
        &self.workspace
    }
}

impl<D> Drop for OperatorHost<D>
where
    D: DispatchOperator,
{
    fn drop(&mut self) {
        for worker in self.operators.get_mut().values() {
            worker.delete_inputs();
        }
        self.operators.get_mut().clear();
    }
}

fn open_or_create_keydir(path: &Path) -> io::Result<StateCatalog> {
    if path.exists() {
        StateCatalog::open(path, KeyDirConfig::default())
    } else {
        StateCatalog::create(path, KeyDirConfig::default())
    }
}

fn open_or_create_operator_catalog<D>(path: &Path) -> io::Result<OperatorCatalog<D::Config>>
where
    D: DispatchOperator,
{
    if path.exists() {
        OperatorCatalog::open(path, KeyDirConfig::default())
    } else {
        OperatorCatalog::create(path, KeyDirConfig::default())
    }
}

fn open_or_create_timers(path: &Path) -> io::Result<TimerStore> {
    if path.exists() {
        TimerStore::open(path, BPlusTreeConfig::default())
    } else {
        TimerStore::create(path, BPlusTreeConfig::default())
    }
}
