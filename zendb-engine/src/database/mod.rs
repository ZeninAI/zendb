//! Eager database lifecycle and resource ownership.

mod operators;
mod states;
mod tables;
mod timers;

use std::{
    any::Any,
    fs, io,
    path::{Path, PathBuf},
    sync::{Arc, Weak},
    time::{SystemTime, UNIX_EPOCH},
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

#[derive(Debug, Clone, Encode, Decode)]
pub struct DatabaseConfig {}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {}
    }
}

pub type ConcurrentTable = Arc<RwLock<Table>>;
pub type ConcurrentState<K, V> = Arc<RwLock<State<K, V>>>;
pub(super) type ErasedStateHandle = Arc<dyn Any + Send + Sync>;

/// A durable, weak reference to a database table.
///
/// The database is the single owner of every table, so handles never keep a
/// table (or the database) alive. Call [`TableHandle::get`] to obtain a strong
/// guard for an operation; once the owning database is dropped, `get` fails
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
    /// database has been dropped.
    pub fn get(&self) -> io::Result<ConcurrentTable> {
        self.inner.upgrade().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotConnected,
                format!("table {:?} is unavailable because its database was dropped", self.name),
            )
        })
    }
}

/// A durable, weak reference to a typed database state, mirroring
/// [`TableHandle`]. The database owns the state; the handle never keeps it (or
/// the database) alive.
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
    /// database has been dropped.
    pub fn get(&self) -> io::Result<ConcurrentState<K, V>> {
        self.inner.upgrade().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotConnected,
                format!("state {:?} is unavailable because its database was dropped", self.name),
            )
        })
    }
}

/// The database is the single lifecycle root. It holds the only strong
/// references to its tables, states, and operator workers; everything it owns
/// is torn down deterministically when the last `Arc<Database>` is dropped.
pub struct Database<D>
where
    D: DispatchOperator,
{
    path: PathBuf,
    config: DatabaseConfig,
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

impl<D> Database<D>
where
    D: DispatchOperator,
{
    /// Create a new database at `path`. Fails if the directory already contains a
    /// database; use [`Database::open`] to reopen an existing one.
    pub fn create(
        path: &Path,
        executor: Arc<dyn Executor>,
        config: DatabaseConfig,
    ) -> io::Result<Arc<Self>> {
        fs::create_dir_all(path)?;
        let table_catalog =
            TableCatalog::create(&path.join(TABLE_CATALOG_FILE), KeyDirConfig::default())?;
        let state_catalog =
            StateCatalog::create(&path.join(STATE_CATALOG_FILE), KeyDirConfig::default())?;
        let operator_catalog = OperatorCatalog::<D::Config>::create(
            &path.join(OPERATOR_CATALOG_FILE),
            KeyDirConfig::default(),
        )?;
        let timers = TimerStore::create(&path.join(TIMERS_FILE), BPlusTreeConfig::default())?;
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

    /// Open an existing database at `path`. Fails if the directory does not
    /// contain a valid database.
    pub fn open(
        path: &Path,
        executor: Arc<dyn Executor>,
        config: DatabaseConfig,
    ) -> io::Result<Arc<Self>> {
        let table_catalog =
            TableCatalog::open(&path.join(TABLE_CATALOG_FILE), KeyDirConfig::default())?;
        let state_catalog =
            StateCatalog::open(&path.join(STATE_CATALOG_FILE), KeyDirConfig::default())?;
        let operator_catalog = OperatorCatalog::<D::Config>::open(
            &path.join(OPERATOR_CATALOG_FILE),
            KeyDirConfig::default(),
        )?;
        let timers = TimerStore::open(&path.join(TIMERS_FILE), BPlusTreeConfig::default())?;
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

    /// Assemble a `Database` from its constituent parts and spawn the background timer scheduler.
    fn from_parts(
        path: &Path,
        table_catalog: TableCatalog,
        state_catalog: StateCatalog,
        operator_catalog: OperatorCatalog<D::Config>,
        timers: TimerStore,
        executor: Arc<dyn Executor>,
        config: DatabaseConfig,
    ) -> io::Result<Arc<Self>> {
        let timer_notify = Arc::new((Mutex::new(()), Condvar::new()));
        let database = Arc::new(Self {
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
        let db_weak = Arc::downgrade(&database);
        database
            .executor
            .spawn(Box::pin(run_scheduler(db_weak, timer_notify)));
        Ok(database)
    }

    pub(crate) fn executor(&self) -> Arc<dyn Executor> {
        Arc::clone(&self.executor)
    }

    /// Return a reference to the database configuration.
    pub fn config(&self) -> &DatabaseConfig {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operator::prelude::{MerkleTreeConfig, MerkleTreeOperator};
    use crate::{
        Change, Operator, OperatorDirective, OperatorRuntimeConfig, Subscription, TableConfig,
    };
    use parking_lot::Mutex;
    use std::sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        OnceLock,
    };
    use std::time::{Duration, Instant};
    use zendb_storage::core::traits::Backend;
    use zendb_types::{
        device_id, init_device_id, Event, Hlc, Op, Path as ValuePath, PrimaryKey, Value,
    };

    struct ThreadExecutor;

    impl Executor for ThreadExecutor {
        fn spawn(&self, future: crate::RuntimeFuture) {
            std::thread::spawn(move || futures::executor::block_on(future));
        }

        fn idle(&self) -> crate::RuntimeFuture {
            Box::pin(async { std::thread::sleep(Duration::from_millis(1)) })
        }

        fn sleep(&self, duration: Duration) -> crate::RuntimeFuture {
            Box::pin(async move { std::thread::sleep(duration) })
        }
    }

    fn wait_until(condition: impl Fn() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(1);
        while !condition() {
            assert!(Instant::now() < deadline, "condition was not reached");
            std::thread::yield_now();
        }
    }

    #[derive(Debug, Clone, PartialEq, Encode, Decode)]
    struct CountingConfig {
        tracker: String,
        finish: bool,
    }

    struct CountingOperator {
        count: Arc<AtomicUsize>,
        finish: bool,
        buffer: StateHandle<String, u64>,
        index: StateHandle<Vec<u8>, Vec<u8>>,
        output: TableHandle,
    }

    impl Operator for CountingOperator {
        type Config = CountingConfig;
        type Timer = ();

        fn create<'a, D>(
            db: &'a Arc<Database<D>>,
            name: &'a str,
            config: &'a Self::Config,
        ) -> crate::BoxFuture<'a, io::Result<Self>>
        where
            D: crate::DispatchOperator,
            Self: Sized,
        {
            Box::pin(async move {
                let buffer = db.state("counter/buffer", Some(StateConfig::default()))?;
                let index = db.state("index", Some(StateConfig::default()))?;
                let output = db.table("users", None)?;
                Ok(Self {
                    count: lookup_counter(&config.tracker)?,
                    finish: config.finish,
                    buffer,
                    index,
                    output,
                })
            })
        }

        fn process<'a, D>(
            &'a mut self,
            changes: Vec<Change>,
            _db: &'a Arc<Database<D>>,
            _name: &'a str,
            _config: &'a Self::Config,
        ) -> crate::BoxFuture<'a, io::Result<OperatorDirective>>
        where
            D: crate::DispatchOperator,
        {
            Box::pin(async move {
                self.count.fetch_add(changes.len(), Ordering::Relaxed);
                self.index.get()?.write().put(
                    b"count".to_vec(),
                    self.count.load(Ordering::Relaxed).to_le_bytes().to_vec(),
                )?;
                {
                    let key = "count".to_owned();
                    let state = self.buffer.get()?;
                    let mut state = state.write();
                    let count = state.get(&key).map(|value| value.into_owned()).unwrap_or(0)
                        + changes.len() as u64;
                    state.put(key, count)?;
                }
                Ok(if self.finish {
                    OperatorDirective::Finish
                } else {
                    OperatorDirective::Continue
                })
            })
        }
    }

    #[derive(Debug, Clone, PartialEq, Encode, Decode)]
    struct FailingOperatorConfig {
        attempts_tracker: String,
    }

    struct FailingOperator {
        attempts: Arc<AtomicUsize>,
    }

    impl Operator for FailingOperator {
        type Config = FailingOperatorConfig;
        type Timer = ();

        fn create<'a, D>(
            db: &'a Arc<Database<D>>,
            name: &'a str,
            config: &'a Self::Config,
        ) -> crate::BoxFuture<'a, io::Result<Self>>
        where
            D: crate::DispatchOperator,
            Self: Sized,
        {
            Box::pin(async move {
                Ok(Self {
                    attempts: lookup_counter(&config.attempts_tracker)?,
                })
            })
        }

        fn process<'a, D>(
            &'a mut self,
            changes: Vec<Change>,
            _db: &'a Arc<Database<D>>,
            _name: &'a str,
            _config: &'a Self::Config,
        ) -> crate::BoxFuture<'a, io::Result<OperatorDirective>>
        where
            D: crate::DispatchOperator,
        {
            Box::pin(async move {
                let _ = changes;
                self.attempts.fetch_add(1, Ordering::Relaxed);
                Err(io::Error::other("expected failure"))
            })
        }
    }

    #[derive(Debug, Clone, PartialEq, Encode, Decode)]
    struct TimerOperatorConfig {
        tracker: String,
    }

    struct TimerOperator {
        fired: Arc<AtomicUsize>,
    }

    impl Operator for TimerOperator {
        type Config = TimerOperatorConfig;
        type Timer = ();

        fn create<'a, D>(
            db: &'a Arc<Database<D>>,
            name: &'a str,
            config: &'a Self::Config,
        ) -> crate::BoxFuture<'a, io::Result<Self>>
        where
            D: crate::DispatchOperator,
            Self: Sized,
        {
            Box::pin(async move {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64;
                db.register_timer(name, now, &())?;
                Ok(Self {
                    fired: lookup_counter(&config.tracker)?,
                })
            })
        }

        fn process<'a, D>(
            &'a mut self,
            _changes: Vec<Change>,
            _db: &'a Arc<Database<D>>,
            _name: &'a str,
            _config: &'a Self::Config,
        ) -> crate::BoxFuture<'a, io::Result<OperatorDirective>>
        where
            D: crate::DispatchOperator,
        {
            Box::pin(async { Ok(OperatorDirective::Continue) })
        }

        fn on_timer<'a, D>(
            &'a mut self,
            _payload: (),
            _fire_at_ms: u64,
            _db: &'a Arc<Database<D>>,
            _name: &'a str,
            _config: &'a Self::Config,
        ) -> crate::BoxFuture<'a, io::Result<OperatorDirective>>
        where
            D: crate::DispatchOperator,
        {
            Box::pin(async move {
                self.fired.fetch_add(1, Ordering::Relaxed);
                Ok(OperatorDirective::Finish)
            })
        }
    }

    #[derive(Debug, Clone, PartialEq, Encode, Decode)]
    struct InputLifecycleConfig {
        tracker: String,
    }

    struct InputLifecycleOperator {
        opened: Arc<Mutex<Vec<String>>>,
        closed: Arc<Mutex<Vec<String>>>,
    }

    impl Operator for InputLifecycleOperator {
        type Config = InputLifecycleConfig;
        type Timer = ();

        fn create<'a, D>(
            db: &'a Arc<Database<D>>,
            name: &'a str,
            config: &'a Self::Config,
        ) -> crate::BoxFuture<'a, io::Result<Self>>
        where
            D: crate::DispatchOperator,
            Self: Sized,
        {
            Box::pin(async move {
                let (opened, closed) = lookup_input_tracker(&config.tracker)?;
                Ok(Self { opened, closed })
            })
        }

        fn process<'a, D>(
            &'a mut self,
            _changes: Vec<Change>,
            _db: &'a Arc<Database<D>>,
            _name: &'a str,
            _config: &'a Self::Config,
        ) -> crate::BoxFuture<'a, io::Result<OperatorDirective>>
        where
            D: crate::DispatchOperator,
        {
            Box::pin(async { Ok(OperatorDirective::Continue) })
        }

        fn on_input_opened<'a, D>(
            &'a mut self,
            table: String,
            _db: &'a Arc<Database<D>>,
            _name: &'a str,
            _config: &'a Self::Config,
        ) -> crate::BoxFuture<'a, io::Result<OperatorDirective>>
        where
            D: crate::DispatchOperator,
        {
            Box::pin(async move {
                self.opened.lock().push(table);
                Ok(OperatorDirective::Continue)
            })
        }

        fn on_input_closed<'a, D>(
            &'a mut self,
            table: String,
            _db: &'a Arc<Database<D>>,
            _name: &'a str,
            _config: &'a Self::Config,
        ) -> crate::BoxFuture<'a, io::Result<OperatorDirective>>
        where
            D: crate::DispatchOperator,
        {
            Box::pin(async move {
                self.closed.lock().push(table);
                Ok(OperatorDirective::Continue)
            })
        }
    }

    #[derive(Debug, Clone, PartialEq, Encode, Decode)]
    struct ShutdownLifecycleConfig {
        tracker: String,
    }

    struct ShutdownLifecycleOperator {
        log: Arc<Mutex<Vec<String>>>,
    }

    impl Operator for ShutdownLifecycleOperator {
        type Config = ShutdownLifecycleConfig;
        type Timer = ();

        fn create<'a, D>(
            db: &'a Arc<Database<D>>,
            name: &'a str,
            config: &'a Self::Config,
        ) -> crate::BoxFuture<'a, io::Result<Self>>
        where
            D: crate::DispatchOperator,
            Self: Sized,
        {
            Box::pin(async move {
                let log = lookup_lifecycle_log(&config.tracker)?;
                log.lock().push("create".to_owned());
                Ok(Self { log })
            })
        }

        fn process<'a, D>(
            &'a mut self,
            changes: Vec<Change>,
            _db: &'a Arc<Database<D>>,
            _name: &'a str,
            _config: &'a Self::Config,
        ) -> crate::BoxFuture<'a, io::Result<OperatorDirective>>
        where
            D: crate::DispatchOperator,
        {
            Box::pin(async move {
                self.log.lock().push(format!("process:{}", changes.len()));
                Ok(OperatorDirective::Finish)
            })
        }

        fn on_input_opened<'a, D>(
            &'a mut self,
            table: String,
            _db: &'a Arc<Database<D>>,
            _name: &'a str,
            _config: &'a Self::Config,
        ) -> crate::BoxFuture<'a, io::Result<OperatorDirective>>
        where
            D: crate::DispatchOperator,
        {
            Box::pin(async move {
                self.log.lock().push(format!("opened:{table}"));
                Ok(OperatorDirective::Continue)
            })
        }

        fn on_input_closed<'a, D>(
            &'a mut self,
            table: String,
            _db: &'a Arc<Database<D>>,
            _name: &'a str,
            _config: &'a Self::Config,
        ) -> crate::BoxFuture<'a, io::Result<OperatorDirective>>
        where
            D: crate::DispatchOperator,
        {
            Box::pin(async move {
                self.log.lock().push(format!("closed:{table}"));
                Ok(OperatorDirective::Continue)
            })
        }

        fn teardown<'a, D>(
            &'a mut self,
            _reason: &'a crate::TeardownReason,
            _db: &'a Arc<Database<D>>,
            _name: &'a str,
            _config: &'a Self::Config,
        ) -> crate::BoxFuture<'a, io::Result<()>>
        where
            D: crate::DispatchOperator,
        {
            Box::pin(async move {
                self.log.lock().push("teardown".to_owned());
                Ok(())
            })
        }
    }

    #[derive(Debug, Clone, PartialEq, Encode, Decode)]
    struct SpawnerConfig {
        child_tracker: String,
    }

    struct SpawnerOperator;

    impl Operator for SpawnerOperator {
        type Config = SpawnerConfig;
        type Timer = ();

        fn create<'a, D>(
            db: &'a Arc<Database<D>>,
            name: &'a str,
            config: &'a Self::Config,
        ) -> crate::BoxFuture<'a, io::Result<Self>>
        where
            D: crate::DispatchOperator,
            Self: Sized,
        {
            Box::pin(async move {
                db.dispatch_operator::<CountingOperator>(
                    "spawned-counter",
                    CountingConfig {
                        tracker: config.child_tracker.clone(),
                        finish: false,
                    },
                    OperatorRuntimeConfig {
                        subscriptions: vec![Subscription::pattern("users")],
                        ..OperatorRuntimeConfig::default()
                    },
                )?;

                db.dispatch_operator::<MerkleTreeOperator>(
                    "spawned-merkle",
                    MerkleTreeConfig::default(),
                    OperatorRuntimeConfig {
                        subscriptions: vec![Subscription::pattern("users")],
                        ..OperatorRuntimeConfig::default()
                    },
                )?;

                Ok(Self)
            })
        }

        fn process<'a, D>(
            &'a mut self,
            _changes: Vec<Change>,
            _db: &'a Arc<Database<D>>,
            _name: &'a str,
            _config: &'a Self::Config,
        ) -> crate::BoxFuture<'a, io::Result<OperatorDirective>>
        where
            D: crate::DispatchOperator,
        {
            Box::pin(async { Ok(OperatorDirective::Continue) })
        }
    }

    crate::define_operator_set! {
        mod test_operators {
            Count(CountingOperator),
            Retry(FailingOperator),
            Timer(TimerOperator),
            InputLifecycle(InputLifecycleOperator),
            ShutdownLifecycle(ShutdownLifecycleOperator),
            Spawner(SpawnerOperator),
        }
    }

    type TestDatabase = Database<test_operators::OperatorInstance>;
    fn counter_trackers() -> &'static Mutex<HashMap<String, Arc<AtomicUsize>>> {
        static TRACKERS: OnceLock<Mutex<HashMap<String, Arc<AtomicUsize>>>> = OnceLock::new();
        TRACKERS.get_or_init(|| Mutex::new(HashMap::new()))
    }

    fn new_tracker(prefix: &str) -> (String, Arc<AtomicUsize>) {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let key = format!(
            "{prefix}_{}_{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        );
        let counter = Arc::new(AtomicUsize::new(0));
        counter_trackers()
            .lock()
            .insert(key.clone(), Arc::clone(&counter));
        (key, counter)
    }

    fn lookup_counter(key: &str) -> io::Result<Arc<AtomicUsize>> {
        counter_trackers().lock().get(key).cloned().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("missing test counter tracker {key:?}"),
            )
        })
    }

    type InputTracker = (Arc<Mutex<Vec<String>>>, Arc<Mutex<Vec<String>>>);

    fn input_trackers() -> &'static Mutex<HashMap<String, InputTracker>> {
        static TRACKERS: OnceLock<Mutex<HashMap<String, InputTracker>>> = OnceLock::new();
        TRACKERS.get_or_init(|| Mutex::new(HashMap::new()))
    }

    fn new_input_tracker(
        prefix: &str,
    ) -> (String, Arc<Mutex<Vec<String>>>, Arc<Mutex<Vec<String>>>) {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let key = format!(
            "{prefix}_{}_{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        );
        let opened = Arc::new(Mutex::new(Vec::new()));
        let closed = Arc::new(Mutex::new(Vec::new()));
        input_trackers()
            .lock()
            .insert(key.clone(), (Arc::clone(&opened), Arc::clone(&closed)));
        (key, opened, closed)
    }

    fn lookup_input_tracker(key: &str) -> io::Result<InputTracker> {
        input_trackers().lock().get(key).cloned().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("missing test input tracker {key:?}"),
            )
        })
    }

    fn lifecycle_logs() -> &'static Mutex<HashMap<String, Arc<Mutex<Vec<String>>>>> {
        static TRACKERS: OnceLock<Mutex<HashMap<String, Arc<Mutex<Vec<String>>>>>> =
            OnceLock::new();
        TRACKERS.get_or_init(|| Mutex::new(HashMap::new()))
    }

    fn new_lifecycle_log(prefix: &str) -> (String, Arc<Mutex<Vec<String>>>) {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let key = format!(
            "{prefix}_{}_{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        );
        let log = Arc::new(Mutex::new(Vec::new()));
        lifecycle_logs()
            .lock()
            .insert(key.clone(), Arc::clone(&log));
        (key, log)
    }

    fn lookup_lifecycle_log(key: &str) -> io::Result<Arc<Mutex<Vec<String>>>> {
        lifecycle_logs().lock().get(key).cloned().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("missing lifecycle log tracker {key:?}"),
            )
        })
    }

    fn runtime_config(subscription: Subscription) -> OperatorRuntimeConfig {
        OperatorRuntimeConfig {
            subscriptions: vec![subscription],
            poll_size: 128,
        }
    }

    fn counting_config(tracker: String, finish: bool) -> CountingConfig {
        CountingConfig { tracker, finish }
    }

    fn counting_runtime_config(subscription: Subscription) -> OperatorRuntimeConfig {
        runtime_config(subscription)
    }

    fn retry_config(attempts_tracker: String) -> FailingOperatorConfig {
        FailingOperatorConfig { attempts_tracker }
    }

    fn retry_runtime_config() -> OperatorRuntimeConfig {
        runtime_config(Subscription::pattern("users"))
    }

    fn timer_config(tracker: String) -> TimerOperatorConfig {
        TimerOperatorConfig { tracker }
    }

    fn timer_runtime_config() -> OperatorRuntimeConfig {
        runtime_config(Subscription::pattern("users"))
    }

    fn input_lifecycle_config(tracker: String) -> InputLifecycleConfig {
        InputLifecycleConfig { tracker }
    }

    fn input_lifecycle_runtime_config(subscription: Subscription) -> OperatorRuntimeConfig {
        runtime_config(subscription)
    }

    fn spawner_config(child_tracker: String) -> SpawnerConfig {
        SpawnerConfig { child_tracker }
    }

    fn spawner_runtime_config() -> OperatorRuntimeConfig {
        runtime_config(Subscription::pattern("users"))
    }

    fn shutdown_lifecycle_config(tracker: String) -> ShutdownLifecycleConfig {
        ShutdownLifecycleConfig { tracker }
    }

    fn shutdown_lifecycle_runtime_config() -> OperatorRuntimeConfig {
        runtime_config(Subscription::pattern("users"))
    }

    fn event(table: &str, value: i64, ms: u64) -> Event {
        init_device_id();
        Event {
            table_id: table.into(),
            primary_key: PrimaryKey::String("u1".into()),
            path: ValuePath::new(),
            op: Op::Replace {
                value: Value::Int(value),
            },
            hlc: Hlc::with_device_id(ms, 0, device_id()).unwrap(),
            sync: false,
            signature: Vec::new(),
        }
    }

    /// RAII guard that removes the test directory when dropped.
    struct TmpDir(PathBuf);

    impl Drop for TmpDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    impl std::ops::Deref for TmpDir {
        type Target = std::path::Path;
        fn deref(&self) -> &std::path::Path {
            &self.0
        }
    }

    fn tmp(name: &str) -> TmpDir {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        TmpDir(std::env::temp_dir().join(format!(
            "zendb_database_{name}_{}_{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        )))
    }

    #[test]
    fn direct_table_writes_drive_operators() {
        let path = tmp("direct");
        let (tracker, count) = new_tracker("direct");
        let db = TestDatabase::create(&path, Arc::new(ThreadExecutor), DatabaseConfig::default())
            .unwrap();
        let table = db.table("users", Some(TableConfig::default())).unwrap();
        db.dispatch_operator::<CountingOperator>(
            "counter",
            counting_config(tracker, false),
            counting_runtime_config(Subscription::pattern("users")),
        )
        .unwrap();

        table
            .get()
            .unwrap()
            .write()
            .insert_event(event("users", 1, 100))
            .unwrap();
        wait_until(|| count.load(Ordering::Relaxed) == 1);
        assert_eq!(
            table
                .get()
                .unwrap()
                .read()
                .get(&PrimaryKey::String("u1".into()))
                .unwrap()
                .value,
            Some(Value::Int(1))
        );
    }

    #[test]
    fn failed_process_transitions_operator_to_failed() {
        let path = tmp("failed");
        let (attempts_key, attempts) = new_tracker("retry_attempts");
        let db = TestDatabase::create(&path, Arc::new(ThreadExecutor), DatabaseConfig::default())
            .unwrap();
        let table = db.table("users", Some(TableConfig::default())).unwrap();
        db.dispatch_operator::<FailingOperator>(
            "retry",
            retry_config(attempts_key),
            retry_runtime_config(),
        )
        .unwrap();

        table
            .get()
            .unwrap()
            .write()
            .insert_event(event("users", 1, 100))
            .unwrap();
        table
            .get()
            .unwrap()
            .write()
            .insert_event(event("users", 2, 110))
            .unwrap();

        wait_until(|| {
            db.operator_phase("retry")
                == Some(OperatorPhase::Failed {
                    error: "expected failure".to_owned(),
                })
        });
        wait_until(|| !db.is_operator_open("retry"));
        assert_eq!(attempts.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn states_preserve_first_opened_key_and_value_types() {
        let path = tmp("typed_state");
        let (tracker, _) = new_tracker("typed_state");
        let db = TestDatabase::create(&path, Arc::new(ThreadExecutor), DatabaseConfig::default())
            .unwrap();
        db.table("users", Some(TableConfig::default())).unwrap();
        db.dispatch_operator::<CountingOperator>(
            "counter",
            counting_config(tracker, false),
            counting_runtime_config(Subscription::pattern("users")),
        )
        .unwrap();

        wait_until(|| db.state::<Vec<u8>, Vec<u8>>("index", None).is_ok());
        let index = db.state::<Vec<u8>, Vec<u8>>("index", None).unwrap();
        index
            .get()
            .unwrap()
            .write()
            .put(b"users".to_vec(), 42_u64.to_le_bytes().to_vec())
            .unwrap();
        assert_eq!(
            index
                .get()
                .unwrap()
                .read()
                .get(&b"users".to_vec())
                .map(|value| value.into_owned()),
            Some(42_u64.to_le_bytes().to_vec())
        );
        assert!(db.state::<u64, u64>("index", None).is_err());
    }

    #[test]
    fn typed_states_reopen_from_catalog() {
        let path = tmp("typed_state_reopen");
        let (tracker, _) = new_tracker("typed_state_reopen");
        {
            let db =
                TestDatabase::create(&path, Arc::new(ThreadExecutor), DatabaseConfig::default())
                    .unwrap();
            db.table("users", Some(TableConfig::default())).unwrap();
            db.dispatch_operator::<CountingOperator>(
                "counter",
                counting_config(tracker.clone(), false),
                counting_runtime_config(Subscription::pattern("users")),
            )
            .unwrap();
            wait_until(|| db.state::<Vec<u8>, Vec<u8>>("index", None).is_ok());
            db.state::<Vec<u8>, Vec<u8>>("index", None)
                .unwrap()
                .get()
                .unwrap()
                .write()
                .put(b"users".to_vec(), 42_u64.to_le_bytes().to_vec())
                .unwrap();
        }

        let db =
            TestDatabase::open(&path, Arc::new(ThreadExecutor), DatabaseConfig::default()).unwrap();
        assert_eq!(
            db.state::<Vec<u8>, Vec<u8>>("index", None)
                .unwrap()
                .get()
                .unwrap()
                .read()
                .get(&b"users".to_vec())
                .map(|value| value.into_owned()),
            Some(42_u64.to_le_bytes().to_vec())
        );
    }

    #[test]
    fn active_operators_respawn_after_database_reopen() {
        let path = tmp("operator_reopen");
        let (tracker, count) = new_tracker("operator_reopen");
        {
            let db =
                TestDatabase::create(&path, Arc::new(ThreadExecutor), DatabaseConfig::default())
                    .unwrap();
            let users = db.table("users", Some(TableConfig::default())).unwrap();
            db.dispatch_operator::<CountingOperator>(
                "counter",
                counting_config(tracker.clone(), false),
                counting_runtime_config(Subscription::pattern("users")),
            )
            .unwrap();

            users
                .get()
                .unwrap()
                .write()
                .insert_event(event("users", 1, 100))
                .unwrap();
            wait_until(|| count.load(Ordering::Relaxed) == 1);
            assert_eq!(db.operator_phase("counter"), Some(OperatorPhase::Active));
        }

        let db =
            TestDatabase::open(&path, Arc::new(ThreadExecutor), DatabaseConfig::default()).unwrap();
        let users = db.table("users", None).unwrap();
        assert_eq!(db.operator_phase("counter"), Some(OperatorPhase::Active));

        users
            .get()
            .unwrap()
            .write()
            .insert_event(event("users", 2, 110))
            .unwrap();
        wait_until(|| count.load(Ordering::Relaxed) == 2);
    }

    #[test]
    fn cancelled_operator_is_permanent_and_not_reopened() {
        let path = tmp("operator_cancel");
        let (tracker, log) = new_lifecycle_log("operator_cancel");
        {
            let db =
                TestDatabase::create(&path, Arc::new(ThreadExecutor), DatabaseConfig::default())
                    .unwrap();
            db.table("users", Some(TableConfig::default())).unwrap();
            db.dispatch_operator::<ShutdownLifecycleOperator>(
                "lifecycle",
                shutdown_lifecycle_config(tracker.clone()),
                shutdown_lifecycle_runtime_config(),
            )
            .unwrap();

            wait_until(|| log.lock().contains(&"opened:users".to_owned()));
            db.cancel_operator("lifecycle").unwrap();
            wait_until(|| db.operator_phase("lifecycle") == Some(OperatorPhase::Cancelled));
            wait_until(|| !db.is_operator_open("lifecycle"));
        }

        {
            let log = log.lock();
            assert!(log.contains(&"closed:users".to_owned()), "{log:?}");
            assert!(log.contains(&"teardown".to_owned()), "{log:?}");
        }

        let db =
            TestDatabase::open(&path, Arc::new(ThreadExecutor), DatabaseConfig::default()).unwrap();
        db.table("users", None).unwrap();
        std::thread::sleep(Duration::from_millis(20));
        assert_eq!(
            db.operator_phase("lifecycle"),
            Some(OperatorPhase::Cancelled)
        );
        assert!(!db.is_operator_open("lifecycle"));
        assert_eq!(
            log.lock()
                .iter()
                .filter(|entry| entry.as_str() == "opened:users")
                .count(),
            1
        );
    }

    #[test]
    fn processing_time_timers_fire_and_survive_restart() {
        let path = tmp("timers");
        let (tracker, fired) = new_tracker("timers");
        let db = TestDatabase::create(&path, Arc::new(ThreadExecutor), DatabaseConfig::default())
            .unwrap();
        db.table("users", Some(TableConfig::default())).unwrap();
        db.dispatch_operator::<TimerOperator>(
            "ticker",
            timer_config(tracker),
            timer_runtime_config(),
        )
        .unwrap();

        wait_until(|| fired.load(Ordering::Relaxed) >= 1);
    }

    #[test]
    fn operators_can_spawn_user_and_prelude_operators_from_context() {
        let path = tmp("operator_spawn_from_context");
        let (tracker, child_count) = new_tracker("operator_spawn_from_context");
        let db = TestDatabase::create(&path, Arc::new(ThreadExecutor), DatabaseConfig::default())
            .unwrap();
        let table = db.table("users", Some(TableConfig::default())).unwrap();
        db.dispatch_operator::<SpawnerOperator>(
            "spawner",
            spawner_config(tracker),
            spawner_runtime_config(),
        )
        .unwrap();

        wait_until(|| {
            db.contains_operator("spawned-counter") && db.contains_operator("spawned-merkle")
        });

        table
            .get()
            .unwrap()
            .write()
            .insert_event(event("users", 1, 100))
            .unwrap();

        wait_until(|| child_count.load(Ordering::Relaxed) == 1);
    }

    #[test]
    fn operator_receives_opened_callbacks_for_initial_inputs() {
        let path = tmp("input_lifecycle_initial");
        let (tracker, opened, closed) = new_input_tracker("input_lifecycle_initial");
        let db = TestDatabase::create(&path, Arc::new(ThreadExecutor), DatabaseConfig::default())
            .unwrap();
        db.table("users", Some(TableConfig::default())).unwrap();
        db.dispatch_operator::<InputLifecycleOperator>(
            "inputs",
            input_lifecycle_config(tracker),
            input_lifecycle_runtime_config(Subscription::pattern("users")),
        )
        .unwrap();

        wait_until(|| opened.lock().contains(&"users".to_owned()));
        assert!(closed.lock().is_empty());
    }

    #[test]
    fn operator_receives_opened_callbacks_for_later_inputs() {
        let path = tmp("input_lifecycle_later");
        let (tracker, opened, closed) = new_input_tracker("input_lifecycle_later");
        let db = TestDatabase::create(&path, Arc::new(ThreadExecutor), DatabaseConfig::default())
            .unwrap();
        db.table("users", Some(TableConfig::default())).unwrap();
        db.dispatch_operator::<InputLifecycleOperator>(
            "inputs",
            input_lifecycle_config(tracker),
            input_lifecycle_runtime_config(Subscription::pattern("*")),
        )
        .unwrap();

        wait_until(|| opened.lock().contains(&"users".to_owned()));
        db.table("orders", Some(TableConfig::default())).unwrap();

        wait_until(|| {
            let opened = opened.lock();
            opened.contains(&"users".to_owned()) && opened.contains(&"orders".to_owned())
        });
        assert!(closed.lock().is_empty());
    }

    #[test]
    fn close_table_evicts_cache_notifies_and_allows_reopen() {
        let path = tmp("close_table");
        let (tracker, opened, closed) = new_input_tracker("close_table");
        let db = TestDatabase::create(&path, Arc::new(ThreadExecutor), DatabaseConfig::default())
            .unwrap();
        let users = db.table("users", Some(TableConfig::default())).unwrap();
        db.dispatch_operator::<InputLifecycleOperator>(
            "inputs",
            input_lifecycle_config(tracker),
            input_lifecycle_runtime_config(Subscription::pattern("users")),
        )
        .unwrap();

        wait_until(|| opened.lock().contains(&"users".to_owned()));
        assert!(db.is_table_open("users"));
        assert!(db.is_operator_open("inputs"));

        assert!(db.close_table("users"));

        wait_until(|| closed.lock().contains(&"users".to_owned()));
        wait_until(|| !db.is_operator_open("inputs"));
        assert!(!db.is_table_open("users"));
        assert!(matches!(
            users.get(),
            Err(error) if error.kind() == io::ErrorKind::NotConnected
        ));

        db.table("users", None).unwrap();

        wait_until(|| {
            db.is_operator_open("inputs")
                && opened
                    .lock()
                    .iter()
                    .filter(|table| table.as_str() == "users")
                    .count()
                    == 2
        });
    }

    #[test]
    fn retired_operator_deletes_consumers_from_unopened_tables() {
        let path = tmp("retire_consumers");
        {
            let db =
                TestDatabase::create(&path, Arc::new(ThreadExecutor), DatabaseConfig::default())
                    .unwrap();
            let orders = db.table("orders", Some(TableConfig::default())).unwrap();
            let orders_table = orders.get().unwrap();

            let stale_consumer = orders_table.read().consumer("counter").unwrap();
            drop(stale_consumer);

            orders_table
                .write()
                .insert_event(event("orders", 1, 100))
                .unwrap();
        }

        let (tracker, _) = new_tracker("retire_consumers");
        let db =
            TestDatabase::open(&path, Arc::new(ThreadExecutor), DatabaseConfig::default()).unwrap();
        db.dispatch_operator::<CountingOperator>(
            "counter",
            counting_config(tracker, true),
            counting_runtime_config(Subscription::pattern("*")),
        )
        .unwrap();

        let users = db.table("users", Some(TableConfig::default())).unwrap();
        users
            .get()
            .unwrap()
            .write()
            .insert_event(event("users", 2, 110))
            .unwrap();

        wait_until(|| db.operator_phase("counter") == Some(OperatorPhase::Finished));

        let orders = db.table("orders", None).unwrap();
        let orders_table = orders.get().unwrap();
        let mut consumer = orders_table.read().consumer("counter").unwrap();
        assert!(consumer.next().is_none());
    }

    #[test]
    fn shutdown_runs_input_closed_before_teardown() {
        let path = tmp("shutdown_lifecycle");
        let (tracker, log) = new_lifecycle_log("shutdown_lifecycle");
        let db = TestDatabase::create(&path, Arc::new(ThreadExecutor), DatabaseConfig::default())
            .unwrap();
        let users = db.table("users", Some(TableConfig::default())).unwrap();
        db.dispatch_operator::<ShutdownLifecycleOperator>(
            "shutdown-lifecycle",
            shutdown_lifecycle_config(tracker),
            shutdown_lifecycle_runtime_config(),
        )
        .unwrap();

        wait_until(|| log.lock().contains(&"opened:users".to_owned()));

        users
            .get()
            .unwrap()
            .write()
            .insert_event(event("users", 1, 100))
            .unwrap();

        wait_until(|| db.operator_phase("shutdown-lifecycle") == Some(OperatorPhase::Finished));

        let log = log.lock().clone();
        let closed = log
            .iter()
            .position(|entry| entry == "closed:users")
            .expect("closed event is recorded");
        let teardown = log
            .iter()
            .position(|entry| entry == "teardown")
            .expect("teardown event is recorded");

        assert!(
            closed < teardown,
            "on_input_closed must run before teardown: {log:?}"
        );
    }
}
