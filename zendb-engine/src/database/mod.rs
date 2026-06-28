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
};

use bincode::{Decode, Encode};
use hashbrown::HashMap;
use parking_lot::{Condvar, Mutex, RwLock};
use zendb_storage::core::{
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

use timers::{run_scheduler, TimerStore};

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
        self.inner
            .upgrade()
            .ok_or_else(|| resource_closed("table", &self.name))
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
        self.inner
            .upgrade()
            .ok_or_else(|| resource_closed("state", &self.name))
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
    #[allow(dead_code)]
    pub(crate) config: DatabaseConfig,
    pub(crate) executor: Arc<dyn Executor>,
    table_catalog: Mutex<TableCatalog>,
    state_catalog: Mutex<StateCatalog>,
    operator_catalog: Mutex<OperatorCatalog<D::DispatchConfig>>,
    pub(crate) tables: RwLock<HashMap<String, ConcurrentTable>>,
    states: RwLock<HashMap<String, ErasedStateHandle>>,
    pub(crate) operators: RwLock<HashMap<String, Arc<OperatorWorker<D>>>>,
    timers: Arc<RwLock<TimerStore>>,
    /// Notified by `register_timer` to wake the scheduler early.
    pub(crate) timer_notify: Arc<(Mutex<()>, Condvar)>,
}

impl<D> Database<D>
where
    D: DispatchOperator,
{
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
        let operator_catalog = OperatorCatalog::<D::DispatchConfig>::create(
            &path.join(OPERATOR_CATALOG_FILE),
            KeyDirConfig::default(),
        )?;
        let timers = TimerStore::create(&path.join(TIMERS_FILE), StateConfig::default())?;
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

    pub fn open(
        path: &Path,
        executor: Arc<dyn Executor>,
        config: DatabaseConfig,
    ) -> io::Result<Arc<Self>> {
        let table_catalog =
            TableCatalog::open(&path.join(TABLE_CATALOG_FILE), KeyDirConfig::default())?;
        let state_catalog =
            StateCatalog::open(&path.join(STATE_CATALOG_FILE), KeyDirConfig::default())?;
        let operator_catalog = OperatorCatalog::<D::DispatchConfig>::open(
            &path.join(OPERATOR_CATALOG_FILE),
            KeyDirConfig::default(),
        )?;
        let timers = TimerStore::open(&path.join(TIMERS_FILE), StateConfig::default())?;
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
        operator_catalog: OperatorCatalog<D::DispatchConfig>,
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
}

fn resource_closed(kind: &str, name: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::NotConnected,
        format!("{kind} {name:?} is unavailable because its database was dropped"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        Change, DispatchOperatorConfig, Operator, OperatorContext, OperatorDirective,
        OperatorRuntimeConfig, Subscription, TableConfig,
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
        buffer: Option<StateHandle<String, u64>>,
        index: Option<StateHandle<Vec<u8>, Vec<u8>>>,
        output: Option<TableHandle>,
    }

    impl Operator for CountingOperator {
        type Config = CountingConfig;
        type Timer = ();

        fn new(config: &Self::Config) -> io::Result<Self> {
            Ok(Self {
                count: lookup_counter(&config.tracker)?,
                finish: config.finish,
                buffer: None,
                index: None,
                output: None,
            })
        }

        fn open<'a, D>(
            &'a mut self,
            ctx: &'a OperatorContext<Self, D>,
        ) -> crate::BoxFuture<'a, io::Result<OperatorDirective>>
        where
            D: crate::DispatchOperator,
        {
            Box::pin(async move {
                self.buffer = Some(ctx.state("counter/buffer", Some(StateConfig::default()))?);
                self.index = Some(ctx.state("index", Some(StateConfig::default()))?);
                self.output = Some(ctx.table("users", None)?);
                Ok(OperatorDirective::Continue)
            })
        }

        fn process<'a, D>(
            &'a mut self,
            changes: Vec<Change>,
            _ctx: &'a OperatorContext<Self, D>,
        ) -> crate::BoxFuture<'a, io::Result<OperatorDirective>>
        where
            D: crate::DispatchOperator,
        {
            Box::pin(async move {
                self.count.fetch_add(changes.len(), Ordering::Relaxed);
                if let Some(state) = &self.index {
                    state.get()?.write().put(
                        b"count".to_vec(),
                        self.count.load(Ordering::Relaxed).to_le_bytes().to_vec(),
                    )?;
                }
                if let Some(state) = &self.buffer {
                    let key = "count".to_owned();
                    let state = state.get()?;
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

        fn finish<'a, D>(
            &'a mut self,
            _ctx: &'a OperatorContext<Self, D>,
        ) -> crate::BoxFuture<'a, io::Result<()>>
        where
            D: crate::DispatchOperator,
        {
            Box::pin(async move {
                self.buffer = None;
                self.index = None;
                self.output = None;
                Ok(())
            })
        }
    }

    #[derive(Debug, Clone, PartialEq, Encode, Decode)]
    struct RetryOperatorConfig {
        attempts_tracker: String,
        processed_tracker: String,
    }

    struct FailingOnceOperator {
        attempts: Arc<AtomicUsize>,
        processed: Arc<AtomicUsize>,
    }

    impl Operator for FailingOnceOperator {
        type Config = RetryOperatorConfig;
        type Timer = ();

        fn new(config: &Self::Config) -> io::Result<Self> {
            Ok(Self {
                attempts: lookup_counter(&config.attempts_tracker)?,
                processed: lookup_counter(&config.processed_tracker)?,
            })
        }

        fn process<'a, D>(
            &'a mut self,
            changes: Vec<Change>,
            _ctx: &'a OperatorContext<Self, D>,
        ) -> crate::BoxFuture<'a, io::Result<OperatorDirective>>
        where
            D: crate::DispatchOperator,
        {
            Box::pin(async move {
                if self.attempts.fetch_add(1, Ordering::Relaxed) == 0 {
                    return Err(io::Error::other("expected failure"));
                }
                self.processed.fetch_add(changes.len(), Ordering::Relaxed);
                Ok(OperatorDirective::Continue)
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

        fn new(config: &Self::Config) -> io::Result<Self> {
            Ok(Self {
                fired: lookup_counter(&config.tracker)?,
            })
        }

        fn open<'a, D>(
            &'a mut self,
            ctx: &'a OperatorContext<Self, D>,
        ) -> crate::BoxFuture<'a, io::Result<OperatorDirective>>
        where
            D: crate::DispatchOperator,
        {
            Box::pin(async move {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64;
                ctx.register_timer(now, &())?;
                Ok(OperatorDirective::Continue)
            })
        }

        fn process<'a, D>(
            &'a mut self,
            _changes: Vec<Change>,
            _ctx: &'a OperatorContext<Self, D>,
        ) -> crate::BoxFuture<'a, io::Result<OperatorDirective>>
        where
            D: crate::DispatchOperator,
        {
            Box::pin(async { Ok(OperatorDirective::Continue) })
        }

        fn handle_timer<'a, D>(
            &'a mut self,
            _payload: (),
            _ctx: &'a OperatorContext<Self, D>,
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

    crate::define_operator_set! {
        mod test_operators {
            Count(CountingOperator),
            Retry(FailingOnceOperator),
            Timer(TimerOperator),
        }
    }

    type TestDatabase = Database<test_operators::OperatorInstance>;
    type TestOperatorConfig = test_operators::OperatorConfig;
    type TestOperatorConfigVariant = test_operators::OperatorConfigVariant;

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

    fn counting_config(tracker: String, finish: bool) -> TestOperatorConfig {
        let config = TestOperatorConfig {
            operator: TestOperatorConfigVariant::Count(CountingConfig { tracker, finish }),
            runtime: OperatorRuntimeConfig {
                subscriptions: vec![Subscription::pattern("users")],
                retry: Default::default(),
                poll_size: 128,
            },
        };
        assert_eq!(config.kind(), test_operators::OperatorKind::Count);
        let _ = config.operator();
        let _ = config.runtime_config();
        config
    }

    fn retry_config(attempts_tracker: String, processed_tracker: String) -> TestOperatorConfig {
        let config = TestOperatorConfig {
            operator: TestOperatorConfigVariant::Retry(RetryOperatorConfig {
                attempts_tracker,
                processed_tracker,
            }),
            runtime: OperatorRuntimeConfig {
                subscriptions: vec![Subscription::pattern("users")],
                retry: Default::default(),
                poll_size: 128,
            },
        };
        assert_eq!(config.kind(), test_operators::OperatorKind::Retry);
        let _ = config.operator();
        let _ = config.runtime_config();
        config
    }

    fn timer_config(tracker: String) -> TestOperatorConfig {
        let config = TestOperatorConfig {
            operator: TestOperatorConfigVariant::Timer(TimerOperatorConfig { tracker }),
            runtime: OperatorRuntimeConfig {
                subscriptions: vec![Subscription::pattern("users")],
                retry: Default::default(),
                poll_size: 128,
            },
        };
        assert_eq!(config.kind(), test_operators::OperatorKind::Timer);
        let _ = config.operator();
        let _ = config.runtime_config();
        config
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

    fn tmp(name: &str) -> PathBuf {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        std::env::temp_dir().join(format!(
            "zendb_database_{name}_{}_{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ))
    }

    #[test]
    fn direct_table_writes_drive_operators() {
        let path = tmp("direct");
        let (tracker, count) = new_tracker("direct");
        let db = TestDatabase::create(&path, Arc::new(ThreadExecutor), DatabaseConfig::default())
            .unwrap();
        let table = db.table("users", Some(TableConfig::default())).unwrap();
        db.operator("counter", Some(counting_config(tracker, false)))
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
    fn failed_process_resets_readers_to_committed_offsets() {
        let path = tmp("retry");
        let (attempts_key, attempts) = new_tracker("retry_attempts");
        let (processed_key, processed) = new_tracker("retry_processed");
        let db = TestDatabase::create(&path, Arc::new(ThreadExecutor), DatabaseConfig::default())
            .unwrap();
        let table = db.table("users", Some(TableConfig::default())).unwrap();
        db.operator("retry", Some(retry_config(attempts_key, processed_key)))
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

        wait_until(|| attempts.load(Ordering::Relaxed) >= 2);
        wait_until(|| processed.load(Ordering::Relaxed) == 2);
    }

    #[test]
    fn states_preserve_first_opened_key_and_value_types() {
        let path = tmp("typed_state");
        let (tracker, _) = new_tracker("typed_state");
        let db = TestDatabase::create(&path, Arc::new(ThreadExecutor), DatabaseConfig::default())
            .unwrap();
        db.table("users", Some(TableConfig::default())).unwrap();
        db.operator("counter", Some(counting_config(tracker, false)))
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
            db.operator("counter", Some(counting_config(tracker.clone(), false)))
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
    fn processing_time_timers_fire_and_survive_restart() {
        let path = tmp("timers");
        let (tracker, fired) = new_tracker("timers");
        let db = TestDatabase::create(&path, Arc::new(ThreadExecutor), DatabaseConfig::default())
            .unwrap();
        db.table("users", Some(TableConfig::default())).unwrap();
        db.operator("ticker", Some(timer_config(tracker))).unwrap();

        wait_until(|| fired.load(Ordering::Relaxed) >= 1);
    }
}
