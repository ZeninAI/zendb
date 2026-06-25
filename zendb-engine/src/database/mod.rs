//! Eager database lifecycle and resource ownership.

mod operators;
mod states;
mod tables;
mod timers;

use std::{
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
    operator::{worker::OperatorWorker, OperatorRegistry},
    runtime::Executor,
    OperatorConfig, TableConfig,
};

use states::ErasedStateHandle;
use timers::{run_scheduler, TimerStore};

#[derive(Debug, Clone, Encode, Decode)]
pub(super) enum CatalogEntry {
    Table(TableConfig),
    Operator(OperatorConfig),
    State(StateConfig),
}

pub(super) type Catalog = KeyDir<String, CatalogEntry>;

const META_FILE: &str = "_meta";
pub(crate) const TABLES_DIR: &str = "tables";
pub(crate) const STATES_DIR: &str = "states";
const TIMERS_FILE: &str = "timers";

#[derive(Debug, Clone, Encode, Decode)]
pub struct DatabaseConfig {}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {}
    }
}

pub type ConcurrentTable = Arc<RwLock<Table>>;
pub type ConcurrentState<K, V> = Arc<RwLock<State<K, V>>>;

/// A durable, weak reference to a database table.
///
/// The database is the single owner of every table, so handles never keep a
/// table (or the database) alive. Call [`TableHandle::get`] to obtain a strong
/// guard for an operation; once the owning database is dropped, `get` fails
/// instead of resurrecting a detached table.
#[derive(Clone)]
pub struct TableHandle {
    name: Arc<str>,
    inner: Weak<RwLock<Table>>,
}

impl TableHandle {
    pub(crate) fn new(name: &str, table: &ConcurrentTable) -> Self {
        Self {
            name: Arc::from(name),
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
    name: Arc<str>,
    inner: Weak<RwLock<State<K, V>>>,
}

impl<K, V> StateHandle<K, V>
where
    K: Encode + Decode<()> + std::hash::Hash + Eq + Clone + Ord + Send + Sync + 'static,
    V: Encode + Decode<()> + Clone + Send + Sync + 'static,
{
    pub(crate) fn new(name: &str, state: &ConcurrentState<K, V>) -> Self {
        Self {
            name: Arc::from(name),
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
pub struct Database {
    path: PathBuf,
    #[allow(dead_code)]
    pub(crate) config: DatabaseConfig,
    pub(crate) executor: Arc<dyn Executor>,
    registry: OperatorRegistry,
    pub(crate) lifecycle: Mutex<()>,
    catalog: Mutex<Catalog>,
    pub(crate) tables: RwLock<HashMap<String, ConcurrentTable>>,
    states: RwLock<HashMap<String, ErasedStateHandle>>,
    pub(crate) operators: RwLock<HashMap<String, Arc<OperatorWorker>>>,
    timers: Arc<RwLock<TimerStore>>,
    /// Notified by `register_timer` to wake the scheduler early.
    pub(crate) timer_notify: Arc<(Mutex<()>, Condvar)>,
}

impl Database {
    pub fn create(
        path: &Path,
        executor: Arc<dyn Executor>,
        registry: OperatorRegistry,
        config: DatabaseConfig,
    ) -> io::Result<Arc<Self>> {
        fs::create_dir_all(path)?;
        let catalog = Catalog::create(&path.join(META_FILE), KeyDirConfig::default())?;
        let timers = TimerStore::create(&path.join(TIMERS_FILE), StateConfig::default())?;
        Self::from_parts(path, catalog, timers, executor, registry, config)
    }

    pub fn open(
        path: &Path,
        executor: Arc<dyn Executor>,
        registry: OperatorRegistry,
        config: DatabaseConfig,
    ) -> io::Result<Arc<Self>> {
        let catalog = Catalog::open(&path.join(META_FILE), KeyDirConfig::default())?;
        let timers = TimerStore::open(&path.join(TIMERS_FILE), StateConfig::default())?;
        Self::from_parts(path, catalog, timers, executor, registry, config)
    }

    /// Assemble a `Database` from its constituent parts and spawn the background timer scheduler.
    fn from_parts(
        path: &Path,
        catalog: Catalog,
        timers: TimerStore,
        executor: Arc<dyn Executor>,
        registry: OperatorRegistry,
        config: DatabaseConfig,
    ) -> io::Result<Arc<Self>> {
        let timer_notify = Arc::new((Mutex::new(()), Condvar::new()));
        let database = Arc::new(Self {
            path: path.to_path_buf(),
            config,
            executor,
            registry,
            lifecycle: Mutex::new(()),
            catalog: Mutex::new(catalog),
            tables: RwLock::new(HashMap::new()),
            states: RwLock::new(HashMap::new()),
            operators: RwLock::new(HashMap::new()),
            timers: Arc::new(RwLock::new(timers)),
            timer_notify: Arc::clone(&timer_notify),
        });
        let db_weak = Arc::downgrade(&database);
        let executor = Arc::clone(&database.executor);
        database
            .executor
            .spawn(Box::pin(run_scheduler(db_weak, executor, timer_notify)));
        Ok(database)
    }
}

fn already_exists(kind: &str, name: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::AlreadyExists,
        format!("{kind} {name:?} already exists"),
    )
}

fn not_found(kind: &str, name: &str) -> io::Error {
    io::Error::new(io::ErrorKind::NotFound, format!("no {kind} {name:?}"))
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
        Change, Operator, OperatorConfig, OperatorContext, OperatorRegistry, OperatorStatus,
        Subscription, TableConfig,
    };
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
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

    struct CountingOperator {
        count: Arc<AtomicUsize>,
        finish: bool,
        buffer: Option<StateHandle<String, u64>>,
        index: Option<StateHandle<Vec<u8>, Vec<u8>>>,
        output: Option<TableHandle>,
    }

    impl Operator for CountingOperator {
        type Config = ();
        type Timer = ();
        fn open<'a>(&'a mut self, ctx: OperatorContext) -> crate::BoxFuture<'a, io::Result<()>> {
            Box::pin(async move {
                self.buffer = Some(ctx.state("counter/buffer", Some(StateConfig::default()))?);
                self.index = Some(ctx.state("index", Some(StateConfig::default()))?);
                self.output = Some(ctx.table("users", None)?);
                Ok(())
            })
        }

        fn process<'a>(
            &'a mut self,
            changes: Vec<Change>,
            _ctx: OperatorContext,
        ) -> crate::BoxFuture<'a, io::Result<OperatorStatus>> {
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
                    OperatorStatus::Finish
                } else {
                    OperatorStatus::Continue
                })
            })
        }

        fn finish<'a>(&'a mut self, ctx: OperatorContext) -> crate::BoxFuture<'a, io::Result<()>> {
            Box::pin(async move {
                self.buffer = None;
                self.index = None;
                self.output = None;
                if let Some(database) = ctx.database() {
                    database.drop_state("counter/buffer")?;
                    database.drop_state("index")?;
                }
                Ok(())
            })
        }
    }

    struct FailingOnceOperator {
        attempts: Arc<AtomicUsize>,
        processed: Arc<AtomicUsize>,
    }

    impl Operator for FailingOnceOperator {
        type Config = ();
        type Timer = ();
        fn process<'a>(
            &'a mut self,
            changes: Vec<Change>,
            _ctx: OperatorContext,
        ) -> crate::BoxFuture<'a, io::Result<OperatorStatus>> {
            Box::pin(async move {
                if self.attempts.fetch_add(1, Ordering::Relaxed) == 0 {
                    return Err(io::Error::other("expected failure"));
                }
                self.processed.fetch_add(changes.len(), Ordering::Relaxed);
                Ok(OperatorStatus::Continue)
            })
        }
    }

    struct MultiTableOperator {
        processed: Arc<AtomicUsize>,
    }

    impl Operator for MultiTableOperator {
        type Config = ();
        type Timer = ();
        fn process<'a>(
            &'a mut self,
            changes: Vec<Change>,
            _ctx: OperatorContext,
        ) -> crate::BoxFuture<'a, io::Result<OperatorStatus>> {
            Box::pin(async move {
                self.processed.fetch_add(changes.len(), Ordering::Relaxed);
                Ok(OperatorStatus::Continue)
            })
        }
    }

    struct TimerOperator {
        fired: Arc<AtomicUsize>,
    }

    impl Operator for TimerOperator {
        type Config = ();
        type Timer = ();

        fn open<'a>(&'a mut self, ctx: OperatorContext) -> crate::BoxFuture<'a, io::Result<()>> {
            Box::pin(async move {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64;
                ctx.register_timer_typed(now, &())?;
                Ok(())
            })
        }

        fn process<'a>(
            &'a mut self,
            _changes: Vec<Change>,
            _ctx: OperatorContext,
        ) -> crate::BoxFuture<'a, io::Result<OperatorStatus>> {
            Box::pin(async { Ok(OperatorStatus::Continue) })
        }

        fn handle_timer<'a>(
            &'a mut self,
            _payload: (),
            _ctx: OperatorContext,
        ) -> crate::BoxFuture<'a, io::Result<()>> {
            Box::pin(async move {
                self.fired.fetch_add(1, Ordering::Relaxed);
                Ok(())
            })
        }
    }

    fn registry(count: Arc<AtomicUsize>, finish: bool) -> OperatorRegistry {
        let mut registry = OperatorRegistry::new();
        registry.register_operator::<CountingOperator>("count", move |_: ()| {
            Ok(CountingOperator {
                count: Arc::clone(&count),
                finish,
                buffer: None,
                index: None,
                output: None,
            })
        });
        registry
    }

    fn config() -> OperatorConfig {
        OperatorConfig {
            implementation: "count".into(),
            configuration: Vec::new(),
            subscriptions: vec![Subscription::pattern("users")],
            retry: Default::default(),
            poll_size: 128,
        }
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
        let count = Arc::new(AtomicUsize::new(0));
        let db = Database::create(
            &path,
            Arc::new(ThreadExecutor),
            registry(Arc::clone(&count), false),
            DatabaseConfig::default(),
        )
        .unwrap();
        let table = db.table("users", Some(TableConfig::default())).unwrap();
        db.register_operator("counter", config()).unwrap();

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
    fn lazy_open_tables_and_operators() {
        let path = tmp("reopen");
        let count = Arc::new(AtomicUsize::new(0));
        let db = Database::create(
            &path,
            Arc::new(ThreadExecutor),
            registry(Arc::clone(&count), false),
            DatabaseConfig::default(),
        )
        .unwrap();
        db.table("users", Some(TableConfig::default())).unwrap();
        db.register_operator("counter", config()).unwrap();
        std::thread::sleep(Duration::from_millis(50));
        // Close operator and table so we can re-open lazily in same DB
        db.close_operator("counter").unwrap();
        db.close_table("users").unwrap();
        // Nothing in memory now
        assert!(!db.tables.read().contains_key("users"));
        assert!(!db.operators.read().contains_key("counter"));
        // Opening the table automatically starts the matching catalog operator
        db.table("users", None).unwrap();
        assert!(db.operators.read().contains_key("counter"));
        db.table("users", None)
            .unwrap()
            .get()
            .unwrap()
            .write()
            .insert_event(event("users", 1, 100))
            .unwrap();
        wait_until(|| count.load(Ordering::Relaxed) == 1);
    }

    #[test]
    fn finish_can_cleanup_states_explicitly() {
        let path = tmp("finish");
        let count = Arc::new(AtomicUsize::new(0));
        let db = Database::create(
            &path,
            Arc::new(ThreadExecutor),
            registry(Arc::clone(&count), true),
            DatabaseConfig::default(),
        )
        .unwrap();
        let table = db.table("users", Some(TableConfig::default())).unwrap();
        db.register_operator("counter", config()).unwrap();

        table
            .get()
            .unwrap()
            .write()
            .insert_event(event("users", 1, 100))
            .unwrap();
        wait_until(|| count.load(Ordering::Relaxed) == 1);
        wait_until(|| !db.operators.read().contains_key("counter"));
        assert!(matches!(
            db.state::<Vec<u8>, Vec<u8>>("index", None),
            Err(error) if error.kind() == io::ErrorKind::NotFound
        ));
        wait_until(|| db.register_operator("counter", config()).is_ok());
    }

    #[test]
    fn drop_table_invalidates_outstanding_handles() {
        let path = tmp("drop_table");
        let db = Database::create(
            &path,
            Arc::new(ThreadExecutor),
            OperatorRegistry::new(),
            DatabaseConfig::default(),
        )
        .unwrap();
        // A table handle is a weak reference: holding it does not keep the
        // table alive, so the database can drop it as the single owner.
        let table = db.table("users", Some(TableConfig::default())).unwrap();
        assert!(path.join(TABLES_DIR).join("users").exists());
        db.drop_table("users").unwrap();
        assert!(!path.join(TABLES_DIR).join("users").exists());
        // The outstanding handle now fails to upgrade instead of resurrecting a
        // detached table.
        assert_eq!(
            table.get().err().unwrap().kind(),
            io::ErrorKind::NotConnected
        );
    }

    #[test]
    fn drop_table_blocks_while_handle_is_upgraded() {
        let path = tmp("drop_table_inflight");
        let db = Database::create(
            &path,
            Arc::new(ThreadExecutor),
            OperatorRegistry::new(),
            DatabaseConfig::default(),
        )
        .unwrap();
        let table = db.table("users", Some(TableConfig::default())).unwrap();
        // An in-flight upgrade holds a strong reference for the duration of an
        // operation and blocks teardown until released.
        let strong = table.get().unwrap();
        assert_eq!(
            db.drop_table("users").unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        drop(strong);
        db.drop_table("users").unwrap();
        assert!(!path.join(TABLES_DIR).join("users").exists());
    }

    #[test]
    fn drop_table_unsubscribes_and_drops_orphaned_operator() {
        let path = tmp("drop_table_orphan");
        let count = Arc::new(AtomicUsize::new(0));
        let db = Database::create(
            &path,
            Arc::new(ThreadExecutor),
            registry(Arc::clone(&count), false),
            DatabaseConfig::default(),
        )
        .unwrap();
        let table = db.table("users", Some(TableConfig::default())).unwrap();
        db.register_operator("counter", config()).unwrap();

        table
            .get()
            .unwrap()
            .write()
            .insert_event(event("users", 1, 100))
            .unwrap();
        wait_until(|| count.load(Ordering::Relaxed) == 1);
        drop(table);

        db.drop_table("users").unwrap();

        assert!(!db.operators.read().contains_key("counter"));
        assert!(!path.join(TABLES_DIR).join("users").exists());
        assert!(matches!(
            db.table("users", None),
            Err(error) if error.kind() == io::ErrorKind::NotFound
        ));
    }

    #[test]
    fn drop_table_keeps_multi_subscription_operator_running() {
        let path = tmp("drop_table_survivor");
        let processed = Arc::new(AtomicUsize::new(0));
        let mut registry = OperatorRegistry::new();
        let factory_processed = Arc::clone(&processed);
        registry.register_operator::<MultiTableOperator>("multi", move |_: ()| {
            Ok(MultiTableOperator {
                processed: Arc::clone(&factory_processed),
            })
        });
        let db = Database::create(
            &path,
            Arc::new(ThreadExecutor),
            registry,
            DatabaseConfig::default(),
        )
        .unwrap();
        let users = db.table("users", Some(TableConfig::default())).unwrap();
        let posts = db.table("posts", Some(TableConfig::default())).unwrap();
        db.register_operator(
            "multi",
            OperatorConfig {
                implementation: "multi".into(),
                configuration: Vec::new(),
                subscriptions: vec![
                    Subscription::pattern("users"),
                    Subscription::pattern("posts"),
                ],
                retry: Default::default(),
                poll_size: 128,
            },
        )
        .unwrap();

        drop(users);
        db.drop_table("users").unwrap();

        assert!(db.operators.read().contains_key("multi"));
        posts
            .get()
            .unwrap()
            .write()
            .insert_event(event("posts", 1, 100))
            .unwrap();
        wait_until(|| processed.load(Ordering::Relaxed) == 1);
    }

    #[test]
    fn drop_state_invalidates_outstanding_handles() {
        let path = tmp("drop_state");
        let db = Database::create(
            &path,
            Arc::new(ThreadExecutor),
            OperatorRegistry::new(),
            DatabaseConfig::default(),
        )
        .unwrap();
        let state = db
            .state::<String, u64>("counter/buffer", Some(StateConfig::default()))
            .unwrap();
        // Holding the weak handle does not block teardown; an in-flight upgrade
        // does.
        let strong = state.get().unwrap();
        assert_eq!(
            db.drop_state("counter/buffer").unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        drop(strong);
        db.drop_state("counter/buffer").unwrap();
        assert_eq!(
            state.get().err().unwrap().kind(),
            io::ErrorKind::NotConnected
        );
        assert!(!path
            .join(STATES_DIR)
            .join("counter")
            .join("buffer")
            .exists());
    }

    #[test]
    fn failed_process_resets_readers_to_committed_offsets() {
        let path = tmp("retry");
        let attempts = Arc::new(AtomicUsize::new(0));
        let processed = Arc::new(AtomicUsize::new(0));
        let mut registry = OperatorRegistry::new();
        let factory_attempts = Arc::clone(&attempts);
        let factory_processed = Arc::clone(&processed);
        registry.register_operator::<FailingOnceOperator>("retry", move |_: ()| {
            Ok(FailingOnceOperator {
                attempts: Arc::clone(&factory_attempts),
                processed: Arc::clone(&factory_processed),
            })
        });
        let db = Database::create(
            &path,
            Arc::new(ThreadExecutor),
            registry,
            DatabaseConfig::default(),
        )
        .unwrap();
        let table = db.table("users", Some(TableConfig::default())).unwrap();
        db.register_operator(
            "retry",
            OperatorConfig {
                implementation: "retry".into(),
                configuration: Vec::new(),
                subscriptions: vec![Subscription::pattern("users")],
                retry: Default::default(),
                poll_size: 128,
            },
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

        wait_until(|| attempts.load(Ordering::Relaxed) >= 2);
        wait_until(|| processed.load(Ordering::Relaxed) == 2);
    }

    #[test]
    fn states_preserve_first_opened_key_and_value_types() {
        let path = tmp("typed_state");
        let db = Database::create(
            &path,
            Arc::new(ThreadExecutor),
            registry(Arc::new(AtomicUsize::new(0)), false),
            DatabaseConfig::default(),
        )
        .unwrap();
        db.table("users", Some(TableConfig::default())).unwrap();
        db.register_operator("counter", config()).unwrap();

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
        {
            let db = Database::create(
                &path,
                Arc::new(ThreadExecutor),
                registry(Arc::new(AtomicUsize::new(0)), false),
                DatabaseConfig::default(),
            )
            .unwrap();
            db.table("users", Some(TableConfig::default())).unwrap();
            db.register_operator("counter", config()).unwrap();
            wait_until(|| db.state::<Vec<u8>, Vec<u8>>("index", None).is_ok());
            db.state::<Vec<u8>, Vec<u8>>("index", None)
                .unwrap()
                .get()
                .unwrap()
                .write()
                .put(b"users".to_vec(), 42_u64.to_le_bytes().to_vec())
                .unwrap();
        }

        let db = Database::open(
            &path,
            Arc::new(ThreadExecutor),
            registry(Arc::new(AtomicUsize::new(0)), false),
            DatabaseConfig::default(),
        )
        .unwrap();
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
        let fired = Arc::new(AtomicUsize::new(0));
        let mut registry = OperatorRegistry::new();
        let factory_fired = Arc::clone(&fired);
        registry.register_operator::<TimerOperator>("timer", move |_: ()| {
            Ok(TimerOperator {
                fired: Arc::clone(&factory_fired),
            })
        });
        let db = Database::create(
            &path,
            Arc::new(ThreadExecutor),
            registry,
            DatabaseConfig::default(),
        )
        .unwrap();
        db.table("users", Some(TableConfig::default())).unwrap();
        db.register_operator(
            "ticker",
            OperatorConfig {
                implementation: "timer".into(),
                configuration: Vec::new(),
                subscriptions: vec![Subscription::pattern("users")],
                retry: Default::default(),
                poll_size: 128,
            },
        )
        .unwrap();

        wait_until(|| fired.load(Ordering::Relaxed) >= 1);
    }

    #[test]
    fn far_future_timer_is_persistent_and_swept_on_drop() {
        let path = tmp("timers_persist");
        let db = Database::create(
            &path,
            Arc::new(ThreadExecutor),
            OperatorRegistry::new(),
            DatabaseConfig::default(),
        )
        .unwrap();
        let far_future = u64::MAX;
        db.register_timer("ghost", far_future, b"never".to_vec())
            .unwrap();
        assert_eq!(db.timers.read().size(), 1);

        // Sweeping a (never registered) operator removes only its timers.
        db.sweep_operator_timers("ghost");
        assert_eq!(db.timers.read().size(), 0);
    }
}
