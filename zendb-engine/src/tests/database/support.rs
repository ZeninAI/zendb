use std::collections::HashMap;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;

use bincode::{Decode, Encode};
use crate::database::*;
use crate::operator::prelude::{MerkleTreeConfig, MerkleTreeOperator};
use crate::runtime::Executor;
use crate::{Change, Operator, OperatorDirective, OperatorRuntimeConfig, Subscription};
use parking_lot::Mutex;
use std::future::Future;
use std::sync::{
    atomic::{AtomicU64, AtomicUsize, Ordering},
    OnceLock,
};
use std::time::{Duration, Instant};
use zendb_storage::core::traits::Backend;
use zendb_storage::frontend::state::StateConfig;
use zendb_types::{
    device_id, init_device_id, Event, Hlc, Op, Path as ValuePath, PrimaryKey, Value,
};

// ---------------------------------------------------------------------------
// Shared test infrastructure (pub(crate) for reuse across test modules)
// ---------------------------------------------------------------------------

pub(crate) struct ThreadExecutor;

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

/// Wait until a condition is true, with a 1 second timeout.
pub(crate) fn wait_until(condition: impl Fn() -> bool) {
    wait_until_timeout(condition, Duration::from_secs(1));
}

/// Wait until a condition is true, with a custom timeout.
pub(crate) fn wait_until_timeout(condition: impl Fn() -> bool, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    while !condition() {
        assert!(Instant::now() < deadline, "condition was not reached within {:?}", timeout);
        std::thread::yield_now();
    }
}

pub(crate) fn database_config() -> DatabaseConfig {
    DatabaseConfig {
        graceful_shutdown_max_duration: Duration::from_millis(100),
    }
}

pub(crate) fn hlc(ms: u64) -> Hlc {
    init_device_id();
    Hlc::with_device_id(ms, 0, device_id()).unwrap()
}

pub(crate) fn string_event(table: &str, key: &str, value: &str, ms: u64) -> Event {
    Event {
        table_id: table.into(),
        primary_key: PrimaryKey::String(key.into()),
        path: ValuePath::new(),
        op: Op::Replace {
            value: Value::String(value.into()),
        },
        hlc: hlc(ms),
        sync: false,
        signature: Vec::new(),
    }
}

/// RAII guard that removes the test directory when dropped.
pub(crate) struct TmpDir(PathBuf);

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

pub(crate) fn tmp(prefix: &str) -> TmpDir {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    TmpDir(std::env::temp_dir().join(format!(
        "zendb_test_{prefix}_{}_{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    )))
}

// ---------------------------------------------------------------------------
// Database-specific test operators
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub(super) struct CountingConfig {
    tracker: String,
    finish: bool,
}

pub(super) struct CountingOperator {
    count: Arc<AtomicUsize>,
    finish: bool,
    buffer: StateHandle<String, u64>,
    index: StateHandle<Vec<u8>, Vec<u8>>,
}

impl Operator for CountingOperator {
    type Config = CountingConfig;
    type Timer = ();
    type Facet = ();

    fn create<'a, D>(
        db: &'a Arc<Database<D>>,
        _name: &'a str,
        config: &'a Self::Config,
    ) -> impl Future<Output = io::Result<Self>> + Send + 'a
    where
        D: crate::DispatchOperator,
        Self: Sized,
    {
        async move {
            let buffer = db.state("counter/buffer", Some(StateConfig::default()))?;
            let index = db.state("index", Some(StateConfig::default()))?;
            Ok(Self {
                count: lookup_counter(&config.tracker)?,
                finish: config.finish,
                buffer,
                index,
            })
        }
    }

    fn facet(&self) {}

    fn process<'a, D>(
        &'a mut self,
        changes: Vec<Change>,
        _db: &'a Arc<Database<D>>,
        _name: &'a str,
        _config: &'a Self::Config,
    ) -> impl Future<Output = io::Result<OperatorDirective>> + Send + 'a
    where
        D: crate::DispatchOperator,
    {
        async move {
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
        }
    }
}

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub(super) struct FailingOperatorConfig {
    attempts_tracker: String,
}

pub(super) struct FailingOperator {
    attempts: Arc<AtomicUsize>,
}

impl Operator for FailingOperator {
    type Config = FailingOperatorConfig;
    type Timer = ();
    type Facet = ();

    fn create<'a, D>(
        _db: &'a Arc<Database<D>>,
        _name: &'a str,
        config: &'a Self::Config,
    ) -> impl Future<Output = io::Result<Self>> + Send + 'a
    where
        D: crate::DispatchOperator,
        Self: Sized,
    {
        async move {
            Ok(Self {
                attempts: lookup_counter(&config.attempts_tracker)?,
            })
        }
    }

    fn facet(&self) {}

    fn process<'a, D>(
        &'a mut self,
        changes: Vec<Change>,
        _db: &'a Arc<Database<D>>,
        _name: &'a str,
        _config: &'a Self::Config,
    ) -> impl Future<Output = io::Result<OperatorDirective>> + Send + 'a
    where
        D: crate::DispatchOperator,
    {
        async move {
            let _ = changes;
            self.attempts.fetch_add(1, Ordering::Relaxed);
            Err(io::Error::other("expected failure"))
        }
    }
}

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub(super) struct TimerOperatorConfig {
    tracker: String,
}

pub(super) struct TimerOperator {
    fired: Arc<AtomicUsize>,
}

impl Operator for TimerOperator {
    type Config = TimerOperatorConfig;
    type Timer = ();
    type Facet = ();

    fn create<'a, D>(
        db: &'a Arc<Database<D>>,
        name: &'a str,
        config: &'a Self::Config,
    ) -> impl Future<Output = io::Result<Self>> + Send + 'a
    where
        D: crate::DispatchOperator,
        Self: Sized,
    {
        async move {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;
            db.register_timer(name, now, &())?;
            Ok(Self {
                fired: lookup_counter(&config.tracker)?,
            })
        }
    }

    fn facet(&self) {}

    fn process<'a, D>(
        &'a mut self,
        _changes: Vec<Change>,
        _db: &'a Arc<Database<D>>,
        _name: &'a str,
        _config: &'a Self::Config,
    ) -> impl Future<Output = io::Result<OperatorDirective>> + Send + 'a
    where
        D: crate::DispatchOperator,
    {
        async { Ok(OperatorDirective::Continue) }
    }

    fn on_timer<'a, D>(
        &'a mut self,
        _payload: (),
        _fire_at_ms: u64,
        _db: &'a Arc<Database<D>>,
        _name: &'a str,
        _config: &'a Self::Config,
    ) -> impl Future<Output = io::Result<OperatorDirective>> + Send + 'a
    where
        D: crate::DispatchOperator,
    {
        async move {
            self.fired.fetch_add(1, Ordering::Relaxed);
            Ok(OperatorDirective::Finish)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub(super) struct InputLifecycleConfig {
    tracker: String,
}

pub(super) struct InputLifecycleOperator {
    opened: Arc<Mutex<Vec<String>>>,
    closed: Arc<Mutex<Vec<String>>>,
}

impl Operator for InputLifecycleOperator {
    type Config = InputLifecycleConfig;
    type Timer = ();
    type Facet = ();

    fn create<'a, D>(
        _db: &'a Arc<Database<D>>,
        _name: &'a str,
        config: &'a Self::Config,
    ) -> impl Future<Output = io::Result<Self>> + Send + 'a
    where
        D: crate::DispatchOperator,
        Self: Sized,
    {
        async move {
            let (opened, closed) = lookup_input_tracker(&config.tracker)?;
            Ok(Self { opened, closed })
        }
    }

    fn facet(&self) {}

    fn process<'a, D>(
        &'a mut self,
        _changes: Vec<Change>,
        _db: &'a Arc<Database<D>>,
        _name: &'a str,
        _config: &'a Self::Config,
    ) -> impl Future<Output = io::Result<OperatorDirective>> + Send + 'a
    where
        D: crate::DispatchOperator,
    {
        async { Ok(OperatorDirective::Continue) }
    }

    fn on_input_opened<'a, D>(
        &'a mut self,
        table: String,
        _db: &'a Arc<Database<D>>,
        _name: &'a str,
        _config: &'a Self::Config,
    ) -> impl Future<Output = io::Result<OperatorDirective>> + Send + 'a
    where
        D: crate::DispatchOperator,
    {
        async move {
            self.opened.lock().push(table);
            Ok(OperatorDirective::Continue)
        }
    }

    fn on_input_closed<'a, D>(
        &'a mut self,
        table: String,
        _db: &'a Arc<Database<D>>,
        _name: &'a str,
        _config: &'a Self::Config,
    ) -> impl Future<Output = io::Result<OperatorDirective>> + Send + 'a
    where
        D: crate::DispatchOperator,
    {
        async move {
            self.closed.lock().push(table);
            Ok(OperatorDirective::Continue)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub(super) struct ShutdownLifecycleConfig {
    tracker: String,
}

pub(super) struct ShutdownLifecycleOperator {
    log: Arc<Mutex<Vec<String>>>,
}

impl Operator for ShutdownLifecycleOperator {
    type Config = ShutdownLifecycleConfig;
    type Timer = ();
    type Facet = ();

    fn create<'a, D>(
        _db: &'a Arc<Database<D>>,
        _name: &'a str,
        config: &'a Self::Config,
    ) -> impl Future<Output = io::Result<Self>> + Send + 'a
    where
        D: crate::DispatchOperator,
        Self: Sized,
    {
        async move {
            let log = lookup_lifecycle_log(&config.tracker)?;
            log.lock().push("create".to_owned());
            Ok(Self { log })
        }
    }

    fn facet(&self) {}

    fn process<'a, D>(
        &'a mut self,
        changes: Vec<Change>,
        _db: &'a Arc<Database<D>>,
        _name: &'a str,
        _config: &'a Self::Config,
    ) -> impl Future<Output = io::Result<OperatorDirective>> + Send + 'a
    where
        D: crate::DispatchOperator,
    {
        async move {
            self.log.lock().push(format!("process:{}", changes.len()));
            Ok(OperatorDirective::Finish)
        }
    }

    fn on_input_opened<'a, D>(
        &'a mut self,
        table: String,
        _db: &'a Arc<Database<D>>,
        _name: &'a str,
        _config: &'a Self::Config,
    ) -> impl Future<Output = io::Result<OperatorDirective>> + Send + 'a
    where
        D: crate::DispatchOperator,
    {
        async move {
            self.log.lock().push(format!("opened:{table}"));
            Ok(OperatorDirective::Continue)
        }
    }

    fn on_input_closed<'a, D>(
        &'a mut self,
        table: String,
        _db: &'a Arc<Database<D>>,
        _name: &'a str,
        _config: &'a Self::Config,
    ) -> impl Future<Output = io::Result<OperatorDirective>> + Send + 'a
    where
        D: crate::DispatchOperator,
    {
        async move {
            self.log.lock().push(format!("closed:{table}"));
            Ok(OperatorDirective::Continue)
        }
    }

    fn teardown<'a, D>(
        &'a mut self,
        _phase: &'a crate::OperatorPhase,
        _db: &'a Arc<Database<D>>,
        _name: &'a str,
        _config: &'a Self::Config,
    ) -> impl Future<Output = io::Result<()>> + Send + 'a
    where
        D: crate::DispatchOperator,
    {
        async move {
            self.log.lock().push("teardown".to_owned());
            Ok(())
        }
    }
}

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub(super) struct SpawnerConfig {
    child_tracker: String,
}

pub(super) struct SpawnerOperator;

impl Operator for SpawnerOperator {
    type Config = SpawnerConfig;
    type Timer = ();
    type Facet = ();

    fn create<'a, D>(
        db: &'a Arc<Database<D>>,
        _name: &'a str,
        config: &'a Self::Config,
    ) -> impl Future<Output = io::Result<Self>> + Send + 'a
    where
        D: crate::DispatchOperator,
        Self: Sized,
    {
        async move {
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
        }
    }

    fn facet(&self) {}

    fn process<'a, D>(
        &'a mut self,
        _changes: Vec<Change>,
        _db: &'a Arc<Database<D>>,
        _name: &'a str,
        _config: &'a Self::Config,
    ) -> impl Future<Output = io::Result<OperatorDirective>> + Send + 'a
    where
        D: crate::DispatchOperator,
    {
        async { Ok(OperatorDirective::Continue) }
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

pub(super) type TestDatabase = Database<test_operators::OperatorInstance>;

pub(super) fn counter_trackers() -> &'static Mutex<HashMap<String, Arc<AtomicUsize>>> {
    static TRACKERS: OnceLock<Mutex<HashMap<String, Arc<AtomicUsize>>>> = OnceLock::new();
    TRACKERS.get_or_init(|| Mutex::new(HashMap::new()))
}

pub(super) fn new_tracker(prefix: &str) -> (String, Arc<AtomicUsize>) {
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

pub(super) fn lookup_counter(key: &str) -> io::Result<Arc<AtomicUsize>> {
    counter_trackers().lock().get(key).cloned().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            format!("missing test counter tracker {key:?}"),
        )
    })
}

pub(super) type InputTracker = (Arc<Mutex<Vec<String>>>, Arc<Mutex<Vec<String>>>);

pub(super) fn input_trackers() -> &'static Mutex<HashMap<String, InputTracker>> {
    static TRACKERS: OnceLock<Mutex<HashMap<String, InputTracker>>> = OnceLock::new();
    TRACKERS.get_or_init(|| Mutex::new(HashMap::new()))
}

pub(super) fn new_input_tracker(
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

pub(super) fn lookup_input_tracker(key: &str) -> io::Result<InputTracker> {
    input_trackers().lock().get(key).cloned().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            format!("missing test input tracker {key:?}"),
        )
    })
}

pub(super) fn lifecycle_logs() -> &'static Mutex<HashMap<String, Arc<Mutex<Vec<String>>>>> {
    static TRACKERS: OnceLock<Mutex<HashMap<String, Arc<Mutex<Vec<String>>>>>> = OnceLock::new();
    TRACKERS.get_or_init(|| Mutex::new(HashMap::new()))
}

pub(super) fn new_lifecycle_log(prefix: &str) -> (String, Arc<Mutex<Vec<String>>>) {
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

pub(super) fn lookup_lifecycle_log(key: &str) -> io::Result<Arc<Mutex<Vec<String>>>> {
    lifecycle_logs().lock().get(key).cloned().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            format!("missing lifecycle log tracker {key:?}"),
        )
    })
}

pub(super) fn runtime_config(subscription: Subscription) -> OperatorRuntimeConfig {
    OperatorRuntimeConfig {
        subscriptions: vec![subscription],
        poll_size: 128,
    }
}

pub(super) fn counting_config(tracker: String, finish: bool) -> CountingConfig {
    CountingConfig { tracker, finish }
}

pub(super) fn counting_runtime_config(subscription: Subscription) -> OperatorRuntimeConfig {
    runtime_config(subscription)
}

pub(super) fn retry_config(attempts_tracker: String) -> FailingOperatorConfig {
    FailingOperatorConfig { attempts_tracker }
}

pub(super) fn retry_runtime_config() -> OperatorRuntimeConfig {
    runtime_config(Subscription::pattern("users"))
}

pub(super) fn timer_config(tracker: String) -> TimerOperatorConfig {
    TimerOperatorConfig { tracker }
}

pub(super) fn timer_runtime_config() -> OperatorRuntimeConfig {
    runtime_config(Subscription::pattern("users"))
}

pub(super) fn input_lifecycle_config(tracker: String) -> InputLifecycleConfig {
    InputLifecycleConfig { tracker }
}

pub(super) fn input_lifecycle_runtime_config(subscription: Subscription) -> OperatorRuntimeConfig {
    runtime_config(subscription)
}

pub(super) fn spawner_config(child_tracker: String) -> SpawnerConfig {
    SpawnerConfig { child_tracker }
}

pub(super) fn spawner_runtime_config() -> OperatorRuntimeConfig {
    runtime_config(Subscription::pattern("users"))
}

pub(super) fn shutdown_lifecycle_config(tracker: String) -> ShutdownLifecycleConfig {
    ShutdownLifecycleConfig { tracker }
}

pub(super) fn shutdown_lifecycle_runtime_config() -> OperatorRuntimeConfig {
    runtime_config(Subscription::pattern("users"))
}

pub(super) fn event(table: &str, value: i64, ms: u64) -> Event {
    Event {
        table_id: table.into(),
        primary_key: PrimaryKey::String("u1".into()),
        path: ValuePath::new(),
        op: Op::Replace {
            value: Value::Int(value),
        },
        hlc: hlc(ms),
        sync: false,
        signature: Vec::new(),
    }
}
