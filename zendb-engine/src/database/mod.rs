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

#[derive(Debug, Clone, Encode, Decode)]
pub struct DatabaseConfig {
    pub graceful_shutdown_max_duration: Duration,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            graceful_shutdown_max_duration: Duration::from_secs(7),
        }
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
                format!(
                    "table {:?} is unavailable because its database was dropped",
                    self.name
                ),
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
                format!(
                    "state {:?} is unavailable because its database was dropped",
                    self.name
                ),
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
        info!("creating database at {:?}", path);
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
        info!("opening database at {:?}", path);
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
        debug!("spawning background timer scheduler");
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

// Close table loop
impl<D> Drop for Database<D>
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

#[cfg(test)]
mod tests;
