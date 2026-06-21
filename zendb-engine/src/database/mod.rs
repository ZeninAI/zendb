//! Eager database lifecycle and resource ownership.

mod catalog;
mod states;
mod tables;

use std::{
    fs, io,
    path::{Path, PathBuf},
    sync::Arc,
};

use bincode::{Decode, Encode};
use hashbrown::HashMap;
use parking_lot::{Mutex, RwLock};
use zendb_storage::core::{
    keydir::KeyDirConfig,
    traits::{Backend, DurableStorage},
};
use zendb_storage::frontend::{
    state::StateConfig,
    table::{Table, TableConfig},
};

use crate::{
    operator::{
        worker::{OperatorInput, OperatorWorker},
        ConcurrentState, Operator, OperatorConfig, OperatorRegistry, StateKey, StateValue,
        Subscription,
    },
    runtime::Executor,
};

use catalog::{Catalog, CatalogEntry};
use states::ErasedStateHandle;

const META_FILE: &str = "_meta";
pub(crate) const TABLES_DIR: &str = "tables";
pub(crate) const STATES_DIR: &str = "states";

#[derive(Debug, Clone, Encode, Decode)]
pub struct DatabaseConfig {
    pub operator_poll_size: usize,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            operator_poll_size: 128,
        }
    }
}

pub type ConcurrentTable = Arc<RwLock<Table>>;

pub(crate) struct DatabaseInner {
    path: PathBuf,
    pub(crate) config: DatabaseConfig,
    pub(crate) executor: Arc<dyn Executor>,
    registry: OperatorRegistry,
    pub(crate) lifecycle: Mutex<()>,
    catalog: Mutex<Catalog>,
    tables: RwLock<HashMap<String, ConcurrentTable>>,
    states: RwLock<HashMap<String, ErasedStateHandle>>,
    operators: RwLock<HashMap<String, Arc<OperatorWorker>>>,
}

pub struct Database {
    pub(crate) inner: Arc<DatabaseInner>,
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
        Ok(Self::from_parts(path, catalog, executor, registry, config))
    }

    pub fn open(
        path: &Path,
        executor: Arc<dyn Executor>,
        registry: OperatorRegistry,
        config: DatabaseConfig,
    ) -> io::Result<Arc<Self>> {
        let catalog = Catalog::open(&path.join(META_FILE), KeyDirConfig::default())?;
        let database = Self::from_parts(path, catalog, executor, registry, config);
        database.open_catalog()?;
        Ok(database)
    }

    fn from_parts(
        path: &Path,
        catalog: Catalog,
        executor: Arc<dyn Executor>,
        registry: OperatorRegistry,
        mut config: DatabaseConfig,
    ) -> Arc<Self> {
        config.operator_poll_size = config.operator_poll_size.max(1);
        Arc::new(Self {
            inner: Arc::new(DatabaseInner {
                path: path.to_path_buf(),
                config,
                executor,
                registry,
                lifecycle: Mutex::new(()),
                catalog: Mutex::new(catalog),
                tables: RwLock::new(HashMap::new()),
                states: RwLock::new(HashMap::new()),
                operators: RwLock::new(HashMap::new()),
            }),
        })
    }

    fn open_catalog(self: &Arc<Self>) -> io::Result<()> {
        let entries: Vec<_> = self
            .inner
            .catalog
            .lock()
            .entries()
            .map(|(name, entry)| (name.into_owned(), entry.into_owned()))
            .collect();

        for (name, entry) in &entries {
            if let CatalogEntry::Table(config) = entry {
                let table = Arc::new(RwLock::new(Table::open(
                    &self.inner.path.join(TABLES_DIR).join(name),
                    config.clone(),
                )?));
                self.inner.tables.write().insert(name.clone(), table);
            }
        }
        for (name, entry) in entries {
            if let CatalogEntry::Operator(config) = entry {
                let (worker, operator) = self.inner.build_worker(name.clone(), config)?;
                self.inner
                    .operators
                    .write()
                    .insert(name, Arc::clone(&worker));
                self.spawn(worker, operator);
            }
        }
        Ok(())
    }

    pub fn table(
        self: &Arc<Self>,
        name: &str,
        config: Option<TableConfig>,
    ) -> io::Result<ConcurrentTable> {
        if config.is_some() {
            let _lifecycle = self.inner.lifecycle.lock();
            return self.inner.table(name, config);
        }
        self.inner.table(name, None)
    }

    pub fn drop_table(self: &Arc<Self>, name: &str) -> io::Result<()> {
        let orphaned = {
            let _lifecycle = self.inner.lifecycle.lock();
            self.inner.prepare_drop_table(name)?
        };
        for worker in orphaned {
            worker.wait_finished();
        }

        let _lifecycle = self.inner.lifecycle.lock();
        self.inner.finish_drop_table(name)
    }

    pub fn state<K: StateKey, V: StateValue>(
        self: &Arc<Self>,
        name: &str,
        config: Option<StateConfig>,
    ) -> io::Result<ConcurrentState<K, V>> {
        if config.is_some() {
            let _lifecycle = self.inner.lifecycle.lock();
            return self.inner.state(name, config);
        }
        self.inner.state(name, None)
    }

    pub fn drop_state(self: &Arc<Self>, name: &str) -> io::Result<()> {
        let _lifecycle = self.inner.lifecycle.lock();
        self.inner.drop_state(name)
    }

    pub fn register_operator(
        self: &Arc<Self>,
        name: &str,
        config: OperatorConfig,
    ) -> io::Result<()> {
        let _lifecycle = self.inner.lifecycle.lock();
        if self.inner.catalog.lock().contains(&name.to_owned()) {
            return Err(already_exists("catalog resource", name));
        }

        let (worker, operator) = self.inner.build_worker(name.to_owned(), config.clone())?;
        if let Err(error) = self
            .inner
            .catalog
            .lock()
            .put(name.to_owned(), CatalogEntry::Operator(config.clone()))
        {
            self.inner.remove_worker_consumers(&worker);
            return Err(error);
        }
        self.inner
            .operators
            .write()
            .insert(name.to_owned(), Arc::clone(&worker));
        self.spawn(worker, operator);
        Ok(())
    }

    pub fn drop_operator(self: &Arc<Self>, name: &str) -> io::Result<()> {
        let worker = {
            let _lifecycle = self.inner.lifecycle.lock();
            self.inner
                .retire_operator_locked(name)?
                .ok_or_else(|| not_found("operator", name))?
        };
        worker.wait_finished();
        Ok(())
    }

    fn spawn(self: &Arc<Self>, worker: Arc<OperatorWorker>, operator: Box<dyn Operator>) {
        OperatorWorker::spawn(self, worker, operator);
    }
}

impl DatabaseInner {
    fn build_worker(
        self: &Arc<Self>,
        name: String,
        config: OperatorConfig,
    ) -> io::Result<(Arc<OperatorWorker>, Box<dyn Operator>)> {
        for subscription in &config.subscriptions {
            if let Subscription::Table(table) = subscription {
                if self.table(table, None).is_err() {
                    return Err(not_found("subscribed table", table));
                }
            }
        }

        let instance = self
            .registry
            .create_operator(&config.implementation, &config.configuration)?;
        let mut inputs = Vec::new();
        for (table_name, table) in self.tables.read().iter() {
            if config
                .subscriptions
                .iter()
                .any(|subscription| subscription.matches(table_name))
            {
                let reader = match table.read().consumer(&name) {
                    Ok(reader) => reader,
                    Err(error) => {
                        Self::delete_consumers(inputs, &name);
                        return Err(error);
                    }
                };
                inputs.push(OperatorInput {
                    table_name: table_name.clone(),
                    reader,
                });
            }
        }
        Ok((OperatorWorker::new(name, config, inputs), instance))
    }

    pub(crate) fn finish_operator_locked(&self, name: &str) -> io::Result<()> {
        self.retire_operator_locked(name).map(|_| ())
    }

    pub(crate) fn retire_operator_locked(
        &self,
        name: &str,
    ) -> io::Result<Option<Arc<OperatorWorker>>> {
        let worker = self.operators.write().remove(name);
        if let Some(worker) = &worker {
            worker.stop();
            self.remove_worker_consumers(worker);
        }

        let mut catalog = self.catalog.lock();
        match catalog.get(&name.to_owned()) {
            Some(entry) if matches!(entry.as_ref(), CatalogEntry::Operator(_)) => {
                catalog.delete(&name.to_owned())?;
            }
            Some(_) => return Err(already_exists("catalog resource", name)),
            None => {}
        }

        Ok(worker)
    }

    fn remove_worker_consumers(&self, worker: &OperatorWorker) {
        let inputs = std::mem::take(&mut *worker.inputs.lock());
        Self::delete_consumers(inputs, &worker.name);
    }

    fn delete_consumers(inputs: Vec<OperatorInput>, operator: &str) {
        for input in inputs {
            if let Err(error) = input.reader.delete() {
                log::error!(
                    "failed deleting consumer {operator:?} from table {:?}: {error}",
                    input.table_name
                );
            }
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Change, ConcurrentState, Operator, OperatorRegistry, OperatorStatus};
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
    use std::sync::Weak;
    use std::time::{Duration, Instant};
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
        buffer: Option<ConcurrentState<String, u64>>,
        index: Option<ConcurrentState<Vec<u8>, Vec<u8>>>,
        output: Option<ConcurrentTable>,
    }

    impl Operator for CountingOperator {
        fn open<'a>(
            &'a mut self,
            database: Weak<Database>,
        ) -> crate::BoxFuture<'a, io::Result<()>> {
            Box::pin(async move {
                let database = database.upgrade().ok_or_else(|| {
                    io::Error::new(io::ErrorKind::NotConnected, "database is closed")
                })?;
                self.buffer = Some(database.state("counter/buffer", Some(StateConfig::default()))?);
                self.index = Some(database.state("index", Some(StateConfig::default()))?);
                self.output = Some(database.table("users", None)?);
                Ok(())
            })
        }

        fn process<'a>(
            &'a mut self,
            changes: Vec<Change>,
            _database: Weak<Database>,
        ) -> crate::BoxFuture<'a, io::Result<OperatorStatus>> {
            Box::pin(async move {
                self.count.fetch_add(changes.len(), Ordering::Relaxed);
                if let Some(state) = &self.index {
                    state.write().put(
                        b"count".to_vec(),
                        self.count.load(Ordering::Relaxed).to_le_bytes().to_vec(),
                    )?;
                }
                if let Some(state) = &self.buffer {
                    let key = "count".to_owned();
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

        fn finish<'a>(
            &'a mut self,
            database: Weak<Database>,
        ) -> crate::BoxFuture<'a, io::Result<()>> {
            Box::pin(async move {
                self.buffer = None;
                self.index = None;
                self.output = None;
                if let Some(database) = database.upgrade() {
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
        fn process<'a>(
            &'a mut self,
            changes: Vec<Change>,
            _database: Weak<Database>,
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
        fn process<'a>(
            &'a mut self,
            changes: Vec<Change>,
            _database: Weak<Database>,
        ) -> crate::BoxFuture<'a, io::Result<OperatorStatus>> {
            Box::pin(async move {
                self.processed.fetch_add(changes.len(), Ordering::Relaxed);
                Ok(OperatorStatus::Continue)
            })
        }
    }

    fn registry(count: Arc<AtomicUsize>, finish: bool) -> OperatorRegistry {
        let mut registry = OperatorRegistry::new();
        registry.register("count", move |_| {
            Ok(Box::new(CountingOperator {
                count: Arc::clone(&count),
                finish,
                buffer: None,
                index: None,
                output: None,
            }))
        });
        registry
    }

    fn config() -> OperatorConfig {
        OperatorConfig {
            implementation: "count".into(),
            configuration: Vec::new(),
            subscriptions: vec![Subscription::Table("users".into())],
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

        table.write().insert_event(event("users", 1, 100)).unwrap();
        wait_until(|| count.load(Ordering::Relaxed) == 1);
        assert_eq!(
            table
                .read()
                .get(&PrimaryKey::String("u1".into()))
                .unwrap()
                .value,
            Some(Value::Int(1))
        );
    }

    #[test]
    fn open_eagerly_opens_tables_and_starts_operators() {
        let path = tmp("reopen");
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
        }

        let count = Arc::new(AtomicUsize::new(0));
        let db = Database::open(
            &path,
            Arc::new(ThreadExecutor),
            registry(Arc::clone(&count), false),
            DatabaseConfig::default(),
        )
        .unwrap();
        db.table("users", None)
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

        table.write().insert_event(event("users", 1, 100)).unwrap();
        wait_until(|| count.load(Ordering::Relaxed) == 1);
        wait_until(|| !db.inner.operators.read().contains_key("counter"));
        assert!(matches!(
            db.state::<Vec<u8>, Vec<u8>>("index", None),
            Err(error) if error.kind() == io::ErrorKind::NotFound
        ));
        wait_until(|| db.register_operator("counter", config()).is_ok());
    }

    #[test]
    fn drop_table_requires_releasing_handles() {
        let path = tmp("drop_table");
        let db = Database::create(
            &path,
            Arc::new(ThreadExecutor),
            OperatorRegistry::new(),
            DatabaseConfig::default(),
        )
        .unwrap();
        let table = db.table("users", Some(TableConfig::default())).unwrap();
        assert_eq!(
            db.drop_table("users").unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        assert!(path.join(TABLES_DIR).join("users").exists());
        drop(table);
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

        table.write().insert_event(event("users", 1, 100)).unwrap();
        wait_until(|| count.load(Ordering::Relaxed) == 1);
        drop(table);

        db.drop_table("users").unwrap();

        assert!(!db.inner.operators.read().contains_key("counter"));
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
        registry.register("multi", move |_| {
            Ok(Box::new(MultiTableOperator {
                processed: Arc::clone(&factory_processed),
            }))
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
                    Subscription::Table("users".into()),
                    Subscription::Table("posts".into()),
                ],
            },
        )
        .unwrap();

        drop(users);
        db.drop_table("users").unwrap();

        assert!(db.inner.operators.read().contains_key("multi"));
        posts.write().insert_event(event("posts", 1, 100)).unwrap();
        wait_until(|| processed.load(Ordering::Relaxed) == 1);
    }

    #[test]
    fn drop_state_requires_releasing_handles() {
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
        assert_eq!(
            db.drop_state("counter/buffer").unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        drop(state);
        db.drop_state("counter/buffer").unwrap();
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
        registry.register("retry", move |_| {
            Ok(Box::new(FailingOnceOperator {
                attempts: Arc::clone(&factory_attempts),
                processed: Arc::clone(&factory_processed),
            }))
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
                subscriptions: vec![Subscription::Table("users".into())],
            },
        )
        .unwrap();

        table.write().insert_event(event("users", 1, 100)).unwrap();
        table.write().insert_event(event("users", 2, 110)).unwrap();

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
            .write()
            .put(b"users".to_vec(), 42_u64.to_le_bytes().to_vec())
            .unwrap();
        assert_eq!(
            index
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
                .read()
                .get(&b"users".to_vec())
                .map(|value| value.into_owned()),
            Some(42_u64.to_le_bytes().to_vec())
        );
    }
}
