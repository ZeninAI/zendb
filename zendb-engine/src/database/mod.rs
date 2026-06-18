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
use zendb_storage::frontend::table::{Table as RawTable, TableConfig};

use crate::{
    computation::{
        state::ErasedState,
        worker::{ComputationInput, ComputationWorker},
        Computation, ComputationConfig, ComputationRegistry, StateKey, StateRef, StateValue,
        Subscription,
    },
    runtime::Executor,
};

use catalog::{Catalog, CatalogEntry};

const META_FILE: &str = "_meta";
const SHARED_STATES_DIR: &str = "_states";
const COMPUTATIONS_DIR: &str = "_computations";

#[derive(Debug, Clone, Encode, Decode)]
pub struct DatabaseConfig {
    pub computation_poll_size: usize,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            computation_poll_size: 128,
        }
    }
}

pub type Table = Arc<RwLock<RawTable>>;

pub(crate) struct DatabaseInner {
    path: PathBuf,
    pub(crate) config: DatabaseConfig,
    pub(crate) executor: Arc<dyn Executor>,
    registry: ComputationRegistry,
    pub(crate) lifecycle: Mutex<()>,
    catalog: Mutex<Catalog>,
    tables: RwLock<HashMap<String, Arc<RwLock<RawTable>>>>,
    shared_states: RwLock<HashMap<String, ErasedState>>,
    local_states: RwLock<HashMap<String, ErasedState>>,
    computations: RwLock<HashMap<String, Arc<ComputationWorker>>>,
}

#[derive(Clone)]
pub struct Database {
    inner: Arc<DatabaseInner>,
}

impl Database {
    pub fn create(
        path: &Path,
        executor: Arc<dyn Executor>,
        registry: ComputationRegistry,
        config: DatabaseConfig,
    ) -> io::Result<Self> {
        fs::create_dir_all(path)?;
        let catalog = Catalog::create(&path.join(META_FILE), KeyDirConfig::default())?;
        Ok(Self::from_parts(path, catalog, executor, registry, config))
    }

    pub fn open(
        path: &Path,
        executor: Arc<dyn Executor>,
        registry: ComputationRegistry,
        config: DatabaseConfig,
    ) -> io::Result<Self> {
        let catalog = Catalog::open(&path.join(META_FILE), KeyDirConfig::default())?;
        let database = Self::from_parts(path, catalog, executor, registry, config);
        database.open_catalog()?;
        Ok(database)
    }

    fn from_parts(
        path: &Path,
        catalog: Catalog,
        executor: Arc<dyn Executor>,
        registry: ComputationRegistry,
        mut config: DatabaseConfig,
    ) -> Self {
        config.computation_poll_size = config.computation_poll_size.max(1);
        Self {
            inner: Arc::new(DatabaseInner {
                path: path.to_path_buf(),
                config,
                executor,
                registry,
                lifecycle: Mutex::new(()),
                catalog: Mutex::new(catalog),
                tables: RwLock::new(HashMap::new()),
                shared_states: RwLock::new(HashMap::new()),
                local_states: RwLock::new(HashMap::new()),
                computations: RwLock::new(HashMap::new()),
            }),
        }
    }

    fn open_catalog(&self) -> io::Result<()> {
        let entries: Vec<_> = self
            .inner
            .catalog
            .lock()
            .entries()
            .map(|(name, entry)| (name.into_owned(), entry.into_owned()))
            .collect();

        for (name, entry) in &entries {
            if let CatalogEntry::Table(config) = entry {
                let table = RawTable::open(&self.inner.path.join(name), config.clone())?;
                self.inner
                    .tables
                    .write()
                    .insert(name.clone(), Arc::new(RwLock::new(table)));
            }
        }
        for (name, entry) in &entries {
            if let CatalogEntry::SharedState {
                implementation,
                config,
                ..
            } = entry
            {
                let state = self.inner.registry.open_state(
                    implementation,
                    &self.inner.path.join(SHARED_STATES_DIR).join(name),
                    config.clone(),
                )?;
                self.inner.shared_states.write().insert(name.clone(), state);
            }
        }
        for (name, entry) in entries {
            if let CatalogEntry::Computation(config) = entry {
                self.inner.open_local_states(&name, &config)?;
                let (slot, computation) = self.inner.build_slot(name.clone(), config)?;
                self.inner
                    .computations
                    .write()
                    .insert(name, Arc::clone(&slot));
                self.inner.spawn(slot, computation);
            }
        }
        Ok(())
    }

    pub fn table(&self, name: &str) -> io::Result<Table> {
        self.inner.table(name)
    }

    pub fn create_table(&self, name: &str, config: TableConfig) -> io::Result<Table> {
        let _lifecycle = self.inner.lifecycle.lock();
        if self.inner.catalog.lock().contains(&name.to_owned()) {
            return Err(already_exists("catalog resource", name));
        }

        let raw = RawTable::create(&self.inner.path.join(name), config.clone())?;
        let table = Arc::new(RwLock::new(raw));
        self.inner
            .catalog
            .lock()
            .put(name.to_owned(), CatalogEntry::Table(config))?;
        self.inner
            .tables
            .write()
            .insert(name.to_owned(), Arc::clone(&table));
        if let Err(error) = self.inner.attach_table_to_all_subscribers(name, &table) {
            self.inner.detach_table(name);
            self.inner.tables.write().remove(name);
            let _ = self.inner.catalog.lock().delete(&name.to_owned());
            drop(table);
            let _ = fs::remove_dir_all(self.inner.path.join(name));
            return Err(error);
        }
        Ok(table)
    }

    pub fn drop_table(&self, name: &str) -> io::Result<()> {
        let _lifecycle = self.inner.lifecycle.lock();
        if self.inner.computations.read().values().any(|slot| {
            slot.config
                .subscriptions
                .iter()
                .any(|subscription| subscription.matches(name))
        }) {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                format!("table {name:?} is subscribed to by a computation"),
            ));
        }
        let mut tables = self.inner.tables.write();
        let table = tables.get(name).ok_or_else(|| not_found("table", name))?;
        if Arc::strong_count(table) != 1 {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                format!("table {name:?} still has active handles"),
            ));
        }
        let table = tables.remove(name).unwrap();
        drop(tables);
        self.inner.catalog.lock().delete(&name.to_owned())?;
        drop(table);
        fs::remove_dir_all(self.inner.path.join(name))?;
        Ok(())
    }

    pub fn create_computation(&self, name: &str, config: ComputationConfig) -> io::Result<()> {
        let _lifecycle = self.inner.lifecycle.lock();
        if self.inner.catalog.lock().contains(&name.to_owned()) {
            return Err(already_exists("catalog resource", name));
        }

        if let Err(error) = self.inner.create_states(name, &config) {
            let _ = self.inner.remove_states(name, &config);
            return Err(error);
        }
        let (slot, computation) = match self.inner.build_slot(name.to_owned(), config.clone()) {
            Ok(slot) => slot,
            Err(error) => {
                let _ = self.inner.remove_states(name, &config);
                return Err(error);
            }
        };
        if let Err(error) = self
            .inner
            .catalog
            .lock()
            .put(name.to_owned(), CatalogEntry::Computation(config.clone()))
        {
            self.inner.remove_slot_consumers(&slot);
            let _ = self.inner.remove_states(name, &config);
            return Err(error);
        }
        self.inner
            .computations
            .write()
            .insert(name.to_owned(), Arc::clone(&slot));
        self.inner.spawn(slot, computation);
        Ok(())
    }

    pub fn drop_computation(&self, name: &str) -> io::Result<()> {
        let _lifecycle = self.inner.lifecycle.lock();
        self.inner.drop_computation_locked(name)
    }

    pub fn shared_state<K: StateKey, V: StateValue>(
        &self,
        name: &str,
    ) -> io::Result<StateRef<K, V>> {
        self.inner.shared_state(name)
    }
}

impl DatabaseInner {
    fn build_slot(
        self: &Arc<Self>,
        name: String,
        config: ComputationConfig,
    ) -> io::Result<(Arc<ComputationWorker>, Box<dyn Computation>)> {
        for subscription in &config.subscriptions {
            if let Subscription::Table(table) = subscription {
                if !self.tables.read().contains_key(table) {
                    return Err(not_found("subscribed table", table));
                }
            }
        }
        let instance = self
            .registry
            .create_computation(&config.implementation, &config.configuration)?;
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
                inputs.push(ComputationInput {
                    table_name: table_name.clone(),
                    reader,
                });
            }
        }
        Ok((ComputationWorker::new(name, config, inputs), instance))
    }

    fn spawn(self: &Arc<Self>, slot: Arc<ComputationWorker>, computation: Box<dyn Computation>) {
        ComputationWorker::spawn(self, slot, computation);
    }

    pub(crate) fn drop_computation_locked(&self, name: &str) -> io::Result<()> {
        let slot = self
            .computations
            .write()
            .remove(name)
            .ok_or_else(|| not_found("computation", name))?;
        slot.stop();
        self.remove_slot_consumers(&slot);
        self.remove_states(name, &slot.config)?;
        self.catalog.lock().delete(&name.to_owned())?;
        Ok(())
    }

    fn remove_slot_consumers(&self, slot: &ComputationWorker) {
        let inputs = std::mem::take(&mut *slot.inputs.lock());
        Self::delete_consumers(inputs, &slot.name);
    }

    fn delete_consumers(inputs: Vec<ComputationInput>, computation: &str) {
        for input in inputs {
            if let Err(error) = input.reader.delete() {
                log::error!(
                    "failed deleting consumer {computation:?} from table {:?}: {error}",
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
    use crate::{Change, ComputationContext, ComputationStatus, StateVisibility};
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
    use std::time::{Duration, Instant};
    use zendb_storage::frontend::state::StateConfig;
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

    struct CountingComputation {
        count: Arc<AtomicUsize>,
        finish: bool,
    }

    impl Computation for CountingComputation {
        fn process<'a>(
            &'a mut self,
            changes: Vec<Change>,
            context: ComputationContext,
        ) -> crate::BoxFuture<'a, io::Result<ComputationStatus>> {
            Box::pin(async move {
                self.count.fetch_add(changes.len(), Ordering::Relaxed);
                if let Ok(state) = context.shared_state("index") {
                    state.put(
                        b"count".to_vec(),
                        self.count.load(Ordering::Relaxed).to_le_bytes().to_vec(),
                    )?;
                }
                Ok(if self.finish {
                    ComputationStatus::Finish
                } else {
                    ComputationStatus::Continue
                })
            })
        }
    }

    struct FailingOnceComputation {
        attempts: Arc<AtomicUsize>,
        processed: Arc<AtomicUsize>,
    }

    impl Computation for FailingOnceComputation {
        fn process<'a>(
            &'a mut self,
            changes: Vec<Change>,
            _context: ComputationContext,
        ) -> crate::BoxFuture<'a, io::Result<ComputationStatus>> {
            Box::pin(async move {
                if self.attempts.fetch_add(1, Ordering::Relaxed) == 0 {
                    return Err(io::Error::other("expected failure"));
                }
                self.processed.fetch_add(changes.len(), Ordering::Relaxed);
                Ok(ComputationStatus::Continue)
            })
        }
    }

    fn registry(count: Arc<AtomicUsize>, finish: bool) -> ComputationRegistry {
        let mut registry = ComputationRegistry::new();
        registry.register_state::<Vec<u8>, Vec<u8>>("bytes");
        registry.register("count", move |_| {
            Ok(Box::new(CountingComputation {
                count: Arc::clone(&count),
                finish,
            }))
        });
        registry
    }

    fn config(states: Vec<crate::StateDefinition>) -> ComputationConfig {
        ComputationConfig {
            implementation: "count".into(),
            configuration: Vec::new(),
            subscriptions: vec![Subscription::Table("users".into())],
            states,
        }
    }

    fn event(value: i64, ms: u64) -> Event {
        init_device_id();
        Event {
            table_id: "users".into(),
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
    fn direct_table_writes_drive_computations() {
        let path = tmp("direct");
        let count = Arc::new(AtomicUsize::new(0));
        let db = Database::create(
            &path,
            Arc::new(ThreadExecutor),
            registry(Arc::clone(&count), false),
            DatabaseConfig::default(),
        )
        .unwrap();
        let table = db.create_table("users", TableConfig::default()).unwrap();
        db.create_computation("counter", config(Vec::new()))
            .unwrap();

        table.write().insert_event(event(1, 100)).unwrap();
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
    fn open_eagerly_opens_tables_and_starts_computations() {
        let path = tmp("reopen");
        {
            let db = Database::create(
                &path,
                Arc::new(ThreadExecutor),
                registry(Arc::new(AtomicUsize::new(0)), false),
                DatabaseConfig::default(),
            )
            .unwrap();
            db.create_table("users", TableConfig::default()).unwrap();
            db.create_computation("counter", config(Vec::new()))
                .unwrap();
        }

        let count = Arc::new(AtomicUsize::new(0));
        let db = Database::open(
            &path,
            Arc::new(ThreadExecutor),
            registry(Arc::clone(&count), false),
            DatabaseConfig::default(),
        )
        .unwrap();
        db.table("users")
            .unwrap()
            .write()
            .insert_event(event(1, 100))
            .unwrap();
        wait_until(|| count.load(Ordering::Relaxed) == 1);
    }

    #[test]
    fn finish_removes_consumers_catalog_and_owned_states() {
        let path = tmp("finish");
        let count = Arc::new(AtomicUsize::new(0));
        let db = Database::create(
            &path,
            Arc::new(ThreadExecutor),
            registry(Arc::clone(&count), true),
            DatabaseConfig::default(),
        )
        .unwrap();
        let table = db.create_table("users", TableConfig::default()).unwrap();
        db.create_computation(
            "counter",
            config(vec![
                crate::StateDefinition {
                    name: "buffer".into(),
                    visibility: StateVisibility::Local,
                    implementation: "bytes".into(),
                    config: StateConfig::default(),
                },
                crate::StateDefinition {
                    name: "index".into(),
                    visibility: StateVisibility::Shared,
                    implementation: "bytes".into(),
                    config: StateConfig::default(),
                },
            ]),
        )
        .unwrap();

        table.write().insert_event(event(1, 100)).unwrap();
        wait_until(|| count.load(Ordering::Relaxed) == 1);
        wait_until(|| db.shared_state::<Vec<u8>, Vec<u8>>("index").is_err());
        db.create_computation("counter", config(Vec::new()))
            .unwrap();
    }

    #[test]
    fn drop_table_requires_releasing_handles() {
        let path = tmp("drop_table");
        let db = Database::create(
            &path,
            Arc::new(ThreadExecutor),
            ComputationRegistry::new(),
            DatabaseConfig::default(),
        )
        .unwrap();
        let table = db.create_table("users", TableConfig::default()).unwrap();
        assert_eq!(
            db.drop_table("users").unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        assert!(path.join("users").exists());
        drop(table);
        db.drop_table("users").unwrap();
        assert!(!path.join("users").exists());
    }

    #[test]
    fn failed_process_resets_readers_to_committed_offsets() {
        let path = tmp("retry");
        let attempts = Arc::new(AtomicUsize::new(0));
        let processed = Arc::new(AtomicUsize::new(0));
        let mut registry = ComputationRegistry::new();
        let factory_attempts = Arc::clone(&attempts);
        let factory_processed = Arc::clone(&processed);
        registry.register("retry", move |_| {
            Ok(Box::new(FailingOnceComputation {
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
        let table = db.create_table("users", TableConfig::default()).unwrap();
        db.create_computation(
            "retry",
            ComputationConfig {
                implementation: "retry".into(),
                configuration: Vec::new(),
                subscriptions: vec![Subscription::Table("users".into())],
                states: Vec::new(),
            },
        )
        .unwrap();

        table.write().insert_event(event(1, 100)).unwrap();
        table.write().insert_event(event(2, 110)).unwrap();

        wait_until(|| attempts.load(Ordering::Relaxed) >= 2);
        wait_until(|| processed.load(Ordering::Relaxed) == 2);
    }

    #[test]
    fn shared_states_preserve_registered_key_and_value_types() {
        let path = tmp("typed_state");
        let mut registry = registry(Arc::new(AtomicUsize::new(0)), false);
        registry.register_state::<String, u64>("string-u64");
        let db = Database::create(
            &path,
            Arc::new(ThreadExecutor),
            registry,
            DatabaseConfig::default(),
        )
        .unwrap();
        db.create_table("users", TableConfig::default()).unwrap();
        db.create_computation(
            "counter",
            config(vec![crate::StateDefinition {
                name: "totals".into(),
                visibility: StateVisibility::Shared,
                implementation: "string-u64".into(),
                config: StateConfig::default(),
            }]),
        )
        .unwrap();

        let totals = db.shared_state::<String, u64>("totals").unwrap();
        totals.put("users".into(), 42).unwrap();
        assert_eq!(totals.get(&"users".into()).unwrap(), Some(42));
        assert!(db.shared_state::<u64, u64>("totals").is_err());
    }

    #[test]
    fn typed_shared_states_reopen_from_catalog() {
        let path = tmp("typed_state_reopen");
        {
            let mut registry = registry(Arc::new(AtomicUsize::new(0)), false);
            registry.register_state::<String, u64>("string-u64");
            let db = Database::create(
                &path,
                Arc::new(ThreadExecutor),
                registry,
                DatabaseConfig::default(),
            )
            .unwrap();
            db.create_table("users", TableConfig::default()).unwrap();
            db.create_computation(
                "counter",
                config(vec![crate::StateDefinition {
                    name: "totals".into(),
                    visibility: StateVisibility::Shared,
                    implementation: "string-u64".into(),
                    config: StateConfig::default(),
                }]),
            )
            .unwrap();
            db.shared_state::<String, u64>("totals")
                .unwrap()
                .put("users".into(), 42)
                .unwrap();
        }

        let mut registry = registry(Arc::new(AtomicUsize::new(0)), false);
        registry.register_state::<String, u64>("string-u64");
        let db = Database::open(
            &path,
            Arc::new(ThreadExecutor),
            registry,
            DatabaseConfig::default(),
        )
        .unwrap();
        assert_eq!(
            db.shared_state::<String, u64>("totals")
                .unwrap()
                .get(&"users".into())
                .unwrap(),
            Some(42)
        );
    }
}
