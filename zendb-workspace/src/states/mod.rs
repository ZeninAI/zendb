//! State catalog management: typed State lifecycle and declarations.

mod handle;

use std::{
    any::Any,
    collections::HashMap,
    fs,
    hash::Hash,
    path::{Path, PathBuf},
    sync::Arc,
};

use bincode::{Decode, Encode};
use parking_lot::{Mutex, RwLock};
use zendb_storage::{DurableStorage, ReadBackend, State, StateConfig, WriteBackend};

pub use handle::StateHandle;
use handle::StateKind;

use crate::{
    Error, Result,
    system::{STATE_CATALOG_NAME, STATES_DIR, SYSTEM_STATE_CONFIG, is_system_state},
};

/// State catalog management: owns the `_catalog` State and the in-memory
/// map of opened (erased) `State` handles. The typed catalog field and its
/// erased map entry share one `Arc<StateHandle<_, _>>`, so every catalog access is
/// synchronized by the same lock.
///
/// The catalog remains open for the workspace lifetime. Application states are
/// lazy: a state may be declared in `_catalog` but not currently open, and
/// `get` opens it on first access. A separate lifecycle mutex serializes cold
/// storage operations without holding the open-state registry write lock
/// across filesystem I/O.
///
/// Exposes lifecycle operations (upsert/get/list/contains/close/delete),
/// durability barriers, and typed [`StateHandle`]s via [`States::get`]. State
/// reads and writes are performed through the handle, not on this handler.
pub struct States {
    root: PathBuf,
    catalog_handle: Arc<StateHandle<String, StateConfig>>,
    lifecycle: Mutex<()>,
    open_states: RwLock<HashMap<String, AnyStateHandle>>,
}

trait ErasedState: Any + Send + Sync {
    fn persist(&self, barrier: zendb_types::Barrier) -> Result<()>;
}

impl<K, V> ErasedState for StateHandle<K, V>
where
    K: Encode + Decode<()> + Hash + Eq + Clone + Ord + Send + Sync + 'static,
    V: Encode + Decode<()> + Clone + Send + Sync + 'static,
{
    fn persist(&self, barrier: zendb_types::Barrier) -> Result<()> {
        self.state.write().persist(barrier)?;
        Ok(())
    }
}

type AnyStateHandle = Arc<dyn ErasedState>;

impl States {
    pub(crate) fn create(root: &Path) -> Result<Self> {
        let root = root.join(STATES_DIR);
        fs::create_dir_all(&root)?;
        let mut catalog =
            State::create(&root.join(STATE_CATALOG_NAME), SYSTEM_STATE_CONFIG.clone())?;
        catalog.put(STATE_CATALOG_NAME.to_owned(), SYSTEM_STATE_CONFIG.clone())?;
        let catalog = Arc::new(StateHandle {
            name: STATE_CATALOG_NAME.to_owned(),
            state: RwLock::new(catalog),
            kind: StateKind::Catalog,
        });
        let erased_catalog: AnyStateHandle = catalog.clone();
        let states = Self {
            root,
            catalog_handle: catalog,
            lifecycle: Mutex::new(()),
            open_states: RwLock::new(HashMap::from([(
                STATE_CATALOG_NAME.to_owned(),
                erased_catalog,
            )])),
        };
        Ok(states)
    }

    pub(crate) fn open(root: &Path) -> Result<Self> {
        let root = root.join(STATES_DIR);
        let catalog = Arc::new(StateHandle {
            name: STATE_CATALOG_NAME.to_owned(),
            state: RwLock::new(State::open(
                &root.join(STATE_CATALOG_NAME),
                SYSTEM_STATE_CONFIG.clone(),
            )?),
            kind: StateKind::Catalog,
        });
        let erased_catalog: AnyStateHandle = catalog.clone();
        Ok(Self {
            root,
            catalog_handle: catalog,
            lifecycle: Mutex::new(()),
            open_states: RwLock::new(HashMap::from([(
                STATE_CATALOG_NAME.to_owned(),
                erased_catalog,
            )])),
        })
    }

    pub fn contains(&self, name: &str) -> bool {
        if self.open_states.read().contains_key(name) {
            return true;
        }
        self.catalog_handle.read().contains(&name.to_owned())
    }

    pub fn list(&self) -> Vec<String> {
        self.catalog_handle
            .read()
            .keys()
            .map(|name| name.into_owned())
            .collect()
    }

    pub(crate) fn persist(&self, barrier: zendb_types::Barrier) -> Result<()> {
        let _lifecycle = self.lifecycle.lock();
        let open: Vec<_> = self.open_states.read().values().cloned().collect();
        for state in open {
            state.persist(barrier)?;
        }
        Ok(())
    }

    /// Declare a new state or update its config. Creates the declaration and
    /// the physical state if it does not exist; if it already exists, updates
    /// the config only when it differs. Returns `true` if a change was made
    /// (created or config changed), `false` if the declaration was already
    /// present with the same config. The caller obtains a typed handle
    /// separately via [`States::get`]. Refuses system states.
    pub fn upsert(&self, name: &str, config: StateConfig) -> Result<bool> {
        if is_system_state(name) {
            return Err(Error::SystemStateReadOnly(name.to_owned()));
        }
        let _lifecycle = self.lifecycle.lock();
        let name = name.to_owned();
        let mut catalog = self.catalog_handle.write_internal();
        if let Some(existing) = catalog.get(&name).map(|v| v.into_owned()) {
            if existing == config {
                return Ok(false);
            }
            catalog.put(name, config)?;
            return Ok(true);
        }
        // Not yet declared: write the declaration and materialize the physical
        // state so a subsequent `get` only needs to open it.
        catalog.put(name.clone(), config.clone())?;
        drop(catalog);
        let path = self.root.join(&name);
        // Do not hold the catalog lock across filesystem I/O. The declaration
        // intentionally exists before materialization, so a failed create is
        // reported to the caller rather than silently repaired here.
        State::<(), ()>::create(&path, config)?;
        Ok(true)
    }

    /// Obtain a typed handle to an existing state. Returns the handle if the
    /// state is already open, or opens the physical state declared in
    /// `_catalog` and returns it. Returns an error if the state is not
    /// declared (neither open nor in the catalog). `get` never creates a state;
    /// declaration and physical creation are the job of [`States::upsert`].
    /// Returns handles to system states too.
    pub fn get<K, V>(&self, name: &str) -> Result<Arc<StateHandle<K, V>>>
    where
        K: Encode + Decode<()> + Hash + Eq + Clone + Ord + Send + Sync + 'static,
        V: Encode + Decode<()> + Clone + Send + Sync + 'static,
    {
        if let Some(erased) = self.open_states.read().get(name).cloned() {
            let erased: Arc<dyn Any + Send + Sync> = erased;
            return Arc::downcast::<StateHandle<K, V>>(erased)
                .map_err(|_| Error::StateTypeMismatch(name.to_owned()));
        }

        let _lifecycle = self.lifecycle.lock();
        if let Some(erased) = self.open_states.read().get(name).cloned() {
            let erased: Arc<dyn Any + Send + Sync> = erased;
            return Arc::downcast::<StateHandle<K, V>>(erased)
                .map_err(|_| Error::StateTypeMismatch(name.to_owned()));
        }
        // Lifecycle serialization prevents concurrent misses from opening
        // distinct handles while leaving unrelated registry reads unblocked.
        let name = name.to_owned();
        let saved = self
            .catalog_handle
            .read()
            .get(&name)
            .map(|value| value.into_owned())
            .ok_or_else(|| Error::StateNotFound(name.clone()))?;
        let path = self.root.join(&name);
        let state = <State<K, V> as DurableStorage>::open(&path, saved)?;
        let handle = Arc::new(StateHandle {
            name: name.clone(),
            state: RwLock::new(state),
            kind: StateKind::Application,
        });
        let erased: AnyStateHandle = handle.clone();
        self.open_states.write().insert(name, erased);
        Ok(handle)
    }

    pub fn contains_open(&self, name: &str) -> bool {
        self.open_states.read().contains_key(name)
    }

    pub fn list_open(&self) -> Vec<String> {
        self.open_states.read().keys().cloned().collect()
    }

    /// Close an open application state without deleting its declaration or
    /// physical storage. Returns `false` when the state is not open.
    pub fn close(&self, name: &str) -> Result<bool> {
        if is_system_state(name) {
            return Err(Error::SystemStateReadOnly(name.to_owned()));
        }
        let _lifecycle = self.lifecycle.lock();
        let mut open = self.open_states.write();
        if open
            .get(name)
            .is_some_and(|state| Arc::strong_count(state) > 1)
        {
            return Err(Error::StateInUse(name.to_owned()));
        }
        let removed = open.remove(name);
        drop(open);
        let was_open = removed.is_some();
        drop(removed);
        Ok(was_open)
    }

    pub fn delete(&self, name: &str) -> Result<bool> {
        if is_system_state(name) {
            return Err(Error::SystemStateReadOnly(name.to_owned()));
        }
        let _lifecycle = self.lifecycle.lock();
        let mut open = self.open_states.write();
        if open
            .get(name)
            .is_some_and(|state| Arc::strong_count(state) > 1)
        {
            return Err(Error::StateInUse(name.to_owned()));
        }
        let removed = open.remove(name);
        drop(open);
        drop(removed);
        let mut catalog = self.catalog_handle.write_internal();
        if !catalog.delete(&name.to_owned())? {
            return Ok(false);
        }
        drop(catalog);

        let path = self.root.join(name);
        if path.exists() {
            // Guard against in-memory state
            fs::remove_file(path)?;
        }
        Ok(true)
    }
}
