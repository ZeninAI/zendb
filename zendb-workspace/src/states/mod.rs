//! State catalog management: typed State lifecycle and declarations.

mod runtime;

use std::{
    any::Any,
    collections::HashMap,
    fs,
    hash::Hash,
    path::{Path, PathBuf},
    sync::Arc,
};

use bincode::{Decode, Encode};
use parking_lot::RwLock;
use zendb_storage::{DurableStorage, ReadBackend, State, StateConfig, WriteBackend};

pub use runtime::StateHandle;

use crate::{
    consts::{is_system_state, STATES_DIR, STATE_CATALOG_NAME, SYSTEM_STATE_CONFIG},
    Error, Result,
};

/// State catalog management: owns the `_state_catalog` State and the in-memory
/// map of opened (erased) `State` handles. The typed catalog field and its
/// erased map entry share one `Arc<StateHandle<_, _>>`, so every catalog access is
/// synchronized by the same lock.
///
/// States are **lazy**: a state may be declared in `_state_catalog` but not
/// currently open. `get` lazy-loads on first access.
///
/// Exposes lifecycle operations (upsert/get/list/contains/close/delete) and
/// returns typed [`StateHandle`]s via [`States::get`]. State reads and writes are
/// performed through the handle, not on this handler.
pub struct States {
    root: PathBuf,
    catalog: Arc<StateHandle<String, StateConfig>>,
    states: RwLock<HashMap<String, ErasedStateHandle>>,
}

type ErasedStateHandle = Arc<dyn Any + Send + Sync>;

impl States {
    pub(crate) fn create(root: &Path) -> Result<Arc<Self>> {
        let root = root.join(STATES_DIR);
        fs::create_dir_all(&root)?;
        let config = SYSTEM_STATE_CONFIG.clone();
        let mut catalog = State::create(&root.join(STATE_CATALOG_NAME), config.clone())?;
        catalog.put(STATE_CATALOG_NAME.to_owned(), config)?;
        let catalog = Arc::new(StateHandle {
            name: STATE_CATALOG_NAME.to_owned(),
            state: RwLock::new(catalog),
            is_system: true,
        });
        let erased: ErasedStateHandle = catalog.clone();
        Ok(Arc::new(Self {
            root,
            catalog,
            states: RwLock::new(HashMap::from([(STATE_CATALOG_NAME.to_owned(), erased)])),
        }))
    }

    pub(crate) fn open(root: &Path) -> Result<Arc<Self>> {
        let root = root.join(STATES_DIR);
        let catalog = Arc::new(StateHandle {
            name: STATE_CATALOG_NAME.to_owned(),
            state: RwLock::new(State::open(
                &root.join(STATE_CATALOG_NAME),
                SYSTEM_STATE_CONFIG.clone(),
            )?),
            is_system: true,
        });
        let erased: ErasedStateHandle = catalog.clone();
        Ok(Arc::new(Self {
            root,
            catalog,
            states: RwLock::new(HashMap::from([(STATE_CATALOG_NAME.to_owned(), erased)])),
        }))
    }

    pub fn contains(&self, name: &str) -> bool {
        self.catalog.read().contains(&name.to_owned())
    }

    pub fn list(&self) -> Vec<String> {
        self.catalog
            .read()
            .keys()
            .map(|name| name.into_owned())
            .collect()
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
        self.upsert_internal(name, config)
    }

    /// Workspace mutation path used to bootstrap system-state declarations.
    pub(crate) fn upsert_internal(&self, name: &str, config: StateConfig) -> Result<bool> {
        let mut catalog = self.catalog.write_internal();
        if let Some(existing) = catalog.get(&name.to_owned()).map(|v| v.into_owned()) {
            if existing == config {
                return Ok(false);
            }
            catalog.put(name.to_owned(), config)?;
            return Ok(true);
        }
        // Not yet declared: write the declaration and materialize the physical
        // state so a subsequent `get` only needs to open it.
        catalog.put(name.to_owned(), config.clone())?;
        drop(catalog);
        let path = self.root.join(name);
        if !path.exists() {
            State::<(), ()>::create(&path, config)?;
        }
        Ok(true)
    }

    /// Obtain a typed handle to an existing state. Returns the handle if the
    /// state is already open, or opens the physical state declared in
    /// `_state_catalog` and returns it. Returns an error if the state is not
    /// declared (neither open nor in the catalog). `get` never creates a state;
    /// declaration and physical creation are the job of [`States::upsert`].
    /// Returns handles to system states too.
    pub fn get<K, V>(&self, name: &str) -> Result<Arc<StateHandle<K, V>>>
    where
        K: Encode + Decode<()> + Hash + Eq + Clone + Ord + Send + Sync + 'static,
        V: Encode + Decode<()> + Clone + Send + Sync + 'static,
    {
        let mut open = self.states.write();
        if let Some(erased) = open.get(name).cloned() {
            return Arc::downcast::<StateHandle<K, V>>(erased)
                .map_err(|_| Error::TypeMismatch(name.to_owned()));
        }

        let saved = self
            .catalog
            .read()
            .get(&name.to_owned())
            .map(|value| value.into_owned())
            .ok_or_else(|| Error::NotFound(name.to_owned()))?;
        let path = self.root.join(name);
        let state = <State<K, V> as DurableStorage>::open(&path, saved)?;
        let handle = Arc::new(StateHandle {
            name: name.to_owned(),
            state: RwLock::new(state),
            is_system: is_system_state(name),
        });
        let erased: ErasedStateHandle = handle.clone();
        open.insert(name.to_owned(), erased);
        Ok(handle)
    }

    pub fn list_open(&self) -> Vec<String> {
        self.states.read().keys().cloned().collect()
    }

    pub fn close(&self, name: &str) -> bool {
        let mut open = self.states.write();
        if open
            .get(name)
            .is_none_or(|state| Arc::strong_count(state) != 1)
        {
            return false;
        }
        open.remove(name);
        true
    }

    pub fn delete(&self, name: &str) -> Result<bool> {
        if is_system_state(name) {
            return Err(Error::SystemStateReadOnly(name.to_owned()));
        }
        let mut open = self.states.write();
        if open
            .get(name)
            .is_some_and(|state| Arc::strong_count(state) > 1)
        {
            return Err(Error::ResourceBusy(name.to_owned()));
        }
        open.remove(name);
        let mut catalog = self.catalog.write_internal();
        if !catalog.delete(&name.to_owned())? {
            return Ok(false);
        }
        drop(catalog);
        drop(open);

        let path = self.root.join(name);
        if path.exists() {
            fs::remove_dir_all(path)?;
        }
        Ok(true)
    }
}
