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
use parking_lot::RwLock;
use zendb_storage::{DurableStorage, ReadBackend, State, StateConfig, WriteBackend};
use zendb_types::InstallationId;

pub use handle::StateHandle;
use handle::StateKind;

use crate::{
    Error, Result,
    causal::CausalState,
    system::{
        CAUSAL_STATE_NAME, STATE_CATALOG_NAME, STATES_DIR, SYSTEM_STATE_CONFIG, is_system_state,
    },
};

/// State catalog management: owns the `_catalog` State and the in-memory
/// map of opened (erased) `State` handles. The typed catalog field and its
/// erased map entry share one `Arc<StateHandle<_, _>>`, so every catalog access is
/// synchronized by the same lock.
///
/// System states remain open for the workspace lifetime. Application states
/// are lazy: a state may be declared in `_catalog` but not currently open, and
/// `get` opens it on first access.
///
/// Exposes lifecycle operations (upsert/get/list/contains/close/delete),
/// durability barriers, and typed [`StateHandle`]s via [`States::get`]. State
/// reads and writes are performed through the handle, not on this handler.
pub struct States {
    root: PathBuf,
    catalog_handle: Arc<StateHandle<String, StateConfig>>,
    open_states: RwLock<HashMap<String, AnyStateHandle>>,
}

trait ErasedState: Any + Send + Sync {
    fn flush(&self) -> Result<()>;
    fn sync(&self) -> Result<()>;
}

impl<K, V> ErasedState for StateHandle<K, V>
where
    K: Encode + Decode<()> + Hash + Eq + Clone + Ord + Send + Sync + 'static,
    V: Encode + Decode<()> + Clone + Send + Sync + 'static,
{
    fn flush(&self) -> Result<()> {
        self.state.write().flush()?;
        Ok(())
    }

    fn sync(&self) -> Result<()> {
        self.state.write().sync()?;
        Ok(())
    }
}

type AnyStateHandle = Arc<dyn ErasedState>;

impl States {
    pub(crate) fn create(root: &Path) -> Result<Arc<Self>> {
        let root = root.join(STATES_DIR);
        fs::create_dir_all(&root)?;
        let mut catalog =
            State::create(&root.join(STATE_CATALOG_NAME), SYSTEM_STATE_CONFIG.clone())?;
        let causal_state = State::<InstallationId, CausalState>::create(
            &root.join(CAUSAL_STATE_NAME),
            SYSTEM_STATE_CONFIG.clone(),
        )?;
        // System declarations and handles are created together so catalog and
        // causal state are available before any application state is requested.
        catalog.put(STATE_CATALOG_NAME.to_owned(), SYSTEM_STATE_CONFIG.clone())?;
        catalog.put(CAUSAL_STATE_NAME.to_owned(), SYSTEM_STATE_CONFIG.clone())?;
        let catalog = Arc::new(StateHandle {
            name: STATE_CATALOG_NAME.to_owned(),
            state: RwLock::new(catalog),
            kind: StateKind::Catalog,
        });
        let erased_catalog: AnyStateHandle = catalog.clone();
        let causal = Arc::new(StateHandle {
            name: CAUSAL_STATE_NAME.to_owned(),
            state: RwLock::new(causal_state),
            kind: StateKind::Causal,
        });
        let erased_causal: AnyStateHandle = causal;
        let states = Arc::new(Self {
            root,
            catalog_handle: catalog,
            open_states: RwLock::new(HashMap::from([
                (STATE_CATALOG_NAME.to_owned(), erased_catalog),
                (CAUSAL_STATE_NAME.to_owned(), erased_causal),
            ])),
        });
        Ok(states)
    }

    pub(crate) fn open(root: &Path) -> Result<Arc<Self>> {
        let root = root.join(STATES_DIR);
        let catalog = Arc::new(StateHandle {
            name: STATE_CATALOG_NAME.to_owned(),
            state: RwLock::new(State::open(
                &root.join(STATE_CATALOG_NAME),
                SYSTEM_STATE_CONFIG.clone(),
            )?),
            kind: StateKind::Catalog,
        });
        let erased: AnyStateHandle = catalog.clone();
        let causal = Arc::new(StateHandle {
            name: CAUSAL_STATE_NAME.to_owned(),
            state: RwLock::new(State::<InstallationId, CausalState>::open(
                &root.join(CAUSAL_STATE_NAME),
                SYSTEM_STATE_CONFIG.clone(),
            )?),
            kind: StateKind::Causal,
        });
        let erased_causal: AnyStateHandle = causal;
        Ok(Arc::new(Self {
            root,
            catalog_handle: catalog,
            open_states: RwLock::new(HashMap::from([
                (STATE_CATALOG_NAME.to_owned(), erased),
                (CAUSAL_STATE_NAME.to_owned(), erased_causal),
            ])),
        }))
    }

    pub fn contains(&self, name: &str) -> bool {
        self.catalog_handle.read().contains(&name.to_owned())
    }

    pub fn list(&self) -> Vec<String> {
        self.catalog_handle
            .read()
            .keys()
            .map(|name| name.into_owned())
            .collect()
    }

    pub(crate) fn flush(&self) -> Result<()> {
        for state in self.open_states.read().values() {
            state.flush()?;
        }
        Ok(())
    }

    pub(crate) fn sync(&self) -> Result<()> {
        for state in self.open_states.read().values() {
            state.sync()?;
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
        let mut catalog = self.catalog_handle.write_internal();
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
        let mut open = self.open_states.write();
        if let Some(erased) = open.get(name).cloned() {
            let erased: Arc<dyn Any + Send + Sync> = erased;
            return Arc::downcast::<StateHandle<K, V>>(erased)
                .map_err(|_| Error::StateTypeMismatch(name.to_owned()));
        }
        // Not yet opened state handle
        let saved = self
            .catalog_handle
            .read()
            .get(&name.to_owned())
            .map(|value| value.into_owned())
            .ok_or_else(|| Error::StateNotFound(name.to_owned()))?;
        let path = self.root.join(name);
        // Keep the registry write lock through open and insertion so concurrent
        // callers cannot create two handles for the same declared state.
        let state = <State<K, V> as DurableStorage>::open(&path, saved)?;
        let handle = Arc::new(StateHandle {
            name: name.to_owned(),
            state: RwLock::new(state),
            kind: StateKind::Application,
        });
        let erased: AnyStateHandle = handle.clone();
        open.insert(name.to_owned(), erased);
        Ok(handle)
    }

    pub fn list_open(&self) -> Vec<String> {
        self.open_states.read().keys().cloned().collect()
    }

    pub fn close(&self, name: &str) -> bool {
        if is_system_state(name) {
            return false;
        }
        let mut open = self.open_states.write();
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
        let mut open = self.open_states.write();
        if open
            .get(name)
            .is_some_and(|state| Arc::strong_count(state) > 1)
        {
            return Err(Error::StateInUse(name.to_owned()));
        }
        open.remove(name);
        let mut catalog = self.catalog_handle.write_internal();
        if !catalog.delete(&name.to_owned())? {
            return Ok(false);
        }
        drop(catalog);
        drop(open);

        let path = self.root.join(name);
        // Remove the physical state only after all registry locks and the last
        // internal handle have been released.
        fs::remove_dir_all(path)?;
        Ok(true)
    }
}
