//! State catalog management: typed State lifecycle and declarations.

use std::{
    any::Any,
    collections::HashMap,
    fs,
    hash::Hash,
    path::{Path, PathBuf},
    sync::Arc,
};

use bincode::{Decode, Encode};
use parking_lot::{Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};
use zendb_storage::{DurableStorage, KeyDirConfig, ReadBackend, State, StateConfig, WriteBackend};

use crate::{Error, Result};

pub const STATE_CATALOG_NAME: &str = "_state_catalog";
pub const PEER_STATE_NAME: &str = "_peer_state";

pub(crate) fn is_system_state(name: &str) -> bool {
    matches!(name, STATE_CATALOG_NAME | PEER_STATE_NAME)
}

/// Public handler for state catalog management.
///
/// Exposes lifecycle operations (create/open/list/contains/close/delete) and
/// returns typed [`StateHandle`]s via [`States::open`]. State reads and writes
/// are performed through the handle, not on this handler.
#[derive(Clone)]
pub struct States {
    core: Arc<StatesCore>,
}

impl States {
    pub(crate) fn new(core: Arc<StatesCore>) -> Self {
        Self { core }
    }

    pub fn contains(&self, name: &str) -> bool {
        !is_system_state(name) && self.core.contains(name)
    }

    pub fn list(&self) -> Vec<String> {
        self.core
            .list()
            .into_iter()
            .filter(|name| !is_system_state(name))
            .collect()
    }

    /// Declare and physically create a new state. Void-returning: the caller
    /// obtains a typed handle separately via [`States::open`]. Refuses system
    /// states and already-declared states.
    pub fn create(&self, name: &str, config: StateConfig) -> Result<()> {
        if is_system_state(name) {
            return Err(Error::NotFound(name.to_owned()));
        }
        self.core.declare(name, config)
    }

    /// Obtain a typed handle to an existing state. Lazy-loads: if the state
    /// is already in the open map, downcast and return; otherwise read the
    /// declaration from `_state_catalog`, physically open the state, insert
    /// into the open map, and return the typed handle. Returns handles to
    /// system states too; system handles refuse `write()` (see
    /// [`StateHandle::is_system`]).
    pub fn open<K, V>(&self, name: &str) -> Result<StateHandle<K, V>>
    where
        K: Encode + Decode<()> + Hash + Eq + Clone + Ord + Send + Sync + 'static,
        V: Encode + Decode<()> + Clone + Send + Sync + 'static,
    {
        self.core.open_state::<K, V>(name)
    }

    pub fn list_open(&self) -> Vec<String> {
        self.core
            .list_open()
            .into_iter()
            .filter(|name| !is_system_state(name))
            .collect()
    }

    pub fn config(&self, name: &str) -> Option<StateConfig> {
        (!is_system_state(name))
            .then(|| self.core.config(name))
            .flatten()
    }

    pub fn close(&self, name: &str) -> bool {
        !is_system_state(name) && self.core.close(name)
    }

    pub fn delete(&self, name: &str) -> Result<bool> {
        if is_system_state(name) {
            return Err(Error::ResourceBusy(name.to_owned()));
        }
        self.core.delete(name)
    }
}

/// A typed handle to an open [`State`]. Reads go through [`StateHandle::read`];
/// writes go through [`StateHandle::write`], which refuses system states.
pub struct StateHandle<K: Ord, V> {
    name: String,
    state: Arc<RwLock<State<K, V>>>,
    is_system: bool,
}

impl<K: Ord, V> Clone for StateHandle<K, V> {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            state: self.state.clone(),
            is_system: self.is_system,
        }
    }
}

impl<K: Ord, V> StateHandle<K, V> {
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns `true` if this handle refers to a system state.
    ///
    /// System states are openable for reads but cannot be written through
    /// this handle; the workspace maintains them internally.
    pub fn is_system(&self) -> bool {
        self.is_system
    }

    pub fn read(&self) -> RwLockReadGuard<'_, State<K, V>> {
        self.state.read()
    }

    pub fn write(&self) -> Result<RwLockWriteGuard<'_, State<K, V>>> {
        if self.is_system {
            return Err(Error::PermissionDenied);
        }
        Ok(self.state.write())
    }
}

type ErasedState = Arc<dyn Any + Send + Sync>;

struct StatesCoreInner {
    declarations: State<String, StateConfig>,
    open: HashMap<String, ErasedState>,
}

pub(crate) struct StatesCore {
    states_root: PathBuf,
    inner: Mutex<StatesCoreInner>,
}

impl StatesCore {
    pub(crate) fn create(root: &Path) -> Result<Arc<Self>> {
        let states_root = root.join("states");
        fs::create_dir_all(&states_root)?;
        let config = catalog_config();
        let mut declarations =
            State::create(&states_root.join(STATE_CATALOG_NAME), config.clone())?;
        declarations.put(STATE_CATALOG_NAME.to_owned(), config)?;
        declarations.sync()?;
        Ok(Arc::new(Self {
            states_root,
            inner: Mutex::new(StatesCoreInner {
                declarations,
                open: HashMap::new(),
            }),
        }))
    }

    pub(crate) fn open(root: &Path) -> Result<Arc<Self>> {
        let states_root = root.join("states");
        Ok(Arc::new(Self {
            inner: Mutex::new(StatesCoreInner {
                declarations: State::open(&states_root.join(STATE_CATALOG_NAME), catalog_config())?,
                open: HashMap::new(),
            }),
            states_root,
        }))
    }

    /// Declare a new state in `_state_catalog`. Void-returning. Does NOT
    /// physically create the state — the physical state is created on first
    /// `open_state` call, when the concrete `K`, `V` types are known.
    fn declare(&self, name: &str, config: StateConfig) -> Result<()> {
        let mut inner = self.inner.lock();
        if inner.declarations.contains(&name.to_owned()) {
            return Err(Error::AlreadyExists(name.to_owned()));
        }
        inner.declarations.put(name.to_owned(), config)?;
        inner.declarations.sync()?;
        Ok(())
    }

    /// Obtain a typed handle. Lazy-loads: if the state is already open,
    /// downcast and return; otherwise read the declaration, physically create
    /// or open the state, insert into the open map, and return.
    fn open_state<K, V>(&self, name: &str) -> Result<StateHandle<K, V>>
    where
        K: Encode + Decode<()> + Hash + Eq + Clone + Ord + Send + Sync + 'static,
        V: Encode + Decode<()> + Clone + Send + Sync + 'static,
    {
        let mut inner = self.inner.lock();
        if let Some(erased) = inner.open.get(name).cloned() {
            return state_handle(name, erased, is_system_state(name));
        }

        let saved = inner
            .declarations
            .get(&name.to_owned())
            .map(|value| value.into_owned())
            .ok_or_else(|| Error::NotFound(name.to_owned()))?;
        let path = self.states_root.join(name);
        // Create the physical state if the directory doesn't exist yet
        // (first open after `declare`); otherwise open it (reopen after
        // workspace restart).
        let state = if path.exists() {
            <State<K, V> as DurableStorage>::open(&path, saved)?
        } else {
            <State<K, V> as DurableStorage>::create(&path, saved)?
        };
        let state = Arc::new(RwLock::new(state));
        let erased: ErasedState = state.clone();
        inner.open.insert(name.to_owned(), erased);
        Ok(StateHandle {
            name: name.to_owned(),
            state,
            is_system: is_system_state(name),
        })
    }

    /// Opens the `_peer_state` system State for Devices bootstrap. Returns a
    /// handle with `is_system` set so `write()` is refused; internal callers
    /// (`PeerStore`) use [`Self::raw_peer_state`] for direct write access.
    pub(crate) fn peer_state<K, V>(&self) -> Result<StateHandle<K, V>>
    where
        K: Encode + Decode<()> + Hash + Eq + Clone + Ord + Send + Sync + 'static,
        V: Encode + Decode<()> + Clone + Send + Sync + 'static,
    {
        // Ensure the state is declared, then lazy-load via open_state.
        let mut inner = self.inner.lock();
        if !inner.declarations.contains(&PEER_STATE_NAME.to_owned()) {
            let config = StateConfig::Unordered(KeyDirConfig::default());
            inner.declarations.put(PEER_STATE_NAME.to_owned(), config)?;
            inner.declarations.sync()?;
        }
        drop(inner);
        self.open_state::<K, V>(PEER_STATE_NAME)
    }

    /// Direct accessor for the raw `Arc<RwLock<State>>` backing `_peer_state`,
    /// bypassing the `StateHandle` `is_system` check. Used by `PeerStore` for
    /// internal writes to the system state.
    pub(crate) fn raw_peer_state<K, V>(&self) -> Result<Arc<RwLock<State<K, V>>>>
    where
        K: Encode + Decode<()> + Hash + Eq + Clone + Ord + Send + Sync + 'static,
        V: Encode + Decode<()> + Clone + Send + Sync + 'static,
    {
        // Ensure declared/open via peer_state, then downcast the erased Arc.
        let _ = self.peer_state::<K, V>()?;
        let inner = self.inner.lock();
        let erased = inner
            .open
            .get(PEER_STATE_NAME)
            .ok_or_else(|| Error::NotFound(PEER_STATE_NAME.to_owned()))?
            .clone();
        let state = Arc::downcast::<RwLock<State<K, V>>>(erased)
            .map_err(|_| Error::TypeMismatch(PEER_STATE_NAME.to_owned()))?;
        Ok(state)
    }

    fn contains(&self, name: &str) -> bool {
        self.inner.lock().declarations.contains(&name.to_owned())
    }

    fn list(&self) -> Vec<String> {
        let mut names: Vec<_> = self
            .inner
            .lock()
            .declarations
            .keys()
            .map(|name| name.into_owned())
            .collect();
        names.sort();
        names
    }

    fn list_open(&self) -> Vec<String> {
        let mut names: Vec<_> = self.inner.lock().open.keys().cloned().collect();
        names.sort();
        names
    }

    fn config(&self, name: &str) -> Option<StateConfig> {
        self.inner
            .lock()
            .declarations
            .get(&name.to_owned())
            .map(|config| config.into_owned())
    }

    fn close(&self, name: &str) -> bool {
        let mut inner = self.inner.lock();
        if inner
            .open
            .get(name)
            .is_none_or(|state| Arc::strong_count(state) != 1)
        {
            return false;
        }
        inner.open.remove(name);
        true
    }

    fn delete(&self, name: &str) -> Result<bool> {
        if name == STATE_CATALOG_NAME {
            return Err(Error::ResourceBusy(name.to_owned()));
        }

        let mut inner = self.inner.lock();
        if inner
            .open
            .get(name)
            .is_some_and(|state| Arc::strong_count(state) > 1)
        {
            return Err(Error::ResourceBusy(name.to_owned()));
        }
        inner.open.remove(name);
        if !inner.declarations.delete(&name.to_owned())? {
            return Ok(false);
        }
        inner.declarations.sync()?;
        drop(inner);

        let path = self.states_root.join(name);
        if path.exists() {
            fs::remove_dir_all(path)?;
        }
        Ok(true)
    }
}

fn catalog_config() -> StateConfig {
    StateConfig::Unordered(KeyDirConfig::default())
}

fn state_handle<K: Ord + Send + Sync + 'static, V: Send + Sync + 'static>(
    name: &str,
    erased: ErasedState,
    is_system: bool,
) -> Result<StateHandle<K, V>> {
    let state = Arc::downcast::<RwLock<State<K, V>>>(erased)
        .map_err(|_| Error::TypeMismatch(name.to_owned()))?;
    Ok(StateHandle {
        name: name.to_owned(),
        state,
        is_system,
    })
}
