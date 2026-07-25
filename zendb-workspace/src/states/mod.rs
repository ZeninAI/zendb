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
/// Exposes lifecycle operations (open/declare, list, contains, close, delete)
/// and returns typed [`StateHandle`]s. State reads and writes are performed
/// through the handle, not on this handler.
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

    pub fn open<K, V>(&self, name: &str, config: Option<StateConfig>) -> Result<StateHandle<K, V>>
    where
        K: Encode + Decode<()> + Hash + Eq + Clone + Ord + Send + Sync + 'static,
        V: Encode + Decode<()> + Clone + Send + Sync + 'static,
    {
        if is_system_state(name) {
            return Err(Error::NotFound(name.to_owned()));
        }
        self.core.state(name, config)
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

pub struct StateHandle<K: Ord, V> {
    name: String,
    state: Arc<RwLock<State<K, V>>>,
}

impl<K: Ord, V> Clone for StateHandle<K, V> {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            state: self.state.clone(),
        }
    }
}

impl<K: Ord, V> StateHandle<K, V> {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn read(&self) -> RwLockReadGuard<'_, State<K, V>> {
        self.state.read()
    }

    pub fn write(&self) -> RwLockWriteGuard<'_, State<K, V>> {
        self.state.write()
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

    pub(crate) fn state<K, V>(
        &self,
        name: &str,
        config: Option<StateConfig>,
    ) -> Result<StateHandle<K, V>>
    where
        K: Encode + Decode<()> + Hash + Eq + Clone + Ord + Send + Sync + 'static,
        V: Encode + Decode<()> + Clone + Send + Sync + 'static,
    {
        let mut inner = self.inner.lock();
        if let Some(erased) = inner.open.get(name).cloned() {
            return state_handle(name, erased);
        }

        let saved = inner
            .declarations
            .get(&name.to_owned())
            .map(|value| value.into_owned());
        let selected = saved.clone().or(config).unwrap_or_default();
        let path = self.states_root.join(name);
        let state = if saved.is_some() {
            State::open(&path, selected)?
        } else {
            let state = State::create(&path, selected.clone())?;
            inner.declarations.put(name.to_owned(), selected)?;
            inner.declarations.sync()?;
            state
        };

        let state = Arc::new(RwLock::new(state));
        let erased: ErasedState = state.clone();
        inner.open.insert(name.to_owned(), erased);
        Ok(StateHandle {
            name: name.to_owned(),
            state,
        })
    }

    /// Opens the `_peer_state` system State for Devices bootstrap.
    pub(crate) fn peer_state<K, V>(&self) -> Result<StateHandle<K, V>>
    where
        K: Encode + Decode<()> + Hash + Eq + Clone + Ord + Send + Sync + 'static,
        V: Encode + Decode<()> + Clone + Send + Sync + 'static,
    {
        self.state(
            PEER_STATE_NAME,
            Some(StateConfig::Unordered(KeyDirConfig::default())),
        )
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
) -> Result<StateHandle<K, V>> {
    let state = Arc::downcast::<RwLock<State<K, V>>>(erased)
        .map_err(|_| Error::TypeMismatch(name.to_owned()))?;
    Ok(StateHandle {
        name: name.to_owned(),
        state,
    })
}
