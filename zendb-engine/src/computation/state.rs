//! Typed, concurrently shared computation state handles.

use std::{
    any::Any,
    fs,
    hash::Hash,
    io,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use bincode::{Decode, Encode};
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use zendb_storage::core::backend::{Backend, FileBackedBackend};
use zendb_storage::frontend::state::{State, StateConfig};

pub trait StateKey: Encode + Decode<()> + Hash + Eq + Clone + Ord + Send + Sync + 'static {}

impl<T> StateKey for T where T: Encode + Decode<()> + Hash + Eq + Clone + Ord + Send + Sync + 'static
{}

pub trait StateValue: Encode + Decode<()> + Clone + Send + Sync + 'static {}

impl<T> StateValue for T where T: Encode + Decode<()> + Clone + Send + Sync + 'static {}

#[derive(Clone)]
pub(crate) struct ErasedState {
    value: Arc<dyn Any + Send + Sync>,
    lifecycle: Arc<dyn StateLifecycle>,
}

pub(crate) struct StateResource<K: StateKey, V: StateValue> {
    inner: RwLock<State<K, V>>,
    path: PathBuf,
    active: AtomicBool,
    delete_on_drop: AtomicBool,
}

impl<K: StateKey, V: StateValue> StateResource<K, V> {
    pub(crate) fn create(path: &Path, config: StateConfig) -> io::Result<ErasedState> {
        Ok(Self::erased(Self {
            inner: RwLock::new(State::create(path, config)?),
            path: path.to_path_buf(),
            active: AtomicBool::new(true),
            delete_on_drop: AtomicBool::new(false),
        }))
    }

    pub(crate) fn open(path: &Path, config: StateConfig) -> io::Result<ErasedState> {
        Ok(Self::erased(Self {
            inner: RwLock::new(State::open(path, config)?),
            path: path.to_path_buf(),
            active: AtomicBool::new(true),
            delete_on_drop: AtomicBool::new(false),
        }))
    }

    fn erased(resource: Self) -> ErasedState {
        let resource = Arc::new(resource);
        ErasedState {
            value: resource.clone(),
            lifecycle: resource,
        }
    }

    fn ensure_active(&self) -> io::Result<()> {
        if self.active.load(Ordering::Acquire) {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::NotFound,
                "state has been dropped",
            ))
        }
    }
}

impl<K: StateKey, V: StateValue> Drop for StateResource<K, V> {
    fn drop(&mut self) {
        if self.delete_on_drop.load(Ordering::Acquire) {
            let _ = fs::remove_dir_all(&self.path);
            let _ = fs::remove_file(&self.path);
        }
    }
}

#[derive(Clone)]
pub struct StateRef<K: StateKey, V: StateValue> {
    resource: Arc<StateResource<K, V>>,
}

impl<K: StateKey, V: StateValue> StateRef<K, V> {
    pub(crate) fn from_erased(state: ErasedState) -> io::Result<Self> {
        state
            .value
            .downcast::<StateResource<K, V>>()
            .map(|resource| Self { resource })
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "state type mismatch"))
    }

    pub fn read(&self) -> io::Result<RwLockReadGuard<'_, State<K, V>>> {
        self.resource.ensure_active()?;
        Ok(self.resource.inner.read())
    }

    pub fn write(&self) -> io::Result<RwLockWriteGuard<'_, State<K, V>>> {
        self.resource.ensure_active()?;
        Ok(self.resource.inner.write())
    }

    pub fn get(&self, key: &K) -> io::Result<Option<V>> {
        Ok(self.read()?.get(key).map(|value| value.into_owned()))
    }

    pub fn put(&self, key: K, value: V) -> io::Result<()> {
        self.write()?.put(key, value)
    }

    pub fn delete(&self, key: &K) -> io::Result<bool> {
        self.write()?.delete(key)
    }
}

pub(crate) fn deactivate(state: &ErasedState, delete_on_drop: bool) {
    state.lifecycle.deactivate(delete_on_drop);
}

trait StateLifecycle: Send + Sync {
    fn deactivate(&self, delete_on_drop: bool);
}

impl<K: StateKey, V: StateValue> StateLifecycle for StateResource<K, V> {
    fn deactivate(&self, delete_on_drop: bool) {
        self.active.store(false, Ordering::Release);
        self.delete_on_drop.store(delete_on_drop, Ordering::Release);
    }
}
