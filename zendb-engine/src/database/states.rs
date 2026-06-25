//! Guarded database state lifecycle.

use std::{any::Any, fs, io, sync::Arc};

use parking_lot::RwLock;
use zendb_storage::{
    core::traits::{Backend, DurableStorage},
    frontend::state::{State, StateConfig},
};

use super::{already_exists, not_found, CatalogEntry, ConcurrentState, Database, StateHandle, STATES_DIR};
use crate::{StateKey, StateValue};

pub(super) type ErasedStateHandle = Arc<dyn Any + Send + Sync>;

impl Database {
    /// Return an existing state (in-memory or via catalog) or create it when
    /// `config` is supplied. The lifecycle lock is taken only on the creation path.
    pub fn state<K: StateKey, V: StateValue>(
        self: &Arc<Self>,
        name: &str,
        config: Option<StateConfig>,
    ) -> io::Result<StateHandle<K, V>> {
        let _lifecycle = config.is_some().then(|| self.lifecycle.lock());

        if let Some(erased) = self.states.read().get(name).cloned() {
            let state = downcast_state::<K, V>(erased)?;
            return Ok(StateHandle::new(name, &state));
        }

        let mut catalog = self.catalog.lock();
        let state = match catalog.get(&name.to_owned()) {
            Some(entry) => match entry.as_ref() {
                CatalogEntry::State(config) => Arc::new(RwLock::new(State::<K, V>::open(
                    &self.path.join(STATES_DIR).join(name),
                    config.clone(),
                )?)),
                _ => return Err(already_exists("catalog resource", name)),
            },
            None => {
                let Some(config) = config else {
                    return Err(not_found("state", name));
                };
                let path = self.path.join(STATES_DIR).join(name);
                if let Some(parent) = path.parent() {
                    fs::create_dir_all(parent)?;
                }
                let state = Arc::new(RwLock::new(State::<K, V>::create(&path, config.clone())?));
                catalog.put(name.to_owned(), CatalogEntry::State(config))?;
                state
            }
        };

        self.states
            .write()
            .insert(name.to_owned(), state.clone() as ErasedStateHandle);
        Ok(StateHandle::new(name, &state))
    }

    /// Guard against live handles, remove the state from memory, catalog, and disk.
    pub fn drop_state(self: &Arc<Self>, name: &str) -> io::Result<()> {
        let _lifecycle = self.lifecycle.lock();
        {
            let mut states = self.states.write();
            if let Some(state) = states.get(name) {
                if Arc::strong_count(state) != 1 {
                    return Err(io::Error::new(
                        io::ErrorKind::WouldBlock,
                        format!("state {name:?} still has active handles"),
                    ));
                }
                states.remove(name);
            }
        }

        let mut catalog = self.catalog.lock();
        match catalog.get(&name.to_owned()) {
            Some(entry) if matches!(entry.as_ref(), CatalogEntry::State(_)) => {}
            Some(_) => return Err(already_exists("catalog resource", name)),
            None => return Err(not_found("state", name)),
        }
        catalog.delete(&name.to_owned())?;
        let path = self.path.join(STATES_DIR).join(name);
        let _ = fs::remove_dir_all(&path);
        let _ = fs::remove_file(&path);
        Ok(())
    }
}

/// Downcast the type-erased `Any` handle to the concrete `State<K, V>`.
fn downcast_state<K: StateKey, V: StateValue>(
    state: ErasedStateHandle,
) -> io::Result<ConcurrentState<K, V>> {
    state
        .downcast::<RwLock<State<K, V>>>()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "state type mismatch"))
}
