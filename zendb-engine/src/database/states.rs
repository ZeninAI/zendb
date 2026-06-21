//! Guarded database state lifecycle.

use std::{any::Any, fs, io, sync::Arc};

use parking_lot::RwLock;
use zendb_storage::{
    core::traits::{Backend, DurableStorage},
    frontend::state::StateConfig,
};

use super::{already_exists, not_found, CatalogEntry, DatabaseInner, STATES_DIR};
use crate::{ConcurrentState, State, StateKey, StateValue};

pub(super) type ErasedStateHandle = Arc<dyn Any + Send + Sync>;

impl DatabaseInner {
    pub(crate) fn state<K: StateKey, V: StateValue>(
        &self,
        name: &str,
        config: Option<StateConfig>,
    ) -> io::Result<ConcurrentState<K, V>> {
        if let Some(state) = self.states.read().get(name).cloned() {
            return downcast_state(state);
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
        Ok(state)
    }

    pub(crate) fn drop_state(&self, name: &str) -> io::Result<()> {
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

        match self.catalog.lock().get(&name.to_owned()) {
            Some(entry) if matches!(entry.as_ref(), CatalogEntry::State(_)) => {}
            Some(_) => return Err(already_exists("catalog resource", name)),
            None => return Err(not_found("state", name)),
        }
        self.catalog.lock().delete(&name.to_owned())?;
        let path = self.path.join(STATES_DIR).join(name);
        let _ = fs::remove_dir_all(&path);
        let _ = fs::remove_file(&path);
        Ok(())
    }
}

fn downcast_state<K: StateKey, V: StateValue>(
    state: ErasedStateHandle,
) -> io::Result<ConcurrentState<K, V>> {
    state
        .downcast::<RwLock<State<K, V>>>()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "state type mismatch"))
}
