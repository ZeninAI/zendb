//! Guarded database state lifecycle.

use std::{any::Any, fs, io, sync::Arc};

use parking_lot::RwLock;
use zendb_storage::{
    core::traits::{Backend, DurableStorage},
    frontend::state::{State, StateConfig},
};
use bincode::{Decode, Encode};

use super::{already_exists, not_found, CatalogEntry, ConcurrentState, Database, StateHandle, STATES_DIR};

pub(super) type ErasedStateHandle = Arc<dyn Any + Send + Sync>;

impl Database {
    /// Return an open state, opening it lazily from the catalog or creating it
    /// with `config`. If the state is in the catalog and a different `config` is
    /// supplied, the catalog is updated before opening.
    pub fn state<K, V>(
        self: &Arc<Self>,
        name: &str,
        config: Option<StateConfig>,
    ) -> io::Result<StateHandle<K, V>>
    where
        K: Encode + Decode<()> + std::hash::Hash + Eq + Clone + Ord + Send + Sync + 'static,
        V: Encode + Decode<()> + Clone + Send + Sync + 'static,
    {
        // Fast path: already open
        if let Some(erased) = self.states.read().get(name).cloned() {
            let state = downcast_state::<K, V>(erased)?;
            return Ok(StateHandle::new(name, &state));
        }

        let _lifecycle = self.lifecycle.lock();
        // Double-check under lifecycle lock to avoid racing with another opener
        if let Some(erased) = self.states.read().get(name).cloned() {
            let state = downcast_state::<K, V>(erased)?;
            return Ok(StateHandle::new(name, &state));
        }

        let mut catalog = self.catalog.lock();
        let state = match catalog.get(&name.to_owned()) {
            Some(entry) => match entry.as_ref() {
                CatalogEntry::State(saved_config) => {
                    let effective_config = match &config {
                        Some(new_config) if new_config != saved_config => {
                            catalog.put(name.to_owned(), CatalogEntry::State(new_config.clone()))?;
                            new_config.clone()
                        }
                        _ => saved_config.clone(),
                    };
                    Arc::new(RwLock::new(State::<K, V>::open(
                        &self.path.join(STATES_DIR).join(name),
                        effective_config,
                    )?))
                }
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

    /// Evict a state from memory without removing it from the catalog or disk.
    pub fn close_state(self: &Arc<Self>, name: &str) -> io::Result<()> {
        let _lifecycle = self.lifecycle.lock();
        let mut states = self.states.write();
        match states.get(name) {
            Some(state) if Arc::strong_count(state) != 1 => Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                format!("state {name:?} still has active handles"),
            )),
            Some(_) => {
                states.remove(name);
                Ok(())
            }
            None => {
                if self.catalog.lock().contains(&name.to_owned()) {
                    Ok(()) // State exists in catalog but is not open; nothing to evict
                } else {
                    Err(not_found("state", name))
                }
            }
        }
    }
}

/// Downcast the type-erased `Any` handle to the concrete `State<K, V>`.
fn downcast_state<K, V>(
    state: ErasedStateHandle,
) -> io::Result<ConcurrentState<K, V>>
where
    K: Encode + Decode<()> + std::hash::Hash + Eq + Clone + Ord + Send + Sync + 'static,
    V: Encode + Decode<()> + Clone + Send + Sync + 'static,
{
    state
        .downcast::<RwLock<State<K, V>>>()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "state type mismatch"))
}
