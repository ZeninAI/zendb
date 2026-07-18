//! Guarded workspace state lifecycle.

use std::{fs, io, sync::Arc};

use bincode::{Decode, Encode};
use log::{info, trace};
use parking_lot::RwLock;
use zendb_storage::{DurableStorage, ReadBackend, State, StateConfig, WriteBackend};

use crate::DispatchOperator;

use crate::host::{ConcurrentState, ErasedStateHandle, OperatorHost, StateHandle, STATES_DIR};

impl<D> OperatorHost<D>
where
    D: DispatchOperator,
{
    /// Return `true` if a state exists in the durable state catalog.
    pub fn contains_state(&self, name: &str) -> bool {
        self.state_catalog.lock().contains(&name.to_owned())
    }

    /// Return `true` if a state is currently loaded in memory.
    pub fn is_state_open(&self, name: &str) -> bool {
        self.states.read().contains_key(name)
    }

    /// List every state known to the durable state catalog.
    pub fn list_states(&self) -> Vec<String> {
        self.state_catalog
            .lock()
            .keys()
            .map(|name| name.into_owned())
            .collect()
    }

    /// List every state currently loaded in memory.
    pub fn list_open_states(&self) -> Vec<String> {
        self.states.read().keys().cloned().collect()
    }

    /// Return the persisted config for a state, if the catalog contains one.
    pub fn state_config(&self, name: &str) -> Option<StateConfig> {
        self.state_catalog
            .lock()
            .get(&name.to_owned())
            .map(|config| config.into_owned())
    }

    /// Remove an open state from the in-memory cache. The durable state remains
    /// in the catalog and can be reopened later with [`Workspace::state`].
    pub fn close_state(&self, name: &str) -> bool {
        let removed = self.states.write().remove(name).is_some();
        if removed {
            info!("closing state {name:?}");
        }
        removed
    }

    /// Delete a state entirely: evict from memory, remove from the catalog,
    /// and delete its on-disk directory. Returns `Ok(true)` if the state
    /// existed, `Ok(false)` if it was not in the catalog.
    pub fn delete_state(&self, name: &str) -> io::Result<bool> {
        self.states.write().remove(name);

        if !self.state_catalog.lock().delete(&name.to_owned())? {
            return Ok(false);
        }

        let path = self.path.join(STATES_DIR).join(name);
        if path.exists() {
            fs::remove_dir_all(&path)?;
        }
        info!("deleted state {name:?}");
        Ok(true)
    }

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
            trace!("state {name:?} already open, returning cached handle");
            let state = downcast_state::<K, V>(erased)?;
            return Ok(StateHandle::new(name, &state));
        }

        let mut catalog = self.state_catalog.lock();
        // Double-check under catalog lock to avoid racing with another opener
        if let Some(erased) = self.states.read().get(name).cloned() {
            trace!("state {name:?} already open (race), returning cached handle");
            let state = downcast_state::<K, V>(erased)?;
            return Ok(StateHandle::new(name, &state));
        }

        let state = match catalog.get(&name.to_owned()) {
            Some(saved_config) => {
                let saved_config = saved_config.as_ref();
                let effective_config = match &config {
                    Some(new_config) if new_config != saved_config => {
                        catalog.put(name.to_owned(), new_config.clone())?;
                        new_config.clone()
                    }
                    _ => saved_config.clone(),
                };
                info!("opening existing state {name:?}");
                Arc::new(RwLock::new(State::<K, V>::open(
                    &self.path.join(STATES_DIR).join(name),
                    effective_config,
                )?))
            }
            None => {
                let config = config.unwrap_or_default();
                let path = self.path.join(STATES_DIR).join(name);
                if let Some(parent) = path.parent() {
                    fs::create_dir_all(parent)?;
                }
                info!("creating new state {name:?}");
                let state = Arc::new(RwLock::new(State::<K, V>::create(&path, config.clone())?));
                catalog.put(name.to_owned(), config)?;
                state
            }
        };

        self.states
            .write()
            .insert(name.to_owned(), state.clone() as ErasedStateHandle);
        Ok(StateHandle::new(name, &state))
    }
}

/// Downcast the type-erased `Any` handle to the concrete `State<K, V>`.
fn downcast_state<K, V>(state: ErasedStateHandle) -> io::Result<ConcurrentState<K, V>>
where
    K: Encode + Decode<()> + std::hash::Hash + Eq + Clone + Ord + Send + Sync + 'static,
    V: Encode + Decode<()> + Clone + Send + Sync + 'static,
{
    state
        .downcast::<RwLock<State<K, V>>>()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "state type mismatch"))
}
