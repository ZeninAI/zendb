//! Guarded database state lifecycle.

use std::{fs, io, sync::Arc};

use bincode::{Decode, Encode};
use parking_lot::RwLock;
use zendb_storage::{
    core::traits::{Backend, DurableStorage},
    frontend::state::{State, StateConfig},
};

use crate::DispatchOperator;

use super::{ConcurrentState, Database, ErasedStateHandle, StateHandle, STATES_DIR};

impl<D> Database<D>
where
    D: DispatchOperator,
{
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

        let mut catalog = self.state_catalog.lock();
        // Double-check under catalog lock to avoid racing with another opener
        if let Some(erased) = self.states.read().get(name).cloned() {
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
