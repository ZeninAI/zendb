//! Typed computation state lifecycle.

use std::{fs, io};

use zendb_storage::core::backend::Backend;

use super::{
    already_exists, not_found, CatalogEntry, DatabaseInner, COMPUTATIONS_DIR, SHARED_STATES_DIR,
};
use crate::{
    computation::state::deactivate, ComputationConfig, StateKey, StateRef, StateValue,
    StateVisibility,
};

impl DatabaseInner {
    pub(crate) fn shared_state<K: StateKey, V: StateValue>(
        &self,
        name: &str,
    ) -> io::Result<StateRef<K, V>> {
        let state = self
            .shared_states
            .read()
            .get(name)
            .cloned()
            .ok_or_else(|| not_found("shared state", name))?;
        StateRef::from_erased(state)
    }

    pub(crate) fn local_state<K: StateKey, V: StateValue>(
        &self,
        computation: &str,
        name: &str,
    ) -> io::Result<StateRef<K, V>> {
        let state = self
            .local_states
            .read()
            .get(&local_state_key(computation, name))
            .cloned()
            .ok_or_else(|| not_found("local state", name))?;
        StateRef::from_erased(state)
    }

    pub(super) fn create_states(
        &self,
        computation: &str,
        config: &ComputationConfig,
    ) -> io::Result<()> {
        self.validate_states(computation, config)?;
        for definition in &config.states {
            match definition.visibility {
                StateVisibility::Local => {
                    let path = self
                        .path
                        .join(COMPUTATIONS_DIR)
                        .join(computation)
                        .join(&definition.name);
                    fs::create_dir_all(path.parent().unwrap())?;
                    let state = self.registry.create_state(
                        &definition.implementation,
                        &path,
                        definition.config.clone(),
                    )?;
                    self.local_states
                        .write()
                        .insert(local_state_key(computation, &definition.name), state);
                }
                StateVisibility::Shared => {
                    if self.shared_states.read().contains_key(&definition.name)
                        || self.catalog.lock().contains(&definition.name)
                    {
                        return Err(already_exists("shared state", &definition.name));
                    }
                    let path = self.path.join(SHARED_STATES_DIR).join(&definition.name);
                    fs::create_dir_all(path.parent().unwrap())?;
                    let state = self.registry.create_state(
                        &definition.implementation,
                        &path,
                        definition.config.clone(),
                    )?;
                    self.catalog.lock().put(
                        definition.name.clone(),
                        CatalogEntry::SharedState {
                            owner: computation.to_owned(),
                            implementation: definition.implementation.clone(),
                            config: definition.config.clone(),
                        },
                    )?;
                    self.shared_states
                        .write()
                        .insert(definition.name.clone(), state);
                }
            }
        }
        Ok(())
    }

    pub(super) fn open_local_states(
        &self,
        computation: &str,
        config: &ComputationConfig,
    ) -> io::Result<()> {
        for definition in config
            .states
            .iter()
            .filter(|state| state.visibility == StateVisibility::Local)
        {
            let path = self
                .path
                .join(COMPUTATIONS_DIR)
                .join(computation)
                .join(&definition.name);
            let state = self.registry.open_state(
                &definition.implementation,
                &path,
                definition.config.clone(),
            )?;
            self.local_states
                .write()
                .insert(local_state_key(computation, &definition.name), state);
        }
        Ok(())
    }

    pub(super) fn remove_states(
        &self,
        computation: &str,
        config: &ComputationConfig,
    ) -> io::Result<()> {
        for definition in &config.states {
            match definition.visibility {
                StateVisibility::Local => {
                    if let Some(state) = self
                        .local_states
                        .write()
                        .remove(&local_state_key(computation, &definition.name))
                    {
                        deactivate(&state, true);
                    }
                }
                StateVisibility::Shared => {
                    if let Some(state) = self.shared_states.write().remove(&definition.name) {
                        deactivate(&state, true);
                    }
                    self.catalog.lock().delete(&definition.name)?;
                }
            }
        }
        Ok(())
    }

    fn validate_states(&self, computation: &str, config: &ComputationConfig) -> io::Result<()> {
        let mut names = std::collections::HashSet::new();
        for definition in &config.states {
            if !names.insert((definition.visibility.clone(), definition.name.clone())) {
                return Err(already_exists("computation state", &definition.name));
            }
            if definition.visibility == StateVisibility::Shared
                && (definition.name == computation
                    || self.shared_states.read().contains_key(&definition.name)
                    || self.catalog.lock().contains(&definition.name))
            {
                return Err(already_exists("shared state", &definition.name));
            }
        }
        Ok(())
    }
}

pub(super) fn local_state_key(computation: &str, state: &str) -> String {
    format!("{computation}/{state}")
}
