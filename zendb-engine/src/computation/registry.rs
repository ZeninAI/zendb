//! Application registrations for computation and typed state implementations.

use std::{collections::HashMap, io, marker::PhantomData, path::Path, sync::Arc};

use zendb_storage::frontend::state::StateConfig;

use super::{
    state::{ErasedState, StateResource},
    Computation, StateKey, StateValue,
};

type ComputationFactory = dyn Fn(&[u8]) -> io::Result<Box<dyn Computation>> + Send + Sync + 'static;

trait StateFactory: Send + Sync {
    fn create(&self, path: &Path, config: StateConfig) -> io::Result<ErasedState>;
    fn open(&self, path: &Path, config: StateConfig) -> io::Result<ErasedState>;
}

struct TypedStateFactory<K, V>(PhantomData<fn() -> (K, V)>);

impl<K: StateKey, V: StateValue> StateFactory for TypedStateFactory<K, V> {
    fn create(&self, path: &Path, config: StateConfig) -> io::Result<ErasedState> {
        StateResource::<K, V>::create(path, config)
    }

    fn open(&self, path: &Path, config: StateConfig) -> io::Result<ErasedState> {
        StateResource::<K, V>::open(path, config)
    }
}

#[derive(Default)]
pub struct ComputationRegistry {
    computations: HashMap<String, Arc<ComputationFactory>>,
    states: HashMap<String, Arc<dyn StateFactory>>,
}

impl ComputationRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register<F>(&mut self, implementation: impl Into<String>, factory: F)
    where
        F: Fn(&[u8]) -> io::Result<Box<dyn Computation>> + Send + Sync + 'static,
    {
        self.computations
            .insert(implementation.into(), Arc::new(factory));
    }

    pub fn register_state<K: StateKey, V: StateValue>(
        &mut self,
        implementation: impl Into<String>,
    ) {
        self.states.insert(
            implementation.into(),
            Arc::new(TypedStateFactory::<K, V>(PhantomData)),
        );
    }

    pub(crate) fn create_computation(
        &self,
        implementation: &str,
        configuration: &[u8],
    ) -> io::Result<Box<dyn Computation>> {
        self.computations.get(implementation).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("computation implementation {implementation:?} is not registered"),
            )
        })?(configuration)
    }

    pub(crate) fn create_state(
        &self,
        implementation: &str,
        path: &Path,
        config: StateConfig,
    ) -> io::Result<ErasedState> {
        self.state_factory(implementation)?.create(path, config)
    }

    pub(crate) fn open_state(
        &self,
        implementation: &str,
        path: &Path,
        config: StateConfig,
    ) -> io::Result<ErasedState> {
        self.state_factory(implementation)?.open(path, config)
    }

    fn state_factory(&self, implementation: &str) -> io::Result<&dyn StateFactory> {
        self.states
            .get(implementation)
            .map(AsRef::as_ref)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("state implementation {implementation:?} is not registered"),
                )
            })
    }
}
