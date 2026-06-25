//! Application registrations for operator implementations.

use std::{collections::HashMap, io, sync::Arc};

use bincode::Decode;

use super::{ErasedOperator, Operator};

type OperatorFactory = dyn Fn(&[u8]) -> io::Result<Box<dyn ErasedOperator>> + Send + Sync + 'static;

#[derive(Default)]
pub struct OperatorRegistry {
    operators: HashMap<String, Arc<OperatorFactory>>,
}

impl OperatorRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a factory for an [`Operator`].
    ///
    /// The factory receives the decoded `O::Config` and returns an `O`.
    pub fn register_operator<O: Operator>(
        &mut self,
        implementation: impl Into<String>,
        factory: impl Fn(O::Config) -> io::Result<O> + Send + Sync + 'static,
    ) {
        self.register_typed::<O::Config, _>(implementation, move |config| {
            factory(config).map(|op| Box::new(op) as Box<dyn ErasedOperator>)
        });
    }

    fn register_typed<C, F>(&mut self, implementation: impl Into<String>, factory: F)
    where
        C: Decode<()> + 'static,
        F: Fn(C) -> io::Result<Box<dyn ErasedOperator>> + Send + Sync + 'static,
    {
        self.register(implementation, move |bytes| {
            let (config, _) =
                bincode::decode_from_slice::<C, _>(bytes, bincode::config::standard())
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
            factory(config)
        });
    }

    fn register<F>(&mut self, implementation: impl Into<String>, factory: F)
    where
        F: Fn(&[u8]) -> io::Result<Box<dyn ErasedOperator>> + Send + Sync + 'static,
    {
        self.operators
            .insert(implementation.into(), Arc::new(factory));
    }

    /// Look up the registered factory by name and invoke it with the raw config bytes.
    pub(crate) fn create_operator(
        &self,
        implementation: &str,
        configuration: &[u8],
    ) -> io::Result<Box<dyn ErasedOperator>> {
        self.operators.get(implementation).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("operator implementation {implementation:?} is not registered"),
            )
        })?(configuration)
    }
}

