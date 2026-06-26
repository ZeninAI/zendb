//! Application registrations for operator implementations.

use std::{collections::HashMap, io, sync::Arc};

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
        self.operators.insert(
            implementation.into(),
            Arc::new(move |bytes| {
                let (config, _) =
                    bincode::decode_from_slice::<O::Config, _>(bytes, bincode::config::standard())
                        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
                factory(config).map(|op| Box::new(op) as Box<dyn ErasedOperator>)
            }),
        );
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
