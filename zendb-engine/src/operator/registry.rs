//! Application registrations for operator implementations.

use std::{collections::HashMap, io, sync::Arc};

use super::Operator;

type OperatorFactory = dyn Fn(&[u8]) -> io::Result<Box<dyn Operator>> + Send + Sync + 'static;

#[derive(Default)]
pub struct OperatorRegistry {
    operators: HashMap<String, Arc<OperatorFactory>>,
}

impl OperatorRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register<F>(&mut self, implementation: impl Into<String>, factory: F)
    where
        F: Fn(&[u8]) -> io::Result<Box<dyn Operator>> + Send + Sync + 'static,
    {
        self.operators
            .insert(implementation.into(), Arc::new(factory));
    }

    pub(crate) fn create_operator(
        &self,
        implementation: &str,
        configuration: &[u8],
    ) -> io::Result<Box<dyn Operator>> {
        self.operators.get(implementation).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("operator implementation {implementation:?} is not registered"),
            )
        })?(configuration)
    }
}
