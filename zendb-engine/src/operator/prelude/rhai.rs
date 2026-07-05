use std::future::Future;
use std::io;
use std::sync::Arc;

use bincode::{Decode, Encode};

use crate::{Database, DispatchOperator, Operator};

/// Configuration for the Rhai scripting operator.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct RhaiOperatorConfig {
    pub script: String,
}

/// Placeholder Rhai scripting operator (no-op).
pub struct RhaiOperator;

impl Operator for RhaiOperator {
    type Config = RhaiOperatorConfig;
    type Timer = ();
    type Facet = ();

    fn create<'a, D>(
        _db: &'a Arc<Database<D>>,
        _name: &'a str,
        _config: &'a Self::Config,
    ) -> impl Future<Output = io::Result<Self>> + Send + 'a
    where
        D: DispatchOperator,
        Self: Sized,
    {
        async { Ok(Self) }
    }

    fn facet(&self) {}
}
