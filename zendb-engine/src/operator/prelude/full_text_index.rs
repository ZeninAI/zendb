use std::io;
use std::sync::Arc;

use bincode::{Decode, Encode};

use crate::{BoxFuture, Database, DispatchOperator, Operator};

/// Configuration for the full-text index operator.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct FullTextIndexConfig {
    pub state: String,
}

impl Default for FullTextIndexConfig {
    fn default() -> Self {
        Self {
            state: "operator/prelude/full-text-index".to_owned(),
        }
    }
}

/// Placeholder full-text index operator (no-op).
pub struct FullTextIndexOperator;

impl Operator for FullTextIndexOperator {
    type Config = FullTextIndexConfig;
    type Timer = ();

    fn create<'a, D>(
        _db: &'a Arc<Database<D>>,
        _name: &'a str,
        _config: &'a Self::Config,
    ) -> BoxFuture<'a, io::Result<Self>>
    where
        D: DispatchOperator,
        Self: Sized,
    {
        Box::pin(async { Ok(Self) })
    }
}
