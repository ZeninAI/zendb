use std::io;
use std::sync::Arc;

use bincode::{Decode, Encode};

use crate::{BoxFuture, Database, DispatchOperator, Operator};

/// Configuration for the Merkle tree operator.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct MerkleTreeConfig {
    pub state: String,
}

impl Default for MerkleTreeConfig {
    fn default() -> Self {
        Self {
            state: "operator/prelude/merkle-tree".to_owned(),
        }
    }
}

/// Placeholder Merkle tree operator (no-op).
pub struct MerkleTreeOperator;

impl Operator for MerkleTreeOperator {
    type Config = MerkleTreeConfig;
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
