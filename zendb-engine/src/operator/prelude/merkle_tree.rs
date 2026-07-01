use std::io;

use bincode::{Decode, Encode};
use zendb_storage::core::traits::Backend;

use crate::{
    BoxFuture, Change, DispatchOperator, Operator, OperatorContext, OperatorDirective, StateConfig,
    StateHandle,
};

/// Key used to store the current Merkle root in the state.
const ROOT_KEY: &str = "__root";

/// Configuration for the Merkle tree operator.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct MerkleTreeConfig {
    /// State key used to persist leaves and the root.
    pub state: String,
}

impl Default for MerkleTreeConfig {
    fn default() -> Self {
        Self {
            state: "operator/prelude/merkle-tree".to_owned(),
        }
    }
}

/// A leaf node in the Merkle tree, repesenting a single table row.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct MerkleLeaf {
    pub table: String,
    pub key: Vec<u8>,
    pub hash: Vec<u8>,
}

/// Incrementally maintains a Merkle tree over subscribed tables, recomputing
/// the root hash after every batch of changes.
pub struct MerkleTreeOperator {
    state: Option<StateHandle<String, MerkleLeaf>>,
}

impl Operator for MerkleTreeOperator {
    type Config = MerkleTreeConfig;
    type Timer = ();

    fn new(_config: &Self::Config) -> io::Result<Self> {
        Ok(Self { state: None })
    }

    fn open<'a, D>(
        &'a mut self,
        ctx: &'a OperatorContext<Self, D>,
    ) -> BoxFuture<'a, io::Result<OperatorDirective>>
    where
        D: DispatchOperator,
    {
        Box::pin(async move {
            self.state = Some(ctx.state(&ctx.config().state, Some(StateConfig::default()))?);
            Ok(OperatorDirective::Continue)
        })
    }

    fn process<'a, D>(
        &'a mut self,
        changes: Vec<Change>,
        _ctx: &'a OperatorContext<Self, D>,
    ) -> BoxFuture<'a, io::Result<OperatorDirective>>
    where
        D: DispatchOperator,
    {
        Box::pin(async move {
            let state = self
                .state
                .as_ref()
                .expect("merkle state must be initialized by open")
                .get()?;
            let mut state = state.write();

            for change in changes {
                let leaf_key = leaf_key(&change)?;
                match &change.current {
                    Some(current) => {
                        state.put(
                            leaf_key,
                            MerkleLeaf {
                                table: change.event.table_id.clone(),
                                key: encode_key(&change.event.primary_key)?,
                                hash: hash_cell(current)?,
                            },
                        )?;
                    }
                    None => {
                        state.delete(&leaf_key)?;
                    }
                }
            }

            let root = compute_root(
                state
                    .entries()
                    .filter(|(key, _)| key.as_ref() != ROOT_KEY)
                    .map(|(key, leaf)| (key.into_owned(), leaf.into_owned())),
            );
            state.put(
                ROOT_KEY.to_owned(),
                MerkleLeaf {
                    table: String::new(),
                    key: Vec::new(),
                    hash: root,
                },
            )?;

            Ok(OperatorDirective::Continue)
        })
    }
}

/// Build a state key for a leaf from the table ID and primary key hash.
fn leaf_key(change: &Change) -> io::Result<String> {
    let key = encode_key(&change.event.primary_key)?;
    Ok(format!(
        "leaf/{}/{}",
        change.event.table_id,
        blake3::hash(&key).to_hex()
    ))
}

/// Serialize a primary key to bytes for hashing.
fn encode_key(key: &zendb_types::PrimaryKey) -> io::Result<Vec<u8>> {
    bincode::encode_to_vec(key, bincode::config::standard())
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))
}

/// Hash a cell value with BLAKE3.
fn hash_cell(cell: &zendb_types::Cell) -> io::Result<Vec<u8>> {
    let bytes = bincode::encode_to_vec(cell, bincode::config::standard())
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))?;
    Ok(blake3::hash(&bytes).as_bytes().to_vec())
}

/// Compute the Merkle root from an unordered set of (key, leaf) pairs.
/// Sorts by key before hashing to ensure determinism.
fn compute_root(leaves: impl IntoIterator<Item = (String, MerkleLeaf)>) -> Vec<u8> {
    let mut leaves: Vec<_> = leaves.into_iter().collect();
    leaves.sort_unstable_by(|a, b| a.0.cmp(&b.0));

    let mut hasher = blake3::Hasher::new();
    for (key, leaf) in leaves {
        hasher.update(key.as_bytes());
        hasher.update(&leaf.hash);
    }
    hasher.finalize().as_bytes().to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn root_is_independent_of_input_order() {
        let a = MerkleLeaf {
            table: "users".to_owned(),
            key: b"a".to_vec(),
            hash: b"one".to_vec(),
        };
        let b = MerkleLeaf {
            table: "users".to_owned(),
            key: b"b".to_vec(),
            hash: b"two".to_vec(),
        };

        let left = compute_root([("b".to_owned(), b.clone()), ("a".to_owned(), a.clone())]);
        let right = compute_root([("a".to_owned(), a), ("b".to_owned(), b)]);

        assert_eq!(left, right);
    }
}
