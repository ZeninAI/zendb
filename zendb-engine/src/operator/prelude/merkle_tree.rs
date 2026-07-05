use std::borrow::Cow;
use std::io;
use std::sync::Arc;

use bincode::{Decode, Encode};
use hashbrown::{HashMap, HashSet};
use zendb_storage::core::traits::Backend;
use zendb_types::{Cell, PrimaryKey};

use crate::{
    BoxFuture, Change, Database, DispatchOperator, Operator, OperatorDirective, StateConfig,
    StateHandle,
};

const DEFAULT_LEAF_BITS: u8 = 16;
const DIGEST_BYTES: usize = 32;
const ZERO_DIGEST: [u8; DIGEST_BYTES] = [0; DIGEST_BYTES];

/// Configuration for the Merkle tree operator.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct MerkleTreeConfig {
    /// State namespace used to persist all Merkle summaries.
    pub state: String,
    /// Number of token-prefix bits used as leaf buckets.
    ///
    /// `16` gives 65,536 fixed buckets over the hashed keyspace. The operator
    /// stores only non-empty leaves and their ancestors.
    pub leaf_bits: u8,
}

impl Default for MerkleTreeConfig {
    fn default() -> Self {
        Self {
            state: "operator/prelude/merkle-tree".to_owned(),
            leaf_bits: DEFAULT_LEAF_BITS,
        }
    }
}

/// Durable key used by [`MerkleTreeOperator`] state.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Encode, Decode)]
pub enum MerkleTreeStateKey {
    Root {
        table: String,
    },
    Node {
        table: String,
        level: u8,
        index: u64,
    },
    Leaf {
        table: String,
        index: u64,
    },
    Entry {
        table: String,
        key: PrimaryKey,
    },
}

impl MerkleTreeStateKey {
    fn table(&self) -> &str {
        match self {
            Self::Root { table }
            | Self::Node { table, .. }
            | Self::Leaf { table, .. }
            | Self::Entry { table, .. } => table,
        }
    }
}

/// Hash summary for a complete table.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct MerkleRoot {
    pub leaf_bits: u8,
    pub entries: u64,
    pub hash: [u8; DIGEST_BYTES],
}

/// Hash summary for an internal range node.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct MerkleNode {
    pub entries: u64,
    pub hash: [u8; DIGEST_BYTES],
}

/// Hash summary for a fixed token-prefix leaf bucket.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct MerkleLeaf {
    pub entries: u64,
    pub xor: [u8; DIGEST_BYTES],
    pub hash: [u8; DIGEST_BYTES],
}

impl MerkleLeaf {
    fn empty(index: u64) -> Self {
        leaf_summary(index, 0, ZERO_DIGEST)
    }
}

/// Digest stored for one table entry.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct MerkleEntry {
    pub leaf: u64,
    pub hash: [u8; DIGEST_BYTES],
}

/// Persisted Merkle state values.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub enum MerkleTreeStateValue {
    Root(MerkleRoot),
    Node(MerkleNode),
    Leaf(MerkleLeaf),
    Entry(MerkleEntry),
}

/// Maintains one sparse range Merkle tree per subscribed table.
pub struct MerkleTreeOperator {
    state: StateHandle<MerkleTreeStateKey, MerkleTreeStateValue>,
    leaf_bits: u8,
}

impl Operator for MerkleTreeOperator {
    type Config = MerkleTreeConfig;
    type Timer = ();

    fn create<'a, D>(
        db: &'a Arc<Database<D>>,
        _name: &'a str,
        config: &'a Self::Config,
    ) -> BoxFuture<'a, io::Result<Self>>
    where
        D: DispatchOperator,
        Self: Sized,
    {
        Box::pin(async move {
            let leaf_bits = validate_leaf_bits(config.leaf_bits)?;
            let state = db.state(&config.state, Some(StateConfig::default()))?;
            Ok(Self { state, leaf_bits })
        })
    }

    fn on_input_opened<'a, D>(
        &'a mut self,
        table: String,
        db: &'a Arc<Database<D>>,
        _name: &'a str,
        _config: &'a Self::Config,
    ) -> BoxFuture<'a, io::Result<OperatorDirective>>
    where
        D: DispatchOperator,
    {
        Box::pin(async move {
            self.rebuild_table(db, &table)?;
            Ok(OperatorDirective::Continue)
        })
    }

    fn on_input_closed<'a, D>(
        &'a mut self,
        _table: String,
        _db: &'a Arc<Database<D>>,
        _name: &'a str,
        _config: &'a Self::Config,
    ) -> BoxFuture<'a, io::Result<OperatorDirective>>
    where
        D: DispatchOperator,
    {
        Box::pin(async { Ok(OperatorDirective::Continue) })
    }

    fn process<'a, D>(
        &'a mut self,
        changes: Vec<Change>,
        _db: &'a Arc<Database<D>>,
        _name: &'a str,
        _config: &'a Self::Config,
    ) -> BoxFuture<'a, io::Result<OperatorDirective>>
    where
        D: DispatchOperator,
    {
        Box::pin(async move {
            let mut touched = HashMap::<String, HashSet<u64>>::new();
            {
                let state = self.state.get()?;
                let mut state = state.write();
                for change in changes {
                    let table = change.event.table_id.clone();
                    let key = change.event.primary_key.clone();
                    let previous = change
                        .previous
                        .as_ref()
                        .map(|cell| entry_digest(self.leaf_bits, &key, cell))
                        .transpose()?;
                    let current = change
                        .current
                        .as_ref()
                        .map(|cell| entry_digest(self.leaf_bits, &key, cell))
                        .transpose()?;
                    if let Some(entry) = &previous {
                        touched.entry(table.clone()).or_default().insert(entry.leaf);
                    }
                    if let Some(entry) = &current {
                        touched.entry(table.clone()).or_default().insert(entry.leaf);
                    }
                    apply_entry_change(&mut state, &table, key, previous, current)?;
                }

                for (table, leaves) in touched {
                    rebuild_tree_from_leaves(&mut state, &table, self.leaf_bits, leaves)?;
                }
            }
            Ok(OperatorDirective::Continue)
        })
    }
}

impl MerkleTreeOperator {
    /// Return the persisted root for `table`, if the operator has seen it.
    pub fn root<D>(
        db: &Arc<Database<D>>,
        config: &MerkleTreeConfig,
        table: &str,
    ) -> io::Result<Option<MerkleRoot>>
    where
        D: DispatchOperator,
    {
        let state: StateHandle<MerkleTreeStateKey, MerkleTreeStateValue> =
            db.state(&config.state, None)?;
        Ok(state
            .get()?
            .read()
            .get(&MerkleTreeStateKey::Root {
                table: table.to_owned(),
            })
            .and_then(|value| match value.into_owned() {
                MerkleTreeStateValue::Root(root) => Some(root),
                _ => None,
            }))
    }

    fn rebuild_table<D>(&mut self, db: &Arc<Database<D>>, table: &str) -> io::Result<()>
    where
        D: DispatchOperator,
    {
        let table_handle = db.table(table, None)?;
        let table_guard = table_handle.get()?;
        let table_read = table_guard.read();
        let mut entries = Vec::new();
        let mut leaves = HashMap::<u64, MerkleLeaf>::new();

        for (key, cell) in table_read.entries() {
            let key = key.into_owned();
            let entry = entry_digest(self.leaf_bits, &key, cell.as_ref())?;
            apply_leaf_delta(
                leaves
                    .entry(entry.leaf)
                    .or_insert_with(|| MerkleLeaf::empty(entry.leaf)),
                entry.leaf,
                None,
                Some(entry.hash),
            )?;
            entries.push((key, entry));
        }
        drop(table_read);

        let state = self.state.get()?;
        let mut state = state.write();
        delete_table_state(&mut state, table)?;

        for (key, entry) in entries {
            state.put(
                MerkleTreeStateKey::Entry {
                    table: table.to_owned(),
                    key,
                },
                MerkleTreeStateValue::Entry(entry),
            )?;
        }

        for (index, leaf) in &leaves {
            state.put(
                MerkleTreeStateKey::Leaf {
                    table: table.to_owned(),
                    index: *index,
                },
                MerkleTreeStateValue::Leaf(leaf.clone()),
            )?;
        }

        rebuild_tree_from_leaf_map(&mut state, table, self.leaf_bits, leaves)
    }
}

fn validate_leaf_bits(leaf_bits: u8) -> io::Result<u8> {
    if (1..=63).contains(&leaf_bits) {
        Ok(leaf_bits)
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "MerkleTreeConfig.leaf_bits must be in 1..=63",
        ))
    }
}

fn delete_table_state(
    state: &mut crate::State<MerkleTreeStateKey, MerkleTreeStateValue>,
    table: &str,
) -> io::Result<()> {
    let keys: Vec<_> = state
        .keys()
        .filter(|key| key.table() == table)
        .map(Cow::into_owned)
        .collect();
    for key in keys {
        state.delete(&key)?;
    }
    Ok(())
}

fn apply_entry_change(
    state: &mut crate::State<MerkleTreeStateKey, MerkleTreeStateValue>,
    table: &str,
    key: PrimaryKey,
    previous: Option<MerkleEntry>,
    current: Option<MerkleEntry>,
) -> io::Result<()> {
    if let Some(previous) = previous {
        update_leaf(state, table, previous.leaf, Some(previous.hash), None)?;
    }

    let entry_key = MerkleTreeStateKey::Entry {
        table: table.to_owned(),
        key,
    };
    match current {
        Some(current) => {
            update_leaf(state, table, current.leaf, None, Some(current.hash))?;
            state.put(entry_key, MerkleTreeStateValue::Entry(current))?;
        }
        None => {
            state.delete(&entry_key)?;
        }
    }
    Ok(())
}

fn update_leaf(
    state: &mut crate::State<MerkleTreeStateKey, MerkleTreeStateValue>,
    table: &str,
    index: u64,
    remove: Option<[u8; DIGEST_BYTES]>,
    add: Option<[u8; DIGEST_BYTES]>,
) -> io::Result<()> {
    let key = MerkleTreeStateKey::Leaf {
        table: table.to_owned(),
        index,
    };
    let mut leaf = match state.get(&key).map(Cow::into_owned) {
        Some(MerkleTreeStateValue::Leaf(leaf)) => leaf,
        Some(_) => return Err(invalid_state("expected Merkle leaf state value")),
        None => MerkleLeaf::empty(index),
    };

    apply_leaf_delta(&mut leaf, index, remove, add)?;
    if leaf.entries == 0 {
        state.delete(&key)?;
    } else {
        state.put(key, MerkleTreeStateValue::Leaf(leaf))?;
    }
    Ok(())
}

fn apply_leaf_delta(
    leaf: &mut MerkleLeaf,
    index: u64,
    remove: Option<[u8; DIGEST_BYTES]>,
    add: Option<[u8; DIGEST_BYTES]>,
) -> io::Result<()> {
    if let Some(hash) = remove {
        if leaf.entries == 0 {
            return Err(invalid_state("Merkle leaf entry count underflow"));
        }
        xor_into(&mut leaf.xor, &hash);
        leaf.entries -= 1;
    }
    if let Some(hash) = add {
        xor_into(&mut leaf.xor, &hash);
        leaf.entries += 1;
    }
    *leaf = leaf_summary(index, leaf.entries, leaf.xor);
    Ok(())
}

fn rebuild_tree_from_leaves(
    state: &mut crate::State<MerkleTreeStateKey, MerkleTreeStateValue>,
    table: &str,
    leaf_bits: u8,
    touched_leaves: HashSet<u64>,
) -> io::Result<()> {
    let mut touched = touched_leaves;
    for level in 1..=leaf_bits {
        let parents: HashSet<u64> = touched.iter().map(|index| index >> 1).collect();
        for parent_index in &parents {
            let left_index = parent_index << 1;
            let right_index = left_index | 1;
            let left = child_summary(state, table, level - 1, left_index)?;
            let right = child_summary(state, table, level - 1, right_index)?;
            let entries = left.entries + right.entries;
            let hash = node_hash(level, *parent_index, entries, left.hash, right.hash)?;

            if level == leaf_bits {
                state.put(
                    MerkleTreeStateKey::Root {
                        table: table.to_owned(),
                    },
                    MerkleTreeStateValue::Root(MerkleRoot {
                        leaf_bits,
                        entries,
                        hash,
                    }),
                )?;
            } else {
                let key = MerkleTreeStateKey::Node {
                    table: table.to_owned(),
                    level,
                    index: *parent_index,
                };
                if entries == 0 {
                    state.delete(&key)?;
                } else {
                    state.put(
                        key,
                        MerkleTreeStateValue::Node(MerkleNode { entries, hash }),
                    )?;
                }
            }
        }
        touched = parents;
    }
    Ok(())
}

fn rebuild_tree_from_leaf_map(
    state: &mut crate::State<MerkleTreeStateKey, MerkleTreeStateValue>,
    table: &str,
    leaf_bits: u8,
    leaves: HashMap<u64, MerkleLeaf>,
) -> io::Result<()> {
    let mut current = leaves
        .into_iter()
        .map(|(index, leaf)| {
            (
                index,
                MerkleNode {
                    entries: leaf.entries,
                    hash: leaf.hash,
                },
            )
        })
        .collect::<HashMap<_, _>>();

    for level in 1..=leaf_bits {
        let mut parents = HashMap::<u64, MerkleNode>::new();
        for (&index, node) in &current {
            let parent_index = index >> 1;
            let entry = parents.entry(parent_index).or_insert_with(|| MerkleNode {
                entries: 0,
                hash: ZERO_DIGEST,
            });
            entry.entries += node.entries;
        }

        for (&parent_index, parent) in parents.iter_mut() {
            let left = current
                .get(&(parent_index << 1))
                .map(|node| node.hash)
                .unwrap_or(ZERO_DIGEST);
            let right = current
                .get(&((parent_index << 1) | 1))
                .map(|node| node.hash)
                .unwrap_or(ZERO_DIGEST);
            parent.hash = node_hash(level, parent_index, parent.entries, left, right)?;
        }

        if level < leaf_bits {
            for (index, node) in &parents {
                state.put(
                    MerkleTreeStateKey::Node {
                        table: table.to_owned(),
                        level,
                        index: *index,
                    },
                    MerkleTreeStateValue::Node(node.clone()),
                )?;
            }
        }
        current = parents;
    }

    let root_node = current.remove(&0).unwrap_or(MerkleNode {
        entries: 0,
        hash: empty_root_hash(leaf_bits)?,
    });
    state.put(
        MerkleTreeStateKey::Root {
            table: table.to_owned(),
        },
        MerkleTreeStateValue::Root(MerkleRoot {
            leaf_bits,
            entries: root_node.entries,
            hash: root_node.hash,
        }),
    )?;
    Ok(())
}

fn child_summary(
    state: &crate::State<MerkleTreeStateKey, MerkleTreeStateValue>,
    table: &str,
    level: u8,
    index: u64,
) -> io::Result<MerkleNode> {
    if level == 0 {
        let key = MerkleTreeStateKey::Leaf {
            table: table.to_owned(),
            index,
        };
        return match state.get(&key).map(Cow::into_owned) {
            Some(MerkleTreeStateValue::Leaf(leaf)) => Ok(MerkleNode {
                entries: leaf.entries,
                hash: leaf.hash,
            }),
            Some(_) => Err(invalid_state("expected Merkle leaf state value")),
            None => Ok(MerkleNode {
                entries: 0,
                hash: ZERO_DIGEST,
            }),
        };
    }

    let key = MerkleTreeStateKey::Node {
        table: table.to_owned(),
        level,
        index,
    };
    match state.get(&key).map(Cow::into_owned) {
        Some(MerkleTreeStateValue::Node(node)) => Ok(node),
        Some(_) => Err(invalid_state("expected Merkle node state value")),
        None => Ok(MerkleNode {
            entries: 0,
            hash: ZERO_DIGEST,
        }),
    }
}

fn entry_digest(leaf_bits: u8, key: &PrimaryKey, cell: &Cell) -> io::Result<MerkleEntry> {
    let key_bytes = encode(key)?;
    let token = token(&key_bytes);
    let leaf = token >> (64 - leaf_bits);
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"zendb.merkle.entry.v1");
    hasher.update(&(key_bytes.len() as u64).to_be_bytes());
    hasher.update(&key_bytes);
    hasher.update(cell.hlc.as_bytes());
    hasher.update(&[u8::from(cell.is_tombstone())]);
    hasher.update(&value_hash(cell)?.as_bytes()[..]);
    Ok(MerkleEntry {
        leaf,
        hash: *hasher.finalize().as_bytes(),
    })
}

fn value_hash(cell: &Cell) -> io::Result<blake3::Hash> {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"zendb.merkle.value.v1");
    if let Some(value) = &cell.value {
        hasher.update(&encode(value)?);
    }
    Ok(hasher.finalize())
}

fn token(key_bytes: &[u8]) -> u64 {
    let hash = blake3::hash(key_bytes);
    let mut bytes = [0; 8];
    bytes.copy_from_slice(&hash.as_bytes()[..8]);
    u64::from_be_bytes(bytes)
}

fn leaf_summary(index: u64, entries: u64, xor: [u8; DIGEST_BYTES]) -> MerkleLeaf {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"zendb.merkle.leaf.v1");
    hasher.update(&index.to_be_bytes());
    hasher.update(&entries.to_be_bytes());
    hasher.update(&xor);
    MerkleLeaf {
        entries,
        xor,
        hash: *hasher.finalize().as_bytes(),
    }
}

fn node_hash(
    level: u8,
    index: u64,
    entries: u64,
    left: [u8; DIGEST_BYTES],
    right: [u8; DIGEST_BYTES],
) -> io::Result<[u8; DIGEST_BYTES]> {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"zendb.merkle.node.v1");
    hasher.update(&[level]);
    hasher.update(&index.to_be_bytes());
    hasher.update(&entries.to_be_bytes());
    hasher.update(&left);
    hasher.update(&right);
    Ok(*hasher.finalize().as_bytes())
}

fn empty_root_hash(leaf_bits: u8) -> io::Result<[u8; DIGEST_BYTES]> {
    node_hash(leaf_bits, 0, 0, ZERO_DIGEST, ZERO_DIGEST)
}

fn encode<T: Encode>(value: &T) -> io::Result<Vec<u8>> {
    bincode::encode_to_vec(value, bincode::config::standard())
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))
}

fn xor_into(target: &mut [u8; DIGEST_BYTES], hash: &[u8; DIGEST_BYTES]) {
    for (target, value) in target.iter_mut().zip(hash.iter()) {
        *target ^= *value;
    }
}

fn invalid_state(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
