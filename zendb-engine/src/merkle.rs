//! MerkleTree — anti-entropy primitive built from table row hashes.
//!
//! ## Structure
//!
//! ```text
//!                     ┌─────────┐
//!                     │  Root   │  H(L0 || L1 || ...)
//!                     └────┬────┘
//!              ┌───────────┴───────────┐
//!         ┌────┴────┐             ┌────┴────┐
//!         │ Leaf 0  │             │ Leaf 1  │
//!         │"aa"-"mz"│             │"na"-"zz"│
//!         └─────────┘             └─────────┘
//! ```
//!
//! Each leaf covers a fixed number of rows.  Leaf hashes include the row
//! HLC so that any write to a row changes the leaf hash and cascades up
//! to the root.  Two devices compare root hashes; if they differ, they
//! descend to isolate the differing leaf ranges.
//!
//! ## Hash formula
//!
//! ```text
//! row_hash = blake3(key || hlc || encoded_cell)
//! leaf_hash = blake3(row_hash_0 || ... || row_hash_N-1)
//! node_hash = blake3(left_child || right_child)
//! ```

use std::io;

use blake3::Hasher;
use zendb_storage::core::backend::Backend;
use zendb_types::Cell;

/// Rows per Merkle leaf.
const LEAF_SIZE: usize = 256;

pub struct MerkleTree {
    /// Leaf-level hashes.
    leaves: Vec<MerkleLeaf>,
    /// Internal node hashes, one Vec per level.  `levels[0]` is above leaves.
    levels: Vec<Vec<[u8; 32]>>,
    /// Root hash.
    root: [u8; 32],
}

#[derive(Clone)]
pub struct MerkleLeaf {
    /// Starting key (inclusive).
    pub key_start: Vec<u8>,
    /// Ending key (inclusive, or empty for last leaf).
    pub key_end: Vec<u8>,
    /// Hash of all row hashes in this leaf.
    pub hash: [u8; 32],
    /// Number of rows in this leaf.
    pub row_count: usize,
}

impl MerkleTree {
    /// Build a Merkle tree from a backend's current contents.
    ///
    /// Entries are read via `backend.entries()`. For ordered backends
    /// (e.g. `BPlusTree`) this yields keys in sort order, which is what
    /// peers need for deterministic leaf boundaries; unordered backends
    /// will produce stable-but-arbitrary leaf hashes and should be used
    /// only when the caller doesn't rely on inter-peer comparability.
    /// Generic over any backend. No `dyn` because the `Backend` trait
    /// is not object-safe (its iterator methods return `impl Iterator`).
    pub fn build<B: Backend<Vec<u8>, Vec<u8>>>(backend: &B) -> io::Result<MerkleTree> {
        let mut leaves: Vec<MerkleLeaf> = Vec::new();
        let mut current_hasher = Hasher::new();
        let mut row_count = 0usize;
        let mut first_key: Option<Vec<u8>> = None;
        let mut last_key: Vec<u8> = Vec::new();

        for (key, value) in backend.entries() {
            // `key` / `value` are `&ArchivedVec<u8>` borrowed from mmap.
            let key_bytes: Vec<u8> = key.as_slice().to_vec();
            if row_count == 0 {
                first_key = Some(key_bytes.clone());
            }
            last_key = key_bytes.clone();

            // Decode cell and compute row hash.
            let value_bytes: Vec<u8> = value.as_slice().to_vec();
            if let Some(cell) = decode_cell(&value_bytes) {
                let rh = row_hash(&key_bytes, &cell);
                current_hasher.update(&rh);
            }

            row_count += 1;

            if row_count >= LEAF_SIZE {
                let leaf = MerkleLeaf {
                    key_start: first_key.take().unwrap_or_default(),
                    key_end: last_key.clone(),
                    hash: current_hasher.finalize().into(),
                    row_count,
                };
                leaves.push(leaf);
                current_hasher = Hasher::new();
                row_count = 0;
            }
        }

        // Final partial leaf.
        if row_count > 0 {
            leaves.push(MerkleLeaf {
                key_start: first_key.unwrap_or_default(),
                key_end: last_key,
                hash: current_hasher.finalize().into(),
                row_count,
            });
        }

        let levels = build_levels(&leaves);
        let root = if levels.is_empty() {
            if leaves.len() == 1 {
                leaves[0].hash
            } else {
                [0u8; 32]
            }
        } else {
            levels.last().unwrap()[0]
        };

        Ok(MerkleTree {
            leaves,
            levels,
            root,
        })
    }

    /// Root hash for comparison with a remote peer.
    pub fn root_hash(&self) -> &[u8; 32] {
        &self.root
    }

    /// Number of leaves.
    pub fn leaf_count(&self) -> usize {
        self.leaves.len()
    }

    /// Get a leaf by index.
    pub fn leaf(&self, i: usize) -> Option<&MerkleLeaf> {
        self.leaves.get(i)
    }

    /// Compare with a remote tree and return the differing leaf indices.
    ///
    /// The caller provides a function that fetches a node hash from the
    /// remote peer at a given level and index.  Returns the indices of
    /// leaves whose hashes differ.
    pub fn diff(
        &self,
        remote_root: &[u8; 32],
        mut fetch_node: impl FnMut(usize, usize) -> io::Result<[u8; 32]>,
    ) -> io::Result<Vec<usize>> {
        if remote_root == &self.root {
            return Ok(Vec::new());
        }

        // No internal levels — compare leaves directly.
        if self.levels.is_empty() {
            let mut diff = Vec::new();
            for i in 0..self.leaves.len() {
                let remote = fetch_node(0, i)?;
                if remote != self.leaves[i].hash {
                    diff.push(i);
                }
            }
            return Ok(diff);
        }

        // Walk down from root.
        let mut pending: Vec<(usize, usize)> = vec![(self.levels.len() - 1, 0)];
        let mut diff_leaves = Vec::new();

        while let Some((level, idx)) = pending.pop() {
            let remote = fetch_node(level, idx)?;
            let local = if level == self.levels.len() - 1 && idx == 0 {
                self.root
            } else {
                self.levels[level][idx]
            };

            if remote == local {
                continue;
            }

            if level == 0 {
                // Children are leaves.
                let left = idx * 2;
                let right = (idx * 2 + 1).min(self.leaves.len().saturating_sub(1));
                for leaf_idx in left..=right {
                    let remote_leaf = fetch_node(0, leaf_idx)?; // level 0 = leaves
                    if remote_leaf != self.leaves[leaf_idx].hash {
                        diff_leaves.push(leaf_idx);
                    }
                }
            } else {
                let left = idx * 2;
                let right = (idx * 2 + 1).min(self.levels[level - 1].len() - 1);
                pending.push((level - 1, right));
                pending.push((level - 1, left));
            }
        }

        Ok(diff_leaves)
    }
}

/// Compute the hash of a single row: blake3(key || hlc_bytes || encoded_cell_bytes).
fn row_hash(key: &[u8], cell: &Cell) -> [u8; 32] {
    let mut h = Hasher::new();
    h.update(key);
    h.update(&cell.hlc.as_bytes()[..]);
    let mut buf = Vec::new();
    if cell.encode(&mut buf).is_ok() {
        h.update(&buf);
    }
    h.finalize().into()
}

fn decode_cell(bytes: &[u8]) -> Option<Cell> {
    Cell::decode(bytes).ok().map(|(c, _)| c)
}

/// Build internal node levels from leaf hashes.
fn build_levels(leaves: &[MerkleLeaf]) -> Vec<Vec<[u8; 32]>> {
    if leaves.is_empty() {
        return Vec::new();
    }

    let mut levels: Vec<Vec<[u8; 32]>> = Vec::new();
    let mut current: Vec<[u8; 32]> = leaves.iter().map(|l| l.hash).collect();

    while current.len() > 1 {
        let mut parent = Vec::with_capacity((current.len() + 1) / 2);
        for chunk in current.chunks(2) {
            let mut h = Hasher::new();
            h.update(&chunk[0]);
            if chunk.len() > 1 {
                h.update(&chunk[1]);
            } else {
                h.update(&chunk[0]); // duplicate for odd child
            }
            parent.push(h.finalize().into());
        }
        levels.push(parent.clone());
        current = parent;
    }

    levels
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_tree() {
        let tree = MerkleTree {
            leaves: Vec::new(),
            levels: Vec::new(),
            root: [0u8; 32],
        };
        assert_eq!(tree.leaf_count(), 0);
        assert_eq!(tree.root_hash(), &[0u8; 32]);
    }

    #[test]
    fn single_leaf() {
        let leaf = MerkleLeaf {
            key_start: b"a".to_vec(),
            key_end: b"z".to_vec(),
            hash: [1u8; 32],
            row_count: 10,
        };
        let levels = build_levels(&[leaf.clone()]);
        assert!(levels.is_empty()); // single leaf, no internal nodes
    }

    #[test]
    fn two_leaves_produce_parent() {
        let leaves = vec![
            MerkleLeaf {
                key_start: b"a".to_vec(),
                key_end: b"m".to_vec(),
                hash: [1u8; 32],
                row_count: 5,
            },
            MerkleLeaf {
                key_start: b"n".to_vec(),
                key_end: b"z".to_vec(),
                hash: [2u8; 32],
                row_count: 5,
            },
        ];
        let levels = build_levels(&leaves);
        assert_eq!(levels.len(), 1);
        assert_eq!(levels[0].len(), 1);
        // Parent hash should differ from children.
        assert_ne!(levels[0][0], [1u8; 32]);
        assert_ne!(levels[0][0], [2u8; 32]);
    }

    #[test]
    fn diff_identical_returns_empty() {
        let leaves = vec![MerkleLeaf {
            key_start: b"a".to_vec(),
            key_end: b"z".to_vec(),
            hash: [42u8; 32],
            row_count: 1,
        }];
        let levels = build_levels(&leaves);
        let root = if levels.is_empty() {
            leaves[0].hash
        } else {
            levels[0][0]
        };
        let tree = MerkleTree {
            leaves,
            levels,
            root,
        };

        let differing = tree.diff(&root, |_level, _idx| Ok([42u8; 32])).unwrap();
        assert!(differing.is_empty());
    }
}
