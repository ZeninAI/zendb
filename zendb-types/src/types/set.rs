//! Set - a deduplicated collection of primary-key-typed values.
//!
//! Every element is identified by its [`PrimaryKey`]. Membership is resolved
//! via per-element LWW metadata: an element is live when `updated > deleted`.

use std::collections::BTreeMap;

use bincode::{Decode, Encode};

use crate::{core::traits::Type, Hlc, PrimaryKey};

/// Per-element LWW clock pair that determines set membership.
#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub struct Meta {
    /// HLC of the latest Add operation targeting this element.
    pub updated: Hlc,
    /// HLC of the latest Remove operation targeting this element.
    pub deleted: Hlc,
}

impl Meta {
    pub const fn new(updated: Hlc, deleted: Hlc) -> Self {
        Self { updated, deleted }
    }

    pub fn is_live(&self) -> bool {
        self.updated > self.deleted
    }
}

impl Default for Meta {
    fn default() -> Self {
        Self {
            updated: Hlc::ZERO,
            deleted: Hlc::ZERO,
        }
    }
}

pub type Set = BTreeMap<PrimaryKey, Meta>;

#[derive(Debug, Clone, Encode, Decode)]
pub enum SetOp {
    Add { key: PrimaryKey },
    Remove { key: PrimaryKey },
}

#[derive(Debug)]
pub enum SetError {}

impl std::fmt::Display for SetError {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {}
    }
}

impl std::error::Error for SetError {}

impl Type for Set {
    type Op = SetOp;
    type Error = SetError;

    fn apply(&mut self, op: &SetOp, _local_hlc: Hlc, op_hlc: Hlc) -> Result<bool, SetError> {
        match op {
            SetOp::Add { key } => {
                let meta = self.entry(key.clone()).or_default();
                if op_hlc.beats(meta.updated) {
                    meta.updated = op_hlc;
                    Ok(true)
                } else {
                    Ok(false)
                }
            }
            SetOp::Remove { key } => {
                let meta = self.entry(key.clone()).or_default();
                if op_hlc.beats(meta.deleted) {
                    meta.deleted = op_hlc;
                    Ok(true)
                } else {
                    Ok(false)
                }
            }
        }
    }

    fn merge(&mut self, remote: &Set, _local_hlc: Hlc, _remote_hlc: Hlc) -> Result<bool, SetError> {
        let mut changed = false;

        for (key, remote_meta) in remote {
            match self.get_mut(key) {
                Some(local_meta) => {
                    if remote_meta.updated.beats(local_meta.updated) {
                        local_meta.updated = remote_meta.updated;
                        changed = true;
                    }
                    if remote_meta.deleted.beats(local_meta.deleted) {
                        local_meta.deleted = remote_meta.deleted;
                        changed = true;
                    }
                }
                None => {
                    self.insert(key.clone(), remote_meta.clone());
                    changed = true;
                }
            }
        }

        Ok(changed)
    }

    fn max_hlc(&self) -> Hlc {
        self.values().fold(Hlc::ZERO, |max, meta| {
            max.max(meta.updated).max(meta.deleted)
        })
    }
}

pub fn set_contains_key(set: &Set, key: &PrimaryKey) -> bool {
    set.get(key).is_some_and(|meta| meta.is_live())
}

pub fn set_keys(set: &Set) -> impl Iterator<Item = &PrimaryKey> {
    set.iter()
        .filter_map(|(key, meta)| meta.is_live().then_some(key))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bincode::{config, decode_from_slice, encode_to_vec};

    fn hlc(ms: u64, device: u8) -> Hlc {
        Hlc::with_device_id(ms, 0, [device; 8]).unwrap()
    }

    fn apply(set: &mut Set, op: SetOp, at: Hlc) -> bool {
        Type::apply(set, &op, Hlc::ZERO, at).unwrap()
    }

    fn merge_order(sets: &[Set; 3], order: [usize; 3]) -> Set {
        let mut merged = sets[order[0]].clone();
        Type::merge(&mut merged, &sets[order[1]], Hlc::ZERO, Hlc::ZERO).unwrap();
        Type::merge(&mut merged, &sets[order[2]], Hlc::ZERO, Hlc::ZERO).unwrap();
        merged
    }

    #[test]
    fn add_makes_element_live() {
        let mut set = Set::new();
        apply(
            &mut set,
            SetOp::Add {
                key: PrimaryKey::Int(1),
            },
            hlc(100, 1),
        );

        assert!(set_contains_key(&set, &PrimaryKey::Int(1)));
        assert_eq!(set_keys(&set).count(), 1);
    }

    #[test]
    fn remove_tombstones_element() {
        let key = PrimaryKey::String("x".into());
        let mut set = Set::new();
        apply(&mut set, SetOp::Add { key: key.clone() }, hlc(100, 1));
        apply(&mut set, SetOp::Remove { key: key.clone() }, hlc(200, 1));

        assert!(!set_contains_key(&set, &key));
        assert_eq!(set_keys(&set).count(), 0);
    }

    #[test]
    fn duplicate_add_is_idempotent() {
        let key = PrimaryKey::Bool(true);
        let mut set = Set::new();
        assert!(apply(
            &mut set,
            SetOp::Add { key: key.clone() },
            hlc(100, 1)
        ));
        assert!(!apply(
            &mut set,
            SetOp::Add { key: key.clone() },
            hlc(100, 1)
        ));
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn stale_add_does_not_resurrect_tombstone() {
        let key = PrimaryKey::Timestamp(42);
        let mut set = Set::new();
        apply(&mut set, SetOp::Add { key: key.clone() }, hlc(100, 1));
        apply(&mut set, SetOp::Remove { key: key.clone() }, hlc(200, 2));
        // Stale Add at 150 beats the old Add at 100, so meta.updated is raised.
        // But deleted=200 still dominates, so the element stays dead.
        assert!(apply(
            &mut set,
            SetOp::Add { key: key.clone() },
            hlc(150, 3)
        ));
        assert!(!set_contains_key(&set, &key));
    }

    #[test]
    fn newer_add_beats_stale_remove() {
        let key = PrimaryKey::Blob(vec![1, 2, 3]);
        let mut set = Set::new();
        apply(&mut set, SetOp::Remove { key: key.clone() }, hlc(100, 1));
        apply(&mut set, SetOp::Add { key: key.clone() }, hlc(200, 2));

        assert!(set_contains_key(&set, &key));
    }

    #[test]
    fn remove_before_first_add_is_visible_as_tombstone() {
        let key = PrimaryKey::Int(0);
        let mut set = Set::new();
        apply(&mut set, SetOp::Remove { key: key.clone() }, hlc(200, 2));
        apply(&mut set, SetOp::Add { key: key.clone() }, hlc(100, 1));

        assert!(!set_contains_key(&set, &key));
    }

    #[test]
    fn different_types_are_distinct_elements() {
        let mut set = Set::new();
        apply(
            &mut set,
            SetOp::Add {
                key: PrimaryKey::String("42".into()),
            },
            hlc(100, 1),
        );
        apply(
            &mut set,
            SetOp::Add {
                key: PrimaryKey::Int(42),
            },
            hlc(100, 2),
        );

        assert_eq!(set_keys(&set).count(), 2);
    }

    #[test]
    fn all_primary_key_types_are_accepted() {
        let mut set = Set::new();
        apply(
            &mut set,
            SetOp::Add {
                key: PrimaryKey::Bool(true),
            },
            hlc(100, 1),
        );
        apply(
            &mut set,
            SetOp::Add {
                key: PrimaryKey::Int(1),
            },
            hlc(100, 1),
        );
        apply(
            &mut set,
            SetOp::Add {
                key: PrimaryKey::String("s".into()),
            },
            hlc(100, 1),
        );
        apply(
            &mut set,
            SetOp::Add {
                key: PrimaryKey::Timestamp(0),
            },
            hlc(100, 1),
        );
        apply(
            &mut set,
            SetOp::Add {
                key: PrimaryKey::Blob(vec![1, 2, 3]),
            },
            hlc(100, 1),
        );

        assert_eq!(set_keys(&set).count(), 5);
    }

    #[test]
    fn merge_converges_for_every_replica_order() {
        let key = PrimaryKey::String("shared".into());
        let mut sets = [Set::new(), Set::new(), Set::new()];
        apply(&mut sets[0], SetOp::Add { key: key.clone() }, hlc(100, 1));
        apply(
            &mut sets[1],
            SetOp::Remove { key: key.clone() },
            hlc(200, 2),
        );
        apply(&mut sets[2], SetOp::Add { key: key.clone() }, hlc(300, 3));

        let orders = [
            [0, 1, 2],
            [0, 2, 1],
            [1, 0, 2],
            [1, 2, 0],
            [2, 0, 1],
            [2, 1, 0],
        ];
        let expected = merge_order(&sets, orders[0]);
        for order in orders.into_iter().skip(1) {
            assert_eq!(merge_order(&sets, order), expected);
        }
        // hlc(300,3) Add beats hlc(200,2) Remove — element is live
        assert!(set_contains_key(&expected, &key));
    }

    #[test]
    fn merge_deleted_then_readded_by_different_peers_converges() {
        let key = PrimaryKey::String("tag".into());
        let mut sets = [Set::new(), Set::new(), Set::new()];
        apply(&mut sets[0], SetOp::Add { key: key.clone() }, hlc(100, 1)); // add
        apply(
            &mut sets[1],
            SetOp::Remove { key: key.clone() },
            hlc(200, 2),
        ); // remove
        apply(&mut sets[2], SetOp::Add { key: key.clone() }, hlc(150, 3)); // stale add

        let orders = [
            [0, 1, 2],
            [0, 2, 1],
            [1, 0, 2],
            [1, 2, 0],
            [2, 0, 1],
            [2, 1, 0],
        ];
        let expected = merge_order(&sets, orders[0]);
        for order in orders.into_iter().skip(1) {
            assert_eq!(merge_order(&sets, order), expected);
        }
        // hlc(200,2) Remove beats hlc(150,3) stale Add — element is dead
        assert!(!set_contains_key(&expected, &key));
    }

    #[test]
    fn max_hlc_tracks_highest_clock_across_both_fields() {
        let mut set = Set::new();
        apply(
            &mut set,
            SetOp::Add {
                key: PrimaryKey::Int(1),
            },
            hlc(100, 1),
        );
        apply(
            &mut set,
            SetOp::Remove {
                key: PrimaryKey::Int(1),
            },
            hlc(300, 2),
        );
        apply(
            &mut set,
            SetOp::Add {
                key: PrimaryKey::Int(2),
            },
            hlc(200, 1),
        );

        assert_eq!(set.max_hlc(), hlc(300, 2));
    }

    #[test]
    fn set_bincode_roundtrip() {
        let mut set = Set::new();
        apply(
            &mut set,
            SetOp::Add {
                key: PrimaryKey::String("live".into()),
            },
            hlc(100, 1),
        );
        apply(
            &mut set,
            SetOp::Add {
                key: PrimaryKey::String("dead".into()),
            },
            hlc(100, 2),
        );
        apply(
            &mut set,
            SetOp::Remove {
                key: PrimaryKey::String("dead".into()),
            },
            hlc(200, 2),
        );

        let encoded = encode_to_vec(&set, config::standard()).unwrap();
        let (decoded, consumed): (Set, usize) =
            decode_from_slice(&encoded, config::standard()).unwrap();
        assert_eq!(consumed, encoded.len());
        assert_eq!(decoded, set);
    }
}
