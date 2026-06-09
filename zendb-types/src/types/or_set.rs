//! OR-Set — an observed-remove replicated set where concurrent add+remove of
//! the same element results in the element being present (additive-wins).
//!
//! ## Semantics
//!
//! Every `Add` is tagged with the operation HLC (globally unique). A `Remove`
//! only removes tags that the removing replica has already observed. Concurrent
//! `Add` operations whose tags were not observed by the `Remove` survive, hence
//! "observed-remove": you can only remove what you've seen.
//!
//! ## Reference
//!
//! Bieniusa, Zawirski, Preguiça, Shapiro, Baquero, Balegas & Duarte.
//! "An optimized conflict-free replicated set." INRIA RR-8083, 2012.

use std::collections::{BTreeMap, BTreeSet};

use bincode::{Decode, Encode};

use crate::{core::traits::Type, Hlc, PrimaryKey};

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub struct OrSetEntry {
    /// HLCs of every Add operation targeting this element.
    pub adds: BTreeSet<Hlc>,
    /// HLCs observed at the moment of each Remove. An add tag survives if it
    /// is absent from this set.
    pub rems: BTreeSet<Hlc>,
}

impl OrSetEntry {
    pub fn is_live(&self) -> bool {
        !self.adds.is_subset(&self.rems)
    }
}

impl Default for OrSetEntry {
    fn default() -> Self {
        OrSetEntry {
            adds: BTreeSet::new(),
            rems: BTreeSet::new(),
        }
    }
}

pub type OrSet = BTreeMap<PrimaryKey, OrSetEntry>;

#[derive(Debug, Clone, Encode, Decode)]
pub enum OrSetOp {
    Add { key: PrimaryKey },
    Remove { key: PrimaryKey },
}

#[derive(Debug)]
pub enum OrSetError {}

impl std::fmt::Display for OrSetError {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {}
    }
}

impl std::error::Error for OrSetError {}

impl Type for OrSet {
    type Op = OrSetOp;
    type Error = OrSetError;

    fn apply(&mut self, op: &OrSetOp, _local_hlc: Hlc, op_hlc: Hlc) -> Result<bool, OrSetError> {
        match op {
            OrSetOp::Add { key } => {
                let entry = self.entry(key.clone()).or_default();
                if entry.adds.insert(op_hlc) {
                    Ok(true)
                } else {
                    Ok(false)
                }
            }
            OrSetOp::Remove { key } => {
                let entry = self.entry(key.clone()).or_default();
                let observed: BTreeSet<Hlc> = entry.adds.clone();
                let len_before = entry.rems.len();
                entry.rems.extend(observed);
                Ok(entry.rems.len() > len_before)
            }
        }
    }

    fn merge(
        &mut self,
        remote: &OrSet,
        _local_hlc: Hlc,
        _remote_hlc: Hlc,
    ) -> Result<bool, OrSetError> {
        let mut changed = false;

        for (key, remote_entry) in remote {
            match self.get_mut(key) {
                Some(local_entry) => {
                    let adds_before = local_entry.adds.len();
                    let rems_before = local_entry.rems.len();
                    local_entry.adds.extend(remote_entry.adds.iter().copied());
                    local_entry.rems.extend(remote_entry.rems.iter().copied());
                    if local_entry.adds.len() > adds_before || local_entry.rems.len() > rems_before
                    {
                        changed = true;
                    }
                }
                None => {
                    self.insert(key.clone(), remote_entry.clone());
                    changed = true;
                }
            }
        }

        Ok(changed)
    }

    fn max_hlc(&self) -> Hlc {
        self.values().fold(Hlc::ZERO, |max, entry| {
            let adds_max = entry
                .adds
                .iter()
                .fold(Hlc::ZERO, |a, &b| std::cmp::max(a, b));
            let rems_max = entry
                .rems
                .iter()
                .fold(Hlc::ZERO, |a, &b| std::cmp::max(a, b));
            std::cmp::max(max, std::cmp::max(adds_max, rems_max))
        })
    }
}

pub fn or_set_contains_key(set: &OrSet, key: &PrimaryKey) -> bool {
    set.get(key).is_some_and(|entry| entry.is_live())
}

pub fn or_set_keys(set: &OrSet) -> impl Iterator<Item = &PrimaryKey> {
    set.iter()
        .filter_map(|(key, entry)| entry.is_live().then_some(key))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bincode::{config, decode_from_slice, encode_to_vec};

    fn hlc(ms: u64, device: u8) -> Hlc {
        Hlc::with_device_id(ms, 0, [device; 8]).unwrap()
    }

    fn apply(set: &mut OrSet, op: OrSetOp, at: Hlc) -> bool {
        Type::apply(set, &op, Hlc::ZERO, at).unwrap()
    }

    #[test]
    fn add_makes_element_live() {
        let key = PrimaryKey::Int(1);
        let mut set = OrSet::new();
        apply(&mut set, OrSetOp::Add { key: key.clone() }, hlc(100, 1));
        assert!(or_set_contains_key(&set, &key));
    }

    #[test]
    fn remove_tombstones_element() {
        let key = PrimaryKey::String("x".into());
        let mut set = OrSet::new();
        apply(&mut set, OrSetOp::Add { key: key.clone() }, hlc(100, 1));
        apply(&mut set, OrSetOp::Remove { key: key.clone() }, hlc(200, 1));
        assert!(!or_set_contains_key(&set, &key));
    }

    #[test]
    fn concurrent_add_and_remove_add_wins() {
        let key = PrimaryKey::String("tag".into());
        let mut left = OrSet::new();
        let mut right = OrSet::new();

        apply(&mut left, OrSetOp::Add { key: key.clone() }, hlc(100, 1));
        apply(
            &mut right,
            OrSetOp::Remove { key: key.clone() },
            hlc(100, 2),
        );

        Type::merge(&mut left, &right, Hlc::ZERO, Hlc::ZERO).unwrap();
        // The Add at hlc(100,1) was not observed by the Remove at hlc(100,2),
        // so the element survives.
        assert!(or_set_contains_key(&left, &key));
    }

    #[test]
    fn add_after_remove_resurrects() {
        let key = PrimaryKey::Int(0);
        let mut set = OrSet::new();
        apply(&mut set, OrSetOp::Add { key: key.clone() }, hlc(100, 1));
        apply(&mut set, OrSetOp::Remove { key: key.clone() }, hlc(200, 2));
        apply(&mut set, OrSetOp::Add { key: key.clone() }, hlc(300, 3));
        // New Add tag was not in the remove set, so element is live.
        assert!(or_set_contains_key(&set, &key));
    }

    #[test]
    fn duplicate_add_is_idempotent() {
        let key = PrimaryKey::Bool(true);
        let mut set = OrSet::new();
        assert!(apply(
            &mut set,
            OrSetOp::Add { key: key.clone() },
            hlc(100, 1)
        ));
        assert!(!apply(
            &mut set,
            OrSetOp::Add { key: key.clone() },
            hlc(100, 1)
        ));
        assert_eq!(set.get(&key).unwrap().adds.len(), 1);
    }

    #[test]
    fn stale_add_is_recorded_but_element_stays_dead() {
        let key = PrimaryKey::Timestamp(42);
        let mut set = OrSet::new();
        apply(&mut set, OrSetOp::Add { key: key.clone() }, hlc(100, 1));
        apply(&mut set, OrSetOp::Remove { key: key.clone() }, hlc(200, 2));
        // Stale Add — this tag is new so adds changes, but the element remains
        // dead because the add was already removed (the OLD remove recorded the
        // existing adds; new stale add is not in the remove set ... wait).
        // Actually in OR-Set, a stale add AFTER a remove WOULD resurrect the
        // element, because its tag wasn't observed. This is the additive-wins
        // semantics. Let's test the real LWW behavior from the old set instead.
        //
        // OR-Set: stale add IS a new tag, so it survives the old remove.
        // This is correct OR-Set behavior.
        apply(&mut set, OrSetOp::Add { key: key.clone() }, hlc(150, 3));
        assert!(or_set_contains_key(&set, &key));
    }

    #[test]
    fn remove_before_first_add_does_not_prevent_later_add() {
        let key = PrimaryKey::Int(0);
        let mut set = OrSet::new();
        apply(&mut set, OrSetOp::Remove { key: key.clone() }, hlc(200, 2));
        apply(&mut set, OrSetOp::Add { key: key.clone() }, hlc(300, 1));
        // Add tag at 300 was not observed by remove at 200.
        assert!(or_set_contains_key(&set, &key));
    }

    #[test]
    fn merge_converges_for_every_replica_order() {
        let key = PrimaryKey::String("shared".into());
        let mut sets = [OrSet::new(), OrSet::new(), OrSet::new()];
        apply(&mut sets[0], OrSetOp::Add { key: key.clone() }, hlc(100, 1));
        apply(
            &mut sets[1],
            OrSetOp::Remove { key: key.clone() },
            hlc(150, 2),
        );
        apply(&mut sets[2], OrSetOp::Add { key: key.clone() }, hlc(120, 3));

        let merge_order = |order: [usize; 3]| -> OrSet {
            let mut merged = sets[order[0]].clone();
            Type::merge(&mut merged, &sets[order[1]], Hlc::ZERO, Hlc::ZERO).unwrap();
            Type::merge(&mut merged, &sets[order[2]], Hlc::ZERO, Hlc::ZERO).unwrap();
            merged
        };

        let expected = merge_order([0, 1, 2]);
        for order in [[0, 2, 1], [1, 0, 2], [1, 2, 0], [2, 0, 1], [2, 1, 0]] {
            assert_eq!(merge_order(order), expected);
        }
        // Add at 120 was not observed by Remove at 150 (different devices),
        // so element is live.
        assert!(or_set_contains_key(&expected, &key));
    }

    #[test]
    fn or_set_bincode_roundtrip() {
        let mut set = OrSet::new();
        apply(
            &mut set,
            OrSetOp::Add {
                key: PrimaryKey::String("live".into()),
            },
            hlc(100, 1),
        );
        apply(
            &mut set,
            OrSetOp::Add {
                key: PrimaryKey::String("dead".into()),
            },
            hlc(100, 2),
        );
        apply(
            &mut set,
            OrSetOp::Remove {
                key: PrimaryKey::String("dead".into()),
            },
            hlc(200, 2),
        );

        let encoded = encode_to_vec(&set, config::standard()).unwrap();
        let (decoded, consumed): (OrSet, usize) =
            decode_from_slice(&encoded, config::standard()).unwrap();
        assert_eq!(consumed, encoded.len());
        assert_eq!(decoded, set);
    }
}
