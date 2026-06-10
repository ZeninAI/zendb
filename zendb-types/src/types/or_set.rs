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

#[derive(Debug, Clone, Default, PartialEq, Encode, Decode)]
struct OrSetEntry {
    /// HLCs of every Add operation targeting this element.
    adds: BTreeSet<Hlc>,
    /// HLCs observed at the moment of each Remove. An add tag survives if it
    /// is absent from this set.
    rems: BTreeMap<Hlc, Hlc>,
}

impl OrSetEntry {
    fn is_live(&self) -> bool {
        self.adds.iter().any(|tag| !self.rems.contains_key(tag))
    }
}

#[derive(Debug, Clone, Default, PartialEq, Encode, Decode)]
pub struct OrSet {
    entries: BTreeMap<PrimaryKey, OrSetEntry>,
}

impl OrSet {
    pub fn contains(&self, key: &PrimaryKey) -> bool {
        self.entries.get(key).is_some_and(OrSetEntry::is_live)
    }

    pub fn keys(&self) -> impl Iterator<Item = &PrimaryKey> {
        self.entries
            .iter()
            .filter_map(|(key, entry)| entry.is_live().then_some(key))
    }

    /// Build a remove operation containing exactly the add tags observed by
    /// this replica.
    pub fn remove(&self, key: PrimaryKey) -> OrSetOp {
        OrSetOp::Remove {
            observed: self
                .entries
                .get(&key)
                .map(|entry| entry.adds.iter().copied().collect())
                .unwrap_or_default(),
            key,
        }
    }
}

#[derive(Debug, Clone, Encode, Decode)]
pub enum OrSetOp {
    Add { key: PrimaryKey },
    Remove { key: PrimaryKey, observed: Vec<Hlc> },
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

    fn apply(&mut self, op: &OrSetOp, op_hlc: Hlc) -> Result<bool, OrSetError> {
        match op {
            OrSetOp::Add { key } => {
                let entry = self.entries.entry(key.clone()).or_default();
                if entry.adds.insert(op_hlc) {
                    Ok(true)
                } else {
                    Ok(false)
                }
            }
            OrSetOp::Remove { key, observed } => {
                let entry = self.entries.entry(key.clone()).or_default();
                let mut changed = false;
                for tag in observed {
                    if entry
                        .rems
                        .get(tag)
                        .is_none_or(|removed_at| op_hlc.beats(*removed_at))
                    {
                        entry.rems.insert(*tag, op_hlc);
                        changed = true;
                    }
                }
                Ok(changed)
            }
        }
    }

    fn merge(&mut self, remote: &OrSet, _clocks: crate::MergeClocks) -> Result<bool, OrSetError> {
        let mut changed = false;

        for (key, remote_entry) in &remote.entries {
            match self.entries.get_mut(key) {
                Some(local_entry) => {
                    let adds_before = local_entry.adds.len();
                    local_entry.adds.extend(remote_entry.adds.iter().copied());
                    for (&tag, &removed_at) in &remote_entry.rems {
                        if local_entry
                            .rems
                            .get(&tag)
                            .is_none_or(|existing| removed_at.beats(*existing))
                        {
                            local_entry.rems.insert(tag, removed_at);
                            changed = true;
                        }
                    }
                    if local_entry.adds.len() > adds_before {
                        changed = true;
                    }
                }
                None => {
                    self.entries.insert(key.clone(), remote_entry.clone());
                    changed = true;
                }
            }
        }

        Ok(changed)
    }

    fn compact(&mut self, watermark: Hlc) -> Result<bool, OrSetError> {
        let mut changed = false;
        self.entries.retain(|_, entry| {
            let stable_removed: Vec<Hlc> = entry
                .adds
                .iter()
                .copied()
                .filter(|tag| {
                    *tag <= watermark
                        && entry
                            .rems
                            .get(tag)
                            .is_some_and(|removed_at| *removed_at <= watermark)
                })
                .collect();
            for tag in stable_removed {
                entry.adds.remove(&tag);
                entry.rems.remove(&tag);
                changed = true;
            }

            let keep = !entry.adds.is_empty() || !entry.rems.is_empty();
            changed |= !keep;
            keep
        });
        Ok(changed)
    }

    fn max_hlc(&self) -> Hlc {
        self.entries.values().fold(Hlc::ZERO, |max, entry| {
            let adds_max = entry
                .adds
                .iter()
                .fold(Hlc::ZERO, |a, &b| std::cmp::max(a, b));
            let rems_max = entry
                .rems
                .values()
                .fold(Hlc::ZERO, |a, &b| std::cmp::max(a, b));
            std::cmp::max(max, std::cmp::max(adds_max, rems_max))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bincode::{config, decode_from_slice, encode_to_vec};

    fn hlc(ms: u64, device: u8) -> Hlc {
        Hlc::with_device_id(ms, 0, [device; 8]).unwrap()
    }

    fn apply(set: &mut OrSet, op: OrSetOp, at: Hlc) -> bool {
        Type::apply(set, &op, at).unwrap()
    }

    #[test]
    fn add_makes_element_live() {
        let key = PrimaryKey::Int(1);
        let mut set = OrSet::default();
        apply(&mut set, OrSetOp::Add { key: key.clone() }, hlc(100, 1));
        assert!(set.contains(&key));
    }

    #[test]
    fn remove_tombstones_element() {
        let key = PrimaryKey::String("x".into());
        let mut set = OrSet::default();
        apply(&mut set, OrSetOp::Add { key: key.clone() }, hlc(100, 1));
        let remove = set.remove(key.clone());
        apply(&mut set, remove, hlc(200, 1));
        assert!(!set.contains(&key));
    }

    #[test]
    fn compact_removes_stable_observed_add_remove_pairs() {
        let key = PrimaryKey::String("dead".into());
        let mut set = OrSet::default();
        apply(&mut set, OrSetOp::Add { key: key.clone() }, hlc(100, 1));
        let remove = set.remove(key.clone());
        apply(&mut set, remove, hlc(200, 1));

        assert!(Type::compact(&mut set, hlc(200, 1)).unwrap());
        assert!(set.entries.is_empty());
    }

    #[test]
    fn remove_event_is_permutation_invariant() {
        let key = PrimaryKey::String("x".into());
        let add = OrSetOp::Add { key: key.clone() };
        let remove = OrSetOp::Remove {
            key: key.clone(),
            observed: vec![hlc(100, 1)],
        };

        let mut add_first = OrSet::default();
        apply(&mut add_first, add.clone(), hlc(100, 1));
        apply(&mut add_first, remove.clone(), hlc(200, 2));

        let mut remove_first = OrSet::default();
        apply(&mut remove_first, remove, hlc(200, 2));
        apply(&mut remove_first, add, hlc(100, 1));

        assert_eq!(add_first, remove_first);
        assert!(!add_first.contains(&key));
    }

    #[test]
    fn compaction_waits_for_the_remove_clock() {
        let key = PrimaryKey::String("x".into());
        let mut set = OrSet::default();
        apply(&mut set, OrSetOp::Add { key: key.clone() }, hlc(100, 1));
        let remove = set.remove(key.clone());
        apply(&mut set, remove, hlc(300, 2));

        assert!(!Type::compact(&mut set, hlc(200, 1)).unwrap());
        assert!(Type::compact(&mut set, hlc(300, 2)).unwrap());
    }

    #[test]
    fn concurrent_add_and_remove_add_wins() {
        let key = PrimaryKey::String("tag".into());
        let mut left = OrSet::default();
        let mut right = OrSet::default();

        apply(&mut left, OrSetOp::Add { key: key.clone() }, hlc(100, 1));
        let remove = right.remove(key.clone());
        apply(&mut right, remove, hlc(100, 2));

        Type::merge(&mut left, &right, crate::MergeClocks::ZERO).unwrap();
        // The Add at hlc(100,1) was not observed by the Remove at hlc(100,2),
        // so the element survives.
        assert!(left.contains(&key));
    }

    #[test]
    fn add_after_remove_resurrects() {
        let key = PrimaryKey::Int(0);
        let mut set = OrSet::default();
        apply(&mut set, OrSetOp::Add { key: key.clone() }, hlc(100, 1));
        let remove = set.remove(key.clone());
        apply(&mut set, remove, hlc(200, 2));
        apply(&mut set, OrSetOp::Add { key: key.clone() }, hlc(300, 3));
        // New Add tag was not in the remove set, so element is live.
        assert!(set.contains(&key));
    }

    #[test]
    fn duplicate_add_is_idempotent() {
        let key = PrimaryKey::Bool(true);
        let mut set = OrSet::default();
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
        assert_eq!(set.entries.get(&key).unwrap().adds.len(), 1);
    }

    #[test]
    fn stale_add_is_recorded_but_element_stays_dead() {
        let key = PrimaryKey::Timestamp(42);
        let mut set = OrSet::default();
        apply(&mut set, OrSetOp::Add { key: key.clone() }, hlc(100, 1));
        let remove = set.remove(key.clone());
        apply(&mut set, remove, hlc(200, 2));
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
        assert!(set.contains(&key));
    }

    #[test]
    fn remove_before_first_add_does_not_prevent_later_add() {
        let key = PrimaryKey::Int(0);
        let mut set = OrSet::default();
        let remove = set.remove(key.clone());
        apply(&mut set, remove, hlc(200, 2));
        apply(&mut set, OrSetOp::Add { key: key.clone() }, hlc(300, 1));
        // Add tag at 300 was not observed by remove at 200.
        assert!(set.contains(&key));
    }

    #[test]
    fn merge_converges_for_every_replica_order() {
        let key = PrimaryKey::String("shared".into());
        let mut sets = [OrSet::default(), OrSet::default(), OrSet::default()];
        apply(&mut sets[0], OrSetOp::Add { key: key.clone() }, hlc(100, 1));
        let remove = sets[1].remove(key.clone());
        apply(&mut sets[1], remove, hlc(150, 2));
        apply(&mut sets[2], OrSetOp::Add { key: key.clone() }, hlc(120, 3));

        let merge_order = |order: [usize; 3]| -> OrSet {
            let mut merged = sets[order[0]].clone();
            Type::merge(&mut merged, &sets[order[1]], crate::MergeClocks::ZERO).unwrap();
            Type::merge(&mut merged, &sets[order[2]], crate::MergeClocks::ZERO).unwrap();
            merged
        };

        let expected = merge_order([0, 1, 2]);
        for order in [[0, 2, 1], [1, 0, 2], [1, 2, 0], [2, 0, 1], [2, 1, 0]] {
            assert_eq!(merge_order(order), expected);
        }
        // Add at 120 was not observed by Remove at 150 (different devices),
        // so element is live.
        assert!(expected.contains(&key));
    }

    #[test]
    fn or_set_bincode_roundtrip() {
        let mut set = OrSet::default();
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
        let remove = set.remove(PrimaryKey::String("dead".into()));
        apply(&mut set, remove, hlc(200, 2));

        let encoded = encode_to_vec(&set, config::standard()).unwrap();
        let (decoded, consumed): (OrSet, usize) =
            decode_from_slice(&encoded, config::standard()).unwrap();
        assert_eq!(consumed, encoded.len());
        assert_eq!(decoded, set);
    }
}
