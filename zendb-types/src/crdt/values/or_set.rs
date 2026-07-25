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

use crate::{EventStamp, PrimaryKey, Type};

#[derive(Debug, Clone, Default, PartialEq, Encode, Decode)]
struct OrSetEntry {
    /// HLCs of every Add operation targeting this element.
    adds: BTreeSet<EventStamp>,
    /// HLCs observed at the moment of each Remove. An add tag survives if it
    /// is absent from this set.
    rems: BTreeMap<EventStamp, EventStamp>,
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
    Add {
        key: PrimaryKey,
    },
    Remove {
        key: PrimaryKey,
        observed: Vec<EventStamp>,
    },
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

    fn apply(&mut self, op: &OrSetOp, stamps: crate::MergeStamps) -> Result<bool, OrSetError> {
        let stamps = stamps.incoming;
        match op {
            OrSetOp::Add { key } => {
                let entry = self.entries.entry(key.clone()).or_default();
                if entry.adds.insert(stamps) {
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
                        .is_none_or(|removed_at| stamps.beats(*removed_at))
                    {
                        entry.rems.insert(*tag, stamps);
                        changed = true;
                    }
                }
                Ok(changed)
            }
        }
    }

    fn merge(&mut self, remote: &OrSet, _stamps: crate::MergeStamps) -> Result<bool, OrSetError> {
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

    fn max_stamp(&self) -> EventStamp {
        self.entries
            .values()
            .fold(EventStamp::zero(), |max, entry| {
                let adds_max = entry
                    .adds
                    .iter()
                    .fold(EventStamp::zero(), |a, &b| std::cmp::max(a, b));
                let rems_max = entry
                    .rems
                    .values()
                    .fold(EventStamp::zero(), |a, &b| std::cmp::max(a, b));
                std::cmp::max(max, std::cmp::max(adds_max, rems_max))
            })
    }
}
