//! Set - a deduplicated collection of primary-key-typed values.
//!
//! Every element is identified by its [`PrimaryKey`]. Membership is resolved
//! via per-element LWW metadata: an element is live when `updated > deleted`.

use std::collections::BTreeMap;

use bincode::{Decode, Encode};

use crate::{EventStamp, PrimaryKey, Type};

/// Per-element LWW clock pair that determines set membership.
#[derive(Debug, Clone, PartialEq, Encode, Decode)]
struct Meta {
    /// HLC of the latest Add operation targeting this element.
    updated: EventStamp,
    /// HLC of the latest Remove operation targeting this element.
    deleted: EventStamp,
}

impl Meta {
    fn is_live(&self) -> bool {
        self.updated > self.deleted
    }
}

impl Default for Meta {
    fn default() -> Self {
        Self {
            updated: EventStamp::default(),
            deleted: EventStamp::default(),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Encode, Decode)]
pub struct Set {
    entries: BTreeMap<PrimaryKey, Meta>,
}

impl Set {
    pub fn contains(&self, key: &PrimaryKey) -> bool {
        self.entries.get(key).is_some_and(Meta::is_live)
    }

    pub fn keys(&self) -> impl Iterator<Item = &PrimaryKey> {
        self.entries
            .iter()
            .filter_map(|(key, meta)| meta.is_live().then_some(key))
    }
}

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

    fn apply(&mut self, op: &SetOp, stamps: crate::MergeStamps) -> Result<bool, SetError> {
        let stamps = stamps.incoming;
        match op {
            SetOp::Add { key } => {
                let meta = self.entries.entry(key.clone()).or_default();
                if stamps > meta.updated {
                    meta.updated = stamps;
                    Ok(true)
                } else {
                    Ok(false)
                }
            }
            SetOp::Remove { key } => {
                let meta = self.entries.entry(key.clone()).or_default();
                if stamps > meta.deleted {
                    meta.deleted = stamps;
                    Ok(true)
                } else {
                    Ok(false)
                }
            }
        }
    }

    fn merge(&mut self, remote: &Set, _stamps: crate::MergeStamps) -> Result<bool, SetError> {
        let mut changed = false;

        for (key, remote_meta) in &remote.entries {
            match self.entries.get_mut(key) {
                Some(local_meta) => {
                    if remote_meta.updated > local_meta.updated {
                        local_meta.updated = remote_meta.updated;
                        changed = true;
                    }
                    if remote_meta.deleted > local_meta.deleted {
                        local_meta.deleted = remote_meta.deleted;
                        changed = true;
                    }
                }
                None => {
                    self.entries.insert(key.clone(), remote_meta.clone());
                    changed = true;
                }
            }
        }

        Ok(changed)
    }

    fn max_stamp(&self) -> EventStamp {
        self.entries
            .values()
            .fold(EventStamp::default(), |max, meta| {
                max.max(meta.updated).max(meta.deleted)
            })
    }
}
