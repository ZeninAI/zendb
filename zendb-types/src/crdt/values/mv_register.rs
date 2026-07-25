//! MV-Register — a replicated register that preserves all concurrent assignments
//! rather than picking a single LWW winner.
//!
//! ## Semantics
//!
//! Each `Assign` records the value and the exact assignment IDs observed and
//! replaced by its creator. Concurrent assignments that were not observed are
//! preserved regardless of wall-clock ordering.
//!
//! ## Reference
//!
//! Shapiro, Preguiça, Baquero & Zawirski. "A comprehensive study of Convergent
//! and Commutative Replicated Data Types." INRIA RR-7506, 2011. §3.1 (MV-Register).
//!
//! Zawirski, Baquero, Bieniusa, Preguiça & Shapiro. "Eventually consistent
//! register revisited." PaPoC 2016.

use std::collections::BTreeMap;

use bincode::{Decode, Encode};

use crate::{EventStamp, Type, Value};

#[derive(Debug, Clone, Default, PartialEq, Encode, Decode)]
pub struct MvRegister {
    entries: BTreeMap<EventStamp, Value>,
    removed: BTreeMap<EventStamp, EventStamp>,
}

impl MvRegister {
    /// Return all currently visible concurrent values ordered by assignment ID.
    pub fn values(&self) -> impl ExactSizeIterator<Item = &Value> {
        self.entries.values()
    }

    pub fn assign(&self, value: Value) -> MvRegisterOp {
        MvRegisterOp::Assign {
            value,
            replaces: self.entries.keys().copied().collect(),
        }
    }
}

#[derive(Debug, Clone, Encode, Decode)]
pub enum MvRegisterOp {
    Assign {
        value: Value,
        replaces: Vec<EventStamp>,
    },
}

#[derive(Debug)]
pub enum MvRegisterError {
    AssignmentConflict { id: EventStamp },
}

impl std::fmt::Display for MvRegisterError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MvRegisterError::AssignmentConflict { id } => {
                write!(f, "assignment {id} has conflicting values")
            }
        }
    }
}

impl std::error::Error for MvRegisterError {}

impl Type for MvRegister {
    type Op = MvRegisterOp;
    type Error = MvRegisterError;

    fn apply(
        &mut self,
        op: &MvRegisterOp,
        stamps: crate::MergeStamps,
    ) -> Result<bool, MvRegisterError> {
        let stamps = stamps.incoming;
        match op {
            MvRegisterOp::Assign { value, replaces } => {
                if self
                    .entries
                    .get(&stamps)
                    .is_some_and(|existing| existing != value)
                {
                    return Err(MvRegisterError::AssignmentConflict { id: stamps });
                }

                let mut changed = false;
                for replaced in replaces {
                    if self
                        .removed
                        .get(replaced)
                        .is_none_or(|existing| stamps.beats(*existing))
                    {
                        self.removed.insert(*replaced, stamps);
                        changed = true;
                    }
                    changed |= self.entries.remove(replaced).is_some();
                }
                if !self.removed.contains_key(&stamps)
                    && self.entries.insert(stamps, value.clone()).is_none()
                {
                    changed = true;
                }
                Ok(changed)
            }
        }
    }

    fn merge(
        &mut self,
        remote: &MvRegister,
        _stamps: crate::MergeStamps,
    ) -> Result<bool, MvRegisterError> {
        let mut changed = false;

        for (&id, &removed_at) in &remote.removed {
            if self
                .removed
                .get(&id)
                .is_none_or(|existing| removed_at.beats(*existing))
            {
                self.removed.insert(id, removed_at);
                changed = true;
            }
        }

        for (&id, value) in &remote.entries {
            if self.removed.contains_key(&id) {
                continue;
            }
            match self.entries.get(&id) {
                Some(existing) if existing != value => {
                    return Err(MvRegisterError::AssignmentConflict { id });
                }
                Some(_) => {}
                None => {
                    self.entries.insert(id, value.clone());
                    changed = true;
                }
            }
        }
        for id in self.removed.keys() {
            changed |= self.entries.remove(id).is_some();
        }

        Ok(changed)
    }

    fn max_stamp(&self) -> EventStamp {
        let entries = self
            .entries
            .keys()
            .fold(EventStamp::zero(), |max, &stamp| std::cmp::max(max, stamp));
        self.removed
            .values()
            .fold(entries, |max, &stamp| std::cmp::max(max, stamp))
    }
}
