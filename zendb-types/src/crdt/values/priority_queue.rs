//! Priority Queue — a replicated min-heap CRDT where concurrent push/pop
//! operations converge to a deterministic state.
//!
//! ## Semantics
//!
//! Each element is identified by the HLC of its push operation and carries a
//! priority value. The total order is `(priority, stamp)` — lower priority values
//! come first, with the insertion HLC breaking ties deterministically.
//!
//! `PriorityQueue::pop` resolves the minimum visible element when constructing
//! the operation. The resulting `Pop { id }` marks that stable element ID as
//! deleted, so delivery order cannot change the target.
//!
//! ## Reference
//!
//! Zhang, Ouyang, Huang & Ma. "Conflict-free replicated priority queue:
//! Design, verification and evaluation." Internetware 2023.

use std::collections::BTreeMap;

use bincode::{Decode, Encode};

use crate::{EventStamp, Type, Value};

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
enum PqEntry {
    PendingPop {
        deleted_at: EventStamp,
    },
    Present {
        priority: i64,
        value: Value,
        deleted_at: Option<EventStamp>,
    },
}

#[derive(Debug, Clone, Default, PartialEq, Encode, Decode)]
pub struct PriorityQueue {
    entries: BTreeMap<EventStamp, PqEntry>,
}

impl PriorityQueue {
    /// Return all live elements in priority order.
    pub fn live(&self) -> Vec<(i64, EventStamp, &Value)> {
        let mut entries: Vec<_> = self
            .entries
            .iter()
            .filter_map(|(&id, entry)| match entry {
                PqEntry::Present {
                    priority,
                    value,
                    deleted_at: None,
                } => Some((*priority, id, value)),
                _ => None,
            })
            .collect();
        entries.sort_by(|(pa, ha, _), (pb, hb, _)| pa.cmp(pb).then_with(|| ha.cmp(hb)));
        entries
    }

    /// Build a pop operation targeting the minimum element observed locally.
    pub fn pop(&self) -> Option<PqOp> {
        self.entries
            .iter()
            .filter_map(|(&id, entry)| match entry {
                PqEntry::Present {
                    priority,
                    deleted_at: None,
                    ..
                } => Some((*priority, id)),
                _ => None,
            })
            .min()
            .map(|(_, id)| PqOp::Pop { id })
    }
}

#[derive(Debug, Clone, Encode, Decode)]
pub enum PqOp {
    Push { priority: i64, value: Value },
    Pop { id: EventStamp },
}

#[derive(Debug)]
pub enum PqError {
    PushConflict { id: EventStamp },
}

impl std::fmt::Display for PqError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PqError::PushConflict { id } => {
                write!(f, "priority queue push {id} has conflicting content")
            }
        }
    }
}

impl std::error::Error for PqError {}

impl Type for PriorityQueue {
    type Op = PqOp;
    type Error = PqError;

    fn apply(&mut self, op: &PqOp, stamps: crate::MergeStamps) -> Result<bool, PqError> {
        let stamps = stamps.incoming;
        match op {
            PqOp::Push { priority, value } => match self.entries.get_mut(&stamps) {
                Some(PqEntry::Present {
                    priority: existing_priority,
                    value: existing_value,
                    ..
                }) if existing_priority != priority || existing_value != value => {
                    Err(PqError::PushConflict { id: stamps })
                }
                Some(PqEntry::Present { .. }) => Ok(false),
                Some(entry @ PqEntry::PendingPop { .. }) => {
                    let PqEntry::PendingPop { deleted_at } = entry else {
                        unreachable!()
                    };
                    let deleted_at = *deleted_at;
                    *entry = PqEntry::Present {
                        priority: *priority,
                        value: value.clone(),
                        deleted_at: Some(deleted_at),
                    };
                    Ok(true)
                }
                None => {
                    self.entries.insert(
                        stamps,
                        PqEntry::Present {
                            priority: *priority,
                            value: value.clone(),
                            deleted_at: None,
                        },
                    );
                    Ok(true)
                }
            },
            PqOp::Pop { id } => {
                if !stamps.beats(*id) {
                    return Ok(false);
                }
                match self.entries.get_mut(id) {
                    Some(PqEntry::PendingPop { deleted_at }) => {
                        Ok(merge_required_clock(deleted_at, stamps))
                    }
                    Some(PqEntry::Present { deleted_at, .. }) => {
                        Ok(merge_clock(deleted_at, Some(stamps)))
                    }
                    None => {
                        self.entries
                            .insert(*id, PqEntry::PendingPop { deleted_at: stamps });
                        Ok(true)
                    }
                }
            }
        }
    }

    fn merge(
        &mut self,
        remote: &PriorityQueue,
        _stamps: crate::MergeStamps,
    ) -> Result<bool, PqError> {
        let mut changed = false;

        for (&id, remote_entry) in &remote.entries {
            match self.entries.get_mut(&id) {
                Some(local_entry) => changed |= merge_entry(local_entry, remote_entry, id)?,
                None => {
                    self.entries.insert(id, remote_entry.clone());
                    changed = true;
                }
            }
        }
        Ok(changed)
    }

    fn max_stamp(&self) -> EventStamp {
        self.entries
            .iter()
            .fold(EventStamp::zero(), |max, (&id, entry)| {
                let deleted_at = match entry {
                    PqEntry::PendingPop { deleted_at } => *deleted_at,
                    PqEntry::Present { deleted_at, .. } => {
                        deleted_at.unwrap_or_else(EventStamp::zero)
                    }
                };
                max.max(id).max(deleted_at)
            })
    }
}

fn merge_entry(local: &mut PqEntry, remote: &PqEntry, id: EventStamp) -> Result<bool, PqError> {
    match (local, remote) {
        (PqEntry::PendingPop { deleted_at: local }, PqEntry::PendingPop { deleted_at: remote }) => {
            Ok(merge_required_clock(local, *remote))
        }
        (
            local @ PqEntry::PendingPop { .. },
            PqEntry::Present {
                priority,
                value,
                deleted_at,
            },
        ) => {
            let PqEntry::PendingPop {
                deleted_at: local_deleted,
            } = local
            else {
                unreachable!()
            };
            let mut merged_deleted = *deleted_at;
            merge_clock(&mut merged_deleted, Some(*local_deleted));
            *local = PqEntry::Present {
                priority: *priority,
                value: value.clone(),
                deleted_at: merged_deleted,
            };
            Ok(true)
        }
        (PqEntry::Present { deleted_at, .. }, PqEntry::PendingPop { deleted_at: remote }) => {
            Ok(merge_clock(deleted_at, Some(*remote)))
        }
        (
            PqEntry::Present {
                priority: local_priority,
                value: local_value,
                deleted_at: local_deleted,
            },
            PqEntry::Present {
                priority: remote_priority,
                value: remote_value,
                deleted_at: remote_deleted,
            },
        ) => {
            if local_priority != remote_priority || local_value != remote_value {
                return Err(PqError::PushConflict { id });
            }
            Ok(merge_clock(local_deleted, *remote_deleted))
        }
    }
}

fn merge_clock(local: &mut Option<EventStamp>, remote: Option<EventStamp>) -> bool {
    let Some(remote) = remote else {
        return false;
    };
    if local.is_none_or(|current| remote.beats(current)) {
        *local = Some(remote);
        true
    } else {
        false
    }
}

fn merge_required_clock(current: &mut EventStamp, incoming: EventStamp) -> bool {
    if incoming.beats(*current) {
        *current = incoming;
        true
    } else {
        false
    }
}
