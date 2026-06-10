//! Priority Queue — a replicated min-heap CRDT where concurrent push/pop
//! operations converge to a deterministic state.
//!
//! ## Semantics
//!
//! Each element is identified by the HLC of its push operation and carries a
//! priority value. The total order is `(priority, hlc)` — lower priority values
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

use crate::{core::traits::Type, Hlc, Value};

#[derive(Debug, Clone, Encode, Decode)]
struct PqEntry {
    priority: i64,
    value: Value,
    deleted_at: Option<Hlc>,
}

impl PartialEq for PqEntry {
    fn eq(&self, other: &Self) -> bool {
        self.priority == other.priority
            && self.value == other.value
            && self.deleted_at == other.deleted_at
    }
}

impl Eq for PqEntry {}

impl PqEntry {
    fn is_live(&self) -> bool {
        self.deleted_at.is_none()
    }
}

#[derive(Debug, Clone, Default, PartialEq, Encode, Decode)]
pub struct PriorityQueue {
    entries: BTreeMap<Hlc, PqEntry>,
    popped: BTreeMap<Hlc, Hlc>,
}

impl PriorityQueue {
    /// Return all live elements in priority order.
    pub fn live(&self) -> Vec<(i64, Hlc, &Value)> {
        let mut entries: Vec<_> = self
            .entries
            .iter()
            .filter(|(_, entry)| entry.is_live())
            .map(|(&id, entry)| (entry.priority, id, &entry.value))
            .collect();
        entries.sort_by(|(pa, ha, _), (pb, hb, _)| pa.cmp(pb).then_with(|| ha.cmp(hb)));
        entries
    }

    /// Build a pop operation targeting the minimum element observed locally.
    pub fn pop(&self) -> Option<PqOp> {
        self.live().first().map(|(_, id, _)| PqOp::Pop { id: *id })
    }
}

#[derive(Debug, Clone, Encode, Decode)]
pub enum PqOp {
    Push { priority: i64, value: Value },
    Pop { id: Hlc },
}

#[derive(Debug)]
pub enum PqError {
    PushConflict { id: Hlc },
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

    fn apply(&mut self, op: &PqOp, op_hlc: Hlc) -> Result<bool, PqError> {
        match op {
            PqOp::Push { priority, value } => {
                let incoming = PqEntry {
                    priority: *priority,
                    value: value.clone(),
                    deleted_at: self.popped.get(&op_hlc).copied(),
                };
                match self.entries.get(&op_hlc) {
                    Some(existing) if existing != &incoming => {
                        Err(PqError::PushConflict { id: op_hlc })
                    }
                    Some(_) => Ok(false),
                    None => {
                        self.entries.insert(op_hlc, incoming);
                        Ok(true)
                    }
                }
            }
            PqOp::Pop { id } => {
                if !op_hlc.beats(*id) {
                    return Ok(false);
                }
                let mut changed = merge_required_clock(&mut self.popped, *id, op_hlc);
                if let Some(entry) = self.entries.get_mut(id) {
                    changed |= merge_clock(&mut entry.deleted_at, Some(op_hlc));
                }
                Ok(changed)
            }
        }
    }

    fn merge(
        &mut self,
        remote: &PriorityQueue,
        _clocks: crate::MergeClocks,
    ) -> Result<bool, PqError> {
        let mut changed = false;

        for (&id, &popped_at) in &remote.popped {
            changed |= merge_required_clock(&mut self.popped, id, popped_at);
        }

        for (&id, remote_entry) in &remote.entries {
            match self.entries.get_mut(&id) {
                Some(local_entry) => {
                    if local_entry.priority != remote_entry.priority
                        || local_entry.value != remote_entry.value
                    {
                        return Err(PqError::PushConflict { id });
                    }
                    // Priority and value are immutable once pushed.
                    // Only the deletion clock may advance.
                    if merge_clock(&mut local_entry.deleted_at, remote_entry.deleted_at) {
                        changed = true;
                    }
                }
                None => {
                    let mut entry = remote_entry.clone();
                    merge_clock(&mut entry.deleted_at, self.popped.get(&id).copied());
                    self.entries.insert(id, entry);
                    changed = true;
                }
            }
        }
        for (&id, &popped_at) in &self.popped {
            if let Some(entry) = self.entries.get_mut(&id) {
                changed |= merge_clock(&mut entry.deleted_at, Some(popped_at));
            }
        }

        Ok(changed)
    }

    fn compact(&mut self, watermark: Hlc) -> Result<bool, PqError> {
        let before_entries = self.entries.len();
        let before_popped = self.popped.len();
        self.entries
            .retain(|_, entry| entry.deleted_at.is_none_or(|deleted| deleted > watermark));
        self.popped.retain(|_, popped_at| *popped_at > watermark);
        Ok(self.entries.len() != before_entries || self.popped.len() != before_popped)
    }

    fn max_hlc(&self) -> Hlc {
        let entries = self.entries.iter().fold(Hlc::ZERO, |max, (&id, entry)| {
            std::cmp::max(
                max,
                std::cmp::max(id, entry.deleted_at.unwrap_or(Hlc::ZERO)),
            )
        });
        self.popped
            .values()
            .fold(entries, |max, &popped_at| max.max(popped_at))
    }
}

fn merge_clock(local: &mut Option<Hlc>, remote: Option<Hlc>) -> bool {
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

fn merge_required_clock(clocks: &mut BTreeMap<Hlc, Hlc>, id: Hlc, incoming: Hlc) -> bool {
    if clocks
        .get(&id)
        .is_none_or(|existing| incoming.beats(*existing))
    {
        clocks.insert(id, incoming);
        true
    } else {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bincode::{config, decode_from_slice, encode_to_vec};

    fn hlc(ms: u64, device: u8) -> Hlc {
        Hlc::with_device_id(ms, 0, [device; 8]).unwrap()
    }

    fn val(i: i64) -> Value {
        Value::Int(i)
    }

    fn apply(queue: &mut PriorityQueue, op: PqOp, at: Hlc) -> Result<bool, PqError> {
        Type::apply(queue, &op, at)
    }

    #[test]
    fn push_and_pop_single_element() {
        let mut q = PriorityQueue::default();
        apply(
            &mut q,
            PqOp::Push {
                priority: 5,
                value: val(42),
            },
            hlc(100, 1),
        )
        .unwrap();
        assert_eq!(q.live().len(), 1);

        let pop = q.pop().unwrap();
        apply(&mut q, pop, hlc(200, 1)).unwrap();
        assert_eq!(q.live().len(), 0);
    }

    #[test]
    fn compact_removes_stably_deleted_entries() {
        let mut queue = PriorityQueue::default();
        apply(
            &mut queue,
            PqOp::Push {
                priority: 1,
                value: val(1),
            },
            hlc(100, 1),
        )
        .unwrap();
        let pop = queue.pop().unwrap();
        apply(&mut queue, pop, hlc(200, 1)).unwrap();

        assert!(!Type::compact(&mut queue, hlc(150, 1)).unwrap());
        assert!(Type::compact(&mut queue, hlc(200, 1)).unwrap());
        assert!(queue.entries.is_empty());
    }

    #[test]
    fn pop_before_push_converges_with_push_before_pop() {
        let id = hlc(100, 1);
        let push = PqOp::Push {
            priority: 1,
            value: val(1),
        };
        let pop = PqOp::Pop { id };

        let mut push_first = PriorityQueue::default();
        apply(&mut push_first, push.clone(), id).unwrap();
        apply(&mut push_first, pop.clone(), hlc(200, 2)).unwrap();

        let mut pop_first = PriorityQueue::default();
        apply(&mut pop_first, pop, hlc(200, 2)).unwrap();
        apply(&mut pop_first, push, id).unwrap();

        assert_eq!(push_first, pop_first);
        assert!(push_first.live().is_empty());
    }

    #[test]
    fn pop_returns_lowest_priority_first() {
        let mut q = PriorityQueue::default();
        apply(
            &mut q,
            PqOp::Push {
                priority: 10,
                value: val(1),
            },
            hlc(100, 1),
        )
        .unwrap();
        apply(
            &mut q,
            PqOp::Push {
                priority: 3,
                value: val(2),
            },
            hlc(101, 1),
        )
        .unwrap();
        apply(
            &mut q,
            PqOp::Push {
                priority: 7,
                value: val(3),
            },
            hlc(102, 1),
        )
        .unwrap();

        let live = q.live();
        assert_eq!(live[0].0, 3);
        assert_eq!(live[1].0, 7);
        assert_eq!(live[2].0, 10);
    }

    #[test]
    fn equal_priorities_ordered_by_insertion_hlc() {
        let mut q = PriorityQueue::default();
        apply(
            &mut q,
            PqOp::Push {
                priority: 5,
                value: val(10),
            },
            hlc(200, 1),
        )
        .unwrap();
        apply(
            &mut q,
            PqOp::Push {
                priority: 5,
                value: val(20),
            },
            hlc(100, 1),
        )
        .unwrap();
        apply(
            &mut q,
            PqOp::Push {
                priority: 5,
                value: val(30),
            },
            hlc(300, 1),
        )
        .unwrap();

        let live = q.live();
        assert_eq!(live[0].2, &val(20)); // hlc(100) first
        assert_eq!(live[1].2, &val(10)); // hlc(200) second
        assert_eq!(live[2].2, &val(30)); // hlc(300) third
    }

    #[test]
    fn pop_on_empty_queue_returns_false() {
        let q = PriorityQueue::default();
        assert!(q.pop().is_none());
    }

    #[test]
    fn duplicate_pop_is_idempotent() {
        let mut q = PriorityQueue::default();
        apply(
            &mut q,
            PqOp::Push {
                priority: 1,
                value: val(99),
            },
            hlc(100, 1),
        )
        .unwrap();
        let pop = q.pop().unwrap();
        assert!(apply(&mut q, pop.clone(), hlc(200, 1)).unwrap());
        assert!(!apply(&mut q, pop, hlc(200, 1)).unwrap());
    }

    #[test]
    fn merge_combines_elements_from_both_replicas() {
        let mut left = PriorityQueue::default();
        let mut right = PriorityQueue::default();
        apply(
            &mut left,
            PqOp::Push {
                priority: 5,
                value: val(10),
            },
            hlc(100, 1),
        )
        .unwrap();
        apply(
            &mut right,
            PqOp::Push {
                priority: 3,
                value: val(20),
            },
            hlc(100, 2),
        )
        .unwrap();

        Type::merge(&mut left, &right, crate::MergeClocks::ZERO).unwrap();
        let live = left.live();
        assert_eq!(live.len(), 2);
        assert_eq!(live[0].0, 3);
    }

    #[test]
    fn merge_propagates_deletions() {
        let mut left = PriorityQueue::default();
        let mut right = PriorityQueue::default();
        apply(
            &mut left,
            PqOp::Push {
                priority: 1,
                value: val(10),
            },
            hlc(100, 1),
        )
        .unwrap();
        apply(
            &mut right,
            PqOp::Push {
                priority: 1,
                value: val(10),
            },
            hlc(100, 1),
        )
        .unwrap();
        let pop = right.pop().unwrap();
        apply(&mut right, pop, hlc(200, 2)).unwrap();

        Type::merge(&mut left, &right, crate::MergeClocks::ZERO).unwrap();
        assert_eq!(left.live().len(), 0);
    }

    #[test]
    fn concurrent_pops_of_same_element_converge() {
        let mut left = PriorityQueue::default();
        let mut right = PriorityQueue::default();
        apply(
            &mut left,
            PqOp::Push {
                priority: 1,
                value: val(10),
            },
            hlc(100, 1),
        )
        .unwrap();
        apply(
            &mut right,
            PqOp::Push {
                priority: 1,
                value: val(10),
            },
            hlc(100, 1),
        )
        .unwrap();

        // Concurrent pops with different HLCs
        let left_pop = left.pop().unwrap();
        let right_pop = right.pop().unwrap();
        apply(&mut left, left_pop, hlc(200, 1)).unwrap();
        apply(&mut right, right_pop, hlc(150, 2)).unwrap();

        Type::merge(&mut left, &right, crate::MergeClocks::ZERO).unwrap();
        assert_eq!(left.live().len(), 0);
    }

    #[test]
    fn stale_pop_does_not_delete_newer_push() {
        let mut q = PriorityQueue::default();
        apply(
            &mut q,
            PqOp::Push {
                priority: 1,
                value: val(10),
            },
            hlc(200, 1),
        )
        .unwrap();
        // Stale pop at earlier clock
        assert!(!apply(&mut q, PqOp::Pop { id: hlc(200, 1) }, hlc(100, 1)).unwrap());
        assert_eq!(q.live().len(), 1);
    }

    #[test]
    fn merge_converges_for_every_replica_order() {
        let mut queues = [
            PriorityQueue::default(),
            PriorityQueue::default(),
            PriorityQueue::default(),
        ];
        apply(
            &mut queues[0],
            PqOp::Push {
                priority: 2,
                value: val(1),
            },
            hlc(100, 1),
        )
        .unwrap();
        apply(
            &mut queues[1],
            PqOp::Push {
                priority: 1,
                value: val(2),
            },
            hlc(200, 2),
        )
        .unwrap();
        apply(
            &mut queues[2],
            PqOp::Push {
                priority: 3,
                value: val(3),
            },
            hlc(50, 3),
        )
        .unwrap();

        let merge_order = |order: [usize; 3]| -> PriorityQueue {
            let mut merged = queues[order[0]].clone();
            Type::merge(&mut merged, &queues[order[1]], crate::MergeClocks::ZERO).unwrap();
            Type::merge(&mut merged, &queues[order[2]], crate::MergeClocks::ZERO).unwrap();
            merged
        };

        let expected = merge_order([0, 1, 2]);
        for order in [[0, 2, 1], [1, 0, 2], [1, 2, 0], [2, 0, 1], [2, 1, 0]] {
            assert_eq!(merge_order(order), expected);
        }
        let live = expected.live();
        assert_eq!(live.len(), 3);
    }

    #[test]
    fn priority_queue_bincode_roundtrip() {
        let mut q = PriorityQueue::default();
        apply(
            &mut q,
            PqOp::Push {
                priority: 5,
                value: val(42),
            },
            hlc(100, 1),
        )
        .unwrap();
        apply(
            &mut q,
            PqOp::Push {
                priority: 3,
                value: val(99),
            },
            hlc(200, 2),
        )
        .unwrap();
        let pop = q.pop().unwrap();
        apply(&mut q, pop, hlc(300, 1)).unwrap();

        let encoded = encode_to_vec(&q, config::standard()).unwrap();
        let (decoded, consumed): (PriorityQueue, usize) =
            decode_from_slice(&encoded, config::standard()).unwrap();
        assert_eq!(consumed, encoded.len());
        assert_eq!(decoded, q);
    }
}
