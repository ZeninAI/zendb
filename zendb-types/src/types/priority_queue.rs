//! Priority Queue — a replicated min-heap CRDT where concurrent push/pop
//! operations converge to a deterministic state.
//!
//! ## Semantics
//!
//! Each element is identified by the HLC of its push operation and carries a
//! priority value. The total order is `(priority, hlc)` — lower priority values
//! come first, with the insertion HLC breaking ties deterministically.
//!
//! A `Pop` marks the minimum visible element as deleted (LWW on the deletion
//! clock). Concurrent pops of the same element converge; concurrent pops of
//! different elements both take effect because the surviving minimum is
//! determined by the global total order.
//!
//! ## Reference
//!
//! Zhang, Ouyang, Huang & Ma. "Conflict-free replicated priority queue:
//! Design, verification and evaluation." Internetware 2023.

use std::collections::BTreeMap;

use bincode::{Decode, Encode};

use crate::{core::traits::Type, Hlc, Value};

/// Compare two HLCs for causal dominance, ignoring device_id.
fn hlc_dominates(a: Hlc, b: Hlc) -> bool {
    (a.physical_ms(), a.logical()) > (b.physical_ms(), b.logical())
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct PqEntry {
    pub priority: i64,
    pub value: Value,
    pub deleted_at: Option<Hlc>,
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
    pub fn is_live(&self) -> bool {
        self.deleted_at.is_none()
    }
}

pub type PriorityQueue = BTreeMap<Hlc, PqEntry>;

#[derive(Debug, Clone, Encode, Decode)]
pub enum PqOp {
    Push { priority: i64, value: Value },
    Pop,
}

#[derive(Debug)]
pub enum PqError {
    Empty,
}

impl std::fmt::Display for PqError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PqError::Empty => f.write_str("cannot pop from an empty priority queue"),
        }
    }
}

impl std::error::Error for PqError {}

impl Type for PriorityQueue {
    type Op = PqOp;
    type Error = PqError;

    fn apply(&mut self, op: &PqOp, _local_hlc: Hlc, op_hlc: Hlc) -> Result<bool, PqError> {
        match op {
            PqOp::Push { priority, value } => {
                self.insert(
                    op_hlc,
                    PqEntry {
                        priority: *priority,
                        value: value.clone(),
                        deleted_at: None,
                    },
                );
                Ok(true)
            }
            PqOp::Pop => {
                let candidate =
                    self.iter()
                        .filter(|(_, e)| e.is_live())
                        .min_by(|(a_h, a_e), (b_h, b_e)| {
                            a_e.priority.cmp(&b_e.priority).then_with(|| a_h.cmp(b_h))
                        });

                match candidate {
                    Some((&id, _)) => {
                        // Reject stale pop: op must be causally after the push.
                        if !hlc_dominates(op_hlc, id) {
                            return Ok(false);
                        }
                        let entry = self.get_mut(&id).unwrap();
                        Ok(merge_hcl(&mut entry.deleted_at, Some(op_hlc)))
                    }
                    None => Ok(false),
                }
            }
        }
    }

    fn merge(
        &mut self,
        remote: &PriorityQueue,
        _local_hlc: Hlc,
        _remote_hlc: Hlc,
    ) -> Result<bool, PqError> {
        let mut changed = false;

        for (&id, remote_entry) in remote {
            match self.get_mut(&id) {
                Some(local_entry) => {
                    // Priority and value are immutable once pushed.
                    // Only the deletion clock may advance.
                    if merge_hcl(&mut local_entry.deleted_at, remote_entry.deleted_at) {
                        changed = true;
                    }
                }
                None => {
                    self.insert(id, remote_entry.clone());
                    changed = true;
                }
            }
        }

        Ok(changed)
    }

    fn max_hlc(&self) -> Hlc {
        self.iter().fold(Hlc::ZERO, |max, (&id, entry)| {
            std::cmp::max(
                max,
                std::cmp::max(id, entry.deleted_at.unwrap_or(Hlc::ZERO)),
            )
        })
    }
}

fn merge_hcl(local: &mut Option<Hlc>, remote: Option<Hlc>) -> bool {
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

/// Return all live elements in priority order: (priority, hlc, value).
pub fn pq_live(queue: &PriorityQueue) -> Vec<(i64, Hlc, &Value)> {
    let mut entries: Vec<_> = queue
        .iter()
        .filter(|(_, e)| e.is_live())
        .map(|(&id, e)| (e.priority, id, &e.value))
        .collect();
    entries.sort_by(|(pa, ha, _), (pb, hb, _)| pa.cmp(pb).then_with(|| ha.cmp(hb)));
    entries
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
        Type::apply(queue, &op, Hlc::ZERO, at)
    }

    #[test]
    fn push_and_pop_single_element() {
        let mut q = PriorityQueue::new();
        apply(
            &mut q,
            PqOp::Push {
                priority: 5,
                value: val(42),
            },
            hlc(100, 1),
        )
        .unwrap();
        assert_eq!(pq_live(&q).len(), 1);

        apply(&mut q, PqOp::Pop, hlc(200, 1)).unwrap();
        assert_eq!(pq_live(&q).len(), 0);
    }

    #[test]
    fn pop_returns_lowest_priority_first() {
        let mut q = PriorityQueue::new();
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

        let live = pq_live(&q);
        assert_eq!(live[0].0, 3);
        assert_eq!(live[1].0, 7);
        assert_eq!(live[2].0, 10);
    }

    #[test]
    fn equal_priorities_ordered_by_insertion_hlc() {
        let mut q = PriorityQueue::new();
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

        let live = pq_live(&q);
        assert_eq!(live[0].2, &val(20)); // hlc(100) first
        assert_eq!(live[1].2, &val(10)); // hlc(200) second
        assert_eq!(live[2].2, &val(30)); // hlc(300) third
    }

    #[test]
    fn pop_on_empty_queue_returns_false() {
        let mut q = PriorityQueue::new();
        assert!(!apply(&mut q, PqOp::Pop, hlc(100, 1)).unwrap());
    }

    #[test]
    fn duplicate_pop_is_idempotent() {
        let mut q = PriorityQueue::new();
        apply(
            &mut q,
            PqOp::Push {
                priority: 1,
                value: val(99),
            },
            hlc(100, 1),
        )
        .unwrap();
        assert!(apply(&mut q, PqOp::Pop, hlc(200, 1)).unwrap());
        assert!(!apply(&mut q, PqOp::Pop, hlc(200, 1)).unwrap());
    }

    #[test]
    fn merge_combines_elements_from_both_replicas() {
        let mut left = PriorityQueue::new();
        let mut right = PriorityQueue::new();
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

        Type::merge(&mut left, &right, Hlc::ZERO, Hlc::ZERO).unwrap();
        let live = pq_live(&left);
        assert_eq!(live.len(), 2);
        assert_eq!(live[0].0, 3);
    }

    #[test]
    fn merge_propagates_deletions() {
        let mut left = PriorityQueue::new();
        let mut right = PriorityQueue::new();
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
        apply(&mut right, PqOp::Pop, hlc(200, 2)).unwrap();

        Type::merge(&mut left, &right, Hlc::ZERO, Hlc::ZERO).unwrap();
        assert_eq!(pq_live(&left).len(), 0);
    }

    #[test]
    fn concurrent_pops_of_same_element_converge() {
        let mut left = PriorityQueue::new();
        let mut right = PriorityQueue::new();
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
        apply(&mut left, PqOp::Pop, hlc(200, 1)).unwrap();
        apply(&mut right, PqOp::Pop, hlc(150, 2)).unwrap();

        Type::merge(&mut left, &right, Hlc::ZERO, Hlc::ZERO).unwrap();
        assert_eq!(pq_live(&left).len(), 0);
    }

    #[test]
    fn stale_pop_does_not_delete_newer_push() {
        let mut q = PriorityQueue::new();
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
        assert!(!apply(&mut q, PqOp::Pop, hlc(100, 1)).unwrap());
        assert_eq!(pq_live(&q).len(), 1);
    }

    #[test]
    fn merge_converges_for_every_replica_order() {
        let mut queues = [
            PriorityQueue::new(),
            PriorityQueue::new(),
            PriorityQueue::new(),
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
            Type::merge(&mut merged, &queues[order[1]], Hlc::ZERO, Hlc::ZERO).unwrap();
            Type::merge(&mut merged, &queues[order[2]], Hlc::ZERO, Hlc::ZERO).unwrap();
            merged
        };

        let expected = merge_order([0, 1, 2]);
        for order in [[0, 2, 1], [1, 0, 2], [1, 2, 0], [2, 0, 1], [2, 1, 0]] {
            assert_eq!(merge_order(order), expected);
        }
        let live = pq_live(&expected);
        assert_eq!(live.len(), 3);
    }

    #[test]
    fn priority_queue_bincode_roundtrip() {
        let mut q = PriorityQueue::new();
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
        apply(&mut q, PqOp::Pop, hlc(300, 1)).unwrap();

        let encoded = encode_to_vec(&q, config::standard()).unwrap();
        let (decoded, consumed): (PriorityQueue, usize) =
            decode_from_slice(&encoded, config::standard()).unwrap();
        assert_eq!(consumed, encoded.len());
        assert_eq!(decoded, q);
    }
}
