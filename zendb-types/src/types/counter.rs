//! PN-Counter — a replicated integer that converges under concurrent increments
//! and decrements.
//!
//! ## Semantics
//!
//! Each replica accumulates positive and negative deltas independently. The
//! value of the counter is Σ(positive) − Σ(negative) across all replicas.
//! Merge takes the element-wise maximum per replica, so concurrent operations
//! from different replicas are never lost.
//!
//! ## Reference
//!
//! Shapiro, Preguiça, Baquero & Zawirski. "A comprehensive study of Convergent
//! and Commutative Replicated Data Types." INRIA RR-7506, 2011. §3.3 (PN-Counter).

use std::collections::BTreeMap;

use bincode::{Decode, Encode};

use crate::{core::traits::Type, DeviceId, Hlc};

#[derive(Debug, Clone, Default, PartialEq, Encode, Decode)]
pub struct Counter {
    entries: BTreeMap<DeviceId, (u64, u64)>,
}

impl Counter {
    /// Compute the current value by summing all device contributions.
    pub fn value(&self) -> i128 {
        self.entries
            .values()
            .map(|(pos, neg)| i128::from(*pos) - i128::from(*neg))
            .sum()
    }
}

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub enum CounterOp {
    /// Add a positive delta. Delta may be negative (equivalent to decrement).
    Increment(i64),
    /// Add a negative delta. Delta may be negative (equivalent to increment).
    Decrement(i64),
}

#[derive(Debug)]
pub enum CounterError {
    Overflow,
}

impl std::fmt::Display for CounterError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CounterError::Overflow => f.write_str("counter component overflow"),
        }
    }
}

impl std::error::Error for CounterError {}

impl Type for Counter {
    type Op = CounterOp;
    type Error = CounterError;

    fn apply(&mut self, op: &CounterOp, op_hlc: Hlc) -> Result<bool, CounterError> {
        let device = op_hlc.device_id();
        let entry = self.entries.entry(device).or_default();

        match op {
            CounterOp::Increment(delta) => {
                if *delta >= 0 {
                    entry.0 = entry
                        .0
                        .checked_add(delta.unsigned_abs())
                        .ok_or(CounterError::Overflow)?;
                } else {
                    entry.1 = entry
                        .1
                        .checked_add(delta.unsigned_abs())
                        .ok_or(CounterError::Overflow)?;
                }
            }
            CounterOp::Decrement(delta) => {
                if *delta >= 0 {
                    entry.1 = entry
                        .1
                        .checked_add(delta.unsigned_abs())
                        .ok_or(CounterError::Overflow)?;
                } else {
                    entry.0 = entry
                        .0
                        .checked_add(delta.unsigned_abs())
                        .ok_or(CounterError::Overflow)?;
                }
            }
        }

        Ok(true)
    }

    fn merge(
        &mut self,
        remote: &Counter,
        _clocks: crate::MergeClocks,
    ) -> Result<bool, CounterError> {
        let mut changed = false;

        for (&device, &(remote_pos, remote_neg)) in &remote.entries {
            match self.entries.get_mut(&device) {
                Some((local_pos, local_neg)) => {
                    if remote_pos > *local_pos {
                        *local_pos = remote_pos;
                        changed = true;
                    }
                    if remote_neg > *local_neg {
                        *local_neg = remote_neg;
                        changed = true;
                    }
                }
                None => {
                    self.entries.insert(device, (remote_pos, remote_neg));
                    changed = true;
                }
            }
        }

        Ok(changed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bincode::{config, decode_from_slice, encode_to_vec};

    fn hlc(ms: u64, device: u8) -> Hlc {
        Hlc::with_device_id(ms, 0, [device; 8]).unwrap()
    }

    fn apply(counter: &mut Counter, op: CounterOp, at: Hlc) -> bool {
        Type::apply(counter, &op, at).unwrap()
    }

    #[test]
    fn increment_and_decrement_from_single_device() {
        let mut c = Counter::default();
        apply(&mut c, CounterOp::Increment(5), hlc(100, 1));
        apply(&mut c, CounterOp::Increment(3), hlc(101, 1));
        apply(&mut c, CounterOp::Decrement(2), hlc(102, 1));
        assert_eq!(c.value(), 6);
    }

    #[test]
    fn negative_deltas_route_to_the_opposite_monotonic_component() {
        let mut left = Counter::default();
        let mut right = Counter::default();
        apply(&mut left, CounterOp::Increment(-5), hlc(100, 1));
        apply(&mut right, CounterOp::Decrement(-3), hlc(100, 2));

        Type::merge(&mut left, &right, crate::MergeClocks::ZERO).unwrap();
        assert_eq!(left.value(), -2);
    }

    #[test]
    fn concurrent_increments_from_different_devices_are_both_preserved() {
        let mut a = Counter::default();
        let mut b = Counter::default();

        apply(&mut a, CounterOp::Increment(10), hlc(100, 1));
        apply(&mut b, CounterOp::Increment(20), hlc(100, 2));

        Type::merge(&mut a, &b, crate::MergeClocks::ZERO).unwrap();
        assert_eq!(a.value(), 30);
    }

    #[test]
    fn concurrent_increment_and_decrement_from_different_devices_merge_correctly() {
        let mut a = Counter::default();
        let mut b = Counter::default();

        apply(&mut a, CounterOp::Increment(100), hlc(100, 1));
        apply(&mut b, CounterOp::Decrement(30), hlc(100, 2));

        Type::merge(&mut a, &b, crate::MergeClocks::ZERO).unwrap();
        assert_eq!(a.value(), 70);
    }

    #[test]
    fn merge_is_idempotent() {
        let mut a = Counter::default();
        apply(&mut a, CounterOp::Increment(5), hlc(100, 1));
        apply(&mut a, CounterOp::Decrement(2), hlc(101, 1));

        let snapshot = a.clone();
        assert!(!Type::merge(&mut a, &snapshot, crate::MergeClocks::ZERO).unwrap());
        assert_eq!(a, snapshot);
    }

    #[test]
    fn merge_is_commutative() {
        let mut x = Counter::default();
        let mut y = Counter::default();
        apply(&mut x, CounterOp::Increment(1), hlc(100, 1));
        apply(&mut x, CounterOp::Decrement(5), hlc(101, 1));
        apply(&mut y, CounterOp::Increment(3), hlc(100, 2));

        let mut x_first = x.clone();
        Type::merge(&mut x_first, &y, crate::MergeClocks::ZERO).unwrap();

        let mut y_first = y.clone();
        Type::merge(&mut y_first, &x, crate::MergeClocks::ZERO).unwrap();

        assert_eq!(x_first, y_first);
        assert_eq!(x_first.value(), -1);
    }

    #[test]
    fn merge_converges_for_every_replica_order() {
        let mut counters = [Counter::default(), Counter::default(), Counter::default()];
        apply(&mut counters[0], CounterOp::Increment(10), hlc(100, 1));
        apply(&mut counters[1], CounterOp::Decrement(3), hlc(200, 2));
        apply(&mut counters[2], CounterOp::Increment(7), hlc(300, 3));

        let merge_order = |order: [usize; 3]| -> Counter {
            let mut merged = counters[order[0]].clone();
            Type::merge(&mut merged, &counters[order[1]], crate::MergeClocks::ZERO).unwrap();
            Type::merge(&mut merged, &counters[order[2]], crate::MergeClocks::ZERO).unwrap();
            merged
        };

        let expected = merge_order([0, 1, 2]);
        for order in [[0, 2, 1], [1, 0, 2], [1, 2, 0], [2, 0, 1], [2, 1, 0]] {
            assert_eq!(merge_order(order), expected);
        }
        assert_eq!(expected.value(), 14);
    }

    #[test]
    fn counter_bincode_roundtrip() {
        let mut c = Counter::default();
        apply(&mut c, CounterOp::Increment(5), hlc(100, 1));
        apply(&mut c, CounterOp::Decrement(2), hlc(200, 2));

        let encoded = encode_to_vec(&c, config::standard()).unwrap();
        let (decoded, consumed): (Counter, usize) =
            decode_from_slice(&encoded, config::standard()).unwrap();
        assert_eq!(consumed, encoded.len());
        assert_eq!(decoded, c);
    }
}
