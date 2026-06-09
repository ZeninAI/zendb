//! MV-Register — a replicated register that preserves all concurrent assignments
//! rather than picking a single LWW winner.
//!
//! ## Semantics
//!
//! Each `Assign` records the value together with the operation HLC. On merge,
//! entries whose HLC is strictly dominated by another entry's HLC are discarded.
//! Entries with incomparable clocks (within the same physical millisecond but from
//! different devices) are both preserved, surfacing true write conflicts to the
//! application layer.
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

use crate::{core::traits::Type, Hlc, Value};

/// Compare two HLCs for causal dominance, ignoring device_id.
/// Two HLCs with the same (physical_ms, logical) are considered
/// concurrent regardless of device_id — only a strictly larger
/// (physical_ms, logical) pair dominates.
fn hlc_dominates(a: Hlc, b: Hlc) -> bool {
    (a.physical_ms(), a.logical()) > (b.physical_ms(), b.logical())
}

pub type MvRegister = BTreeMap<Hlc, Value>;

/// Return all currently visible (non-dominated) values ordered by HLC.
pub fn mv_register_values(reg: &MvRegister) -> Vec<&Value> {
    let keys: Vec<Hlc> = reg.keys().copied().collect();
    let dominated: Vec<bool> = keys
        .iter()
        .map(|&k| keys.iter().any(|&other| hlc_dominates(other, k)))
        .collect();

    keys.iter()
        .zip(dominated.iter())
        .filter_map(
            |(k, &is_dominated)| {
                if !is_dominated {
                    reg.get(k)
                } else {
                    None
                }
            },
        )
        .collect()
}

#[derive(Debug, Clone, Encode, Decode)]
pub enum MvRegisterOp {
    Assign(Value),
}

#[derive(Debug)]
pub enum MvRegisterError {}

impl std::fmt::Display for MvRegisterError {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {}
    }
}

impl std::error::Error for MvRegisterError {}

impl Type for MvRegister {
    type Op = MvRegisterOp;
    type Error = MvRegisterError;

    fn apply(
        &mut self,
        op: &MvRegisterOp,
        _local_hlc: Hlc,
        op_hlc: Hlc,
    ) -> Result<bool, MvRegisterError> {
        match op {
            MvRegisterOp::Assign(value) => {
                // Remove entries dominated by this assignment.
                self.retain(|&existing_hlc, _| !hlc_dominates(op_hlc, existing_hlc));
                self.insert(op_hlc, value.clone());
                Ok(true)
            }
        }
    }

    fn merge(
        &mut self,
        remote: &MvRegister,
        _local_hlc: Hlc,
        _remote_hlc: Hlc,
    ) -> Result<bool, MvRegisterError> {
        let mut changed = false;

        for (&hlc, value) in remote {
            if self.get(&hlc) != Some(value) {
                self.insert(hlc, value.clone());
                changed = true;
            }
        }

        // Filter out dominated entries.
        let keys: Vec<Hlc> = self.keys().copied().collect();
        let mut dominated = Vec::new();
        for &k in &keys {
            if keys.iter().any(|&other| hlc_dominates(other, k)) {
                dominated.push(k);
            }
        }
        for k in dominated {
            self.remove(&k);
            changed = true;
        }

        Ok(changed)
    }

    fn max_hlc(&self) -> Hlc {
        self.keys()
            .fold(Hlc::ZERO, |max, &hlc| std::cmp::max(max, hlc))
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

    fn apply(reg: &mut MvRegister, op: MvRegisterOp, at: Hlc) -> bool {
        Type::apply(reg, &op, Hlc::ZERO, at).unwrap()
    }

    #[test]
    fn single_assign_sets_the_value() {
        let mut reg = MvRegister::new();
        apply(&mut reg, MvRegisterOp::Assign(val(42)), hlc(100, 1));
        assert_eq!(mv_register_values(&reg), vec![&val(42)]);
    }

    #[test]
    fn later_assign_replaces_earlier() {
        let mut reg = MvRegister::new();
        apply(&mut reg, MvRegisterOp::Assign(val(1)), hlc(100, 1));
        apply(&mut reg, MvRegisterOp::Assign(val(2)), hlc(200, 1));
        assert_eq!(mv_register_values(&reg), vec![&val(2)]);
    }

    #[test]
    fn stale_assign_does_not_replace_later_value() {
        let mut reg = MvRegister::new();
        apply(&mut reg, MvRegisterOp::Assign(val(100)), hlc(300, 1));
        apply(&mut reg, MvRegisterOp::Assign(val(0)), hlc(200, 1));
        assert_eq!(mv_register_values(&reg), vec![&val(100)]);
    }

    #[test]
    fn concurrent_assigns_from_different_devices_are_both_preserved() {
        let mut reg = MvRegister::new();
        apply(&mut reg, MvRegisterOp::Assign(val(1)), hlc(100, 1));
        apply(&mut reg, MvRegisterOp::Assign(val(2)), hlc(100, 2));

        let values = mv_register_values(&reg);
        assert_eq!(values.len(), 2);
        assert!(values.contains(&&val(1)));
        assert!(values.contains(&&val(2)));
    }

    #[test]
    fn merge_preserves_concurrent_entries() {
        let mut left = MvRegister::new();
        let mut right = MvRegister::new();
        apply(&mut left, MvRegisterOp::Assign(val(10)), hlc(100, 1));
        apply(&mut right, MvRegisterOp::Assign(val(20)), hlc(100, 2));

        Type::merge(&mut left, &right, Hlc::ZERO, Hlc::ZERO).unwrap();
        let values = mv_register_values(&left);
        assert_eq!(values.len(), 2);
    }

    #[test]
    fn merge_replaces_with_later_value() {
        let mut left = MvRegister::new();
        let mut right = MvRegister::new();
        apply(&mut left, MvRegisterOp::Assign(val(1)), hlc(100, 1));
        apply(&mut right, MvRegisterOp::Assign(val(2)), hlc(200, 1));

        Type::merge(&mut left, &right, Hlc::ZERO, Hlc::ZERO).unwrap();
        assert_eq!(mv_register_values(&left), vec![&val(2)]);
    }

    #[test]
    fn merge_converges_for_every_replica_order() {
        let mut regs = [MvRegister::new(), MvRegister::new(), MvRegister::new()];
        apply(&mut regs[0], MvRegisterOp::Assign(val(1)), hlc(100, 1));
        apply(&mut regs[1], MvRegisterOp::Assign(val(2)), hlc(100, 2));
        apply(&mut regs[2], MvRegisterOp::Assign(val(3)), hlc(100, 3));

        let merge_order = |order: [usize; 3]| -> MvRegister {
            let mut merged = regs[order[0]].clone();
            Type::merge(&mut merged, &regs[order[1]], Hlc::ZERO, Hlc::ZERO).unwrap();
            Type::merge(&mut merged, &regs[order[2]], Hlc::ZERO, Hlc::ZERO).unwrap();
            merged
        };

        let expected = merge_order([0, 1, 2]);
        for order in [[0, 2, 1], [1, 0, 2], [1, 2, 0], [2, 0, 1], [2, 1, 0]] {
            assert_eq!(merge_order(order), expected);
        }
        assert_eq!(mv_register_values(&expected).len(), 3);
    }

    #[test]
    fn later_assign_clears_all_concurrent_entries() {
        let mut reg = MvRegister::new();
        apply(&mut reg, MvRegisterOp::Assign(val(1)), hlc(100, 1));
        apply(&mut reg, MvRegisterOp::Assign(val(2)), hlc(100, 2));
        apply(&mut reg, MvRegisterOp::Assign(val(99)), hlc(200, 1));

        assert_eq!(mv_register_values(&reg), vec![&val(99)]);
    }

    #[test]
    fn max_hlc_tracks_highest_entry() {
        let mut reg = MvRegister::new();
        apply(&mut reg, MvRegisterOp::Assign(val(1)), hlc(100, 1));
        apply(&mut reg, MvRegisterOp::Assign(val(2)), hlc(300, 2));

        assert_eq!(reg.max_hlc(), hlc(300, 2));
    }

    #[test]
    fn mv_register_bincode_roundtrip() {
        let mut reg = MvRegister::new();
        apply(&mut reg, MvRegisterOp::Assign(val(42)), hlc(100, 1));
        apply(&mut reg, MvRegisterOp::Assign(val(99)), hlc(100, 2));

        let encoded = encode_to_vec(&reg, config::standard()).unwrap();
        let (decoded, consumed): (MvRegister, usize) =
            decode_from_slice(&encoded, config::standard()).unwrap();
        assert_eq!(consumed, encoded.len());
        assert_eq!(decoded, reg);
    }
}
