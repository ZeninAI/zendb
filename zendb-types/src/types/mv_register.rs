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

use crate::{core::traits::Type, Hlc, Value};

#[derive(Debug, Clone, Default, PartialEq, Encode, Decode)]
pub struct MvRegister {
    entries: BTreeMap<Hlc, Value>,
    removed: BTreeMap<Hlc, Hlc>,
}

impl MvRegister {
    /// Return all currently visible concurrent values ordered by assignment ID.
    pub fn values(&self) -> Vec<&Value> {
        self.entries.values().collect()
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
    Assign { value: Value, replaces: Vec<Hlc> },
}

#[derive(Debug)]
pub enum MvRegisterError {
    AssignmentConflict { id: Hlc },
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

    fn apply(&mut self, op: &MvRegisterOp, op_hlc: Hlc) -> Result<bool, MvRegisterError> {
        match op {
            MvRegisterOp::Assign { value, replaces } => {
                if self
                    .entries
                    .get(&op_hlc)
                    .is_some_and(|existing| existing != value)
                {
                    return Err(MvRegisterError::AssignmentConflict { id: op_hlc });
                }

                let mut changed = false;
                for replaced in replaces {
                    if self
                        .removed
                        .get(replaced)
                        .is_none_or(|existing| op_hlc.beats(*existing))
                    {
                        self.removed.insert(*replaced, op_hlc);
                        changed = true;
                    }
                    changed |= self.entries.remove(replaced).is_some();
                }
                if !self.removed.contains_key(&op_hlc)
                    && self.entries.insert(op_hlc, value.clone()).is_none()
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
        _clocks: crate::MergeClocks,
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

    fn compact(&mut self, watermark: Hlc) -> Result<bool, MvRegisterError> {
        let before = self.removed.len();
        self.removed
            .retain(|assignment, removed_at| *assignment > watermark || *removed_at > watermark);
        Ok(self.removed.len() != before)
    }

    fn max_hlc(&self) -> Hlc {
        let entries = self
            .entries
            .keys()
            .fold(Hlc::ZERO, |max, &hlc| std::cmp::max(max, hlc));
        self.removed
            .values()
            .fold(entries, |max, &hlc| std::cmp::max(max, hlc))
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

    fn apply(reg: &mut MvRegister, value: Value, at: Hlc) -> bool {
        let op = reg.assign(value);
        Type::apply(reg, &op, at).unwrap()
    }

    #[test]
    fn single_assign_sets_the_value() {
        let mut reg = MvRegister::default();
        apply(&mut reg, val(42), hlc(100, 1));
        assert_eq!(reg.values(), vec![&val(42)]);
    }

    #[test]
    fn later_assign_replaces_earlier() {
        let mut reg = MvRegister::default();
        apply(&mut reg, val(1), hlc(100, 1));
        apply(&mut reg, val(2), hlc(200, 1));
        assert_eq!(reg.values(), vec![&val(2)]);
    }

    #[test]
    fn observed_assignment_replaces_even_with_an_older_wall_clock() {
        let mut reg = MvRegister::default();
        apply(&mut reg, val(100), hlc(300, 1));
        apply(&mut reg, val(0), hlc(200, 1));
        assert_eq!(reg.values(), vec![&val(0)]);
    }

    #[test]
    fn concurrent_assigns_from_different_devices_are_both_preserved() {
        let mut left = MvRegister::default();
        let mut right = MvRegister::default();
        apply(&mut left, val(1), hlc(100, 1));
        apply(&mut right, val(2), hlc(200, 2));
        Type::merge(&mut left, &right, crate::MergeClocks::ZERO).unwrap();

        let values = left.values();
        assert_eq!(values.len(), 2);
        assert!(values.contains(&&val(1)));
        assert!(values.contains(&&val(2)));
    }

    #[test]
    fn merge_preserves_concurrent_entries() {
        let mut left = MvRegister::default();
        let mut right = MvRegister::default();
        apply(&mut left, val(10), hlc(100, 1));
        apply(&mut right, val(20), hlc(100, 2));

        Type::merge(&mut left, &right, crate::MergeClocks::ZERO).unwrap();
        let values = left.values();
        assert_eq!(values.len(), 2);
    }

    #[test]
    fn merge_replaces_with_later_value() {
        let mut left = MvRegister::default();
        apply(&mut left, val(1), hlc(100, 1));
        let mut right = left.clone();
        apply(&mut right, val(2), hlc(200, 1));

        Type::merge(&mut left, &right, crate::MergeClocks::ZERO).unwrap();
        assert_eq!(left.values(), vec![&val(2)]);
    }

    #[test]
    fn replacement_context_suppresses_out_of_order_assignment() {
        let original_id = hlc(300, 1);
        let replacement_id = hlc(200, 2);
        let original = MvRegisterOp::Assign {
            value: val(1),
            replaces: Vec::new(),
        };
        let replacement = MvRegisterOp::Assign {
            value: val(2),
            replaces: vec![original_id],
        };

        let mut original_first = MvRegister::default();
        Type::apply(&mut original_first, &original, original_id).unwrap();
        Type::apply(&mut original_first, &replacement, replacement_id).unwrap();

        let mut replacement_first = MvRegister::default();
        Type::apply(&mut replacement_first, &replacement, replacement_id).unwrap();
        Type::apply(&mut replacement_first, &original, original_id).unwrap();

        assert_eq!(original_first, replacement_first);
        assert_eq!(original_first.values(), vec![&val(2)]);
    }

    #[test]
    fn merge_converges_for_every_replica_order() {
        let mut regs = [
            MvRegister::default(),
            MvRegister::default(),
            MvRegister::default(),
        ];
        apply(&mut regs[0], val(1), hlc(100, 1));
        apply(&mut regs[1], val(2), hlc(100, 2));
        apply(&mut regs[2], val(3), hlc(100, 3));

        let merge_order = |order: [usize; 3]| -> MvRegister {
            let mut merged = regs[order[0]].clone();
            Type::merge(&mut merged, &regs[order[1]], crate::MergeClocks::ZERO).unwrap();
            Type::merge(&mut merged, &regs[order[2]], crate::MergeClocks::ZERO).unwrap();
            merged
        };

        let expected = merge_order([0, 1, 2]);
        for order in [[0, 2, 1], [1, 0, 2], [1, 2, 0], [2, 0, 1], [2, 1, 0]] {
            assert_eq!(merge_order(order), expected);
        }
        assert_eq!(expected.values().len(), 3);
    }

    #[test]
    fn later_assign_clears_all_concurrent_entries() {
        let mut reg = MvRegister::default();
        let mut concurrent = MvRegister::default();
        apply(&mut reg, val(1), hlc(100, 1));
        apply(&mut concurrent, val(2), hlc(100, 2));
        Type::merge(&mut reg, &concurrent, crate::MergeClocks::ZERO).unwrap();
        apply(&mut reg, val(99), hlc(200, 1));

        assert_eq!(reg.values(), vec![&val(99)]);
    }

    #[test]
    fn max_hlc_tracks_highest_entry() {
        let mut reg = MvRegister::default();
        apply(&mut reg, val(1), hlc(100, 1));
        apply(&mut reg, val(2), hlc(300, 2));

        assert_eq!(reg.max_hlc(), hlc(300, 2));
    }

    #[test]
    fn mv_register_bincode_roundtrip() {
        let mut reg = MvRegister::default();
        apply(&mut reg, val(42), hlc(100, 1));
        apply(&mut reg, val(99), hlc(100, 2));

        let encoded = encode_to_vec(&reg, config::standard()).unwrap();
        let (decoded, consumed): (MvRegister, usize) =
            decode_from_slice(&encoded, config::standard()).unwrap();
        assert_eq!(consumed, encoded.len());
        assert_eq!(decoded, reg);
    }
}
