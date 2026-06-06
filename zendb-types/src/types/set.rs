//! Set - an HLC-based last-writer-wins element set.

use std::collections::BTreeMap;

use bincode::{Decode, Encode};

use crate::{core::traits::Type, Hlc};

/// Scalar values accepted as set members.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Encode, Decode)]
pub enum SetValue {
    Bool(bool),
    Int(i64),
    String(String),
    Timestamp(u64),
    Blob(Vec<u8>),
}

impl From<bool> for SetValue {
    fn from(value: bool) -> SetValue {
        SetValue::Bool(value)
    }
}

impl From<i64> for SetValue {
    fn from(value: i64) -> SetValue {
        SetValue::Int(value)
    }
}

impl From<String> for SetValue {
    fn from(value: String) -> SetValue {
        SetValue::String(value)
    }
}

impl From<&str> for SetValue {
    fn from(value: &str) -> SetValue {
        SetValue::String(value.into())
    }
}

impl From<Vec<u8>> for SetValue {
    fn from(value: Vec<u8>) -> SetValue {
        SetValue::Blob(value)
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Encode, Decode)]
pub struct SetEntry {
    pub added_at: Option<Hlc>,
    pub removed_at: Option<Hlc>,
}

pub type Set = BTreeMap<SetValue, SetEntry>;

#[derive(Debug, Clone, Encode, Decode)]
pub enum SetOp {
    Add(SetValue),
    Remove(SetValue),
}

#[derive(Debug)]
pub enum SetError {
    ZeroClock,
}

impl std::fmt::Display for SetError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SetError::ZeroClock => f.write_str("set operation HLC cannot be Hlc::ZERO"),
        }
    }
}

impl std::error::Error for SetError {}

impl Type for Set {
    type Op = SetOp;
    type Error = SetError;

    fn apply(&mut self, op: &SetOp, _local_hlc: Hlc, op_hlc: Hlc) -> Result<bool, SetError> {
        if op_hlc == Hlc::ZERO {
            return Err(SetError::ZeroClock);
        }

        let (value, is_add) = match op {
            SetOp::Add(value) => (value, true),
            SetOp::Remove(value) => (value, false),
        };
        let entry = self.entry(value.clone()).or_default();
        let clock = if is_add {
            &mut entry.added_at
        } else {
            &mut entry.removed_at
        };

        if clock.is_none_or(|current| op_hlc.beats(current)) {
            *clock = Some(op_hlc);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn merge(&mut self, remote: &Set, _local_hlc: Hlc, _remote_hlc: Hlc) -> Result<bool, SetError> {
        let mut changed = false;

        for (value, remote_entry) in remote {
            let local_entry = self.entry(value.clone()).or_default();
            if merge_clock(&mut local_entry.added_at, remote_entry.added_at) {
                changed = true;
            }
            if merge_clock(&mut local_entry.removed_at, remote_entry.removed_at) {
                changed = true;
            }
        }

        Ok(changed)
    }

    fn max_hlc(&self) -> Hlc {
        self.values().fold(Hlc::ZERO, |max, entry| {
            std::cmp::max(
                max,
                std::cmp::max(
                    entry.added_at.unwrap_or(Hlc::ZERO),
                    entry.removed_at.unwrap_or(Hlc::ZERO),
                ),
            )
        })
    }
}

/// True when the latest add is at least as new as the latest remove.
pub fn set_contains(set: &Set, value: &SetValue) -> bool {
    set.get(value).is_some_and(|entry| match entry.added_at {
        Some(added) => entry.removed_at.is_none_or(|removed| added >= removed),
        None => false,
    })
}

pub fn set_values(set: &Set) -> impl Iterator<Item = &SetValue> {
    set.keys().filter(|value| set_contains(set, value))
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

#[cfg(test)]
mod tests {
    use super::*;
    use bincode::{config, decode_from_slice, encode_to_vec};

    fn hlc(ms: u64, device: u8) -> Hlc {
        Hlc::with_device_id(ms, 0, [device; 8]).unwrap()
    }

    fn apply(set: &mut Set, op: SetOp, at: Hlc) -> bool {
        Type::apply(set, &op, Hlc::ZERO, at).unwrap()
    }

    fn merge_order(sets: &[Set; 3], order: [usize; 3]) -> Set {
        let mut merged = sets[order[0]].clone();
        Type::merge(&mut merged, &sets[order[1]], Hlc::ZERO, Hlc::ZERO).unwrap();
        Type::merge(&mut merged, &sets[order[2]], Hlc::ZERO, Hlc::ZERO).unwrap();
        merged
    }

    #[test]
    fn add_remove_and_readd_follow_latest_clock() {
        let value = SetValue::String("tag".into());
        let mut set = Set::new();
        assert!(apply(&mut set, SetOp::Add(value.clone()), hlc(100, 1)));
        assert!(set_contains(&set, &value));
        assert!(apply(&mut set, SetOp::Remove(value.clone()), hlc(200, 1)));
        assert!(!set_contains(&set, &value));
        assert!(apply(&mut set, SetOp::Add(value.clone()), hlc(300, 1)));
        assert!(set_contains(&set, &value));
    }

    #[test]
    fn stale_and_duplicate_operations_are_ignored() {
        let value = SetValue::Int(1);
        let mut set = Set::new();
        assert!(apply(&mut set, SetOp::Add(value.clone()), hlc(200, 1)));
        assert!(!apply(&mut set, SetOp::Add(value.clone()), hlc(100, 1)));
        assert!(!apply(&mut set, SetOp::Add(value.clone()), hlc(200, 1)));
        assert!(set_contains(&set, &value));
    }

    #[test]
    fn exact_tie_is_add_wins() {
        let value = SetValue::Bool(true);
        let at = hlc(100, 1);
        let mut set = Set::new();
        apply(&mut set, SetOp::Remove(value.clone()), at);
        apply(&mut set, SetOp::Add(value.clone()), at);
        assert!(set_contains(&set, &value));
    }

    #[test]
    fn remove_before_add_is_retained() {
        let value = SetValue::Timestamp(42);
        let mut set = Set::new();
        apply(&mut set, SetOp::Remove(value.clone()), hlc(200, 2));
        apply(&mut set, SetOp::Add(value.clone()), hlc(100, 1));
        assert!(!set_contains(&set, &value));
    }

    #[test]
    fn merge_converges_for_every_replica_order() {
        let value = SetValue::Blob(vec![1, 2, 3]);
        let mut sets = [Set::new(), Set::new(), Set::new()];
        apply(&mut sets[0], SetOp::Add(value.clone()), hlc(100, 1));
        apply(&mut sets[1], SetOp::Remove(value.clone()), hlc(200, 2));
        apply(&mut sets[2], SetOp::Add(value.clone()), hlc(300, 3));

        let orders = [
            [0, 1, 2],
            [0, 2, 1],
            [1, 0, 2],
            [1, 2, 0],
            [2, 0, 1],
            [2, 1, 0],
        ];
        let expected = merge_order(&sets, orders[0]);
        for order in orders.into_iter().skip(1) {
            assert_eq!(merge_order(&sets, order), expected);
        }
        assert!(set_contains(&expected, &value));
    }

    #[test]
    fn values_returns_only_visible_members_in_key_order() {
        let mut set = Set::new();
        apply(&mut set, SetOp::Add(SetValue::Int(2)), hlc(100, 1));
        apply(&mut set, SetOp::Add(SetValue::Int(1)), hlc(100, 2));
        apply(&mut set, SetOp::Remove(SetValue::Int(2)), hlc(200, 1));

        assert_eq!(
            set_values(&set).cloned().collect::<Vec<_>>(),
            vec![SetValue::Int(1)]
        );
    }

    #[test]
    fn common_scalar_values_convert_directly() {
        assert_eq!(SetValue::from(true), SetValue::Bool(true));
        assert_eq!(SetValue::from(42_i64), SetValue::Int(42));
        assert_eq!(SetValue::from("tag"), SetValue::String("tag".into()));
        assert_eq!(SetValue::from(vec![1, 2, 3]), SetValue::Blob(vec![1, 2, 3]));
    }

    #[test]
    fn zero_clock_is_rejected() {
        let mut set = Set::new();
        assert!(matches!(
            Type::apply(
                &mut set,
                &SetOp::Add(SetValue::Bool(true)),
                Hlc::ZERO,
                Hlc::ZERO,
            ),
            Err(SetError::ZeroClock)
        ));
        assert!(set.is_empty());
    }

    #[test]
    fn set_bincode_roundtrip() {
        let mut set = Set::new();
        apply(
            &mut set,
            SetOp::Add(SetValue::String("visible".into())),
            hlc(100, 1),
        );
        apply(
            &mut set,
            SetOp::Remove(SetValue::String("deleted".into())),
            hlc(200, 2),
        );

        let encoded = encode_to_vec(&set, config::standard()).unwrap();
        let (decoded, consumed): (Set, usize) =
            decode_from_slice(&encoded, config::standard()).unwrap();
        assert_eq!(consumed, encoded.len());
        assert_eq!(decoded, set);
    }
}
