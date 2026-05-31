//! Atom — the scalar leaf type.

use bincode::{Decode, Encode};

use crate::{core::traits::Type, Hlc, TypeTag};

// ---------------------------------------------------------------------------
// AtomFloat — f64 wrapper with total Eq/Ord/Hash
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, Encode, Decode)]
pub struct AtomFloat(pub f64);

impl PartialEq for AtomFloat {
    fn eq(&self, other: &Self) -> bool {
        self.0.to_bits() == other.0.to_bits()
    }
}
impl Eq for AtomFloat {}
impl PartialOrd for AtomFloat {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for AtomFloat {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.to_bits().cmp(&other.0.to_bits())
    }
}
impl std::hash::Hash for AtomFloat {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.to_bits().hash(state);
    }
}

// ---------------------------------------------------------------------------
// AtomValue
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Encode, Decode)]
pub enum AtomValue {
    Null,
    Bool(bool),
    Int(i64),
    UInt(u64),
    Float(AtomFloat),
    String(String),
    Bytes(Vec<u8>),
    Timestamp(i64),
    Uuid([u8; 16]),
    Ulid([u8; 16]),
}

// ---------------------------------------------------------------------------
// AtomOp
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub enum AtomOp {
    Set(AtomValue),
}

// ---------------------------------------------------------------------------
// AtomError
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub enum AtomError {}

impl std::fmt::Display for AtomError {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {}
    }
}
impl std::error::Error for AtomError {}

// ---------------------------------------------------------------------------
// AtomType
// ---------------------------------------------------------------------------

pub struct AtomType;

impl Type for AtomType {
    const TAG: TypeTag = TypeTag::Atom;
    const NAME: &'static str = "Atom";
    const KEYABLE: bool = true;
    const IS_CONTAINER: bool = false;
    type Value = AtomValue;
    type Op = AtomOp;
    type Error = AtomError;

    fn empty() -> AtomValue {
        AtomValue::Null
    }

    fn apply_op(
        value: &mut AtomValue,
        op: &AtomOp,
        local_hlc: Hlc,
        op_hlc: Hlc,
    ) -> Result<bool, AtomError> {
        match op {
            AtomOp::Set(v) => {
                if local_hlc.beats(op_hlc) {
                    return Ok(false);
                }
                *value = v.clone();
                Ok(true)
            }
        }
    }

    fn merge(
        local: &mut AtomValue,
        local_hlc: Hlc,
        remote: &AtomValue,
        remote_hlc: Hlc,
    ) -> Result<bool, AtomError> {
        if remote_hlc.beats(local_hlc) {
            *local = remote.clone();
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use bincode::{config, decode_from_slice, encode_to_vec};

    #[test]
    fn atom_float_eq_nan() {
        assert_eq!(AtomFloat(f64::NAN), AtomFloat(f64::NAN));
    }

    #[test]
    fn atom_float_ord() {
        assert!(AtomFloat(1.0) < AtomFloat(2.0));
    }

    #[test]
    fn atom_apply_set() {
        let mut val = AtomValue::Null;
        let changed = AtomType::apply_op(
            &mut val,
            &AtomOp::Set(AtomValue::Int(42)),
            Hlc::ZERO,
            Hlc::new(100, 0, 1).unwrap(),
        )
        .unwrap();
        assert!(changed);
        assert_eq!(val, AtomValue::Int(42));
    }

    #[test]
    fn atom_apply_lww_rejects() {
        let mut val = AtomValue::Int(1);
        let changed = AtomType::apply_op(
            &mut val,
            &AtomOp::Set(AtomValue::Int(2)),
            Hlc::new(200, 0, 1).unwrap(),
            Hlc::new(100, 0, 1).unwrap(),
        )
        .unwrap();
        assert!(!changed);
        assert_eq!(val, AtomValue::Int(1));
    }

    #[test]
    fn atom_merge_lww_remote_wins() {
        let mut local = AtomValue::String("local".into());
        let remote = AtomValue::String("remote".into());
        let changed = AtomType::merge(
            &mut local,
            Hlc::new(100, 0, 1).unwrap(),
            &remote,
            Hlc::new(200, 0, 1).unwrap(),
        )
        .unwrap();
        assert!(changed);
        assert_eq!(local, AtomValue::String("remote".into()));
    }

    #[test]
    fn atom_merge_lww_local_wins() {
        let mut local = AtomValue::String("local".into());
        let remote = AtomValue::String("remote".into());
        let changed = AtomType::merge(
            &mut local,
            Hlc::new(300, 0, 1).unwrap(),
            &remote,
            Hlc::new(200, 0, 1).unwrap(),
        )
        .unwrap();
        assert!(!changed);
        assert_eq!(local, AtomValue::String("local".into()));
    }

    #[test]
    fn atom_value_bincode_roundtrip() {
        let values = vec![
            AtomValue::Null,
            AtomValue::Bool(true),
            AtomValue::Bool(false),
            AtomValue::Int(-42),
            AtomValue::UInt(42),
            AtomValue::Float(AtomFloat(3.14)),
            AtomValue::String("hello".into()),
            AtomValue::Bytes(vec![1, 2, 3]),
            AtomValue::Timestamp(1_700_000_000_000_000),
            AtomValue::Uuid([0xAA; 16]),
            AtomValue::Ulid([0xBB; 16]),
        ];
        for val in values {
            let buf = encode_to_vec(&val, config::standard()).unwrap();
            let (decoded, consumed): (AtomValue, usize) =
                decode_from_slice(&buf, config::standard()).unwrap();
            assert_eq!(consumed, buf.len());
            assert_eq!(decoded, val);
        }
    }
}
