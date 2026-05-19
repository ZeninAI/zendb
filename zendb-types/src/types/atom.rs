//! Atom — the scalar leaf type.

use crate::{
    codec::{decode_string, decode_varint, encode_string, encode_varint, read_fixed},
    core::traits::{Type, TypedOp, TypedValue},
    Hlc, TypeTag,
};

// ---------------------------------------------------------------------------
// AtomFloat — f64 wrapper with total Eq/Ord/Hash
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
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

impl TypedValue for AtomValue {
    type Error = AtomError;

    fn encode(&self, out: &mut Vec<u8>) -> Result<(), AtomError> {
        match self {
            AtomValue::Null => out.push(0x00),
            AtomValue::Bool(false) => out.push(0x01),
            AtomValue::Bool(true) => out.push(0x02),
            AtomValue::Int(v) => {
                out.push(0x03);
                out.extend_from_slice(&v.to_be_bytes());
            }
            AtomValue::UInt(v) => {
                out.push(0x04);
                out.extend_from_slice(&v.to_be_bytes());
            }
            AtomValue::Float(v) => {
                out.push(0x05);
                out.extend_from_slice(&v.0.to_be_bytes());
            }
            AtomValue::String(s) => {
                out.push(0x06);
                encode_string(out, s);
            }
            AtomValue::Bytes(b) => {
                out.push(0x07);
                encode_varint(out, b.len() as u64);
                out.extend_from_slice(b);
            }
            AtomValue::Timestamp(v) => {
                out.push(0x08);
                out.extend_from_slice(&v.to_be_bytes());
            }
            AtomValue::Uuid(b) => {
                out.push(0x09);
                out.extend_from_slice(b);
            }
            AtomValue::Ulid(b) => {
                out.push(0x0A);
                out.extend_from_slice(b);
            }
        }
        Ok(())
    }

    fn decode(bytes: &[u8]) -> Result<(Self, usize), AtomError> {
        if bytes.is_empty() {
            return Err(AtomError::Decode("empty input".into()));
        }
        let tag = bytes[0];
        let rest = &bytes[1..];
        match tag {
            0x00 => Ok((AtomValue::Null, 1)),
            0x01 => Ok((AtomValue::Bool(false), 1)),
            0x02 => Ok((AtomValue::Bool(true), 1)),
            0x03 => {
                let b = read_fixed::<8>(rest)
                    .ok_or_else(|| AtomError::Decode("truncated Int".into()))?;
                Ok((AtomValue::Int(i64::from_be_bytes(b)), 9))
            }
            0x04 => {
                let b = read_fixed::<8>(rest)
                    .ok_or_else(|| AtomError::Decode("truncated UInt".into()))?;
                Ok((AtomValue::UInt(u64::from_be_bytes(b)), 9))
            }
            0x05 => {
                let b = read_fixed::<8>(rest)
                    .ok_or_else(|| AtomError::Decode("truncated Float".into()))?;
                Ok((AtomValue::Float(AtomFloat(f64::from_be_bytes(b))), 9))
            }
            0x06 => {
                let (s, n) = decode_string(rest)
                    .ok_or_else(|| AtomError::Decode("truncated string".into()))?;
                Ok((AtomValue::String(s), 1 + n))
            }
            0x07 => {
                let (len, vn) = decode_varint(rest)
                    .ok_or_else(|| AtomError::Decode("truncated bytes len".into()))?;
                let start = vn;
                let end = start + len as usize;
                if rest.len() < end {
                    return Err(AtomError::Decode("truncated bytes".into()));
                }
                Ok((AtomValue::Bytes(rest[start..end].to_vec()), 1 + end))
            }
            0x08 => {
                let b = read_fixed::<8>(rest)
                    .ok_or_else(|| AtomError::Decode("truncated Timestamp".into()))?;
                Ok((AtomValue::Timestamp(i64::from_be_bytes(b)), 9))
            }
            0x09 => {
                let b = read_fixed::<16>(rest)
                    .ok_or_else(|| AtomError::Decode("truncated Uuid".into()))?;
                Ok((AtomValue::Uuid(b), 17))
            }
            0x0A => {
                let b = read_fixed::<16>(rest)
                    .ok_or_else(|| AtomError::Decode("truncated Ulid".into()))?;
                Ok((AtomValue::Ulid(b), 17))
            }
            tag => Err(AtomError::Decode(format!("unknown AtomValue tag: {}", tag))),
        }
    }
}

// ---------------------------------------------------------------------------
// AtomOp
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub enum AtomOp {
    Set(AtomValue),
}

impl TypedOp for AtomOp {
    type Error = AtomError;

    fn encode(&self, out: &mut Vec<u8>) -> Result<(), AtomError> {
        match self {
            AtomOp::Set(v) => {
                out.push(0x00);
                v.encode(out)
            }
        }
    }

    fn decode(bytes: &[u8]) -> Result<(Self, usize), AtomError> {
        if bytes.is_empty() {
            return Err(AtomError::Decode("empty input".into()));
        }
        match bytes[0] {
            0x00 => {
                let (v, n) = AtomValue::decode(&bytes[1..])?;
                Ok((AtomOp::Set(v), 1 + n))
            }
            tag => Err(AtomError::Decode(format!("unknown AtomOp tag: {}", tag))),
        }
    }
}

// ---------------------------------------------------------------------------
// AtomError
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub enum AtomError {
    Decode(String),
}

impl std::fmt::Display for AtomError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AtomError::Decode(msg) => write!(f, "Atom decode: {}", msg),
        }
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
    fn atom_value_encode_decode_roundtrip() {
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
            let mut buf = Vec::new();
            val.encode(&mut buf).unwrap();
            let (decoded, consumed) = AtomValue::decode(&buf).unwrap();
            assert_eq!(consumed, buf.len());
            assert_eq!(decoded, val);
        }
    }
}
