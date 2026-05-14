//! Atom — the scalar leaf type.
//!
//! Atom represents all values that have no children: null, booleans, integers,
//! floats, strings, bytes, timestamps, UUIDs, and ULIDs.
//!
//! ## Type registration
//!
//! `AtomType` implements `Type`. It is registered as a `leaf` type in
//! `register_types!` because it has no children (no `Segment`).

use crate::{Hlc, Type, TypeTag};

// ---------------------------------------------------------------------------
// AtomFloat — f64 wrapper with total Eq/Ord/Hash via bit comparison
// ---------------------------------------------------------------------------

/// A newtype over `f64` that provides `Eq`, `Ord`, and `Hash` by comparing
/// the raw IEEE 754 bit representation.
///
/// This makes `AtomValue` fully hashable (needed for state-hash indexes) and
/// gives a total ordering. NaN bit patterns are equal to each other under this
/// scheme — the exact bit pattern is preserved faithfully.
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

/// A scalar value. One variant per supported scalar kind.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum AtomValue {
    /// The null / unit value.
    Null,
    /// Boolean.
    Bool(bool),
    /// Signed 64-bit integer.
    Int(i64),
    /// Unsigned 64-bit integer.
    UInt(u64),
    /// 64-bit IEEE 754 float. Uses `AtomFloat` for total ordering.
    Float(AtomFloat),
    /// UTF-8 string.
    String(String),
    /// Raw byte buffer.
    Bytes(Vec<u8>),
    /// Microseconds since UNIX epoch (signed for BC dates).
    Timestamp(i64),
    /// UUID (16 bytes).
    Uuid([u8; 16]),
    /// ULID (16 bytes, stored in binary form).
    Ulid([u8; 16]),
}

impl AtomValue {
    /// Returns a string description of the variant kind.
    pub fn variant_name(&self) -> &'static str {
        match self {
            AtomValue::Null => "Null",
            AtomValue::Bool(_) => "Bool",
            AtomValue::Int(_) => "Int",
            AtomValue::UInt(_) => "UInt",
            AtomValue::Float(_) => "Float",
            AtomValue::String(_) => "String",
            AtomValue::Bytes(_) => "Bytes",
            AtomValue::Timestamp(_) => "Timestamp",
            AtomValue::Uuid(_) => "Uuid",
            AtomValue::Ulid(_) => "Ulid",
        }
    }
}

// ---------------------------------------------------------------------------
// AtomOp
// ---------------------------------------------------------------------------

/// Operations on Atom values.
///
/// Currently only `Set` — Atom has no internal structure to modify.
#[derive(Debug, Clone, PartialEq)]
pub enum AtomOp {
    /// Replace the entire value.
    Set(AtomValue),
}

// ---------------------------------------------------------------------------
// AtomType — unit struct implementing the Type trait
// ---------------------------------------------------------------------------

/// The registered type for Atom.
///
/// This unit struct carries no state. Its `Type` impl provides all behaviour.
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

    fn apply_op(_state: AtomValue, op: AtomOp, _hlc: Hlc) -> Result<AtomValue, AtomError> {
        match op {
            AtomOp::Set(v) => Ok(v),
        }
    }

    fn merge(
        local: AtomValue,
        local_hlc: Hlc,
        remote: AtomValue,
        remote_hlc: Hlc,
    ) -> Result<AtomValue, AtomError> {
        // LWW by HLC. If HLCs are equal (should not happen with correct
        // generators), keep the local value.
        if remote_hlc.beats(local_hlc) {
            Ok(remote)
        } else {
            Ok(local)
        }
    }

    fn is_replacement(_op: &AtomOp) -> bool {
        true // Set is always a replacement
    }

    fn encode_value(value: &AtomValue, out: &mut Vec<u8>) -> Result<(), AtomError> {
        encode_atom_value(value, out)
    }

    fn decode_value(bytes: &[u8]) -> Result<(AtomValue, usize), AtomError> {
        decode_atom_value(bytes)
    }

    fn encode_op(op: &AtomOp, out: &mut Vec<u8>) -> Result<(), AtomError> {
        match op {
            AtomOp::Set(v) => {
                out.push(0x00); // variant tag
                encode_atom_value(v, out)
            }
        }
    }

    fn decode_op(bytes: &[u8]) -> Result<(AtomOp, usize), AtomError> {
        if bytes.is_empty() {
            return Err(AtomError::Decode("empty input".into()));
        }
        match bytes[0] {
            0x00 => {
                let (val, consumed) = decode_atom_value(&bytes[1..])?;
                Ok((AtomOp::Set(val), 1 + consumed))
            }
            tag => Err(AtomError::Decode(format!("unknown AtomOp tag: {}", tag))),
        }
    }
}

// ---------------------------------------------------------------------------
// AtomError
// ---------------------------------------------------------------------------

/// Errors specific to Atom operations.
#[derive(Debug)]
pub enum AtomError {
    Decode(String),
}

impl std::fmt::Display for AtomError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AtomError::Decode(msg) => write!(f, "Atom decode error: {}", msg),
        }
    }
}

// ---------------------------------------------------------------------------
// Encoding / decoding helpers
// ---------------------------------------------------------------------------

use crate::codec::{decode_varint, encode_varint};

/// Encode an AtomValue to bytes (no type tag prefix — that's added by Value).
fn encode_atom_value(value: &AtomValue, out: &mut Vec<u8>) -> Result<(), AtomError> {
    match value {
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
            let bytes = s.as_bytes();
            encode_varint(out, bytes.len() as u64);
            out.extend_from_slice(bytes);
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

/// Decode an AtomValue from bytes. Returns the value and bytes consumed.
fn decode_atom_value(bytes: &[u8]) -> Result<(AtomValue, usize), AtomError> {
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
            let b = read_fixed::<8>(rest)?;
            Ok((AtomValue::Int(i64::from_be_bytes(b)), 9))
        }
        0x04 => {
            let b = read_fixed::<8>(rest)?;
            Ok((AtomValue::UInt(u64::from_be_bytes(b)), 9))
        }
        0x05 => {
            let b = read_fixed::<8>(rest)?;
            Ok((AtomValue::Float(AtomFloat(f64::from_be_bytes(b))), 9))
        }
        0x06 => {
            let (len, varint_bytes) = decode_varint(rest)
                .ok_or_else(|| AtomError::Decode("truncated string length".into()))?;
            let start = varint_bytes;
            let end = start + len as usize;
            if rest.len() < end {
                return Err(AtomError::Decode("truncated string body".into()));
            }
            let s = String::from_utf8(rest[start..end].to_vec())
                .map_err(|e| AtomError::Decode(format!("invalid UTF-8: {}", e)))?;
            Ok((AtomValue::String(s), 1 + end))
        }
        0x07 => {
            let (len, varint_bytes) = decode_varint(rest)
                .ok_or_else(|| AtomError::Decode("truncated bytes length".into()))?;
            let start = varint_bytes;
            let end = start + len as usize;
            if rest.len() < end {
                return Err(AtomError::Decode("truncated bytes body".into()));
            }
            Ok((AtomValue::Bytes(rest[start..end].to_vec()), 1 + end))
        }
        0x08 => {
            let b = read_fixed::<8>(rest)?;
            Ok((AtomValue::Timestamp(i64::from_be_bytes(b)), 9))
        }
        0x09 => {
            let b = read_fixed::<16>(rest)?;
            Ok((AtomValue::Uuid(b), 17))
        }
        0x0A => {
            let b = read_fixed::<16>(rest)?;
            Ok((AtomValue::Ulid(b), 17))
        }
        tag => Err(AtomError::Decode(format!("unknown AtomValue tag: {}", tag))),
    }
}

fn read_fixed<const N: usize>(bytes: &[u8]) -> Result<[u8; N], AtomError> {
    if bytes.len() < N {
        return Err(AtomError::Decode(format!(
            "expected {} bytes, got {}",
            N,
            bytes.len()
        )));
    }
    let mut arr = [0u8; N];
    arr.copy_from_slice(&bytes[..N]);
    Ok(arr)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // --- AtomFloat ---

    #[test]
    fn atom_float_eq_nan() {
        let a = AtomFloat(f64::NAN);
        let b = AtomFloat(f64::NAN);
        assert_eq!(a, b); // same bit pattern → equal
    }

    #[test]
    fn atom_float_ord() {
        let a = AtomFloat(1.0);
        let b = AtomFloat(2.0);
        assert!(a < b);
    }

    #[test]
    fn atom_float_hash() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut h1 = DefaultHasher::new();
        AtomFloat(1.5).hash(&mut h1);
        let mut h2 = DefaultHasher::new();
        AtomFloat(1.5).hash(&mut h2);
        assert_eq!(h1.finish(), h2.finish());
    }

    // --- AtomType ---

    #[test]
    fn atom_apply_set() {
        let result =
            AtomType::apply_op(AtomValue::Null, AtomOp::Set(AtomValue::Int(42)), Hlc::ZERO)
                .unwrap();
        assert_eq!(result, AtomValue::Int(42));
    }

    #[test]
    fn atom_merge_lww_remote_wins() {
        let local_hlc = Hlc::new(100, 0, 1).unwrap();
        let remote_hlc = Hlc::new(200, 0, 1).unwrap();
        let result = AtomType::merge(
            AtomValue::String("local".into()),
            local_hlc,
            AtomValue::String("remote".into()),
            remote_hlc,
        )
        .unwrap();
        assert_eq!(result, AtomValue::String("remote".into()));
    }

    #[test]
    fn atom_merge_lww_local_wins() {
        let local_hlc = Hlc::new(300, 0, 1).unwrap();
        let remote_hlc = Hlc::new(200, 0, 1).unwrap();
        let result = AtomType::merge(
            AtomValue::String("local".into()),
            local_hlc,
            AtomValue::String("remote".into()),
            remote_hlc,
        )
        .unwrap();
        assert_eq!(result, AtomValue::String("local".into()));
    }

    #[test]
    fn atom_is_replacement() {
        assert!(AtomType::is_replacement(&AtomOp::Set(AtomValue::Null)));
    }

    #[test]
    fn atom_encode_decode_roundtrip() {
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
            AtomType::encode_value(&val, &mut buf).unwrap();
            let (decoded, consumed) = AtomType::decode_value(&buf).unwrap();
            assert_eq!(consumed, buf.len());
            assert_eq!(decoded, val, "roundtrip failed for {:?}", val);
        }
    }
}
