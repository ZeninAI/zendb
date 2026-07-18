//! Timestamp scalar type.

use bincode::{Decode, Encode};

use crate::{CellCodecError, CrdtCodec, Hlc, Type, Value};

pub type Timestamp = u64;

/// Explicit timestamp codec for `u64` fields.
///
/// `u64` defaults to the `Int` codec. A derived field selects this alternate
/// CRDT representation with `#[cell(codec =
/// "zendb_types::crdt::values::TimestampCodec")]`.
pub struct TimestampCodec;

impl CrdtCodec for TimestampCodec {
    type Rust = u64;

    fn encode(value: &u64, _hlc: Hlc) -> Value {
        Value::Timestamp(*value)
    }

    fn decode(value: &Value) -> Result<u64, CellCodecError> {
        match value {
            Value::Timestamp(value) => Ok(*value),
            _ => Err(CellCodecError::expected("Timestamp")),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub enum TimestampOp {}

#[derive(Debug)]
pub enum TimestampError {}

impl std::fmt::Display for TimestampError {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {}
    }
}

impl std::error::Error for TimestampError {}

impl Type for Timestamp {
    type Op = TimestampOp;
    type Error = TimestampError;

    fn apply(&mut self, op: &TimestampOp, _op_hlc: Hlc) -> Result<bool, TimestampError> {
        match *op {}
    }

    fn merge(
        &mut self,
        remote: &Timestamp,
        clocks: crate::MergeClocks,
    ) -> Result<bool, TimestampError> {
        if clocks.remote.beats(clocks.local) {
            *self = *remote;
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bincode::{config, decode_from_slice, encode_to_vec};

    fn hlc(ms: u64, device: u8) -> Hlc {
        Hlc::with_device_id(ms, 0, crate::DeviceId::from_bytes([device; 16])).unwrap()
    }

    #[test]
    fn merge_uses_write_clock_not_timestamp_value() {
        let mut local = u64::MAX;
        assert!(Type::merge(
            &mut local,
            &0,
            crate::MergeClocks::new(hlc(100, 1), hlc(200, 1)),
        )
        .unwrap());
        assert_eq!(local, 0);
    }

    #[test]
    fn stale_remote_timestamp_is_ignored() {
        let mut local = 1;
        assert!(!Type::merge(
            &mut local,
            &u64::MAX,
            crate::MergeClocks::new(hlc(200, 1), hlc(100, 2)),
        )
        .unwrap());
        assert_eq!(local, 1);
    }

    #[test]
    fn bincode_roundtrips_timestamp_boundaries() {
        for value in [0, 1, u64::MAX] {
            let encoded = encode_to_vec(value, config::standard()).unwrap();
            let (decoded, consumed): (Timestamp, usize) =
                decode_from_slice(&encoded, config::standard()).unwrap();
            assert_eq!(consumed, encoded.len());
            assert_eq!(decoded, value);
        }
    }
}
