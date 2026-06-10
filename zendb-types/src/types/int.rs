//! Integer scalar type.

use bincode::{Decode, Encode};

use crate::{core::traits::Type, Hlc};

pub type Int = i64;

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub enum IntOp {}

#[derive(Debug)]
pub enum IntError {}

impl std::fmt::Display for IntError {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {}
    }
}

impl std::error::Error for IntError {}

impl Type for Int {
    type Op = IntOp;
    type Error = IntError;

    fn apply(&mut self, op: &IntOp, _op_hlc: Hlc) -> Result<bool, IntError> {
        match *op {}
    }

    fn merge(&mut self, remote: &Int, clocks: crate::MergeClocks) -> Result<bool, IntError> {
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
        Hlc::with_device_id(ms, 0, [device; 8]).unwrap()
    }

    #[test]
    fn newer_remote_replaces_regardless_of_numeric_order() {
        let mut local = i64::MAX;
        assert!(Type::merge(
            &mut local,
            &i64::MIN,
            crate::MergeClocks::new(hlc(100, 1), hlc(200, 1)),
        )
        .unwrap());
        assert_eq!(local, i64::MIN);
    }

    #[test]
    fn stale_remote_cannot_replace_local() {
        let mut local: Int = -10;
        assert!(!Type::merge(
            &mut local,
            &100_i64,
            crate::MergeClocks::new(hlc(200, 1), hlc(100, 2)),
        )
        .unwrap());
        assert_eq!(local, -10);
    }

    #[test]
    fn merging_same_state_and_clock_is_idempotent() {
        let mut local: Int = 42;
        assert!(!Type::merge(
            &mut local,
            &42_i64,
            crate::MergeClocks::new(hlc(100, 1), hlc(100, 1)),
        )
        .unwrap());
        assert_eq!(local, 42);
    }

    #[test]
    fn bincode_roundtrips_integer_boundaries() {
        for value in [i64::MIN, -1, 0, 1, i64::MAX] {
            let encoded = encode_to_vec(value, config::standard()).unwrap();
            let (decoded, consumed): (Int, usize) =
                decode_from_slice(&encoded, config::standard()).unwrap();
            assert_eq!(consumed, encoded.len());
            assert_eq!(decoded, value);
        }
    }
}
