//! Boolean scalar type.

use bincode::{Decode, Encode};

use crate::{core::traits::Type, Hlc};

pub type Bool = bool;

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub enum BoolOp {}

#[derive(Debug)]
pub enum BoolError {}

impl std::fmt::Display for BoolError {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {}
    }
}

impl std::error::Error for BoolError {}

impl Type for Bool {
    type Op = BoolOp;
    type Error = BoolError;

    fn apply(&mut self, op: &BoolOp, _op_hlc: Hlc) -> Result<bool, BoolError> {
        match *op {}
    }

    fn merge(&mut self, remote: &Bool, clocks: crate::MergeClocks) -> Result<bool, BoolError> {
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
    fn newer_remote_value_wins() {
        let mut local = false;
        assert!(Type::merge(
            &mut local,
            &true,
            crate::MergeClocks::new(hlc(100, 1), hlc(200, 2)),
        )
        .unwrap());
        assert!(local);
    }

    #[test]
    fn older_and_equal_clock_values_are_ignored() {
        let mut local = true;
        assert!(!Type::merge(
            &mut local,
            &false,
            crate::MergeClocks::new(hlc(200, 2), hlc(100, 1)),
        )
        .unwrap());
        assert!(!Type::merge(
            &mut local,
            &false,
            crate::MergeClocks::new(hlc(200, 2), hlc(200, 2)),
        )
        .unwrap());
        assert!(local);
    }

    #[test]
    fn device_id_breaks_same_time_ties() {
        let mut local = false;
        assert!(Type::merge(
            &mut local,
            &true,
            crate::MergeClocks::new(hlc(100, 1), hlc(100, 2)),
        )
        .unwrap());
        assert!(local);
    }

    #[test]
    fn bincode_roundtrips_both_values() {
        for value in [false, true] {
            let encoded = encode_to_vec(value, config::standard()).unwrap();
            let (decoded, consumed): (Bool, usize) =
                decode_from_slice(&encoded, config::standard()).unwrap();
            assert_eq!(consumed, encoded.len());
            assert_eq!(decoded, value);
        }
    }
}
