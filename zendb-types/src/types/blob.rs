//! Binary blob scalar type.

use bincode::{Decode, Encode};

use crate::{core::traits::Type, Hlc};

pub type Blob = Vec<u8>;

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub enum BlobOp {}

#[derive(Debug)]
pub enum BlobError {}

impl std::fmt::Display for BlobError {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {}
    }
}

impl std::error::Error for BlobError {}

impl Type for Blob {
    type Op = BlobOp;
    type Error = BlobError;

    fn apply(&mut self, op: &BlobOp, _local_hlc: Hlc, _op_hlc: Hlc) -> Result<bool, BlobError> {
        match *op {}
    }

    fn merge(&mut self, remote: &Blob, local_hlc: Hlc, remote_hlc: Hlc) -> Result<bool, BlobError> {
        if remote_hlc.beats(local_hlc) {
            *self = remote.clone();
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
    fn newer_remote_replaces_with_an_independent_clone() {
        let remote = vec![1, 2, 3];
        let mut local = vec![9];
        assert!(Type::merge(&mut local, &remote, hlc(100, 1), hlc(200, 1)).unwrap());
        assert_eq!(local, remote);
        assert_ne!(local.as_ptr(), remote.as_ptr());
    }

    #[test]
    fn stale_remote_blob_is_ignored() {
        let mut local = vec![1, 2, 3];
        assert!(!Type::merge(&mut local, &vec![9], hlc(200, 1), hlc(100, 2)).unwrap());
        assert_eq!(local, vec![1, 2, 3]);
    }

    #[test]
    fn bincode_roundtrips_empty_and_all_byte_values() {
        for value in [Vec::new(), (0u8..=u8::MAX).collect()] {
            let encoded = encode_to_vec(&value, config::standard()).unwrap();
            let (decoded, consumed): (Blob, usize) =
                decode_from_slice(&encoded, config::standard()).unwrap();
            assert_eq!(consumed, encoded.len());
            assert_eq!(decoded, value);
        }
    }
}
