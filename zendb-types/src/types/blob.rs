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
