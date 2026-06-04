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

    fn apply(&mut self, op: &IntOp, _local_hlc: Hlc, _op_hlc: Hlc) -> Result<bool, IntError> {
        match *op {}
    }

    fn merge(&mut self, remote: &Int, local_hlc: Hlc, remote_hlc: Hlc) -> Result<bool, IntError> {
        if remote_hlc.beats(local_hlc) {
            *self = *remote;
            Ok(true)
        } else {
            Ok(false)
        }
    }
}
