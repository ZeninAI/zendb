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

    fn apply(&mut self, op: &BoolOp, _local_hlc: Hlc, _op_hlc: Hlc) -> Result<bool, BoolError> {
        match *op {}
    }

    fn merge(&mut self, remote: &Bool, local_hlc: Hlc, remote_hlc: Hlc) -> Result<bool, BoolError> {
        if remote_hlc.beats(local_hlc) {
            *self = *remote;
            Ok(true)
        } else {
            Ok(false)
        }
    }
}
