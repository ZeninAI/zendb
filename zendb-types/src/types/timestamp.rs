//! Timestamp scalar type.

use bincode::{Decode, Encode};

use crate::{core::traits::Type, Hlc};

pub type Timestamp = u64;

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

    fn apply(
        &mut self,
        op: &TimestampOp,
        _local_hlc: Hlc,
        _op_hlc: Hlc,
    ) -> Result<bool, TimestampError> {
        match *op {}
    }

    fn merge(
        &mut self,
        remote: &Timestamp,
        local_hlc: Hlc,
        remote_hlc: Hlc,
    ) -> Result<bool, TimestampError> {
        if remote_hlc.beats(local_hlc) {
            *self = *remote;
            Ok(true)
        } else {
            Ok(false)
        }
    }
}
