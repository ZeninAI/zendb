//! String scalar type.

use bincode::{Decode, Encode};

use crate::{core::traits::Type, Hlc};

pub type String = std::string::String;

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub enum StringOp {}

#[derive(Debug)]
pub enum StringError {}

impl std::fmt::Display for StringError {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {}
    }
}

impl std::error::Error for StringError {}

impl Type for String {
    type Op = StringOp;
    type Error = StringError;

    fn apply(&mut self, op: &StringOp, _local_hlc: Hlc, _op_hlc: Hlc) -> Result<bool, StringError> {
        match *op {}
    }

    fn merge(
        &mut self,
        remote: &String,
        local_hlc: Hlc,
        remote_hlc: Hlc,
    ) -> Result<bool, StringError> {
        if remote_hlc.beats(local_hlc) {
            *self = remote.clone();
            Ok(true)
        } else {
            Ok(false)
        }
    }
}
