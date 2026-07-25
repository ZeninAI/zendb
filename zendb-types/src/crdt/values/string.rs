//! String scalar type.

use bincode::{Decode, Encode};

use crate::{Cell, CellProxy, CellProxyError, EventStamp, Type, Value};

pub type String = std::string::String;

impl CellProxy for std::string::String {
    fn from_cell(cell: &Cell) -> Result<Self, CellProxyError> {
        match &cell.value {
            Some(Value::String(value)) => Ok(value.clone()),
            _ => Err(CellProxyError::expected("String Cell")),
        }
    }

    fn to_cell(&self, stamp: EventStamp) -> Result<Cell, CellProxyError> {
        Ok(Cell {
            value: Some(Value::String(self.clone())),
            stamp,
        })
    }
}

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

    fn apply(&mut self, op: &StringOp, _stamps: crate::MergeStamps) -> Result<bool, StringError> {
        match *op {}
    }

    fn merge(&mut self, remote: &String, stamps: crate::MergeStamps) -> Result<bool, StringError> {
        if stamps.incoming.beats(stamps.current) {
            *self = remote.clone();
            Ok(true)
        } else {
            Ok(false)
        }
    }
}
