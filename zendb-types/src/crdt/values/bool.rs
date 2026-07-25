//! Boolean scalar type.

use bincode::{Decode, Encode};

use crate::{Cell, CellProxy, CellProxyError, EventStamp, Type, Value};

pub type Bool = bool;

impl CellProxy for bool {
    fn from_cell(cell: &Cell) -> Result<Self, CellProxyError> {
        match &cell.value {
            Some(Value::Bool(value)) => Ok(*value),
            _ => Err(CellProxyError::expected("Bool Cell")),
        }
    }

    fn to_cell(&self, stamp: EventStamp) -> Result<Cell, CellProxyError> {
        Ok(Cell {
            value: Some(Value::Bool(*self)),
            stamp,
        })
    }
}

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

    fn apply(&mut self, op: &BoolOp, _stamps: crate::MergeStamps) -> Result<bool, BoolError> {
        match *op {}
    }

    fn merge(&mut self, remote: &Bool, stamps: crate::MergeStamps) -> Result<bool, BoolError> {
        if stamps.incoming.beats(stamps.current) {
            *self = *remote;
            Ok(true)
        } else {
            Ok(false)
        }
    }
}
