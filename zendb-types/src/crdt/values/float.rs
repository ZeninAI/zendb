//! Floating-point LWW scalar types.
//!
//! Floats store their IEEE-754 bit pattern so equality, hashing, bincode, and
//! CRDT convergence remain deterministic for NaNs and signed zero.

use bincode::{Decode, Encode};

use crate::{Cell, CellProxy, CellProxyError, EventStamp, Type, Value};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Encode, Decode)]
pub struct Float32(u32);

impl Float32 {
    pub fn new(value: f32) -> Self {
        Self(value.to_bits())
    }
    pub fn get(self) -> f32 {
        f32::from_bits(self.0)
    }
}

impl From<f32> for Float32 {
    fn from(value: f32) -> Self {
        Self::new(value)
    }
}

impl From<Float32> for f32 {
    fn from(value: Float32) -> Self {
        value.get()
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Encode, Decode)]
pub struct Float64(u64);

impl Float64 {
    pub fn new(value: f64) -> Self {
        Self(value.to_bits())
    }
    pub fn get(self) -> f64 {
        f64::from_bits(self.0)
    }
}

impl From<f64> for Float64 {
    fn from(value: f64) -> Self {
        Self::new(value)
    }
}

impl From<Float64> for f64 {
    fn from(value: Float64) -> Self {
        value.get()
    }
}

impl CellProxy for f32 {
    fn from_cell(cell: &Cell) -> Result<Self, CellProxyError> {
        match &cell.value {
            Some(Value::Float32(value)) => Ok(value.get()),
            _ => Err(CellProxyError::expected("Float32 Cell")),
        }
    }

    fn to_cell(&self, stamp: EventStamp) -> Result<Cell, CellProxyError> {
        Ok(Cell {
            value: Some(Value::Float32(Float32::new(*self))),
            stamp,
        })
    }
}

impl CellProxy for f64 {
    fn from_cell(cell: &Cell) -> Result<Self, CellProxyError> {
        match &cell.value {
            Some(Value::Float64(value)) => Ok(value.get()),
            _ => Err(CellProxyError::expected("Float64 Cell")),
        }
    }

    fn to_cell(&self, stamp: EventStamp) -> Result<Cell, CellProxyError> {
        Ok(Cell {
            value: Some(Value::Float64(Float64::new(*self))),
            stamp,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub enum Float32Op {}

#[derive(Debug)]
pub enum Float32Error {}

impl std::fmt::Display for Float32Error {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {}
    }
}

impl std::error::Error for Float32Error {}

impl Type for Float32 {
    type Op = Float32Op;
    type Error = Float32Error;

    fn apply(&mut self, op: &Self::Op, _stamps: crate::MergeStamps) -> Result<bool, Self::Error> {
        match *op {}
    }

    fn merge(&mut self, remote: &Self, stamps: crate::MergeStamps) -> Result<bool, Self::Error> {
        if stamps.incoming.beats(stamps.current) {
            *self = *remote;
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub enum Float64Op {}

#[derive(Debug)]
pub enum Float64Error {}

impl std::fmt::Display for Float64Error {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {}
    }
}

impl std::error::Error for Float64Error {}

impl Type for Float64 {
    type Op = Float64Op;
    type Error = Float64Error;

    fn apply(&mut self, op: &Self::Op, _stamps: crate::MergeStamps) -> Result<bool, Self::Error> {
        match *op {}
    }

    fn merge(&mut self, remote: &Self, stamps: crate::MergeStamps) -> Result<bool, Self::Error> {
        if stamps.incoming.beats(stamps.current) {
            *self = *remote;
            Ok(true)
        } else {
            Ok(false)
        }
    }
}
