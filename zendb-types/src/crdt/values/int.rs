//! Integer scalar type.

use bincode::{Decode, Encode};

use crate::{Cell, CellProxy, CellProxyError, EventStamp, Type, Value};

pub type Int = i64;

impl CellProxy for i64 {
    fn from_cell(cell: &Cell) -> Result<Self, CellProxyError> {
        match &cell.value {
            Some(Value::Int(value)) => Ok(*value),
            _ => Err(CellProxyError::expected("Int Cell")),
        }
    }

    fn to_cell(&self, stamp: EventStamp) -> Result<Cell, CellProxyError> {
        Ok(Cell {
            value: Some(Value::Int(*self)),
            stamp,
        })
    }
}

impl CellProxy for i8 {
    fn from_cell(cell: &Cell) -> Result<Self, CellProxyError> {
        let Some(Value::Int(value)) = &cell.value else {
            return Err(CellProxyError::expected("Int Cell"));
        };
        i8::try_from(*value).map_err(|_| CellProxyError::expected("in-range i8 Int Cell"))
    }

    fn to_cell(&self, stamp: EventStamp) -> Result<Cell, CellProxyError> {
        Ok(Cell {
            value: Some(Value::Int(i64::from(*self))),
            stamp,
        })
    }
}

impl CellProxy for i16 {
    fn from_cell(cell: &Cell) -> Result<Self, CellProxyError> {
        let Some(Value::Int(value)) = &cell.value else {
            return Err(CellProxyError::expected("Int Cell"));
        };
        i16::try_from(*value).map_err(|_| CellProxyError::expected("in-range i16 Int Cell"))
    }

    fn to_cell(&self, stamp: EventStamp) -> Result<Cell, CellProxyError> {
        Ok(Cell {
            value: Some(Value::Int(i64::from(*self))),
            stamp,
        })
    }
}

impl CellProxy for i32 {
    fn from_cell(cell: &Cell) -> Result<Self, CellProxyError> {
        let Some(Value::Int(value)) = &cell.value else {
            return Err(CellProxyError::expected("Int Cell"));
        };
        i32::try_from(*value).map_err(|_| CellProxyError::expected("in-range i32 Int Cell"))
    }

    fn to_cell(&self, stamp: EventStamp) -> Result<Cell, CellProxyError> {
        Ok(Cell {
            value: Some(Value::Int(i64::from(*self))),
            stamp,
        })
    }
}

impl CellProxy for isize {
    fn from_cell(cell: &Cell) -> Result<Self, CellProxyError> {
        let Some(Value::Int(value)) = &cell.value else {
            return Err(CellProxyError::expected("Int Cell"));
        };
        isize::try_from(*value).map_err(|_| CellProxyError::expected("in-range isize Int Cell"))
    }

    fn to_cell(&self, stamp: EventStamp) -> Result<Cell, CellProxyError> {
        let value = i64::try_from(*self)
            .map_err(|_| CellProxyError::expected("isize representable as i64"))?;
        Ok(Cell {
            value: Some(Value::Int(value)),
            stamp,
        })
    }
}

impl CellProxy for u8 {
    fn from_cell(cell: &Cell) -> Result<Self, CellProxyError> {
        let Some(Value::Int(value)) = &cell.value else {
            return Err(CellProxyError::expected("Int Cell"));
        };
        u8::try_from(*value).map_err(|_| CellProxyError::expected("in-range u8 Int Cell"))
    }

    fn to_cell(&self, stamp: EventStamp) -> Result<Cell, CellProxyError> {
        Ok(Cell {
            value: Some(Value::Int(i64::from(*self))),
            stamp,
        })
    }
}

impl CellProxy for u16 {
    fn from_cell(cell: &Cell) -> Result<Self, CellProxyError> {
        let Some(Value::Int(value)) = &cell.value else {
            return Err(CellProxyError::expected("Int Cell"));
        };
        u16::try_from(*value).map_err(|_| CellProxyError::expected("in-range u16 Int Cell"))
    }

    fn to_cell(&self, stamp: EventStamp) -> Result<Cell, CellProxyError> {
        Ok(Cell {
            value: Some(Value::Int(i64::from(*self))),
            stamp,
        })
    }
}

impl CellProxy for u32 {
    fn from_cell(cell: &Cell) -> Result<Self, CellProxyError> {
        let Some(Value::Int(value)) = &cell.value else {
            return Err(CellProxyError::expected("Int Cell"));
        };
        u32::try_from(*value).map_err(|_| CellProxyError::expected("in-range u32 Int Cell"))
    }

    fn to_cell(&self, stamp: EventStamp) -> Result<Cell, CellProxyError> {
        Ok(Cell {
            value: Some(Value::Int(i64::from(*self))),
            stamp,
        })
    }
}

impl CellProxy for u64 {
    fn from_cell(cell: &Cell) -> Result<Self, CellProxyError> {
        let Some(Value::Int(value)) = &cell.value else {
            return Err(CellProxyError::expected("Int Cell"));
        };
        u64::try_from(*value).map_err(|_| CellProxyError::expected("non-negative u64 Int Cell"))
    }

    fn to_cell(&self, stamp: EventStamp) -> Result<Cell, CellProxyError> {
        let value = i64::try_from(*self)
            .map_err(|_| CellProxyError::expected("u64 no greater than i64::MAX"))?;
        Ok(Cell {
            value: Some(Value::Int(value)),
            stamp,
        })
    }
}

impl CellProxy for usize {
    fn from_cell(cell: &Cell) -> Result<Self, CellProxyError> {
        let Some(Value::Int(value)) = &cell.value else {
            return Err(CellProxyError::expected("Int Cell"));
        };
        usize::try_from(*value)
            .map_err(|_| CellProxyError::expected("non-negative, in-range usize Int Cell"))
    }

    fn to_cell(&self, stamp: EventStamp) -> Result<Cell, CellProxyError> {
        let value = i64::try_from(*self)
            .map_err(|_| CellProxyError::expected("usize no greater than i64::MAX"))?;
        Ok(Cell {
            value: Some(Value::Int(value)),
            stamp,
        })
    }
}

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

    fn apply(&mut self, op: &IntOp, _stamps: crate::MergeStamps) -> Result<bool, IntError> {
        match *op {}
    }

    fn merge(&mut self, remote: &Int, stamps: crate::MergeStamps) -> Result<bool, IntError> {
        if stamps.incoming > stamps.current {
            *self = *remote;
            Ok(true)
        } else {
            Ok(false)
        }
    }
}
