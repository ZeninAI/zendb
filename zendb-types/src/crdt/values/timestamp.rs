//! Timestamp scalar type.

use bincode::{Decode, Encode};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::{Cell, CellProxy, CellProxyError, EventStamp, Type, Value};

/// A Unix timestamp stored as unsigned milliseconds.
///
/// This is distinct from [`EventStamp`], which orders CRDT operations, and from an
/// ordinary `u64`, whose canonical Cell representation is [`Value::Int`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Encode, Decode)]
pub struct Timestamp(u64);

impl Timestamp {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

impl From<u64> for Timestamp {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl From<Timestamp> for u64 {
    fn from(value: Timestamp) -> Self {
        value.0
    }
}

impl CellProxy for SystemTime {
    fn from_cell(cell: &Cell) -> Result<Self, CellProxyError> {
        match &cell.value {
            Some(Value::Timestamp(value)) => UNIX_EPOCH
                .checked_add(Duration::from_millis(value.get()))
                .ok_or_else(|| CellProxyError::expected("representable SystemTime")),
            _ => Err(CellProxyError::expected("Timestamp Cell")),
        }
    }

    fn to_cell(&self, stamp: EventStamp) -> Result<Cell, CellProxyError> {
        let elapsed = self
            .duration_since(UNIX_EPOCH)
            .map_err(|_| CellProxyError::expected("SystemTime at or after the Unix epoch"))?;
        if elapsed.subsec_nanos() % 1_000_000 != 0 {
            return Err(CellProxyError::expected(
                "SystemTime with millisecond precision",
            ));
        }
        let milliseconds = u64::try_from(elapsed.as_millis())
            .map_err(|_| CellProxyError::expected("SystemTime fitting a u64 millisecond value"))?;
        Ok(Cell {
            value: Some(Value::Timestamp(Timestamp::new(milliseconds))),
            stamp,
        })
    }
}

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
        _stamps: crate::MergeStamps,
    ) -> Result<bool, TimestampError> {
        match *op {}
    }

    fn merge(
        &mut self,
        remote: &Timestamp,
        stamps: crate::MergeStamps,
    ) -> Result<bool, TimestampError> {
        if stamps.incoming > stamps.current {
            *self = *remote;
            Ok(true)
        } else {
            Ok(false)
        }
    }
}
