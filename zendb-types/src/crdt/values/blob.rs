//! Binary blob scalar type.

use std::{io, ops::Deref};

use bincode::{Decode, Encode};

use crate::utils::serdes::{deserialize_from, serialize_to_vec};
use crate::{Cell, CellProxy, CellProxyError, EventStamp, Type, Value};

/// Opaque bytes stored as an LWW scalar.
///
/// `Blob` is deliberately a newtype instead of a `Vec<u8>` alias to prevent
/// accidental confusion with protocol byte buffers.
#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Encode, Decode)]
pub struct Blob(Vec<u8>);

impl Blob {
    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }

    pub fn encode<T: Encode>(value: &T) -> io::Result<Self> {
        serialize_to_vec(value).map(Self)
    }

    pub fn decode<T: Decode<()>>(&self) -> io::Result<T> {
        deserialize_from(self.as_slice())
    }
}

impl From<Vec<u8>> for Blob {
    fn from(value: Vec<u8>) -> Self {
        Self(value)
    }
}

impl From<&[u8]> for Blob {
    fn from(value: &[u8]) -> Self {
        Self(value.to_vec())
    }
}

impl AsRef<[u8]> for Blob {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl Deref for Blob {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl CellProxy for Vec<u8> {
    fn from_cell(cell: &Cell) -> Result<Self, CellProxyError> {
        match &cell.value {
            Some(Value::Blob(value)) => Ok(value.to_vec()),
            _ => Err(CellProxyError::expected("Blob Cell")),
        }
    }

    fn to_cell(&self, stamp: EventStamp) -> Result<Cell, CellProxyError> {
        Ok(Cell {
            value: Some(Value::Blob(Blob::from(self.as_slice()))),
            stamp,
        })
    }
}

impl<const N: usize> CellProxy for [u8; N] {
    fn from_cell(cell: &Cell) -> Result<Self, CellProxyError> {
        let Some(Value::Blob(value)) = &cell.value else {
            return Err(CellProxyError::expected("Blob Cell"));
        };
        value
            .as_slice()
            .try_into()
            .map_err(|_| CellProxyError::expected("fixed-size Blob Cell"))
    }

    fn to_cell(&self, stamp: EventStamp) -> Result<Cell, CellProxyError> {
        Ok(Cell {
            value: Some(Value::Blob(Blob::from(self.as_slice()))),
            stamp,
        })
    }
}

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

    fn apply(&mut self, op: &BlobOp, _stamps: crate::MergeStamps) -> Result<bool, BlobError> {
        match *op {}
    }

    fn merge(&mut self, remote: &Blob, stamps: crate::MergeStamps) -> Result<bool, BlobError> {
        if stamps.incoming.beats(stamps.current) {
            *self = remote.clone();
            Ok(true)
        } else {
            Ok(false)
        }
    }
}
