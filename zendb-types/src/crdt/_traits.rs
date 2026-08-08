//! Core traits for the ZenDB type system.

use bincode::{Decode, Encode};

use crate::Segment;
use crate::crdt::{Cell, EventStamp, Op};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MergeStamps {
    pub current: EventStamp,
    pub incoming: EventStamp,
}

impl MergeStamps {
    pub const fn new(current: EventStamp, incoming: EventStamp) -> Self {
        Self { current, incoming }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CellProxyError {
    MissingField(&'static str),
    InvalidField {
        field: &'static str,
        expected: &'static str,
    },
    InvalidValue(&'static str),
}

impl CellProxyError {
    pub fn expected(expected: &'static str) -> Self {
        Self::InvalidValue(expected)
    }
}

impl std::fmt::Display for CellProxyError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingField(field) => write!(formatter, "missing Cell field {field}"),
            Self::InvalidField { field, expected } => {
                write!(formatter, "invalid Cell field {field}: expected {expected}")
            }
            Self::InvalidValue(expected) => {
                write!(formatter, "invalid Cell value: expected {expected}")
            }
        }
    }
}

impl std::error::Error for CellProxyError {}

pub trait CellProxy: Sized {
    fn from_cell(cell: &Cell) -> Result<Self, CellProxyError>;

    fn to_cell(&self, stamp: EventStamp) -> Result<Cell, CellProxyError>;
}

pub trait Type: Sized + Encode + Decode<()> {
    type Op: Encode + Decode<()>;
    type Error: std::error::Error;

    fn apply(&mut self, op: &Self::Op, stamps: MergeStamps) -> Result<bool, Self::Error>;

    fn merge(&mut self, incoming: &Self, stamps: MergeStamps) -> Result<bool, Self::Error>;

    fn apply_stamp(&self, stamps: MergeStamps, changed: bool) -> Option<EventStamp> {
        (changed && stamps.incoming > stamps.current).then_some(stamps.incoming)
    }

    fn merge_stamp(&self, stamps: MergeStamps, _changed: bool) -> Option<EventStamp> {
        (stamps.incoming > stamps.current).then_some(stamps.incoming)
    }

    fn max_stamp(&self) -> EventStamp {
        EventStamp::default()
    }
}

pub trait ContainerType: Type {
    fn child(&self, segment: &Segment) -> Option<&Cell>;

    fn child_mut(&mut self, segment: &Segment) -> Option<&mut Cell>;

    fn cell_at_path(&self, path: &[Segment]) -> Option<&Cell> {
        let (segment, remaining) = path.split_first()?;
        self.child(segment)?.cell_at_path(remaining)
    }

    fn cell_at_path_mut(&mut self, path: &[Segment]) -> Option<&mut Cell> {
        let (segment, remaining) = path.split_first()?;
        self.child_mut(segment)?.cell_at_path_mut(remaining)
    }

    fn apply_walk(
        &mut self,
        op: &Op,
        stamps: MergeStamps,
        path: &[Segment],
    ) -> Result<bool, Self::Error>;
}
