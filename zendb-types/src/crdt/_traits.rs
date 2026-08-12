//! Public CRDT contracts and internal metadata mutation support.

use bincode::{Decode, Encode};

use crate::TypeTag;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TypeMismatch {
    pub expected: TypeTag,
    pub actual: TypeTag,
}

impl TypeMismatch {
    pub const fn new(expected: TypeTag, actual: TypeTag) -> Self {
        Self { expected, actual }
    }
}

impl std::fmt::Display for TypeMismatch {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "type mismatch: expected {}, got {}",
            self.expected, self.actual
        )
    }
}

impl std::error::Error for TypeMismatch {}

pub trait Type: Sized + Encode + Decode<()> + Clone {
    type Op: Encode + Decode<()> + Clone;
    type Error: std::error::Error;

    fn apply(&mut self, remote: crate::EventTime, op: &Self::Op) -> Result<bool, Self::Error>;
    fn event_time(&self) -> crate::EventTime;
    fn is_tombstone(&self) -> bool;
}

pub trait OpDispatcher {
    fn dispatch(
        &mut self,
        remote: crate::EventTime,
        path: &[crate::Segment],
        op: &crate::TypeOp,
    ) -> Result<bool, crate::TypeError>;
}

pub(crate) trait TypeMetadata {
    fn set_event_time(&mut self, time: crate::EventTime);
    fn set_tombstone(&mut self, tombstone: bool);
}

pub trait ContainerType: Type {
    type Segment: std::fmt::Debug + Clone + PartialEq + Eq + Encode + Decode<()>;

    fn ensure_child(
        &mut self,
        remote: crate::EventTime,
        segment: &Self::Segment,
        expected: TypeTag,
    ) -> Result<Option<&mut crate::Value>, Self::Error>;
}
