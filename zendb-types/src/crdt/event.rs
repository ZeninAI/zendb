//! Durable table events and their per-operation CRDT clocks.

use bincode::{Decode, Encode};

use crate::{EventId, EventTime, PrimaryKey, Segment, TypeOp};

#[derive(Debug, Clone, Encode, Decode)]
pub struct PathOp {
    pub path: Vec<Segment>,
    pub time: EventTime,
    pub op: TypeOp,
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct Event {
    pub id: EventId,
    pub primary_key: PrimaryKey,
    pub operations: Vec<PathOp>,
}
