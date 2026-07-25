//! The durable unit of table mutation.

use bincode::{Decode, Encode};

use crate::{EventStamp, Op, Path, PrimaryKey};

#[derive(Debug, Clone, Encode, Decode)]
pub struct Event {
    pub primary_key: PrimaryKey,
    pub path: Path,
    pub op: Op,
    pub stamp: EventStamp,
}
