//! The durable unit of table mutation.

use bincode::{Decode, Encode};

use crate::{EventStamp, Op, Path, PrimaryKey};

#[derive(Debug, Clone, Encode, Decode)]
pub struct Event {
    pub stamp: EventStamp, // This should be the first field because serialization order
    pub primary_key: PrimaryKey,
    pub path: Path,
    pub op: Op,
}
