//! Pure wire values used to batch table events for replication.

use bincode::{Decode, Encode};

use crate::{EventTime, InstallationId, Op, Path, PrimaryKey};

#[derive(Debug, Clone, Encode, Decode)]
pub struct Envelope {
    pub author: InstallationId,
    pub table: String,
    pub events: Vec<CompactEvent>,
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct CompactEvent {
    pub sequence: u64,
    pub time: EventTime,
    pub primary_key: PrimaryKey,
    pub path: Path,
    pub op: Op,
}
