//! Stable event identity and hybrid logical time.

use bincode::{Decode, Encode};

use crate::InstallationId;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Encode, Decode)]
pub struct EventId {
    pub author: InstallationId,
    pub sequence: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Encode, Decode)]
pub struct EventTime {
    pub physical_ms: u64,
    pub logical: u32,
}

/// A globally ordered event stamp.
///
/// Ordering is time first and identity second:
/// `(physical_ms, logical, author, sequence)`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Encode, Decode)]
pub struct EventStamp {
    pub id: EventId,
    pub time: EventTime,
}

impl PartialOrd for EventStamp {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for EventStamp {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (
            self.time.physical_ms,
            self.time.logical,
            self.id.author,
            self.id.sequence,
        )
            .cmp(&(
                other.time.physical_ms,
                other.time.logical,
                other.id.author,
                other.id.sequence,
            ))
    }
}

impl std::fmt::Display for EventStamp {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "{}:{}:{}:{}",
            self.time.physical_ms, self.time.logical, self.id.author, self.id.sequence
        )
    }
}
