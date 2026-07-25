//! Stable event identity and hybrid logical time.

use bincode::{Decode, Encode};

use crate::PeerId;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Encode, Decode)]
pub struct EventId {
    pub peer_id: PeerId,
    pub sequence: u64,
}

impl EventId {
    pub fn new(peer_id: PeerId, sequence: u64) -> Self {
        Self { peer_id, sequence }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Encode, Decode)]
pub struct EventTime {
    pub physical_ms: u64,
    pub logical: u32,
}

impl EventTime {
    pub const ZERO: Self = Self {
        physical_ms: 0,
        logical: 0,
    };

    pub const fn new(physical_ms: u64, logical: u32) -> Self {
        Self {
            physical_ms,
            logical,
        }
    }
}

/// A globally ordered event stamp.
///
/// Ordering is time first and identity second:
/// `(physical_ms, logical, peer_id, sequence)`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Encode, Decode)]
pub struct EventStamp {
    pub id: EventId,
    pub time: EventTime,
}

impl EventStamp {
    pub const fn new(id: EventId, time: EventTime) -> Self {
        Self { id, time }
    }

    pub fn zero() -> Self {
        Self::default()
    }

    pub const fn physical_ms(self) -> u64 {
        self.time.physical_ms
    }

    pub const fn logical(self) -> u32 {
        self.time.logical
    }

    pub const fn peer_id(self) -> PeerId {
        self.id.peer_id
    }

    pub const fn sequence(self) -> u64 {
        self.id.sequence
    }

    pub fn beats(self, other: Self) -> bool {
        self > other
    }
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
            self.id.peer_id,
            self.id.sequence,
        )
            .cmp(&(
                other.time.physical_ms,
                other.time.logical,
                other.id.peer_id,
                other.id.sequence,
            ))
    }
}

impl std::fmt::Display for EventStamp {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "{}:{}:{}:{}",
            self.time.physical_ms, self.time.logical, self.id.peer_id, self.id.sequence
        )
    }
}
