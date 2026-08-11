//! Stable event identity and hybrid logical CRDT operation time.

use bincode::{Decode, Encode};

use crate::InstallationId;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Encode, Decode)]
pub struct EventId {
    pub author: InstallationId,
    pub sequence: u64,
}

impl EventId {
    pub const ENCODED_SIZE: usize = 16;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Encode, Decode)]
pub struct EventTime {
    pub physical_ms: u64,
    pub logical: u32,
}

impl EventTime {
    pub const ZERO: Self = Self {
        physical_ms: 0,
        logical: 0,
    };
}

impl Default for EventTime {
    fn default() -> Self {
        crate::global_clock().mint()
    }
}

impl std::fmt::Display for EventTime {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}:{}", self.physical_ms, self.logical)
    }
}

impl std::fmt::Display for EventId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}:{}", self.author, self.sequence)
    }
}
