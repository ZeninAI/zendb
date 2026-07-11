//! Replication journal type aliases and local wrapper records.

use bincode::{Decode, Encode};

pub use zendb_types::{EventIdentity, ReplicatedEvent, SyncEnvelope};

/// Journal record wrapper reserved for future append metadata such as ingest
/// time, source peer, and verification status.
#[derive(Debug, Clone, Encode, Decode)]
pub struct SharedJournalRecord {
    pub replicated_event: ReplicatedEvent,
}
