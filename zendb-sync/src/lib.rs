//! # zendb-sync
//!
//! Replication and bootstrap abstractions for ZeninDB.
//!
//! This crate is independent of transport and below product-domain logic. Its
//! role is to define:
//! - replica summaries and version vectors
//! - shared journal envelopes
//! - sync request / response message types
//! - snapshot metadata and chunk transfer records
//!
//! Storage access and synchronization orchestration stay in the concrete
//! `zendb-engine::Workspace`; this crate does not define a storage super-trait.

pub mod journal;
pub mod messages;
pub mod snapshot;
pub mod summary;

pub use journal::{EventIdentity, ReplicatedEvent, SyncEnvelope};
pub use messages::{
    EventBatch, RangeRequest, SyncCapabilities, SyncSnapshotChunk, SyncSnapshotMeta,
    SyncSummaryMessage,
};
pub use snapshot::{SnapshotExport, SnapshotManifest};
pub use summary::{VersionVector, WorkspaceSyncSummary};
