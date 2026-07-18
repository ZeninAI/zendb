//! Durable replication primitives with no Workspace authorization or sockets.

mod journal;
pub mod protocol;
mod table;

pub use journal::SharedJournal;
pub use protocol::{
    EventBatch, RangeRequest, SnapshotExport, SnapshotManifest, SyncSnapshotChunk,
    SyncSnapshotMeta, WorkspaceMessage, WorkspaceSyncSummary,
};
pub use table::{Table, TableConfig, TableStats, DEFAULT_MAX_BUFFERED_RECORDS};
pub use zendb_types::Change;
