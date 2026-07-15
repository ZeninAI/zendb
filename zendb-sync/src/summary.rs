use bincode::{Decode, Encode};
pub use zendb_types::VersionVector;
use zendb_types::{ContiguousFrontier, WorkspaceId};

/// Summary exchanged before requesting snapshots or event ranges.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct WorkspaceSyncSummary {
    pub workspace_id: WorkspaceId,
    /// Contiguous shared-history receipt proof used for exact range requests.
    pub frontier: ContiguousFrontier,
    pub snapshot_generation: Option<u64>,
    /// Latest shared watermark represented by the offered snapshot.
    pub compacted_through: Option<zendb_types::Hlc>,
}
