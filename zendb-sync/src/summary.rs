use bincode::{Decode, Encode};
use zendb_identity::WorkspaceId;
pub use zendb_types::VersionVector;

/// Summary exchanged before requesting snapshots or event ranges.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct WorkspaceSyncSummary {
    pub workspace_id: WorkspaceId,
    pub version_vector: VersionVector,
    pub snapshot_generation: Option<u64>,
    /// The policy snapshot used to determine what this replica is allowed to
    /// know. A version vector without this scope is ambiguous for partial
    /// replicas.
    pub policy_epoch: u64,
    pub resource_scope_hash: [u8; 32],
}
