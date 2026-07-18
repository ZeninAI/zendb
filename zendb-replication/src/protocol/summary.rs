use std::collections::BTreeMap;

use bincode::{Decode, Encode};
use zendb_types::{ContiguousFrontier, Hlc, WorkspaceId};

/// State advertised before peers request journal ranges or a snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct WorkspaceSyncSummary {
    pub workspace_id: WorkspaceId,
    pub frontier: ContiguousFrontier,
    pub snapshot_generation: Option<u64>,
    pub compacted_through: Option<Hlc>,
    /// A local boundary skipped materialization, so matching event frontiers
    /// are insufficient and current shared state must be reconciled.
    pub requests_state_reconciliation: bool,
    /// Canonical roots only for tables without local boundaries.
    pub table_merkle_roots: BTreeMap<String, [u8; 32]>,
}
