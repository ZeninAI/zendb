use bincode::{Decode, Encode};

use crate::claims::SignatureBytes;
use zendb_types::{InviteId, Role, UserId, WorkspaceId};

/// A signed invitation to join a workspace.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct WorkspaceInvite {
    pub invite_id: InviteId,
    pub workspace_id: WorkspaceId,
    pub role: Role,
    pub inviter_user_id: UserId,
    pub target_hint: Option<String>,
    pub created_at_ms: u64,
    pub expires_at_ms: u64,
    pub signature: SignatureBytes,
}
