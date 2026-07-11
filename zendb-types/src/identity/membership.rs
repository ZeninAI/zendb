use bincode::{Decode, Encode};

use crate::identity::{DeviceId, KeyId, PrincipalId, Role, UserId, WorkspaceId};

/// Public key bytes for device identity.
///
/// This is a simplified version - the full signing/verification logic
/// remains in zendb-identity crate.
pub type PublicKeyBytes = Vec<u8>;

/// Durable record that a device belongs to a user and may participate in a workspace.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct DeviceMembership {
    pub workspace_id: WorkspaceId,
    /// A device may represent a user or a scoped guest principal.
    pub principal: PrincipalId,
    pub user_id: Option<UserId>,
    pub device_id: DeviceId,
    pub device_key_id: KeyId,
    pub device_key: PublicKeyBytes,
    pub labels: Vec<String>,
    pub granted_at_ms: u64,
    pub revoked_at_ms: Option<u64>,
}

/// Durable record that a user belongs to a workspace with a specific role.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct WorkspaceMembership {
    pub workspace_id: WorkspaceId,
    pub user_id: UserId,
    pub role: Role,
    pub granted_by: UserId,
    pub granted_at_ms: u64,
    pub revoked_at_ms: Option<u64>,
}
