use bincode::{Decode, Encode};

use zendb_types::{DeviceId, KeyId, PrincipalId, UserId, WorkspaceId};

pub type PublicKeyBytes = Vec<u8>;
pub type SignatureBytes = Vec<u8>;

/// Compatibility name for the shared trust type. Trust classification is
/// authorization input, so it must not have a second identity-crate version.
pub use zendb_types::DeviceTrust as TrustTier;

/// Claims a peer presents during session handshake.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct PeerClaims {
    pub workspace_ids: Vec<WorkspaceId>,
    pub issuer_key_id: zendb_types::KeyId,
    pub issued_at_ms: u64,
    pub expires_at_ms: Option<u64>,
    pub signature: SignatureBytes,
}

/// Authenticated identity attached to a live session.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct PeerIdentity {
    pub device_id: DeviceId,
    /// `None` means the principal is not a user, such as a guest, service,
    /// operator, or device principal. It never means an unauthenticated peer.
    pub user_id: Option<UserId>,
    /// The principal is authoritative; `user_id` is a denormalized convenience
    /// field and must agree with `PrincipalId::User` when present.
    pub principal: PrincipalId,
    /// Identifies the public key used to authenticate this device. It can
    /// rotate independently from the stable device and HLC identity.
    pub device_key_id: KeyId,
    pub device_key: PublicKeyBytes,
    pub claims: PeerClaims,
    pub trust_tier: TrustTier,
}

impl PeerIdentity {
    /// Prevents a denormalized user field from disagreeing with the principal
    /// that the authorization evaluator will actually use.
    pub fn principal_matches_user(&self) -> bool {
        match (&self.principal, &self.user_id) {
            (PrincipalId::User(principal_user), Some(user_id)) => principal_user == user_id,
            (PrincipalId::User(_), None) => false,
            (_, None) => true,
            (_, Some(_)) => false,
        }
    }
}
