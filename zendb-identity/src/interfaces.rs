//! Identity-side interfaces used by bootstrap and authenticated transport.
//!
//! These traits deliberately stop at key proof, credential issuance, and
//! revocation. They do not decide table-level permissions; that is the pure
//! `zendb_types::AuthorizationEvaluator` boundary.

use std::io;

use zendb_types::{CredentialId, DeviceId, KeyId, PrincipalId, WorkspaceId};

use crate::{
    PeerAuthContext, PublicKeyBytes, SignatureBytes, WorkspaceClaims, WorkspaceCredential,
    WorkspaceInvite,
};

/// Evidence accepted by a workspace authority when enrolling a device. The
/// proof is explicit so online login, QR pairing, and invite-based admission do
/// not get conflated into one weak boolean.
pub enum AdmissionProof {
    OnlineUser(WorkspaceClaims),
    TrustedPeer(PeerAuthContext),
    SignedInvite(WorkspaceInvite),
}

/// Access to the private key belonging to one installation/profile.
/// Implementations should keep private key bytes inside an OS keystore when
/// available; callers receive signatures, not the key itself.
pub trait DeviceSigner: Send + Sync + 'static {
    fn device_id(&self) -> DeviceId;
    /// Stable identifier for the public key currently used by this device.
    /// This is distinct from `DeviceId` so a device can rotate keys without
    /// changing its replication identity.
    fn key_id(&self) -> KeyId;
    fn public_key(&self) -> PublicKeyBytes;
    fn sign(&self, message: &[u8]) -> io::Result<SignatureBytes>;
}

/// Local workspace trust storage. Credentials and revocations are kept behind
/// one boundary because they are read from the same local trust snapshot.
/// Implementations must key credentials by workspace, principal, and device.
pub trait WorkspaceTrustStore: Send + Sync + 'static {
    fn get(
        &self,
        workspace_id: &WorkspaceId,
        principal: &PrincipalId,
        device_id: &DeviceId,
    ) -> io::Result<Option<WorkspaceCredential>>;

    fn put(&self, credential: WorkspaceCredential) -> io::Result<()>;

    fn remove(
        &self,
        workspace_id: &WorkspaceId,
        principal: &PrincipalId,
        device_id: &DeviceId,
    ) -> io::Result<()>;
    /// Revocation is checked separately from signature verification. A validly
    /// signed credential can still be revoked by newer workspace state.
    fn is_device_revoked(&self, device_id: &DeviceId, now_ms: u64) -> io::Result<bool>;
    fn is_credential_revoked(&self, credential_id: &CredentialId, now_ms: u64) -> io::Result<bool>;
}
