use std::io;

use bincode::{Decode, Encode};

use zendb_types::{
    CredentialId, DeviceId, GrantId, KeyId, PrincipalId, ResourceSelector, Role, UserId,
    WorkspaceId,
};

use crate::{auth::WorkspaceClaims, claims::SignatureBytes, PeerIdentity};

/// Request to mint a durable workspace credential from online claims.
///
/// This is the point where an online authenticated session asks for a durable
/// workspace-scoped credential that may later be used:
///
/// - during peer-to-peer handshake
/// - during local/offline workspace access decisions
/// - during device bootstrap
///
/// It intentionally narrows the request to:
///
/// - one workspace
/// - one device
/// - one requested scope set
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct CredentialRequest {
    pub workspace_id: WorkspaceId,
    pub device_id: DeviceId,
    pub principal: PrincipalId,
    pub requested_scopes: Vec<String>,
    pub requested_resources: Vec<ResourceSelector>,
    pub requested_grant_id: Option<GrantId>,
    pub expires_at_ms: Option<u64>,
}

/// Durable workspace authorization artifact used for peer-to-peer and offline flows.
///
/// This is distinct from an OIDC access token.
///
/// OIDC token:
/// - user/service-facing
/// - usually short-lived
/// - meant primarily for central APIs
///
/// Workspace credential:
/// - workspace/device-facing
/// - can be validated locally
/// - carried during peer handshake
/// - used as the durable authorization artifact for a device in one workspace
///
/// Role and scope fields are cached claims, not final authority. The receiving
/// replica must still evaluate current membership, policy epoch, resource
/// sensitivity, and revocation state.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct WorkspaceCredential {
    pub credential_id: CredentialId,
    pub workspace_id: WorkspaceId,
    pub principal: PrincipalId,
    pub user_id: Option<UserId>,
    pub device_id: DeviceId,
    /// Coarse default only. The policy evaluator remains authoritative.
    pub role: Option<Role>,
    pub scopes: Vec<String>,
    pub resources: Vec<ResourceSelector>,
    pub grant_id: Option<GrantId>,
    pub policy_epoch: u64,
    pub issued_at_ms: u64,
    pub expires_at_ms: Option<u64>,
    pub issuer: String,
    pub issuer_key_id: KeyId,
    pub signature: SignatureBytes,
}

impl WorkspaceCredential {
    /// Returns whether the durable credential contains a specific granted scope.
    pub fn has_scope(&self, scope: &str) -> bool {
        self.scopes.iter().any(|entry| entry == scope)
    }

    /// Returns whether the credential should be considered expired.
    pub fn is_expired(&self, now_ms: u64) -> bool {
        self.expires_at_ms
            .is_some_and(|expires_at_ms| now_ms >= expires_at_ms)
    }

    /// Returns whether the credential is bound to the expected workspace and
    /// device. This is a useful first-pass check before signature validation.
    pub fn applies_to(&self, workspace_id: &WorkspaceId, device_id: &DeviceId) -> bool {
        &self.workspace_id == workspace_id && &self.device_id == device_id
    }

    /// A credential may describe a user or a guest, but it must always bind to
    /// the device key proved during the transport handshake.
    pub fn applies_to_principal(&self, principal: &PrincipalId) -> bool {
        &self.principal == principal
    }

    /// The optional user field is an audit convenience and must agree with a
    /// user principal. Guest credentials must leave it empty.
    pub fn principal_matches_user(&self) -> bool {
        match (&self.principal, &self.user_id) {
            (PrincipalId::User(principal_user), Some(user_id)) => principal_user == user_id,
            (PrincipalId::User(_), None) => false,
            (_, None) => true,
            (_, Some(_)) => false,
        }
    }
}

/// Credential that has already passed issuer-specific validation.
///
/// The wrapper exists so later layers can distinguish:
///
/// - untrusted serialized credential blobs
/// - credentials that have already passed issuer- and expiry-level checks
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct VerifiedWorkspaceCredential {
    pub credential: WorkspaceCredential,
    pub verified_at_ms: u64,
}

/// Peer authentication context presented during session establishment.
///
/// This binds together:
///
/// - the remote peer's claimed device/user identity
/// - the durable workspace credential authorizing that device for this workspace
///
/// In other words:
///
/// - `PeerIdentity` says who the peer claims to be
/// - `WorkspaceCredential` says whether that identity is allowed for this workspace
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct PeerAuthContext {
    pub peer_identity: PeerIdentity,
    pub workspace_credential: WorkspaceCredential,
}

/// Validates workspace credentials before they are accepted for peer auth.
///
/// A concrete verifier would normally:
///
/// - verify signature
/// - verify issuer trust
/// - verify expiry
/// - verify workspace/device binding
pub trait WorkspaceCredentialVerifier: Send + Sync + 'static {
    fn verify_workspace_credential(
        &self,
        credential: &WorkspaceCredential,
        now_ms: u64,
    ) -> io::Result<VerifiedWorkspaceCredential>;
}

impl CredentialRequest {
    /// Validates the request against trusted workspace claims and returns the
    /// workspace role that should be encoded into the credential.
    ///
    /// This is intentionally a narrow check:
    ///
    /// - does the user have a role in this workspace?
    /// - are the requested scopes permitted by the online claims?
    ///
    /// Resource selectors and guest grants require a workspace admission
    /// authority; they are deliberately not inferred from OIDC scopes here.
    pub fn validate_against(&self, claims: &WorkspaceClaims) -> io::Result<Role> {
        let requested_user = match &self.principal {
            PrincipalId::User(user_id) => user_id,
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "online OIDC claims can only issue user credentials",
                ))
            }
        };
        if requested_user != &claims.user_id {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "credential principal does not match authenticated user",
            ));
        }
        let role = claims.role_for(&self.workspace_id).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::PermissionDenied,
                format!("no workspace role found for {}", self.workspace_id),
            )
        })?;

        for scope in &self.requested_scopes {
            if !claims.has_scope(scope) {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    format!("missing required scope {scope:?}"),
                ));
            }
        }

        Ok(role)
    }
}
