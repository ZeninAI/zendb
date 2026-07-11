use std::collections::BTreeMap;
use std::io;

use bincode::{Decode, Encode};

use zendb_types::{Role, UserId, WorkspaceId};

/// Minimal OIDC provider configuration required to validate and consume tokens.
///
/// This is deliberately service-facing configuration. It belongs to the
/// "online account / central API" side of the system, not to the durable
/// workspace-membership side.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct OidcProviderConfig {
    pub issuer: String,
    pub client_id: String,
    pub audience: Option<String>,
    pub jwks_uri: Option<String>,
}

/// Claims extracted from a validated OIDC/OAuth token.
///
/// These claims answer:
///
/// - who authenticated with the online provider
/// - what online scopes were granted
/// - which workspace roles the central service believes the user has
///
/// These claims are useful for:
///
/// - talking to a registry / relay / REST API
/// - accepting invites online
/// - minting a durable [`WorkspaceCredential`](crate::WorkspaceCredential)
///
/// They are intentionally *not* the final offline peer-to-peer credential.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct OidcClaims {
    pub issuer: String,
    pub subject: String,
    pub user_id: Option<UserId>,
    pub audience: Vec<String>,
    pub scopes: Vec<String>,
    pub workspace_roles: BTreeMap<WorkspaceId, Role>,
    pub issued_at_ms: u64,
    pub expires_at_ms: u64,
}

impl OidcClaims {
    /// Returns whether the validated online session contained a specific scope.
    pub fn has_scope(&self, scope: &str) -> bool {
        self.scopes.iter().any(|entry| entry == scope)
    }

    /// Returns the role claim for a workspace, if the central auth/service
    /// layer attached one to the token.
    pub fn role_for(&self, workspace_id: &WorkspaceId) -> Option<Role> {
        self.workspace_roles.get(workspace_id).copied()
    }

    /// Returns whether the token should be considered expired at `now_ms`.
    pub fn is_expired(&self, now_ms: u64) -> bool {
        now_ms >= self.expires_at_ms
    }
}

/// Active online authentication session backed by an OIDC provider.
///
/// This is the "user logged into the central service" view of authentication.
/// It is expected to be short- or medium-lived and refreshable.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct OidcSession {
    pub provider: OidcProviderConfig,
    pub access_token: String,
    pub refresh_token: Option<String>,
    pub id_token: Option<String>,
    pub claims: OidcClaims,
}

impl OidcSession {
    /// Returns whether the online session must be refreshed before further
    /// central-service calls should be attempted.
    pub fn is_expired(&self, now_ms: u64) -> bool {
        self.claims.is_expired(now_ms)
    }
}

/// Durable workspace-relevant claims extracted from an online OIDC session.
///
/// This is the bridge between:
///
/// - online service authorization (`OidcClaims`)
/// - durable workspace/device authorization (`WorkspaceCredential`)
///
/// The idea is:
///
/// 1. validate an OIDC token
/// 2. extract only the workspace-relevant parts
/// 3. use those claims to mint a workspace-scoped credential
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct WorkspaceClaims {
    pub user_id: UserId,
    pub scopes: Vec<String>,
    pub workspace_roles: BTreeMap<WorkspaceId, Role>,
}

impl WorkspaceClaims {
    /// Returns whether the online claims support a specific requested scope.
    pub fn has_scope(&self, scope: &str) -> bool {
        self.scopes.iter().any(|entry| entry == scope)
    }

    /// Returns the workspace role conveyed by the online claims.
    pub fn role_for(&self, workspace_id: &WorkspaceId) -> Option<Role> {
        self.workspace_roles.get(workspace_id).copied()
    }
}

impl TryFrom<&OidcClaims> for WorkspaceClaims {
    type Error = io::Error;

    fn try_from(value: &OidcClaims) -> Result<Self, Self::Error> {
        let user_id = value.user_id.clone().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "OIDC claims did not contain a mapped workspace user id",
            )
        })?;

        Ok(Self {
            user_id,
            scopes: value.scopes.clone(),
            workspace_roles: value.workspace_roles.clone(),
        })
    }
}

/// Provider abstraction for validating tokens and establishing online sessions.
///
/// A concrete implementation would usually:
///
/// - verify JWT signatures against JWKS
/// - validate issuer / audience / expiry
/// - map the subject to a Zenin user id if necessary
/// - extract workspace roles/scopes from standard or custom claims
pub trait OidcProvider: Send + Sync + 'static {
    fn config(&self) -> &OidcProviderConfig;
    fn validate_access_token(&self, access_token: &str) -> io::Result<OidcClaims>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workspace_claims_require_mapped_user_id() {
        let claims = OidcClaims {
            issuer: "https://issuer.example".into(),
            subject: "sub-1".into(),
            user_id: None,
            audience: vec!["zenin-api".into()],
            scopes: vec!["workspace:read".into()],
            workspace_roles: BTreeMap::new(),
            issued_at_ms: 1,
            expires_at_ms: 10,
        };

        assert!(WorkspaceClaims::try_from(&claims).is_err());
    }

    #[test]
    fn oidc_claim_scope_and_expiry_helpers_work() {
        let claims = OidcClaims {
            issuer: "https://issuer.example".into(),
            subject: "sub-1".into(),
            user_id: Some(UserId::from("user-1")),
            audience: vec!["zenin-api".into()],
            scopes: vec!["workspace:read".into(), "relay:connect".into()],
            workspace_roles: BTreeMap::from([(WorkspaceId::from("ws-1"), Role::Editor)]),
            issued_at_ms: 1,
            expires_at_ms: 10,
        };

        assert!(claims.has_scope("relay:connect"));
        assert_eq!(
            claims.role_for(&WorkspaceId::from("ws-1")),
            Some(Role::Editor)
        );
        assert!(claims.is_expired(10));
    }
}
