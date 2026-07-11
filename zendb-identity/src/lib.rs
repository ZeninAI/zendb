//! # zendb-identity
//!
//! Identity and workspace-trust primitives for ZeninDB.
//!
//! This crate builds on top of the identity types in `zendb-types` and provides:
//! - OIDC integration and authentication flows
//! - Credential issuance and verification
//! - Bootstrap bundle formats
//! - Peer claims and signing
//! - Workspace invites
//!
//! **Core identity types** (UserId, DeviceId, WorkspaceId, Role, memberships)
//! now live in `zendb-types::identity` as they are part of shared truth.

pub mod auth;
pub mod bootstrap;
pub mod claims;
pub mod credential;
pub mod interfaces;
pub mod invite;

// Re-export identity types from zendb-types for convenience
pub use zendb_types::{DeviceId, InviteId, KeyId, PrincipalId, Role, UserId, WorkspaceId};
pub use zendb_types::{DeviceMembership, WorkspaceMembership};

// Export types defined in this crate
pub use auth::{OidcClaims, OidcProvider, OidcProviderConfig, OidcSession, WorkspaceClaims};
pub use bootstrap::{BootstrapBundle, BootstrapEnvelope, SnapshotAnchor};
pub use claims::{PeerClaims, PeerIdentity, PublicKeyBytes, SignatureBytes, TrustTier};
pub use credential::{
    CredentialRequest, PeerAuthContext, VerifiedWorkspaceCredential, WorkspaceCredential,
    WorkspaceCredentialVerifier,
};
pub use interfaces::{AdmissionProof, DeviceSigner, WorkspaceTrustStore};
pub use invite::WorkspaceInvite;
