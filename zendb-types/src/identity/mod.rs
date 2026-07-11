//! Identity primitives for ZeninDB.
//!
//! This module contains the foundational identity types that are part of the
//! shared truth and need to be replicated across devices:
//! - User, device, workspace, and invite identifiers
//! - Role definitions
//! - Membership records
//!
//! Higher-level identity concerns (OIDC integration, credential issuance,
//! bootstrap flows) remain in the `zendb-identity` crate.

#[macro_use]
pub mod _macros;

pub mod ids;
pub mod membership;
pub mod principal;
pub mod role;
pub mod trust;

pub use ids::{
    CapabilityId, CheckpointId, CredentialId, DeviceId, GrantId, GuestId, InviteId, JobId, KeyId,
    LeaseId, OperatorId, ReplicaId, ServiceId, UserId, WorkspaceId,
};
pub use membership::{DeviceMembership, WorkspaceMembership};
pub use principal::PrincipalId;
pub use role::Role;
pub use trust::DeviceTrust;
