//! # zendb-transport
//!
//! Concrete client-side transport mechanics for ZeninDB.
//!
//! This crate intentionally owns:
//! - concrete local discovery sockets and carrier adapters
//! - a durable device signing profile
//! - mutually authenticated encrypted sessions over framed carriers
//! - enrollment proof helpers and local presence tracking
//!
//! It does not own:
//! - replication semantics
//! - CRDT merge
//! - operator placement
//! - product-domain data models

pub mod bootstrap;
pub mod endpoint;
pub mod enrollment;
pub mod identity;
pub mod lan;
pub mod link;
pub mod presence;
pub mod secure;
pub mod tcp;

pub use bootstrap::{BootstrapApproval, BootstrapRequest, BootstrapTicketProof};
pub use endpoint::ConnectionHint;
pub use enrollment::{
    build_direct_request, evidence_from_request, ticket_admission_signing_bytes,
    verify_candidate_request, EnrollmentPresentation,
};
pub use identity::DeviceProfile;
pub use lan::LanDiscoverySocket;
pub use link::FramedLink;
pub use presence::{PresenceStatus, PresenceTracker};
pub use secure::{
    HandshakePeer, SecureSession, SessionPurpose, DEFAULT_MAX_SECURE_FRAME_BYTES,
    SECURE_SESSION_VERSION,
};
pub use tcp::{TcpLink, TcpLinkListener};

pub type TcpSecureSession = SecureSession<TcpLink>;
