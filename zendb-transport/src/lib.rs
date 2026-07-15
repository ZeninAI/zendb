//! # zendb-transport
//!
//! Concrete client-side transport mechanics for ZeninDB.
//!
//! This crate intentionally owns:
//! - peer discovery
//! - rendezvous and pairing entrypoints
//! - a durable device signing profile
//! - mutually authenticated encrypted TCP sessions
//! - enrollment proof helpers and local presence tracking
//!
//! It does not own:
//! - replication semantics
//! - CRDT merge
//! - operator placement
//! - product-domain data models

pub mod bootstrap;
pub mod discovery;
pub mod endpoint;
pub mod enrollment;
pub mod identity;
pub mod presence;
pub mod rendezvous;
pub mod secure_tcp;

pub use bootstrap::{BootstrapApproval, BootstrapRequest, BootstrapTicketProof};
pub use discovery::DiscoveredPeer;
pub use endpoint::{NetworkEndpoint, RelayAddress};
pub use enrollment::{
    build_direct_request, evidence_from_request, ticket_admission_signing_bytes,
    verify_candidate_request, EnrollmentPresentation,
};
pub use identity::DeviceProfile;
pub use presence::{PresenceStatus, PresenceTracker};
pub use rendezvous::{RendezvousPurpose, RendezvousRequest, RendezvousTicket};
pub use secure_tcp::{
    HandshakePeer, SecureTcpSession, SessionPurpose, DEFAULT_MAX_SECURE_FRAME_BYTES,
    SECURE_SESSION_VERSION,
};
