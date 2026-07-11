//! # zendb-transport
//!
//! Transport and discovery abstractions for ZeninDB.
//!
//! This crate intentionally owns:
//! - peer discovery
//! - rendezvous and pairing entrypoints
//! - authenticated transport sessions
//! - framed protocol channels
//!
//! It does not own:
//! - replication semantics
//! - CRDT merge
//! - operator placement
//! - product-domain data models

pub mod bearer;
pub mod bootstrap;
pub mod discovery;
pub mod endpoint;
pub mod frame;
pub mod handshake;
pub mod path;
pub mod protocol;
pub mod rendezvous;
pub mod session;

pub use bearer::{BearerAdapter, BearerCapabilities, BearerKind};
pub use bootstrap::{BootstrapApproval, BootstrapRequest};
pub use discovery::{DiscoveredPeer, DiscoveryProvider};
pub use endpoint::{NetworkEndpoint, RelayAddress};
pub use frame::{StreamId, TransportFrame};
pub use handshake::{
    HandshakeAuthenticate, HandshakeChallenge, HandshakeHello, SessionEstablished,
};
pub use path::PathSelector;
pub use protocol::{ProtocolChannel, RequestId, SessionHealth, SessionId};
pub use rendezvous::{RendezvousProvider, RendezvousPurpose, RendezvousRequest, RendezvousTicket};
pub use session::{RawTransport, SessionMetadata, TransportSession};
