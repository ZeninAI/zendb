use std::net::SocketAddr;

use bincode::{Decode, Encode};

/// Reachability information for a relay-managed endpoint.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Encode, Decode)]
pub struct RelayAddress {
    pub relay_id: String,
    pub connection_hint: String,
}

/// A possible way to reach a peer. This is not yet a live session.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Encode, Decode)]
pub enum NetworkEndpoint {
    Lan(SocketAddr),
    Direct(SocketAddr),
    Relay(RelayAddress),
    Rendezvous(String),
    LocalPairing(String),
}
