//! Persisted installation metadata shared by storage and replication consumers.

use bincode::{Decode, Encode};

use super::{Multiaddr, PublicKey, Role};

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct Installation {
    pub display_name: String,
    pub role: Option<Role>,
    pub public_key: PublicKey,
    pub addresses: Vec<Multiaddr>,
}
