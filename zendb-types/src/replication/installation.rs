//! Persisted installation metadata shared by storage and replication consumers.

use bincode::{Decode, Encode};

use super::{Multiaddr, Permissions, PublicKey};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
pub enum InstallationState {
    Pending,
    Active(Permissions),
}

impl InstallationState {
    pub const fn permissions(self) -> Option<Permissions> {
        match self {
            Self::Pending => None,
            Self::Active(permissions) => Some(permissions),
        }
    }

    pub const fn is_active(self) -> bool {
        matches!(self, Self::Active(_))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct Installation {
    pub display_name: String,
    pub public_key: PublicKey,
    pub addresses: Vec<Multiaddr>,
    pub state: InstallationState,
}
