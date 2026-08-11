//! Installation registry value.

use crate::{Multiaddr, Permissions, PublicKey, zendb_type};
use bincode::{Decode, Encode};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
pub enum InstallationState {
    Pending,
    Rejected,
    Active(Permissions),
}
impl Default for InstallationState {
    fn default() -> Self {
        Self::Pending
    }
}
impl InstallationState {
    pub const fn permissions(self) -> Option<Permissions> {
        match self {
            Self::Active(value) => Some(value),
            _ => None,
        }
    }
    pub const fn is_active(self) -> bool {
        matches!(self, Self::Active(_))
    }
    const fn precedence(self) -> u8 {
        matches!(self, Self::Active(_)) as u8
    }
}

zendb_type! {
    #[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
    pub struct Installation {
        pub display_name: std::string::String,
        pub public_key: PublicKey,
        pub addresses: Vec<Multiaddr>,
        pub state: InstallationState,
    }

    impl Installation {
        pub fn op_set(&mut self, remote: crate::EventTime, incoming: Installation) -> bool {
            let replace = incoming.state.precedence() > self.state.precedence()
                || (incoming.state.precedence() == self.state.precedence() && remote > self.__event_time);
            if replace {
                *self = Self { __event_time: remote, __is_tombstone: false, ..incoming };
            }
            replace
        }
    }
}

impl Default for Installation {
    fn default() -> Self {
        Self {
            display_name: std::string::String::new(),
            public_key: PublicKey::from_libp2p(
                libp2p_identity::Keypair::ed25519_from_bytes([0; 32])
                    .expect("fixed Ed25519 seed is valid")
                    .public(),
            ),
            addresses: Vec::new(),
            state: InstallationState::Pending,
            __event_time: crate::EventTime::default(),
            __is_tombstone: false,
        }
    }
}
