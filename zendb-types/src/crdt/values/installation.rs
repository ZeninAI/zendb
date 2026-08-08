//! Installation registry value with admission-aware merge precedence.

use bincode::{Decode, Encode};

use crate::{MergeStamps, Multiaddr, Permissions, PublicKey, Type};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
pub enum InstallationState {
    Pending,
    Rejected,
    Active(Permissions),
}

impl InstallationState {
    pub const fn permissions(self) -> Option<Permissions> {
        match self {
            Self::Pending | Self::Rejected => None,
            Self::Active(permissions) => Some(permissions),
        }
    }

    pub const fn is_active(self) -> bool {
        matches!(self, Self::Active(_))
    }

    const fn precedence(self) -> u8 {
        match self {
            Self::Active(_) => 1,
            Self::Pending | Self::Rejected => 0,
        }
    }
}

impl Default for InstallationState {
    fn default() -> Self {
        Self::Pending
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct Installation {
    pub display_name: String,
    pub public_key: PublicKey,
    /// Public route hints advertised to peers for outbound dialing.
    pub addresses: Vec<Multiaddr>,
    pub state: InstallationState,
}

impl Default for Installation {
    fn default() -> Self {
        Self {
            display_name: String::new(),
            public_key: PublicKey::from_libp2p(
                libp2p_identity::Keypair::ed25519_from_bytes([0; 32])
                    .expect("the fixed Ed25519 seed is valid")
                    .public(),
            ),
            addresses: Vec::new(),
            state: InstallationState::Pending,
        }
    }
}

impl Installation {
    pub fn set(&self) -> InstallationOp {
        InstallationOp::Set(self.clone())
    }
}

#[derive(Debug, Clone, Encode, Decode)]
pub enum InstallationOp {
    Set(Installation),
}

#[derive(Debug)]
pub enum InstallationError {}

impl std::fmt::Display for InstallationError {
    fn fmt(&self, _formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {}
    }
}

impl std::error::Error for InstallationError {}

impl Type for Installation {
    type Op = InstallationOp;
    type Error = InstallationError;

    fn apply(
        &mut self,
        op: &InstallationOp,
        stamps: MergeStamps,
    ) -> Result<bool, InstallationError> {
        let InstallationOp::Set(incoming) = op;
        self.merge(incoming, stamps)
    }

    fn merge(
        &mut self,
        incoming: &Installation,
        stamps: MergeStamps,
    ) -> Result<bool, InstallationError> {
        let current_precedence = self.state.precedence();
        let incoming_precedence = incoming.state.precedence();
        if incoming_precedence > current_precedence
            || (incoming_precedence == current_precedence && stamps.incoming > stamps.current)
        {
            *self = incoming.clone();
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn apply_stamp(&self, stamps: MergeStamps, changed: bool) -> Option<crate::EventStamp> {
        changed.then_some(stamps.incoming)
    }

    fn merge_stamp(&self, stamps: MergeStamps, changed: bool) -> Option<crate::EventStamp> {
        changed.then_some(stamps.incoming)
    }
}
