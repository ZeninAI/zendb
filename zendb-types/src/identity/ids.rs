//! Libp2p-backed domain identities and inert workspace role data.

use std::str::FromStr;

use bincode::{de::Decoder, enc::Encoder, Decode, Encode};
use libp2p_identity::{Keypair, PeerId as Libp2pPeerId};

pub type IdParseError = libp2p_identity::ParseError;

/// A network peer identity encoded as canonical libp2p PeerId bytes.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PeerId(Libp2pPeerId);

impl PeerId {
    pub fn generate() -> Self {
        let keypair = Keypair::generate_ed25519();
        Self(Libp2pPeerId::from_public_key(&keypair.public()))
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, IdParseError> {
        Libp2pPeerId::from_bytes(bytes).map(Self)
    }

    pub fn to_bytes(self) -> Vec<u8> {
        self.0.to_bytes()
    }

    pub const fn as_libp2p(&self) -> &Libp2pPeerId {
        &self.0
    }
}

impl Default for PeerId {
    fn default() -> Self {
        // A valid identity-multihash with a zero digest is the CRDT sentinel.
        Self::from_bytes(&[
            0, 32, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0,
        ])
        .expect("the zero PeerId sentinel is a valid identity multihash")
    }
}

impl std::fmt::Display for PeerId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}

impl std::fmt::Debug for PeerId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_tuple("PeerId").field(&self.0).finish()
    }
}

impl FromStr for PeerId {
    type Err = IdParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        value.parse().map(Self)
    }
}

impl From<Libp2pPeerId> for PeerId {
    fn from(value: Libp2pPeerId) -> Self {
        Self(value)
    }
}

impl From<PeerId> for Libp2pPeerId {
    fn from(value: PeerId) -> Self {
        value.0
    }
}

impl Encode for PeerId {
    fn encode<E: Encoder>(&self, encoder: &mut E) -> Result<(), bincode::error::EncodeError> {
        self.0.to_bytes().encode(encoder)
    }
}

impl<Context> Decode<Context> for PeerId {
    fn decode<D: Decoder<Context = Context>>(
        decoder: &mut D,
    ) -> Result<Self, bincode::error::DecodeError> {
        let bytes = Vec::<u8>::decode(decoder)?;
        Self::from_bytes(&bytes).map_err(|error| {
            bincode::error::DecodeError::OtherString(format!("invalid PeerId: {error}"))
        })
    }
}

bincode::impl_borrow_decode!(PeerId);

/// A workspace identity using the same primitive and encoding as PeerId.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WorkspaceId(Libp2pPeerId);

impl WorkspaceId {
    pub fn generate() -> Self {
        let keypair = Keypair::generate_ed25519();
        Self(Libp2pPeerId::from_public_key(&keypair.public()))
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, IdParseError> {
        Libp2pPeerId::from_bytes(bytes).map(Self)
    }

    pub fn to_bytes(self) -> Vec<u8> {
        self.0.to_bytes()
    }

    pub const fn as_libp2p(&self) -> &Libp2pPeerId {
        &self.0
    }
}

impl Default for WorkspaceId {
    fn default() -> Self {
        Self(PeerId::default().0)
    }
}

impl std::fmt::Display for WorkspaceId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}

impl std::fmt::Debug for WorkspaceId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_tuple("WorkspaceId").field(&self.0).finish()
    }
}

impl FromStr for WorkspaceId {
    type Err = IdParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        value.parse().map(Self)
    }
}

impl Encode for WorkspaceId {
    fn encode<E: Encoder>(&self, encoder: &mut E) -> Result<(), bincode::error::EncodeError> {
        self.0.to_bytes().encode(encoder)
    }
}

impl<Context> Decode<Context> for WorkspaceId {
    fn decode<D: Decoder<Context = Context>>(
        decoder: &mut D,
    ) -> Result<Self, bincode::error::DecodeError> {
        let bytes = Vec::<u8>::decode(decoder)?;
        Self::from_bytes(&bytes).map_err(|error| {
            bincode::error::DecodeError::OtherString(format!("invalid WorkspaceId: {error}"))
        })
    }
}

bincode::impl_borrow_decode!(WorkspaceId);

/// Persisted workspace capabilities. Enforcement belongs to zendb-workspace.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Encode, Decode)]
pub enum Roles {
    Contributor,
    Operator,
    Dispatcher,
}
