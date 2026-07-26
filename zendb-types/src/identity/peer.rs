//! Network peer identifiers, progressive roles, and signing abstractions.

use std::{fmt, str::FromStr};

use bincode::{de::Decoder, enc::Encoder, Decode, Encode};
use libp2p_identity::{PeerId as Libp2pPeerId, PublicKey, SigningError as Libp2pSigningError};

pub type IdParseError = libp2p_identity::ParseError;

/// A network peer identity encoded as canonical libp2p PeerId bytes.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PeerId(Libp2pPeerId);

impl PeerId {
    pub fn from_public_key(key: &PublicKey) -> Self {
        Self(Libp2pPeerId::from_public_key(key))
    }

    pub fn from_bytes(data: &[u8]) -> Result<Self, IdParseError> {
        Libp2pPeerId::from_bytes(data).map(Self)
    }

    pub fn random() -> Self {
        Self(Libp2pPeerId::random())
    }

    pub fn to_bytes(self) -> Vec<u8> {
        self.0.to_bytes()
    }

    pub fn to_base58(self) -> String {
        self.0.to_base58()
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

impl fmt::Display for PeerId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl fmt::Debug for PeerId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
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

/// Persisted workspace capabilities. Enforcement belongs to zendb-workspace.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Encode, Decode)]
pub enum Role {
    Contributor,
    Operator,
    Admin,
}

impl Role {
    pub const fn has_at_least(self, required: Self) -> bool {
        match required {
            Self::Contributor => true,
            Self::Operator => matches!(self, Self::Operator | Self::Admin),
            Self::Admin => matches!(self, Self::Admin),
        }
    }
}

/// A device's cryptographic identity.
///
/// The workspace uses this trait to obtain the local [`PeerId`] for minting
/// `EventId`s and to sign messages for future replication. It never sees the
/// private key material directly; the implementation decides where the key
/// lives (in-memory, OS keychain, HSM, KMS).
pub trait PeerIdentity: Send + Sync {
    /// The public peer identity of this device.
    fn peer_id(&self) -> PeerId;

    /// Cryptographically sign `message` with this device's private key.
    fn sign(&self, message: &[u8]) -> Result<Signature, SigningError>;
}

/// An opaque owned signature produced by a [`PeerIdentity`].
///
/// The bytes are backend-specific (ed25519, secp256k1, etc.). The type is
/// `Encode`/`Decode` so future event envelopes can carry signatures, but
/// iteration 0003 does not yet attach signatures to events.
#[derive(Clone, PartialEq, Eq)]
pub struct Signature(pub Vec<u8>);

impl Signature {
    /// The raw signature bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Debug for Signature {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("Signature")
            .field(&self.0.len())
            .finish()
    }
}

impl Encode for Signature {
    fn encode<E: Encoder>(&self, encoder: &mut E) -> Result<(), bincode::error::EncodeError> {
        self.0.encode(encoder)
    }
}

impl<Context> Decode<Context> for Signature {
    fn decode<D: Decoder<Context = Context>>(
        decoder: &mut D,
    ) -> Result<Self, bincode::error::DecodeError> {
        Vec::<u8>::decode(decoder).map(Self)
    }
}

bincode::impl_borrow_decode!(Signature);

/// Failures produced by [`PeerIdentity::sign`].
///
/// This is a distinct error type from `zendb_workspace::Error` because
/// signing can occur outside the workspace (e.g. an account layer signing a
/// join request).
#[derive(Debug)]
pub enum SigningError {
    /// The backing key is not available (e.g. keychain locked, HSM offline).
    KeyUnavailable,
    /// The signing backend reported a failure.
    Backend(String),
}

impl fmt::Display for SigningError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::KeyUnavailable => formatter.write_str("signing key is unavailable"),
            Self::Backend(message) => write!(formatter, "signing backend failure: {message}"),
        }
    }
}

impl std::error::Error for SigningError {}

impl From<Libp2pSigningError> for SigningError {
    fn from(value: Libp2pSigningError) -> Self {
        Self::Backend(value.to_string())
    }
}
