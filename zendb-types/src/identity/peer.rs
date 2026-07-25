//! Abstract peer identity trait and a default in-memory implementation.

use std::fmt;

use bincode::{de::Decoder, enc::Encoder, Decode, Encode};
use libp2p_identity::{Keypair, SigningError as Libp2pSigningError};

use crate::PeerId;

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

/// A default in-memory `PeerIdentity` backed by an ed25519 `Keypair`.
///
/// Suitable for tests, examples, and local-only deployments. The keypair
/// lives in process memory and is lost when the process exits; production
/// deployments should supply an impl backed by a persistent key store.
pub struct LocalPeerIdentity {
    keypair: Keypair,
    peer_id: PeerId,
}

impl LocalPeerIdentity {
    /// Generate a fresh ed25519 identity.
    pub fn generate() -> Self {
        let keypair = Keypair::generate_ed25519();
        let peer_id = PeerId::from(libp2p_identity::PeerId::from_public_key(&keypair.public()));
        Self { keypair, peer_id }
    }
}

impl fmt::Debug for LocalPeerIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LocalPeerIdentity")
            .field("peer_id", &self.peer_id)
            .finish_non_exhaustive()
    }
}

impl PeerIdentity for LocalPeerIdentity {
    fn peer_id(&self) -> PeerId {
        self.peer_id
    }

    fn sign(&self, message: &[u8]) -> Result<Signature, SigningError> {
        self.keypair
            .sign(message)
            .map(Signature)
            .map_err(Into::into)
    }
}
