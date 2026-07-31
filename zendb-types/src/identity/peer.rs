//! Persisted device keys, progressive roles, and application identity input.

use bincode::{Decode, Encode, de::Decoder, enc::Encoder};
use libp2p_identity::{Keypair, PublicKey as Libp2pPublicKey};

/// A bincode-capable wrapper around libp2p's generic public key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PublicKey(Libp2pPublicKey);

impl PublicKey {
    pub fn from_libp2p(key: Libp2pPublicKey) -> Self {
        Self(key)
    }

    pub const fn as_libp2p(&self) -> &Libp2pPublicKey {
        &self.0
    }
}

impl Encode for PublicKey {
    fn encode<E: Encoder>(&self, encoder: &mut E) -> Result<(), bincode::error::EncodeError> {
        self.0.encode_protobuf().encode(encoder)
    }
}

impl<Context> Decode<Context> for PublicKey {
    fn decode<D: Decoder<Context = Context>>(
        decoder: &mut D,
    ) -> Result<Self, bincode::error::DecodeError> {
        let bytes = Vec::<u8>::decode(decoder)?;
        Libp2pPublicKey::try_decode_protobuf(&bytes)
            .map(Self)
            .map_err(|error| bincode::error::DecodeError::OtherString(error.to_string()))
    }
}

bincode::impl_borrow_decode!(PublicKey);

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

/// Application-owned account identity used to derive workspace-scoped keys.
pub trait PeerIdentity: Send + Sync {
    fn keypair(&self) -> &Keypair;

    fn display_name(&self) -> &str;
}
