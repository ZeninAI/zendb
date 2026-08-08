//! Bincode-capable wrapper around libp2p public keys.

use bincode::{Decode, Encode, de::Decoder, enc::Encoder};
use libp2p_identity::PublicKey as Libp2pPublicKey;

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
