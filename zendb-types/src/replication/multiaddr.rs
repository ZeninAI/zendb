//! Persistable libp2p routes advertised for enrolled devices.

use std::{fmt, str::FromStr};

use ::multiaddr::{Multiaddr as Libp2pMultiaddr, Protocol};
use bincode::{Decode, Encode, de::Decoder, enc::Encoder};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MultiaddrError(String);

impl fmt::Display for MultiaddrError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl std::error::Error for MultiaddrError {}

/// A durable route to a device whose identity is stored separately.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Multiaddr(Libp2pMultiaddr);

impl Multiaddr {
    pub fn from_libp2p(address: Libp2pMultiaddr) -> Result<Self, MultiaddrError> {
        if matches!(address.iter().last(), Some(Protocol::P2p(_))) {
            return Err(MultiaddrError(
                "device addresses must not end with a destination PeerId".to_owned(),
            ));
        }
        Ok(Self(address))
    }

    pub const fn as_libp2p(&self) -> &Libp2pMultiaddr {
        &self.0
    }

    pub fn into_libp2p(self) -> Libp2pMultiaddr {
        self.0
    }
}

impl FromStr for Multiaddr {
    type Err = MultiaddrError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let address = value
            .parse::<Libp2pMultiaddr>()
            .map_err(|error| MultiaddrError(error.to_string()))?;
        Self::from_libp2p(address)
    }
}

impl fmt::Display for Multiaddr {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl Encode for Multiaddr {
    fn encode<E: Encoder>(&self, encoder: &mut E) -> Result<(), bincode::error::EncodeError> {
        self.0.to_vec().encode(encoder)
    }
}

impl<Context> Decode<Context> for Multiaddr {
    fn decode<D: Decoder<Context = Context>>(
        decoder: &mut D,
    ) -> Result<Self, bincode::error::DecodeError> {
        let bytes = Vec::<u8>::decode(decoder)?;
        let address = Libp2pMultiaddr::try_from(bytes)
            .map_err(|error| bincode::error::DecodeError::OtherString(error.to_string()))?;
        Self::from_libp2p(address)
            .map_err(|error| bincode::error::DecodeError::OtherString(error.to_string()))
    }
}

bincode::impl_borrow_decode!(Multiaddr);
