//! Binary blob scalar type and the portable encoding boundary for opaque data.

use std::{io, marker::PhantomData, ops::Deref};

use bincode::{Decode, Encode};
use zendb_storage::utils::serdes::{deserialize_from, serialize_to_vec};

use crate::{
    CellCodecError, CellCodecKey, CrdtCodec, DefaultCrdtCodec, Hlc, PrimaryKey, Type, Value,
};

/// Opaque bytes stored as an LWW scalar.
///
/// `Blob` is deliberately a newtype instead of a `Vec<u8>` alias. Besides
/// preventing accidental confusion with protocol byte buffers, this gives all
/// crates one strict bincode configuration for catalog and system-table
/// payloads.
#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Encode, Decode)]
pub struct Blob(Vec<u8>);

impl Blob {
    pub fn new(bytes: impl Into<Vec<u8>>) -> Self {
        Self(bytes.into())
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }

    pub fn into_vec(self) -> Vec<u8> {
        self.0
    }

    /// Encode a portable value using ZenDB's canonical blob configuration.
    pub fn encode<T: Encode>(value: &T) -> Result<Self, BlobCodecError> {
        serialize_to_vec(value)
            .map(Self)
            .map_err(BlobCodecError::Encode)
    }

    /// Decode a portable value using ZenDB's canonical blob configuration.
    pub fn decode<T: Decode<()>>(&self) -> Result<T, BlobCodecError> {
        deserialize_from(self.as_slice()).map_err(BlobCodecError::Decode)
    }
}

impl From<Vec<u8>> for Blob {
    fn from(value: Vec<u8>) -> Self {
        Self(value)
    }
}

impl From<&[u8]> for Blob {
    fn from(value: &[u8]) -> Self {
        Self(value.to_vec())
    }
}

impl AsRef<[u8]> for Blob {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl Deref for Blob {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

pub struct BytesCodec;

impl CrdtCodec for BytesCodec {
    type Rust = Vec<u8>;

    fn encode(value: &Vec<u8>, _hlc: Hlc) -> Value {
        Value::Blob(value.clone().into())
    }

    fn decode(value: &Value) -> Result<Vec<u8>, CellCodecError> {
        match value {
            Value::Blob(value) => Ok(value.to_vec()),
            _ => Err(CellCodecError::expected("Blob")),
        }
    }
}

impl DefaultCrdtCodec for Vec<u8> {
    type Codec = BytesCodec;
}

impl CellCodecKey for Vec<u8> {
    fn to_primary_key(&self) -> PrimaryKey {
        PrimaryKey::Blob(self.clone().into())
    }

    fn from_primary_key(key: &PrimaryKey) -> Result<Self, CellCodecError> {
        match key {
            PrimaryKey::Blob(value) => Ok(value.to_vec()),
            _ => Err(CellCodecError::expected("Blob primary key")),
        }
    }
}

pub struct FixedBytesCodec<const N: usize>(PhantomData<[u8; N]>);

impl<const N: usize> CrdtCodec for FixedBytesCodec<N> {
    type Rust = [u8; N];

    fn encode(value: &[u8; N], _hlc: Hlc) -> Value {
        Value::Blob(value.to_vec().into())
    }

    fn decode(value: &Value) -> Result<[u8; N], CellCodecError> {
        let Value::Blob(value) = value else {
            return Err(CellCodecError::expected("Blob"));
        };
        value
            .as_slice()
            .try_into()
            .map_err(|_| CellCodecError::expected("fixed-size Blob"))
    }
}

impl<const N: usize> DefaultCrdtCodec for [u8; N] {
    type Codec = FixedBytesCodec<N>;
}

impl<const N: usize> CellCodecKey for [u8; N] {
    fn to_primary_key(&self) -> PrimaryKey {
        PrimaryKey::Blob(self.to_vec().into())
    }

    fn from_primary_key(key: &PrimaryKey) -> Result<Self, CellCodecError> {
        let PrimaryKey::Blob(value) = key else {
            return Err(CellCodecError::expected("Blob primary key"));
        };
        value
            .as_slice()
            .try_into()
            .map_err(|_| CellCodecError::expected("fixed-size Blob primary key"))
    }
}

#[derive(Debug)]
pub enum BlobCodecError {
    Encode(io::Error),
    Decode(io::Error),
}

impl std::fmt::Display for BlobCodecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Encode(error) => write!(f, "failed to encode blob: {error}"),
            Self::Decode(error) => write!(f, "failed to decode blob: {error}"),
        }
    }
}

impl std::error::Error for BlobCodecError {}

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub enum BlobOp {}

#[derive(Debug)]
pub enum BlobError {}

impl std::fmt::Display for BlobError {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {}
    }
}

impl std::error::Error for BlobError {}

impl Type for Blob {
    type Op = BlobOp;
    type Error = BlobError;

    fn apply(&mut self, op: &BlobOp, _op_hlc: Hlc) -> Result<bool, BlobError> {
        match *op {}
    }

    fn merge(&mut self, remote: &Blob, clocks: crate::MergeClocks) -> Result<bool, BlobError> {
        if clocks.remote.beats(clocks.local) {
            *self = remote.clone();
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bincode::{config, decode_from_slice, encode_to_vec};
    use zendb_storage::utils::serdes::serialize_to_vec;

    fn hlc(ms: u64, device: u8) -> Hlc {
        Hlc::with_device_id(ms, 0, crate::DeviceId::from_bytes([device; 16])).unwrap()
    }

    #[test]
    fn newer_remote_replaces_with_an_independent_clone() {
        let remote = Blob::from(vec![1, 2, 3]);
        let mut local = Blob::from(vec![9]);
        assert!(local
            .merge(&remote, crate::MergeClocks::new(hlc(100, 1), hlc(200, 1)),)
            .unwrap());
        assert_eq!(local, remote);
        assert_ne!(local.as_ptr(), remote.as_ptr());
    }

    #[test]
    fn stale_remote_blob_is_ignored() {
        let mut local = Blob::from(vec![1, 2, 3]);
        assert!(!local
            .merge(
                &Blob::from(vec![9]),
                crate::MergeClocks::new(hlc(200, 1), hlc(100, 2)),
            )
            .unwrap());
        assert_eq!(local.as_slice(), &[1, 2, 3]);
    }

    #[test]
    fn bincode_roundtrips_empty_and_all_byte_values() {
        for value in [
            Blob::default(),
            Blob::from((0u8..=u8::MAX).collect::<Vec<_>>()),
        ] {
            let encoded = encode_to_vec(&value, config::standard()).unwrap();
            let (decoded, consumed): (Blob, usize) =
                decode_from_slice(&encoded, config::standard()).unwrap();
            assert_eq!(consumed, encoded.len());
            assert_eq!(decoded, value);
        }
    }

    #[test]
    fn typed_helpers_use_the_storage_codec() {
        let encoded = Blob::encode(&(42u64, "catalog")).unwrap();
        assert_eq!(
            encoded.as_slice(),
            serialize_to_vec(&(42u64, "catalog")).unwrap()
        );
        let decoded: (u64, String) = encoded.decode().unwrap();
        assert_eq!(decoded, (42, "catalog".into()));
    }
}
