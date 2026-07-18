//! String scalar type.

use bincode::{Decode, Encode};

use crate::{
    CellCodecError, CellCodecKey, CrdtCodec, DefaultCrdtCodec, Hlc, PrimaryKey, Type, Value,
};

pub type String = std::string::String;

pub struct StringCodec;

impl CrdtCodec for StringCodec {
    type Rust = String;

    fn encode(value: &String, _hlc: Hlc) -> Value {
        Value::String(value.clone())
    }

    fn decode(value: &Value) -> Result<String, CellCodecError> {
        match value {
            Value::String(value) => Ok(value.clone()),
            _ => Err(CellCodecError::expected("String")),
        }
    }
}

impl DefaultCrdtCodec for String {
    type Codec = StringCodec;
}

impl CellCodecKey for String {
    fn to_primary_key(&self) -> PrimaryKey {
        PrimaryKey::String(self.clone())
    }

    fn from_primary_key(key: &PrimaryKey) -> Result<Self, CellCodecError> {
        match key {
            PrimaryKey::String(value) => Ok(value.clone()),
            _ => Err(CellCodecError::expected("String primary key")),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub enum StringOp {}

#[derive(Debug)]
pub enum StringError {}

impl std::fmt::Display for StringError {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {}
    }
}

impl std::error::Error for StringError {}

impl Type for String {
    type Op = StringOp;
    type Error = StringError;

    fn apply(&mut self, op: &StringOp, _op_hlc: Hlc) -> Result<bool, StringError> {
        match *op {}
    }

    fn merge(&mut self, remote: &String, clocks: crate::MergeClocks) -> Result<bool, StringError> {
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

    fn hlc(ms: u64, device: u8) -> Hlc {
        Hlc::with_device_id(ms, 0, crate::DeviceId::from_bytes([device; 16])).unwrap()
    }

    #[test]
    fn newer_remote_replaces_with_an_independent_clone() {
        let remote = std::string::String::from("remote");
        let mut local = std::string::String::from("local");
        assert!(local
            .merge(&remote, crate::MergeClocks::new(hlc(100, 1), hlc(200, 1)),)
            .unwrap());
        assert_eq!(local, remote);
        assert_ne!(local.as_ptr(), remote.as_ptr());
    }

    #[test]
    fn stale_remote_does_not_replace_local() {
        let mut local = std::string::String::from("new");
        assert!(!local
            .merge(
                &std::string::String::from("old"),
                crate::MergeClocks::new(hlc(200, 1), hlc(100, 2)),
            )
            .unwrap());
        assert_eq!(local, "new");
    }

    #[test]
    fn bincode_roundtrips_utf8_empty_and_embedded_null() {
        for value in [
            std::string::String::new(),
            std::string::String::from("hello\0world"),
            std::string::String::from("Gr\u{fc}\u{df}e \u{1f642}"),
        ] {
            let encoded = encode_to_vec(&value, config::standard()).unwrap();
            let (decoded, consumed): (String, usize) =
                decode_from_slice(&encoded, config::standard()).unwrap();
            assert_eq!(consumed, encoded.len());
            assert_eq!(decoded, value);
        }
    }
}
