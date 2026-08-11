//! Opaque byte-string scalar CRDT.

use std::{io, ops::Deref};

use bincode::{Decode, Encode};

use crate::{
    utils::serdes::{deserialize_from, serialize_to_vec},
    zendb_type,
};

zendb_type! {
    #[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Encode, Decode)]
    pub struct Blob { pub bytes: Vec<u8> }

    impl Blob {
        pub fn op_set(&mut self, remote: crate::EventTime, bytes: Vec<u8>) -> bool {
            if remote <= self.__event_time { return false; }
            self.bytes = bytes;
            self.__is_tombstone = false;
            self.__event_time = remote;
            true
        }

        pub fn op_delete(&mut self, remote: crate::EventTime) -> bool {
            if remote <= self.__event_time { return false; }
            self.__is_tombstone = true;
            self.__event_time = remote;
            true
        }
    }
}

impl Blob {
    pub fn as_slice(&self) -> &[u8] {
        &self.bytes
    }
    pub fn encode<T: Encode>(value: &T) -> io::Result<Self> {
        serialize_to_vec(value).map(Self::from)
    }
    pub fn decode<T: Decode<()>>(&self) -> io::Result<T> {
        deserialize_from(self.as_slice())
    }
}
impl From<Vec<u8>> for Blob {
    fn from(bytes: Vec<u8>) -> Self {
        Self {
            bytes,
            ..Self::default()
        }
    }
}
impl From<&[u8]> for Blob {
    fn from(bytes: &[u8]) -> Self {
        Self::from(bytes.to_vec())
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
