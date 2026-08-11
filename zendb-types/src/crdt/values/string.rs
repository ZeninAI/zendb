//! UTF-8 string scalar CRDT.

use std::ops::Deref;

use bincode::{Decode, Encode};

use crate::zendb_type;

zendb_type! {
    #[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Encode, Decode)]
    pub struct String { pub value: std::string::String }

    impl String {
        pub fn op_set(&mut self, remote: crate::EventTime, value: std::string::String) -> bool {
            if remote <= self.__event_time { return false; }
            self.value = value;
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

impl From<std::string::String> for String {
    fn from(value: std::string::String) -> Self {
        Self {
            value,
            ..Self::default()
        }
    }
}

impl From<&str> for String {
    fn from(value: &str) -> Self {
        Self::from(value.to_owned())
    }
}

impl Deref for String {
    type Target = str;
    fn deref(&self) -> &Self::Target {
        &self.value
    }
}
