//! Boolean scalar CRDT.

use bincode::{Decode, Encode};

use crate::zendb_type;

zendb_type! {
    #[derive(Debug, Clone, Default, PartialEq, Encode, Decode)]
    pub struct Bool { pub value: bool }

    impl Bool {
        pub fn op_set(&mut self, remote: crate::EventTime, value: bool) -> bool {
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

impl From<bool> for Bool {
    fn from(value: bool) -> Self {
        Self {
            value,
            ..Self::default()
        }
    }
}
