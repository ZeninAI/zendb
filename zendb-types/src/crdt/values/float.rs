//! Deterministic IEEE-754 scalar CRDTs.

use crate::zendb_type;
use bincode::{Decode, Encode};

zendb_type! {
    #[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Encode, Decode)]
    pub struct Float { pub bits: u64 }

    impl Float {
        pub fn op_set(&mut self, remote: crate::EventTime, value: f64) -> bool {
            if remote <= self.__event_time { return false; }
            let bits = value.to_bits();
            self.bits = bits;
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
impl Float {
    pub fn new(value: f64) -> Self {
        Self {
            bits: value.to_bits(),
            ..Self::default()
        }
    }
    pub fn get(self) -> f64 {
        f64::from_bits(self.bits)
    }
}
impl From<f64> for Float {
    fn from(value: f64) -> Self {
        Self::new(value)
    }
}
impl From<Float> for f64 {
    fn from(value: Float) -> Self {
        value.get()
    }
}
