//! Unix-millisecond timestamp scalar CRDT.

use crate::zendb_type;
use bincode::{Decode, Encode};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimestampConversionError {
    BeforeEpoch,
    SubMillisecond,
    Overflow,
}
impl std::fmt::Display for TimestampConversionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::BeforeEpoch => "timestamp must be at or after the Unix epoch",
            Self::SubMillisecond => "timestamp must have millisecond precision",
            Self::Overflow => "timestamp is outside the representable SystemTime range",
        })
    }
}
impl std::error::Error for TimestampConversionError {}

zendb_type! {
    #[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Encode, Decode)]
    pub struct Timestamp { pub milliseconds: u64 }

    impl Timestamp {
        pub fn op_set(&mut self, remote: crate::EventTime, value: u64) -> bool {
            if remote <= self.__event_time { return false; }
            self.milliseconds = value;
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
impl Timestamp {
    pub fn new(value: u64) -> Self {
        Self {
            milliseconds: value,
            ..Self::default()
        }
    }
    pub const fn get(self) -> u64 {
        self.milliseconds
    }
}
impl From<u64> for Timestamp {
    fn from(value: u64) -> Self {
        Self::new(value)
    }
}
impl From<Timestamp> for u64 {
    fn from(value: Timestamp) -> Self {
        value.get()
    }
}
impl TryFrom<SystemTime> for Timestamp {
    type Error = TimestampConversionError;
    fn try_from(value: SystemTime) -> Result<Self, Self::Error> {
        let elapsed = value
            .duration_since(UNIX_EPOCH)
            .map_err(|_| TimestampConversionError::BeforeEpoch)?;
        if elapsed.subsec_nanos() % 1_000_000 != 0 {
            return Err(TimestampConversionError::SubMillisecond);
        }
        Ok(Self::new(
            u64::try_from(elapsed.as_millis()).map_err(|_| TimestampConversionError::Overflow)?,
        ))
    }
}
impl TryFrom<Timestamp> for SystemTime {
    type Error = TimestampConversionError;
    fn try_from(value: Timestamp) -> Result<Self, Self::Error> {
        UNIX_EPOCH
            .checked_add(Duration::from_millis(value.get()))
            .ok_or(TimestampConversionError::Overflow)
    }
}
