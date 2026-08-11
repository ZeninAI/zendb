//! Durable table change records emitted after an operation changes row state.

use bincode::{Decode, Encode};
use zendb_types::{Event, Value};

#[derive(Debug, Clone, Encode, Decode)]
pub struct Change {
    pub event: Event,
    pub previous: Option<Value>,
    pub current: Option<Value>,
}
