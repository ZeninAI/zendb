//! Durable table change records emitted after an operation changes row state.

use bincode::{Decode, Encode};
use zendb_types::{Cell, Event};

#[derive(Debug, Clone, Encode, Decode)]
pub struct Change {
    pub event: Event,
    pub previous: Option<Cell>,
    pub current: Option<Cell>,
}
