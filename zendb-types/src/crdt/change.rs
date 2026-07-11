use bincode::{Decode, Encode};

use super::event::Event;
use crate::Cell;

/// A change record produced by applying an event to a table.
///
/// The `previous` field holds the cell value before the event was applied
/// (if any), and `current` holds the cell value after the event was applied
/// (if any). When both are `None` the event produced no visible mutation.
#[derive(Debug, Clone, Encode, Decode)]
pub struct Change {
    pub event: Event,
    pub previous: Option<Cell>,
    pub current: Option<Cell>,
}
