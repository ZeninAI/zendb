//! Peer registry, hybrid clock, roles, and duplicate-event tracking.

mod clock;
mod receipts;
mod registry;

pub use clock::{ClockCheckpoint, PeerRecord};
pub use receipts::{ObserveOutcome, ReceiptWindow};
pub use registry::{DeviceRecord, Devices};
