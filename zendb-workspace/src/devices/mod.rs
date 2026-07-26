//! Device registry, cached peer bookkeeping, and receipt tracking.

mod receipts;
mod runtime;

pub(crate) use runtime::PeerState;
pub use runtime::{DeviceRecord, Devices};
