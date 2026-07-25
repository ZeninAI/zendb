//! Synchronous workspace lifecycle and mutation orchestration.

mod catalog;
mod devices;
mod error;
mod workspace;

pub use catalog::{
    CatalogEntry, StateHandle, TableConsumer, TableHandle, TableInfo, TableReadGuard, UpdateOutcome,
};
pub use devices::{
    ClockCheckpoint, DeviceRecord, Devices, ObserveOutcome, PeerRecord, ReceiptWindow,
};
pub use error::{Error, Result};
pub use workspace::{Workspace, WorkspaceConfig};
pub use zendb_types::Roles;
