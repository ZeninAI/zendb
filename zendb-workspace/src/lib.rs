//! Synchronous workspace lifecycle and mutation orchestration.

mod admission;
mod consts;
mod devices;
mod error;
mod replication;
mod states;
mod tables;
mod workspace;

pub use admission::AdmitError;
pub use devices::{DeviceRecord, Devices};
pub use error::{Error, Result};
pub use replication::{BatchConfig, DialConfig, ReplicationConfig, TopologyConfig};
pub use states::{StateHandle, States};
pub use tables::{ChangeListener, TableHandle};
pub use workspace::{JoinHints, Workspace, WorkspaceConfig, derive_workspace_public_key};
pub use zendb_types::{Multiaddr, MultiaddrError, PublicKey, Role};
