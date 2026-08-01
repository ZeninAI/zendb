//! Synchronous workspace lifecycle and mutation orchestration.

mod admission;
mod consts;
mod error;
mod installations;
mod replication;
mod states;
mod tables;
mod workspace;

pub use admission::AdmitError;
pub use error::{Error, Result};
pub use installations::Installations;
pub use replication::{BatchConfig, DialConfig, ReplicationConfig, TopologyConfig};
pub use states::{StateHandle, States};
pub use tables::{ChangeListener, TableHandle};
pub use workspace::{JoinHints, Workspace, WorkspaceConfig, derive_workspace_public_key};
pub use zendb_types::{Installation, Multiaddr, MultiaddrError, PublicKey, Role};
