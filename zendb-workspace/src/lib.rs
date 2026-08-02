//! Synchronous workspace lifecycle and mutation orchestration.

mod admission;
mod causal;
mod core;
mod error;
mod installations;
mod replication;
mod states;
mod system;
mod tables;
mod workspace;

pub use admission::AdmitError;
pub use error::{Error, Result};
pub use installations::Installations;
pub use replication::{BatchConfig, DialConfig, ReplicationConfig, TopologyConfig};
pub use states::{StateHandle, States};
pub use tables::{ChangeListener, TableHandle, Tables};
#[cfg(any(test, feature = "test-support"))]
pub use workspace::derive_workspace_keypair;
pub use workspace::{Workspace, WorkspaceConfig};
pub use zendb_types::{Installation, Multiaddr, MultiaddrError, PublicKey, Role, WorkspaceId};
