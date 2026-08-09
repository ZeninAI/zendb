//! Synchronous workspace lifecycle and mutation orchestration.

mod clock;
mod config;
mod core;
mod error;
mod installations;
mod replication;
mod states;
mod system;
mod tables;
mod workspace;

pub use config::{BatchConfig, ReplicationConfig, SyncConfig, TransportConfig, WorkspaceConfig};
pub use error::{Error, Result};
pub use installations::Installations;
pub use states::{StateHandle, States};
pub use tables::{ChangeListener, TableHandle, Tables};
pub use workspace::Workspace;
#[cfg(any(test, feature = "test-support"))]
pub use workspace::derive_workspace_keypair;
pub use zendb_types::{
    Barrier, Installation, InstallationState, Multiaddr, MultiaddrError, Permission, Permissions,
    PublicKey, WorkspaceId,
};
