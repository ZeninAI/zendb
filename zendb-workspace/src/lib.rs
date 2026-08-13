//! Synchronous workspace lifecycle and mutation orchestration.

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
pub use replication::{DiscoveredPeer, PeerDiscoveryListener};
pub use states::{StateHandle, States};
pub use tables::{ChangeListener, TableHandle, Tables};
pub use workspace::Workspace;
#[cfg(any(test, feature = "test-support"))]
pub use workspace::derive_workspace_keypair;
pub use zendb_storage::Barrier;
pub use zendb_types::{
    Installation, InstallationState, Multiaddr, MultiaddrError, Permission, Permissions, PublicKey,
    WorkspaceId,
};
