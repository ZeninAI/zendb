//! Runtime configuration for a workspace and its direct replication controller.

use std::time::Duration;

use zendb_types::{Multiaddr, WorkspaceId};

/// Replication cadence and bounded range bookkeeping.
#[derive(Debug, Clone)]
pub struct SyncConfig {
    /// How often anti-entropy summaries are exchanged and ready peers are retried.
    pub interval: Duration,
    /// Maximum number of event ranges in a single Fetch request.
    pub max_ranges: usize,
    /// Number of recent events retained for fast anti-entropy responses.
    pub recent_cache_capacity: usize,
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(5),
            max_ranges: 256,
            recent_cache_capacity: 4096,
        }
    }
}

/// Controls when locally committed events are assembled into Push batches.
#[derive(Debug, Clone)]
pub struct BatchConfig {
    /// Soft serialized size threshold for one pending batch, in bytes.
    ///
    /// The event that takes a batch over this threshold remains in that batch,
    /// which is then completed before another event is admitted.
    pub batch_size: usize,
    /// Optional maximum time to retain a non-empty batch. `None` disables the
    /// timer, leaving `batch_size` as the only completion trigger.
    pub linger: Option<Duration>,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            batch_size: 1024 * 1024,
            linger: Some(Duration::from_millis(250)),
        }
    }
}

/// Transport and discovery capabilities used by the replication swarm.
#[derive(Debug, Clone)]
pub struct TransportConfig {
    /// Addresses on which the replication swarm accepts connections.
    pub listener_addresses: Vec<Multiaddr>,
    /// Whether outbound dials may reuse the local transport port.
    ///
    /// When disabled, each outbound dial allocates a fresh local port. This
    /// is useful for integration tests that need simultaneous connections to
    /// remain distinguishable at the TCP layer.
    pub enable_port_reuse: bool,
    /// Whether the swarm advertises and discovers peers through mDNS.
    pub enable_mdns: bool,
    /// Whether TCP transport is included in the swarm.
    pub enable_tcp: bool,
    /// Whether QUIC transport is included in the swarm.
    pub enable_quic: bool,
}

impl Default for TransportConfig {
    fn default() -> Self {
        Self {
            listener_addresses: vec![
                "/ip4/0.0.0.0/tcp/0"
                    .parse()
                    .expect("valid default listen address"),
                "/ip4/0.0.0.0/udp/0/quic-v1"
                    .parse()
                    .expect("valid default QUIC listen address"),
            ],
            enable_port_reuse: true,
            enable_mdns: true,
            enable_tcp: true,
            enable_quic: true,
        }
    }
}

/// Top-level replication configuration exposed through `WorkspaceConfig`.
#[derive(Debug, Clone)]
pub struct ReplicationConfig {
    /// Whether the workspace starts a replication controller and emits
    /// replication notifications.
    pub enabled: bool,
    pub transport: TransportConfig,
    pub sync: SyncConfig,
    pub batch: BatchConfig,
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            transport: TransportConfig::default(),
            sync: SyncConfig::default(),
            batch: BatchConfig::default(),
        }
    }
}

/// Workspace-level runtime configuration.
#[derive(Debug, Clone, Default)]
pub struct WorkspaceConfig {
    /// Workspace ID to create, or an optional assertion when opening.
    pub workspace_id: Option<WorkspaceId>,
    /// Replication runtime settings for this workspace.
    pub replication: ReplicationConfig,
}
