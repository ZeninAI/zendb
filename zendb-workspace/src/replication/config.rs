//! Tuning knobs for the Zenin replication runtime.
//!
//! All durations and sizes are taken at face value; the runtime does not clamp
//! or override them.

use std::time::Duration;

use zendb_types::Multiaddr;

/// Protocol-level settings: sync cadence, frame limits, and batching.
#[derive(Debug, Clone)]
pub struct ZeninConfig {
    /// How often anti-entropy summaries are exchanged with mesh neighbours.
    pub sync_interval: Duration,
    /// Interval between mesh maintenance ticks (graft/prune evaluation).
    pub heartbeat: Duration,
    /// Maximum size of a single wire frame in bytes.
    pub max_frame_bytes: usize,
    /// Maximum bytes returned in a single FetchResponse.
    pub max_sync_bytes: usize,
    /// Maximum number of event ranges in a single Fetch request.
    pub max_ranges: usize,
    /// How long to accumulate local events before flushing a Push batch.
    /// Zero means flush on the next event-loop iteration (no delay).
    pub linger: Duration,
    /// Number of recent events kept in-memory for fast anti-entropy responses.
    pub recent_cache_capacity: usize,
}

impl Default for ZeninConfig {
    fn default() -> Self {
        Self {
            sync_interval: Duration::from_secs(5),
            heartbeat: Duration::from_millis(250),
            max_frame_bytes: 100 * 1024 * 1024,
            max_sync_bytes: 4 * 1024 * 1024,
            max_ranges: 256,
            linger: Duration::ZERO,
            recent_cache_capacity: 4096,
        }
    }
}

/// Mesh topology parameters.
#[derive(Debug, Clone, Copy)]
pub struct MeshConfig {
    /// Target number of active forwarding neighbours.
    pub target_peers: usize,
    /// Maximum peers accepted into the mesh before incoming grafts are refused.
    pub high_watermark: usize,
}

impl Default for MeshConfig {
    fn default() -> Self {
        Self {
            target_peers: 6,
            high_watermark: 12,
        }
    }
}

/// Top-level replication configuration exposed through `WorkspaceConfig`.
#[derive(Debug, Clone)]
pub struct ReplicationConfig {
    pub zenin: ZeninConfig,
    pub mesh: MeshConfig,
    pub listen_addresses: Vec<Multiaddr>,
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            zenin: ZeninConfig::default(),
            mesh: MeshConfig::default(),
            listen_addresses: vec![
                "/ip4/0.0.0.0/tcp/0"
                    .parse()
                    .expect("valid default listen address"),
                "/ip4/0.0.0.0/udp/0/quic-v1"
                    .parse()
                    .expect("valid default QUIC listen address"),
            ],
        }
    }
}
