//! Public tuning values for workspace-owned replication.

use std::time::Duration;

use zendb_types::Multiaddr;

/// Outbound event batching thresholds.
#[derive(Debug, Clone)]
pub struct BatchConfig {
    /// Maximum number of events in one envelope.
    pub max_events: usize,
    /// Approximate serialized byte limit for one envelope.
    pub max_bytes: usize,
    /// Maximum time to retain an incomplete batch before flushing it.
    pub linger: Duration,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            max_events: 16,
            max_bytes: 65_536,
            linger: Duration::from_millis(50),
        }
    }
}

/// Peer scoring and gossipsub mesh settings.
#[derive(Debug, Clone)]
pub struct TopologyConfig {
    /// Application score bonus for peers discovered on the local network.
    pub lan_score_bonus: f64,
    /// Application score penalty weight for round-trip latency.
    pub latency_weight: f64,
    /// Target number of peers in the gossipsub mesh.
    pub mesh_size: usize,
}

impl Default for TopologyConfig {
    fn default() -> Self {
        Self {
            lan_score_bonus: 10.0,
            latency_weight: 0.01,
            mesh_size: 6,
        }
    }
}

/// Dial retry backoff settings for authorized peers.
#[derive(Debug, Clone)]
pub struct DialConfig {
    /// Initial delay before retrying a failed dial.
    pub initial_backoff: Duration,
    /// Maximum delay reached by exponential dial backoff.
    pub max_backoff: Duration,
}

impl Default for DialConfig {
    fn default() -> Self {
        Self {
            initial_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(60),
        }
    }
}

/// Complete runtime configuration for workspace-owned replication.
#[derive(Debug, Clone)]
pub struct ReplicationConfig {
    /// Outbound envelope batching settings.
    pub batch: BatchConfig,
    /// Peer scoring and mesh settings.
    pub topology: TopologyConfig,
    /// Dial retry settings.
    pub dial: DialConfig,
    /// Local multiaddresses on which the replication swarm listens.
    pub listen_addresses: Vec<Multiaddr>,
    /// Capacity of command and inbound admission channels.
    pub channel_capacity: usize,
    /// Maximum gossipsub payload size accepted by the swarm.
    pub gossipsub_max_transmit_size: usize,
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            batch: BatchConfig::default(),
            topology: TopologyConfig::default(),
            dial: DialConfig::default(),
            listen_addresses: vec![
                "/ip4/0.0.0.0/tcp/0"
                    .parse()
                    .expect("the default listen address is valid"),
            ],
            channel_capacity: 1_024,
            gossipsub_max_transmit_size: 100 * 1024 * 1024,
        }
    }
}
