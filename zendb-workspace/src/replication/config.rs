//! Public tuning values for workspace-owned replication.

#[derive(Debug, Clone)]
pub struct BatchConfig {
    pub max_events: usize,
    pub max_bytes: usize,
    pub linger_ms: u64,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            max_events: 16,
            max_bytes: 65_536,
            linger_ms: 50,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TopologyConfig {
    pub lan_score_bonus: f64,
    pub latency_weight: f64,
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

#[derive(Debug, Clone)]
pub struct ReplicationConfig {
    pub batch: BatchConfig,
    pub topology: TopologyConfig,
    pub outbound_capacity: usize,
    pub gossipsub_max_transmit_size: usize,
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            batch: BatchConfig::default(),
            topology: TopologyConfig::default(),
            outbound_capacity: 1_024,
            gossipsub_max_transmit_size: 100 * 1024 * 1024,
        }
    }
}
