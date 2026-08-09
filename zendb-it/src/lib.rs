//! Shared identities and workspace configuration for ZenDB integration tests.

use std::time::Duration;

use libp2p_identity::Keypair;
use zendb_types::{Multiaddr, PeerIdentity};
use zendb_workspace::WorkspaceConfig;

pub struct TestPeerIdentity {
    keypair: Keypair,
    display_name: String,
    addresses: Vec<Multiaddr>,
}

impl TestPeerIdentity {
    pub fn generate(display_name: impl Into<String>) -> Self {
        Self {
            keypair: Keypair::generate_ed25519(),
            display_name: display_name.into(),
            addresses: Vec::new(),
        }
    }

    pub fn from_keypair(
        display_name: impl Into<String>,
        keypair: Keypair,
        addresses: Vec<Multiaddr>,
    ) -> Self {
        Self {
            keypair,
            display_name: display_name.into(),
            addresses,
        }
    }

    pub fn with_addresses(mut self, addresses: Vec<Multiaddr>) -> Self {
        self.addresses = addresses;
        self
    }
}

impl PeerIdentity for TestPeerIdentity {
    fn keypair(&self) -> &Keypair {
        &self.keypair
    }

    fn display_name(&self) -> &str {
        &self.display_name
    }

    fn addresses(&self) -> Vec<Multiaddr> {
        self.addresses.clone()
    }
}

pub fn loopback_workspace_config(port: u16) -> WorkspaceConfig {
    let mut config = WorkspaceConfig::default();
    config.replication.transport.enable_port_reuse = false;
    config.replication.transport.enable_mdns = false;
    config.replication.transport.listener_addresses = vec![
        format!("/ip4/127.0.0.1/tcp/{port}")
            .parse()
            .expect("the loopback test address is valid"),
    ];
    config.replication.sync.interval = Duration::from_millis(250);
    config.replication.batch.linger = Some(Duration::from_millis(50));
    config
}

pub fn offline_workspace_config() -> WorkspaceConfig {
    let mut config = WorkspaceConfig::default();
    config.replication.transport.enable_port_reuse = false;
    config.replication.transport.listener_addresses.clear();
    config.replication.sync.interval = Duration::from_millis(250);
    config
}
