//! Shared identities and workspace configuration for ZenDB integration tests.

use libp2p_identity::Keypair;
use zendb_types::PeerIdentity;
use zendb_workspace::WorkspaceConfig;

pub struct TestPeerIdentity {
    keypair: Keypair,
    display_name: String,
}

impl TestPeerIdentity {
    pub fn generate(display_name: impl Into<String>) -> Self {
        Self {
            keypair: Keypair::generate_ed25519(),
            display_name: display_name.into(),
        }
    }
}

impl PeerIdentity for TestPeerIdentity {
    fn keypair(&self) -> &Keypair {
        &self.keypair
    }

    fn display_name(&self) -> &str {
        &self.display_name
    }
}

pub fn loopback_workspace_config(port: u16) -> WorkspaceConfig {
    let mut config = WorkspaceConfig::default();
    config.replication.listen_addresses = vec![
        format!("/ip4/127.0.0.1/tcp/{port}")
            .parse()
            .expect("the loopback test address is valid"),
    ];
    config.replication.dial.initial_backoff_ms = 100;
    config.replication.dial.max_backoff_ms = 500;
    config.replication.batch.linger_ms = 10;
    config
}

pub fn offline_workspace_config() -> WorkspaceConfig {
    let mut config = WorkspaceConfig::default();
    config.replication.listen_addresses.clear();
    config.replication.dial.initial_backoff_ms = 100;
    config.replication.dial.max_backoff_ms = 500;
    config.replication.batch.linger_ms = 10;
    config
}
