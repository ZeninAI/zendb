//! Fixtures for exercising two live workspaces in one test process.

use std::{
    fs,
    net::TcpListener,
    path::{Path, PathBuf},
    sync::Arc,
};

use zendb_it::{TestPeerIdentity, offline_workspace_config};
use zendb_storage::TableConfig;
use zendb_types::{Multiaddr, PublicKey};
use zendb_workspace::{
    Installation, InstallationState, Permissions, Workspace, derive_workspace_keypair,
};

pub struct WorkspacePair {
    _temp: tempfile::TempDir,
    pub a_root: PathBuf,
    pub b_root: PathBuf,
    pub a_identity: Arc<TestPeerIdentity>,
    pub b_identity: Arc<TestPeerIdentity>,
    pub a_port: u16,
    pub b_port: u16,
}

impl WorkspacePair {
    pub fn seeded(table_names: &[&str]) -> Self {
        let temp = tempfile::tempdir().expect("failed to create workspace fixture");
        let a_root = temp.path().join("workspace-a");
        let b_root = temp.path().join("workspace-b");
        let [a_port, b_port] = available_ports();
        let a_identity = Arc::new(TestPeerIdentity::generate("installation-a"));
        let b_identity = Arc::new(TestPeerIdentity::generate("installation-b"));

        let workspace = Workspace::create(&a_root, a_identity.clone(), offline_workspace_config())
            .expect("failed to create seed workspace");
        let workspace_id = workspace.workspace_id();
        let installation_a = workspace.installations().local_installation_id();
        let mut b_config = offline_workspace_config();
        b_config.workspace_id = Some(workspace_id);
        let b_workspace = Workspace::create(&b_root, b_identity.clone(), b_config)
            .expect("failed to create workspace B fixture");
        let installation_b = b_workspace.installations().local_installation_id();
        drop(b_workspace);

        let mut installation_a_value = workspace
            .installations()
            .get(&installation_a)
            .expect("local installation is enrolled");
        installation_a_value.addresses = vec![loopback_address(a_port)];
        workspace
            .installations()
            .upsert(installation_a, installation_a_value)
            .expect("failed to store installation A route");
        workspace
            .installations()
            .upsert(
                installation_b,
                Installation {
                    display_name: "installation-b".to_owned(),
                    public_key: PublicKey::from_libp2p(
                        derive_workspace_keypair(b_identity.as_ref(), workspace_id, installation_b)
                            .expect("failed to derive installation B workspace key")
                            .public(),
                    ),
                    addresses: vec![loopback_address(b_port)],
                    state: InstallationState::Active(Permissions::CONTRIBUTOR),
                },
            )
            .expect("failed to enroll installation B");
        for name in table_names {
            workspace
                .tables()
                .upsert(name, TableConfig::default())
                .expect("failed to create fixture table");
        }
        workspace.sync().expect("failed to sync seed workspace");
        drop(workspace);

        // Only the trusted table history is copied from A; copying A's states
        // would leave B without the causal row keyed by its own installation.
        copy_directory(&a_root.join("tables"), &b_root.join("tables"));

        Self {
            _temp: temp,
            a_root,
            b_root,
            a_identity,
            b_identity,
            a_port,
            b_port,
        }
    }
}

fn loopback_address(port: u16) -> Multiaddr {
    format!("/ip4/127.0.0.1/tcp/{port}")
        .parse()
        .expect("loopback test address is valid")
}

fn available_ports() -> [u16; 2] {
    let first = TcpListener::bind(("127.0.0.1", 0)).expect("failed to reserve first test port");
    let second = TcpListener::bind(("127.0.0.1", 0)).expect("failed to reserve second test port");
    [
        first
            .local_addr()
            .expect("first test address exists")
            .port(),
        second
            .local_addr()
            .expect("second test address exists")
            .port(),
    ]
}

fn copy_directory(source: &Path, target: &Path) {
    fs::create_dir_all(target).expect("failed to create copied workspace directory");
    for entry in fs::read_dir(source).expect("failed to read workspace directory") {
        let entry = entry.expect("failed to read workspace entry");
        let target_path = target.join(entry.file_name());
        if entry
            .file_type()
            .expect("failed to read workspace entry type")
            .is_dir()
        {
            copy_directory(&entry.path(), &target_path);
        } else {
            fs::copy(entry.path(), target_path).expect("failed to copy workspace file");
        }
    }
}
