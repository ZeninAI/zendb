//! Fixtures for exercising a parent workspace with a separate worker process.

use std::{
    fs,
    net::TcpListener,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::Arc,
    thread,
    time::{Duration, Instant},
};

use zendb_it::{TestPeerIdentity, offline_workspace_config};
use zendb_storage::TableConfig;
use zendb_types::{Multiaddr, PeerIdentity};
use zendb_workspace::Workspace;

pub const WORKER_MODE: &str = "ZENDB_REPLICATION_WORKER";
pub const WORKER_ROOT: &str = "ZENDB_REPLICATION_WORKER_ROOT";
pub const WORKER_KEYPAIR: &str = "ZENDB_REPLICATION_WORKER_KEYPAIR";
pub const WORKER_PORT: &str = "ZENDB_REPLICATION_WORKER_PORT";
pub const WORKER_READY: &str = "ZENDB_REPLICATION_WORKER_READY";
pub const WORKER_STOP: &str = "ZENDB_REPLICATION_WORKER_STOP";

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
        let a_identity = Arc::new(
            TestPeerIdentity::generate("installation-a")
                .with_addresses(vec![loopback_address(a_port)]),
        );
        let b_identity = Arc::new(
            TestPeerIdentity::generate("installation-b")
                .with_addresses(vec![loopback_address(b_port)]),
        );

        let mut seed_config = offline_workspace_config();
        seed_config.replication.enabled = false;
        let workspace_a = Workspace::create(&a_root, a_identity.clone(), seed_config.clone())
            .expect("failed to create seed workspace");
        let workspace_id = workspace_a.workspace_id();
        let mut workspace_b_config = seed_config;
        workspace_b_config.workspace_id = Some(workspace_id);
        let workspace_b = Workspace::create(&b_root, b_identity.clone(), workspace_b_config)
            .expect("failed to create workspace B fixture");
        let installation_b = workspace_b.installations().local_installation_id();
        let installation_b_value = workspace_b
            .installations()
            .get(&installation_b)
            .expect("local installation B is enrolled");
        drop(workspace_b);

        workspace_a
            .installations()
            .upsert(installation_b, installation_b_value)
            .expect("failed to enroll installation B");
        for name in table_names {
            workspace_a
                .tables()
                .upsert(name, TableConfig::default())
                .expect("failed to create fixture table");
        }
        workspace_a
            .persist(zendb_workspace::Barrier::Sync)
            .expect("failed to sync seed workspace");
        drop(workspace_a);

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

pub struct WorkspaceWorker {
    child: Child,
    stop_path: PathBuf,
    finished: bool,
}

impl WorkspaceWorker {
    pub fn spawn(fixture: &WorkspacePair) -> Self {
        let keypair_path = fixture._temp.path().join("worker-keypair");
        let ready_path = fixture._temp.path().join("worker-ready");
        let stop_path = fixture._temp.path().join("worker-stop");
        fs::write(
            &keypair_path,
            fixture
                .b_identity
                .keypair()
                .to_protobuf_encoding()
                .expect("test keypair can be encoded"),
        )
        .expect("failed to write worker keypair");

        let mut child = Command::new(std::env::current_exe().expect("test executable exists"))
            .args([
                "--exact",
                "workspaces_replicate_events_and_table_lifecycle",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(WORKER_MODE, "1")
            .env(WORKER_ROOT, &fixture.b_root)
            .env(WORKER_KEYPAIR, &keypair_path)
            .env(WORKER_PORT, fixture.b_port.to_string())
            .env(WORKER_READY, &ready_path)
            .env(WORKER_STOP, &stop_path)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("failed to spawn replication worker");

        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            if ready_path.exists() {
                break;
            }
            if let Some(status) = child
                .try_wait()
                .expect("failed to inspect replication worker")
            {
                panic!("replication worker exited before opening workspace: {status}");
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                panic!("timed out waiting for replication worker");
            }
            thread::sleep(Duration::from_millis(25));
        }

        Self {
            child,
            stop_path,
            finished: false,
        }
    }

    pub fn finish(mut self) {
        fs::write(&self.stop_path, b"stop").expect("failed to signal replication worker");
        let status = self
            .child
            .wait()
            .expect("failed to wait for replication worker");
        assert!(status.success(), "replication worker exited with {status}");
        self.finished = true;
    }
}

impl Drop for WorkspaceWorker {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        let _ = fs::write(&self.stop_path, b"stop");
        let _ = self.child.kill();
        let _ = self.child.wait();
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
