//! End-to-end replication with the secondary workspace in a separate process.

mod common;
mod support;

use std::{fs, path::PathBuf, sync::Arc, thread, time::Duration};

use support::{
    WORKER_KEYPAIR, WORKER_MODE, WORKER_PORT, WORKER_READY, WORKER_ROOT, WORKER_STOP,
    WorkspacePair, WorkspaceWorker,
};
use zendb_it::{TestPeerIdentity, loopback_workspace_config, offline_workspace_config};
use zendb_storage::{ReadBackend, TableConfig};
use zendb_types::{Op, Path, PrimaryKey, Value};
use zendb_workspace::{TableHandle, Workspace};

#[test]
fn workspaces_replicate_events_and_table_lifecycle() {
    common::init_logging();
    if std::env::var_os(WORKER_MODE).is_some() {
        run_replication_worker();
        return;
    }

    let fixture = WorkspacePair::seeded(&["messages"]);
    let worker = WorkspaceWorker::spawn(&fixture);
    thread::sleep(Duration::from_millis(1000));
    let workspace_a = Workspace::open(
        &fixture.a_root,
        fixture.a_identity.clone(),
        loopback_workspace_config(fixture.a_port),
    )
    .expect("failed to open workspace A");

    // Give the worker listener time to become reachable after its workspace
    // has been opened.
    thread::sleep(Duration::from_millis(750));
    let messages_a = workspace_a
        .tables()
        .get("messages")
        .expect("messages table is present in workspace A");
    insert_string(&messages_a, "from-a", "alpha");

    workspace_a
        .tables()
        .upsert("shared", TableConfig::default())
        .expect("failed to create replicated table");
    let shared_a = workspace_a
        .tables()
        .get("shared")
        .expect("shared table is present in workspace A");
    insert_string(&shared_a, "from-a", "catalog-replicated");

    workspace_a
        .tables()
        .upsert("obsolete", TableConfig::default())
        .expect("failed to create table for deletion");
    workspace_a
        .tables()
        .delete("obsolete")
        .expect("failed to delete replicated table");

    workspace_a
        .persist(zendb_workspace::Barrier::Sync)
        .expect("failed to sync workspace A");
    // Allow the asynchronous replication write to leave A before its swarm is
    // shut down.
    thread::sleep(Duration::from_millis(250));
    drop(messages_a);
    drop(shared_a);
    drop(workspace_a);

    // Let the worker apply the final catalog and data events before it is
    // asked to close and flush its workspace.
    thread::sleep(Duration::from_secs(5));
    worker.finish();

    assert_durable_replica(
        &fixture.a_root,
        fixture.a_identity.clone(),
        &[("from-a", "alpha")],
    );
    assert_durable_replica(
        &fixture.b_root,
        fixture.b_identity.clone(),
        &[("from-a", "alpha")],
    );
}

fn insert_string(table: &TableHandle, key: &str, value: &str) {
    table
        .insert(
            PrimaryKey::String(key.to_owned()),
            Path::new(),
            Op::Upsert {
                value: Value::String(value.to_owned()),
            },
        )
        .expect("failed to insert test value");
}

fn run_replication_worker() {
    let root =
        PathBuf::from(std::env::var(WORKER_ROOT).expect("replication worker root is configured"));
    let keypair_path = PathBuf::from(
        std::env::var(WORKER_KEYPAIR).expect("replication worker keypair is configured"),
    );
    let port = std::env::var(WORKER_PORT)
        .expect("replication worker port is configured")
        .parse()
        .expect("replication worker port is valid");
    let ready_path = PathBuf::from(
        std::env::var(WORKER_READY).expect("replication worker ready path is configured"),
    );
    let stop_path = PathBuf::from(
        std::env::var(WORKER_STOP).expect("replication worker stop path is configured"),
    );
    let keypair = libp2p_identity::Keypair::from_protobuf_encoding(
        &fs::read(keypair_path).expect("failed to read replication worker keypair"),
    )
    .expect("replication worker keypair is valid");
    let identity = Arc::new(TestPeerIdentity::from_keypair(
        "installation-b",
        keypair,
        vec![
            format!("/ip4/127.0.0.1/tcp/{port}")
                .parse()
                .expect("replication worker address is valid"),
        ],
    ));
    let workspace = Workspace::open(&root, identity, loopback_workspace_config(port))
        .expect("replication worker failed to open workspace B");
    assert_eq!(
        workspace.installations().list().len(),
        2,
        "replication worker workspace has both installations enrolled"
    );
    fs::write(ready_path, b"ready").expect("failed to signal replication worker readiness");

    while !stop_path.exists() {
        thread::sleep(Duration::from_millis(25));
    }
    drop(workspace);
}

fn assert_durable_replica(
    root: &std::path::Path,
    identity: Arc<TestPeerIdentity>,
    messages: &[(&str, &str)],
) {
    let workspace = Workspace::open(root, identity, offline_workspace_config())
        .expect("failed to reopen replicated workspace");
    assert_eq!(
        workspace.installations().list().len(),
        2,
        "durable workspace has both installations enrolled"
    );
    let message_table = workspace
        .tables()
        .get("messages")
        .expect("durable messages table is present");
    for (key, value) in messages {
        assert_eq!(
            message_table
                .read()
                .get(&PrimaryKey::String((*key).to_owned()))
                .and_then(|cell| cell.value.clone()),
            Some(Value::String((*value).to_owned())),
            "missing {key} in {}",
            root.display()
        );
    }

    let shared = workspace
        .tables()
        .get("shared")
        .expect("durable replicated table is present");
    assert_eq!(
        shared
            .read()
            .get(&PrimaryKey::String("from-a".to_owned()))
            .and_then(|cell| cell.value.clone()),
        Some(Value::String("catalog-replicated".to_owned()))
    );
    assert!(!workspace.tables().contains("obsolete"));
}
