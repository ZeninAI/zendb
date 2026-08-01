//! End-to-end replication between two live workspaces on loopback TCP.

mod support;

use std::{
    thread,
    time::{Duration, Instant},
};

use support::WorkspacePair;
use zendb_it::{loopback_workspace_config, offline_workspace_config};
use zendb_storage::{ReadBackend, TableConfig};
use zendb_types::{Op, Path, PrimaryKey, Value};
use zendb_workspace::{TableHandle, Workspace};

const TIMEOUT: Duration = Duration::from_secs(20);

#[test]
fn workspaces_replicate_events_and_table_lifecycle() {
    let fixture = WorkspacePair::seeded(&["messages"]);
    let workspace_a = Workspace::open(
        &fixture.a_root,
        fixture.a_identity.clone(),
        loopback_workspace_config(fixture.a_port),
    )
    .expect("failed to open workspace A");

    // A must tolerate its persisted route being offline and recover after B
    // starts listening.
    thread::sleep(Duration::from_millis(750));
    let workspace_b = Workspace::open(
        &fixture.b_root,
        fixture.b_identity.clone(),
        loopback_workspace_config(fixture.b_port),
    )
    .expect("failed to open workspace B");

    let messages_a = workspace_a
        .tables()
        .get("messages")
        .expect("messages table is present in workspace A");
    let messages_b = workspace_b
        .tables()
        .get("messages")
        .expect("messages table is present in workspace B");
    wait_for_mesh(&messages_a, &messages_b);

    insert_string(&messages_a, "from-a", "alpha");
    insert_string(&messages_b, "from-b", "bravo");
    wait_for_value(&messages_a, "from-b", "bravo");
    wait_for_value(&messages_b, "from-a", "alpha");

    workspace_a
        .tables()
        .upsert("shared", TableConfig::default())
        .expect("failed to create replicated table");
    wait_until("workspace B to open the replicated table", || {
        workspace_b.tables().contains("shared")
    });

    let shared_a = workspace_a
        .tables()
        .get("shared")
        .expect("shared table is present in workspace A");
    let shared_b = workspace_b
        .tables()
        .get("shared")
        .expect("shared table is present in workspace B");
    insert_string(&shared_b, "from-b", "new-table-listener");
    wait_for_value(&shared_a, "from-b", "new-table-listener");
    insert_string(&shared_a, "from-a", "catalog-replicated");
    wait_for_value(&shared_b, "from-a", "catalog-replicated");

    workspace_a
        .tables()
        .upsert("obsolete", TableConfig::default())
        .expect("failed to create table for deletion");
    wait_until("workspace B to open the table for deletion", || {
        workspace_b.tables().contains("obsolete")
    });
    workspace_a
        .tables()
        .delete("obsolete")
        .expect("failed to delete replicated table");
    wait_until("workspace B to apply the table deletion", || {
        !workspace_b.tables().contains("obsolete")
    });

    workspace_a.sync().expect("failed to sync workspace A");
    workspace_b.sync().expect("failed to sync workspace B");
    drop(messages_a);
    drop(messages_b);
    drop(shared_a);
    drop(shared_b);
    drop(workspace_a);
    drop(workspace_b);

    assert_durable_replica(
        &fixture.a_root,
        fixture.a_identity.clone(),
        &[("from-a", "alpha"), ("from-b", "bravo")],
    );
    assert_durable_replica(
        &fixture.b_root,
        fixture.b_identity.clone(),
        &[("from-a", "alpha"), ("from-b", "bravo")],
    );
}

fn wait_for_mesh(table_a: &TableHandle, table_b: &TableHandle) {
    let deadline = Instant::now() + TIMEOUT;
    let mut attempt = 0_u64;
    loop {
        let key_a = format!("_ready/a/{attempt}");
        let key_b = format!("_ready/b/{attempt}");
        insert_string(table_a, &key_a, "ready");
        insert_string(table_b, &key_b, "ready");

        let a_received = table_b.read().keys().any(
            |key| matches!(key.as_ref(), PrimaryKey::String(key) if key.starts_with("_ready/a/")),
        );
        let b_received = table_a.read().keys().any(
            |key| matches!(key.as_ref(), PrimaryKey::String(key) if key.starts_with("_ready/b/")),
        );
        if a_received && b_received {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out establishing the replication mesh"
        );
        attempt += 1;
        thread::sleep(Duration::from_millis(100));
    }
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

fn wait_for_value(table: &TableHandle, key: &str, value: &str) {
    let key = PrimaryKey::String(key.to_owned());
    let expected = Some(Value::String(value.to_owned()));
    wait_until("replicated value", || {
        table.read().get(&key).and_then(|cell| cell.value.clone()) == expected
    });
}

fn wait_until(description: &str, condition: impl Fn() -> bool) {
    let deadline = Instant::now() + TIMEOUT;
    while !condition() {
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {description}"
        );
        thread::sleep(Duration::from_millis(25));
    }
}

fn assert_durable_replica(
    root: &std::path::Path,
    identity: std::sync::Arc<zendb_it::TestPeerIdentity>,
    messages: &[(&str, &str)],
) {
    let workspace = Workspace::open(root, identity, offline_workspace_config())
        .expect("failed to reopen replicated workspace");
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
            Some(Value::String((*value).to_owned()))
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
    assert_eq!(
        shared
            .read()
            .get(&PrimaryKey::String("from-b".to_owned()))
            .and_then(|cell| cell.value.clone()),
        Some(Value::String("new-table-listener".to_owned()))
    );
    assert!(!workspace.tables().contains("obsolete"));
}
