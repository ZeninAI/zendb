use std::{sync::Arc, time::Duration};

use zendb_storage::core::traits::Backend;
use zendb_types::{DeviceId, Op, Path, PrimaryKey, Value, WorkspaceId};

use crate::{TableConfig, WorkspaceConfig};

use super::support::{tmp, TestWorkspace, ThreadExecutor};

#[test]
fn creating_an_existing_shared_table_is_idempotent() {
    let path = tmp("shared_table_idempotence");
    let device_id = DeviceId::generate().unwrap();
    let workspace = TestWorkspace::create(
        &path,
        Arc::new(ThreadExecutor),
        WorkspaceConfig {
            workspace_id: WorkspaceId::from("shared-table-idempotence-workspace"),
            device_id,
            graceful_shutdown_max_duration: Duration::from_millis(100),
        },
    )
    .unwrap();

    workspace
        .create_shared_table("rows", TableConfig::default())
        .unwrap();
    let first = workspace.shared_frontier().applied_through(&device_id);
    workspace
        .create_shared_table("rows", TableConfig::default())
        .unwrap();

    assert_eq!(
        workspace.shared_frontier().applied_through(&device_id),
        first
    );
}

#[test]
fn stable_snapshot_allows_shared_root_tombstone_compaction() {
    let path = tmp("snapshot_compaction");
    let device_id = DeviceId::generate().unwrap();
    let workspace = TestWorkspace::create(
        &path,
        Arc::new(ThreadExecutor),
        WorkspaceConfig {
            workspace_id: WorkspaceId::from("snapshot-compaction-workspace"),
            device_id,
            graceful_shutdown_max_duration: Duration::from_millis(100),
        },
    )
    .unwrap();
    workspace
        .create_shared_table("rows", TableConfig::default())
        .unwrap();
    let key = PrimaryKey::String("deleted".into());
    workspace
        .mutate(
            "rows",
            key.clone(),
            Path::new(),
            Op::Replace {
                value: Value::String("value".into()),
            },
        )
        .unwrap();
    workspace
        .mutate("rows", key.clone(), Path::new(), Op::Delete)
        .unwrap();
    workspace.checkpoint_local_frontier().unwrap();
    assert!(workspace.compact_shared().unwrap().is_some());
    let table = workspace.table("rows", None).unwrap().get().unwrap();
    assert!(Backend::get(&*table.read(), &key).is_none());
    let snapshot = workspace.export_snapshot().unwrap();
    assert!(!snapshot.bytes.is_empty());
}
