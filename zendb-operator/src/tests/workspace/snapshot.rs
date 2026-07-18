use zendb_storage::backend::_traits::ReadBackend;
use zendb_types::{DeviceId, Op, Path, PrimaryKey, Value, WorkspaceId};

use crate::{TableConfig, WorkspaceConfig};

use super::support::tmp;
use crate::Workspace;

type CoreWorkspace = Workspace;

#[test]
fn creating_an_existing_shared_table_is_idempotent() {
    let path = tmp("shared_table_idempotence");
    let device_id = DeviceId::generate().unwrap();
    let workspace = CoreWorkspace::create(
        &path,
        WorkspaceConfig {
            workspace_id: WorkspaceId::generate().unwrap(),
            device_id,
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
    workspace.table("rows").create().unwrap();

    assert_eq!(
        workspace.shared_frontier().applied_through(&device_id),
        first
    );
    assert!(workspace.is_shared_table("rows").unwrap());
}

#[test]
fn stable_snapshot_retains_tombstones_until_pruning_context_exists() {
    let path = tmp("snapshot_compaction");
    let device_id = DeviceId::generate().unwrap();
    let workspace = CoreWorkspace::create(
        &path,
        WorkspaceConfig {
            workspace_id: WorkspaceId::generate().unwrap(),
            device_id,
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
    let table = workspace.table("rows").open().unwrap().get().unwrap();
    assert!(ReadBackend::get(&*table.read(), &key).is_some_and(|cell| cell.is_tombstone()));
    let snapshot = workspace.export_snapshot().unwrap();
    assert!(!snapshot.bytes.is_empty());
}
