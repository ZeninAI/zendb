use std::{sync::Arc, time::Duration};

use zendb_types::{DeviceId, WorkspaceId};

use crate::WorkspaceConfig;

use super::support::{tmp, TestWorkspace, ThreadExecutor};

#[test]
fn staged_key_promotes_only_after_a_stable_frontier_checkpoint() {
    let path = tmp("key_rotation");
    let device_id = DeviceId::generate().unwrap();
    let workspace = TestWorkspace::create(
        &path,
        Arc::new(ThreadExecutor),
        WorkspaceConfig {
            workspace_id: WorkspaceId::from("key-rotation-workspace"),
            device_id,
            graceful_shutdown_max_duration: Duration::from_millis(100),
        },
    )
    .unwrap();
    let old_key = workspace.device_profile().primary_public_key();
    let staging = workspace.stage_local_key_rotation().unwrap();
    assert_eq!(
        workspace
            .promote_local_key_rotation(staging)
            .unwrap_err()
            .kind(),
        std::io::ErrorKind::WouldBlock
    );
    workspace.checkpoint_local_frontier().unwrap();
    let promotion = workspace.promote_local_key_rotation(staging).unwrap();
    let current = workspace.device(device_id).unwrap().unwrap().key_ring;
    assert_eq!(current.primary_from_seq, promotion.origin_seq);
    assert_eq!(current.secondary_key, Some(old_key));
    assert_eq!(
        workspace.device_profile().primary_public_key(),
        current.primary_key
    );
}
