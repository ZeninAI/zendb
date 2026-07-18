use zendb_types::{DeviceId, WorkspaceId};

use crate::WorkspaceConfig;

use super::support::tmp;
use crate::Workspace;

type CoreWorkspace = Workspace;

#[test]
fn staged_key_promotes_only_after_a_stable_frontier_checkpoint() {
    let path = tmp("key_rotation");
    let device_id = DeviceId::generate().unwrap();
    let workspace = CoreWorkspace::create(
        &path,
        WorkspaceConfig {
            workspace_id: WorkspaceId::generate().unwrap(),
            device_id,
        },
    )
    .unwrap();
    let old_key = workspace
        .device(device_id)
        .unwrap()
        .unwrap()
        .key_ring
        .primary_key;
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
}
