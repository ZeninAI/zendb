use std::{
    collections::BTreeSet,
    sync::Arc,
    thread,
    time::{Duration, Instant},
};

use zendb_storage::core::traits::Backend;
use zendb_types::{
    ContiguousFrontier, DeviceId, DeviceRecord, Op, Path, PrimaryKey, Value, WorkspaceId,
    WorkspaceRole,
};

use crate::{ClusterConfig, TableConfig, WorkspaceConfig};

use super::support::{tmp, TestWorkspace, ThreadExecutor};

fn config(workspace_id: &WorkspaceId, device_id: DeviceId) -> WorkspaceConfig {
    WorkspaceConfig {
        workspace_id: workspace_id.clone(),
        device_id,
        graceful_shutdown_max_duration: Duration::from_millis(100),
    }
}

#[test]
fn direct_bootstrap_requires_pre_admission_and_expected_peer_key() {
    let owner_path = tmp("direct_owner");
    let joiner_path = tmp("direct_joiner");
    let workspace_id = WorkspaceId::from("direct-bootstrap-workspace");
    let owner_id = DeviceId::generate().unwrap();
    let joiner_id = DeviceId::generate().unwrap();
    let owner = TestWorkspace::create(
        &owner_path,
        Arc::new(ThreadExecutor),
        config(&workspace_id, owner_id),
    )
    .unwrap();
    let joiner = TestWorkspace::create_joining(
        &joiner_path,
        Arc::new(ThreadExecutor),
        config(&workspace_id, joiner_id),
        "laptop".into(),
        BTreeSet::new(),
    )
    .unwrap();
    owner
        .admit_device(
            joiner_id,
            DeviceRecord {
                name: "laptop".into(),
                key_ring: joiner.device_profile().initial_key_ring(),
                roles: BTreeSet::new(),
                capabilities: BTreeSet::new(),
                replication_frontier: ContiguousFrontier::default(),
            },
        )
        .unwrap();

    let listener = TestWorkspace::bind_tcp("127.0.0.1:0".parse().unwrap()).unwrap();
    let address = listener.local_addr().unwrap();
    let serving_owner = Arc::clone(&owner);
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        serving_owner.serve_tcp(stream).unwrap();
    });
    joiner
        .bootstrap_direct(
            address,
            owner.device_profile().primary_public_key(),
            "laptop".into(),
            BTreeSet::new(),
        )
        .unwrap();
    server.join().unwrap();
    assert!(joiner.device(joiner_id).unwrap().is_some());
}

#[test]
fn cluster_runtime_reconnects_multiple_peers_and_delivers_departure() {
    let owner_path = tmp("cluster_owner");
    let joiner_path = tmp("cluster_joiner");
    let workspace_id = WorkspaceId::from("cluster-runtime-workspace");
    let owner_id = DeviceId::generate().unwrap();
    let joiner_id = DeviceId::generate().unwrap();
    let owner = TestWorkspace::create(
        &owner_path,
        Arc::new(ThreadExecutor),
        config(&workspace_id, owner_id),
    )
    .unwrap();
    let joiner = TestWorkspace::create_joining(
        &joiner_path,
        Arc::new(ThreadExecutor),
        config(&workspace_id, joiner_id),
        "cluster-peer".into(),
        BTreeSet::new(),
    )
    .unwrap();
    owner
        .admit_device(
            joiner_id,
            DeviceRecord {
                name: "cluster-peer".into(),
                key_ring: joiner.device_profile().initial_key_ring(),
                roles: BTreeSet::new(),
                capabilities: BTreeSet::new(),
                replication_frontier: ContiguousFrontier::default(),
            },
        )
        .unwrap();
    let listener = TestWorkspace::bind_tcp("127.0.0.1:0".parse().unwrap()).unwrap();
    let bootstrap_address = listener.local_addr().unwrap();
    let serving_owner = Arc::clone(&owner);
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        serving_owner.serve_tcp(stream).unwrap();
    });
    joiner
        .bootstrap_direct(
            bootstrap_address,
            owner.device_profile().primary_public_key(),
            "cluster-peer".into(),
            BTreeSet::new(),
        )
        .unwrap();
    server.join().unwrap();

    let mut cluster_config = ClusterConfig::default();
    cluster_config.listen_address = "127.0.0.1:0".parse().unwrap();
    cluster_config.sync_interval = Duration::from_millis(100);
    let owner_runtime = owner.start_cluster(cluster_config.clone()).unwrap();
    let joiner_runtime = joiner.start_cluster(cluster_config).unwrap();
    joiner_runtime.add_peer(owner_runtime.local_address());

    owner
        .create_shared_table("cluster-data", TableConfig::default())
        .unwrap();
    owner
        .mutate(
            "cluster-data",
            PrimaryKey::String("row".into()),
            Path::new(),
            Op::Replace {
                value: Value::String("replicated".into()),
            },
        )
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let converged = joiner
            .contains_table("cluster-data")
            .then(|| joiner.table("cluster-data", None).ok())
            .flatten()
            .and_then(|handle| handle.get().ok())
            .and_then(|table| {
                Backend::get(&*table.read(), &PrimaryKey::String("row".into()))
                    .map(|cell| cell.into_owned())
            })
            .is_some_and(|cell| cell.value == Some(Value::String("replicated".into())));
        if converged {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "cluster peers did not converge: joiner={:?}, owner={:?}",
            joiner_runtime.errors(),
            owner_runtime.errors()
        );
        thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(
        joiner.presence_status(owner_id),
        zendb_transport::PresenceStatus::Direct
    );

    owner_runtime.add_peer(joiner_runtime.local_address());
    owner_runtime.shutdown();
    let deadline = Instant::now() + Duration::from_secs(3);
    while joiner.presence_status(owner_id) != zendb_transport::PresenceStatus::Departed {
        assert!(Instant::now() < deadline, "departure was not observed");
        thread::sleep(Duration::from_millis(20));
    }
    joiner_runtime.shutdown();
}

#[test]
fn ticket_bootstrap_installs_shared_snapshot_and_continues_with_anti_entropy() {
    let owner_path = tmp("ticket_owner");
    let joiner_path = tmp("ticket_joiner");
    let workspace_id = WorkspaceId::from("ticket-bootstrap-workspace");
    let owner_id = DeviceId::generate().unwrap();
    let joiner_id = DeviceId::generate().unwrap();
    let owner = TestWorkspace::create(
        &owner_path,
        Arc::new(ThreadExecutor),
        config(&workspace_id, owner_id),
    )
    .unwrap();
    owner
        .create_shared_table("documents", TableConfig::default())
        .unwrap();
    owner
        .mutate(
            "documents",
            PrimaryKey::String("one".into()),
            Path::new(),
            Op::Replace {
                value: Value::String("before-bootstrap".into()),
            },
        )
        .unwrap();
    let presentation = owner
        .create_enrollment_ticket(Duration::from_secs(60), Vec::new())
        .unwrap();

    let joiner = TestWorkspace::create_joining(
        &joiner_path,
        Arc::new(ThreadExecutor),
        config(&workspace_id, joiner_id),
        "phone".into(),
        BTreeSet::new(),
    )
    .unwrap();
    let listener = TestWorkspace::bind_tcp("127.0.0.1:0".parse().unwrap()).unwrap();
    let address = listener.local_addr().unwrap();
    let serving_owner = Arc::clone(&owner);
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        serving_owner.serve_tcp(stream).unwrap();
    });
    joiner
        .bootstrap_with_ticket(address, &presentation, "phone".into(), BTreeSet::new())
        .unwrap();
    server.join().unwrap();

    let admitted = owner.device(joiner_id).unwrap().unwrap();
    assert!(admitted.roles.is_empty());
    let denied = joiner
        .create_shared_table("reader-cannot-create", TableConfig::default())
        .err()
        .unwrap();
    assert_eq!(denied.kind(), std::io::ErrorKind::PermissionDenied);
    let mut local_config = TableConfig::default();
    local_config.sync = false;
    joiner
        .table("reader-local-data", Some(local_config))
        .unwrap();
    let table = joiner.table("documents", None).unwrap().get().unwrap();
    assert_eq!(
        Backend::get(&*table.read(), &PrimaryKey::String("one".into()))
            .unwrap()
            .value,
        Some(Value::String("before-bootstrap".into()))
    );

    owner
        .set_device_role(joiner_id, WorkspaceRole::Contributor, true)
        .unwrap();
    let listener = TestWorkspace::bind_tcp("127.0.0.1:0".parse().unwrap()).unwrap();
    let address = listener.local_addr().unwrap();
    let serving_owner = Arc::clone(&owner);
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        serving_owner.serve_tcp(stream).unwrap();
    });
    let report = joiner.sync_tcp(address).unwrap();
    server.join().unwrap();
    assert!(report.events_received >= 1);
    assert!(joiner
        .device(joiner_id)
        .unwrap()
        .unwrap()
        .roles
        .contains(&WorkspaceRole::Contributor));
}
