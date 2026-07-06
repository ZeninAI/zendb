use super::support::*;
use crate::operator::prelude::{MerkleTreeConfig, MerkleTreeFacet, MerkleTreeOperator};
use crate::{OperatorPhase, Subscription, TableConfig};
use std::io;
use std::sync::{atomic::Ordering, Arc};
use std::time::Duration;
use zendb_storage::core::traits::Backend;
use zendb_types::{device_id, Event, Hlc, Op, Path as ValuePath, PrimaryKey, Value};

#[test]
fn direct_table_writes_drive_operators() {
    let path = tmp("direct");
    let (tracker, count) = new_tracker("direct");
    let db = TestDatabase::create(&path, Arc::new(ThreadExecutor), database_config()).unwrap();
    let table = db.table("users", Some(TableConfig::default())).unwrap();
    db.dispatch_operator::<CountingOperator>(
        "counter",
        counting_config(tracker, false),
        counting_runtime_config(Subscription::pattern("users")),
    )
    .unwrap();

    table
        .get()
        .unwrap()
        .write()
        .insert_event(event("users", 1, 100))
        .unwrap();
    wait_until(|| count.load(Ordering::Relaxed) == 1);
    assert_eq!(
        table
            .get()
            .unwrap()
            .read()
            .get(&PrimaryKey::String("u1".into()))
            .unwrap()
            .value,
        Some(Value::Int(1))
    );
}

#[test]
fn merkle_tree_operator_maintains_table_root() {
    let path = tmp("merkle");
    let db = TestDatabase::create(&path, Arc::new(ThreadExecutor), database_config()).unwrap();
    let table = db.table("users", Some(TableConfig::default())).unwrap();
    let config = MerkleTreeConfig {
        state: "operator/prelude/merkle-tree-test".to_owned(),
        leaf_bits: 8,
    };
    db.dispatch_operator::<MerkleTreeOperator>(
        "merkle",
        config.clone(),
        runtime_config(Subscription::pattern("users")),
    )
    .unwrap();

    wait_until(|| db.facet::<MerkleTreeFacet>("merkle").is_ok());
    let facet = db.facet::<MerkleTreeFacet>("merkle").unwrap();

    wait_until(|| facet.root("users").unwrap().is_some());
    let empty = facet.root("users").unwrap().unwrap();
    assert_eq!(empty.entries, 0);

    table
        .get()
        .unwrap()
        .write()
        .insert_event(event("users", 1, 100))
        .unwrap();
    wait_until(|| {
        facet
            .root("users")
            .unwrap()
            .is_some_and(|root| root.entries == 1 && root.hash != empty.hash)
    });
    let inserted = facet.root("users").unwrap().unwrap();

    table
        .get()
        .unwrap()
        .write()
        .insert_event(Event {
            table_id: "users".into(),
            primary_key: PrimaryKey::String("u1".into()),
            path: ValuePath::new(),
            op: Op::Delete,
            hlc: Hlc::with_device_id(200, 0, device_id()).unwrap(),
            sync: false,
            signature: Vec::new(),
        })
        .unwrap();

    wait_until(|| {
        facet
            .root("users")
            .unwrap()
            .is_some_and(|root| root.entries == 1 && root.hash != inserted.hash)
    });
}

#[test]
fn failed_process_transitions_operator_to_failed() {
    let path = tmp("failed");
    let (attempts_key, attempts) = new_tracker("retry_attempts");
    let db = TestDatabase::create(&path, Arc::new(ThreadExecutor), database_config()).unwrap();
    let table = db.table("users", Some(TableConfig::default())).unwrap();
    db.dispatch_operator::<FailingOperator>(
        "retry",
        retry_config(attempts_key),
        retry_runtime_config(),
    )
    .unwrap();

    table
        .get()
        .unwrap()
        .write()
        .insert_event(event("users", 1, 100))
        .unwrap();
    table
        .get()
        .unwrap()
        .write()
        .insert_event(event("users", 2, 110))
        .unwrap();

    wait_until(|| {
        db.operator_phase("retry")
            == Some(OperatorPhase::Failed {
                error: "expected failure".to_owned(),
            })
    });
    wait_until(|| !db.is_operator_open("retry"));
    assert_eq!(attempts.load(Ordering::Relaxed), 1);
}

#[test]
fn active_operators_respawn_after_database_reopen() {
    let path = tmp("operator_reopen");
    let (tracker, count) = new_tracker("operator_reopen");
    {
        let db = TestDatabase::create(&path, Arc::new(ThreadExecutor), database_config()).unwrap();
        let users = db.table("users", Some(TableConfig::default())).unwrap();
        db.dispatch_operator::<CountingOperator>(
            "counter",
            counting_config(tracker.clone(), false),
            counting_runtime_config(Subscription::pattern("users")),
        )
        .unwrap();

        users
            .get()
            .unwrap()
            .write()
            .insert_event(event("users", 1, 100))
            .unwrap();
        wait_until(|| count.load(Ordering::Relaxed) == 1);
        assert_eq!(db.operator_phase("counter"), Some(OperatorPhase::Active));
    }

    let db = TestDatabase::open(&path, Arc::new(ThreadExecutor), database_config()).unwrap();
    let users = db.table("users", None).unwrap();
    assert_eq!(db.operator_phase("counter"), Some(OperatorPhase::Active));

    users
        .get()
        .unwrap()
        .write()
        .insert_event(event("users", 2, 110))
        .unwrap();
    wait_until(|| count.load(Ordering::Relaxed) == 2);
}

#[test]
fn cancelled_operator_is_permanent_and_not_reopened() {
    let path = tmp("operator_cancel");
    let (tracker, log) = new_lifecycle_log("operator_cancel");
    {
        let db = TestDatabase::create(&path, Arc::new(ThreadExecutor), database_config()).unwrap();
        db.table("users", Some(TableConfig::default())).unwrap();
        db.dispatch_operator::<ShutdownLifecycleOperator>(
            "lifecycle",
            shutdown_lifecycle_config(tracker.clone()),
            shutdown_lifecycle_runtime_config(),
        )
        .unwrap();

        wait_until(|| log.lock().contains(&"opened:users".to_owned()));
        db.cancel_operator("lifecycle").unwrap();
        wait_until(|| db.operator_phase("lifecycle") == Some(OperatorPhase::Cancelled));
        wait_until(|| !db.is_operator_open("lifecycle"));
    }

    {
        let log = log.lock();
        assert!(log.contains(&"closed:users".to_owned()), "{log:?}");
        assert!(log.contains(&"teardown".to_owned()), "{log:?}");
    }

    let db = TestDatabase::open(&path, Arc::new(ThreadExecutor), database_config()).unwrap();
    db.table("users", None).unwrap();
    std::thread::sleep(Duration::from_millis(20));
    assert_eq!(
        db.operator_phase("lifecycle"),
        Some(OperatorPhase::Cancelled)
    );
    assert!(!db.is_operator_open("lifecycle"));
    assert_eq!(
        log.lock()
            .iter()
            .filter(|entry| entry.as_str() == "opened:users")
            .count(),
        1
    );
}

#[test]
fn operators_can_spawn_user_and_prelude_operators_from_context() {
    let path = tmp("operator_spawn_from_context");
    let (tracker, child_count) = new_tracker("operator_spawn_from_context");
    let db = TestDatabase::create(&path, Arc::new(ThreadExecutor), database_config()).unwrap();
    let table = db.table("users", Some(TableConfig::default())).unwrap();
    db.dispatch_operator::<SpawnerOperator>(
        "spawner",
        spawner_config(tracker),
        spawner_runtime_config(),
    )
    .unwrap();

    wait_until(|| {
        db.contains_operator("spawned-counter") && db.contains_operator("spawned-merkle")
    });

    table
        .get()
        .unwrap()
        .write()
        .insert_event(event("users", 1, 100))
        .unwrap();

    wait_until(|| child_count.load(Ordering::Relaxed) == 1);
}

#[test]
fn operator_receives_opened_callbacks_for_initial_inputs() {
    let path = tmp("input_lifecycle_initial");
    let (tracker, opened, closed) = new_input_tracker("input_lifecycle_initial");
    let db = TestDatabase::create(&path, Arc::new(ThreadExecutor), database_config()).unwrap();
    db.table("users", Some(TableConfig::default())).unwrap();
    db.dispatch_operator::<InputLifecycleOperator>(
        "inputs",
        input_lifecycle_config(tracker),
        input_lifecycle_runtime_config(Subscription::pattern("users")),
    )
    .unwrap();

    wait_until(|| opened.lock().contains(&"users".to_owned()));
    assert!(closed.lock().is_empty());
}

#[test]
fn operator_receives_opened_callbacks_for_later_inputs() {
    let path = tmp("input_lifecycle_later");
    let (tracker, opened, closed) = new_input_tracker("input_lifecycle_later");
    let db = TestDatabase::create(&path, Arc::new(ThreadExecutor), database_config()).unwrap();
    db.table("users", Some(TableConfig::default())).unwrap();
    db.dispatch_operator::<InputLifecycleOperator>(
        "inputs",
        input_lifecycle_config(tracker),
        input_lifecycle_runtime_config(Subscription::pattern("*")),
    )
    .unwrap();

    wait_until(|| opened.lock().contains(&"users".to_owned()));
    db.table("orders", Some(TableConfig::default())).unwrap();

    wait_until(|| {
        let opened = opened.lock();
        opened.contains(&"users".to_owned()) && opened.contains(&"orders".to_owned())
    });
    assert!(closed.lock().is_empty());
}

#[test]
fn close_table_evicts_cache_notifies_and_allows_reopen() {
    let path = tmp("close_table");
    let (tracker, opened, closed) = new_input_tracker("close_table");
    let db = TestDatabase::create(&path, Arc::new(ThreadExecutor), database_config()).unwrap();
    let users = db.table("users", Some(TableConfig::default())).unwrap();
    db.dispatch_operator::<InputLifecycleOperator>(
        "inputs",
        input_lifecycle_config(tracker),
        input_lifecycle_runtime_config(Subscription::pattern("users")),
    )
    .unwrap();

    wait_until(|| opened.lock().contains(&"users".to_owned()));
    assert!(db.is_table_open("users"));
    assert!(db.is_operator_open("inputs"));

    assert!(db.close_table("users"));

    wait_until(|| closed.lock().contains(&"users".to_owned()));
    wait_until(|| !db.is_operator_open("inputs"));
    assert!(!db.is_table_open("users"));
    assert!(matches!(
        users.get(),
        Err(error) if error.kind() == io::ErrorKind::NotConnected
    ));

    db.table("users", None).unwrap();

    wait_until(|| {
        db.is_operator_open("inputs")
            && opened
                .lock()
                .iter()
                .filter(|table| table.as_str() == "users")
                .count()
                == 2
    });
}

#[test]
fn retired_operator_deletes_consumers_from_unopened_tables() {
    let path = tmp("retire_consumers");
    {
        let db = TestDatabase::create(&path, Arc::new(ThreadExecutor), database_config()).unwrap();
        let orders = db.table("orders", Some(TableConfig::default())).unwrap();
        let orders_table = orders.get().unwrap();

        let stale_consumer = orders_table.read().consumer("counter").unwrap();
        drop(stale_consumer);

        orders_table
            .write()
            .insert_event(event("orders", 1, 100))
            .unwrap();
    }

    let (tracker, _) = new_tracker("retire_consumers");
    let db = TestDatabase::open(&path, Arc::new(ThreadExecutor), database_config()).unwrap();
    db.dispatch_operator::<CountingOperator>(
        "counter",
        counting_config(tracker, true),
        counting_runtime_config(Subscription::pattern("*")),
    )
    .unwrap();

    let users = db.table("users", Some(TableConfig::default())).unwrap();
    users
        .get()
        .unwrap()
        .write()
        .insert_event(event("users", 2, 110))
        .unwrap();

    wait_until(|| {
        db.operator_phase("counter") == Some(OperatorPhase::Finished)
            && !db.is_operator_open("counter")
    });

    let orders = db.table("orders", None).unwrap();
    let orders_table = orders.get().unwrap();
    wait_until(|| {
        let mut consumer = orders_table.read().consumer("counter").unwrap();
        consumer.next().is_none()
    });
}

#[test]
fn shutdown_runs_input_closed_before_teardown() {
    let path = tmp("shutdown_lifecycle");
    let (tracker, log) = new_lifecycle_log("shutdown_lifecycle");
    let db = TestDatabase::create(&path, Arc::new(ThreadExecutor), database_config()).unwrap();
    let users = db.table("users", Some(TableConfig::default())).unwrap();
    db.dispatch_operator::<ShutdownLifecycleOperator>(
        "shutdown-lifecycle",
        shutdown_lifecycle_config(tracker),
        shutdown_lifecycle_runtime_config(),
    )
    .unwrap();

    wait_until(|| log.lock().contains(&"opened:users".to_owned()));

    users
        .get()
        .unwrap()
        .write()
        .insert_event(event("users", 1, 100))
        .unwrap();

    wait_until(|| db.operator_phase("shutdown-lifecycle") == Some(OperatorPhase::Finished));

    let log = log.lock().clone();
    let closed = log
        .iter()
        .position(|entry| entry == "closed:users")
        .expect("closed event is recorded");
    let teardown = log
        .iter()
        .position(|entry| entry == "teardown")
        .expect("teardown event is recorded");

    assert!(
        closed < teardown,
        "on_input_closed must run before teardown: {log:?}"
    );
}
