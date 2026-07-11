//! Tests for the Rhai scripting operator.

use std::sync::Arc;
use std::time::Duration;

use crate::operator::prelude::{RhaiOperator, RhaiOperatorConfig};
use crate::tests::workspace::support::{
    string_event, tmp, wait_until_timeout, workspace_config, ThreadExecutor,
};
use crate::workspace::Workspace;
use crate::{OperatorRuntimeConfig, Subscription, TableConfig};
use zendb_storage::core::traits::Backend;
use zendb_types::PrimaryKey;

// Use empty operator set - prelude operators (FullTextIndex, MerkleTree, Rhai) are included automatically
crate::define_operator_set! {
    mod rhai_test_operators {}
}

type TestWorkspace = Workspace<rhai_test_operators::OperatorInstance>;

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Test that a simple Rhai script compiles and runs on_create successfully.
#[test]
fn rhai_operator_creates_successfully() {
    let path = tmp("rhai_create");
    let db = TestWorkspace::create(&path, Arc::new(ThreadExecutor), workspace_config()).unwrap();

    let script = r#"
        fn on_create() {
            // Simple initialization
            let x = 1 + 2;
        }
    "#;

    let config = RhaiOperatorConfig::new(script);

    let runtime_config = OperatorRuntimeConfig {
        subscriptions: vec![Subscription::pattern("input")],
        poll_size: 128,
    };

    // Should create without error
    db.dispatch_operator::<RhaiOperator>("test-rhai", config, runtime_config)
        .expect("Failed to create Rhai operator");

    // Give it a moment to initialize
    std::thread::sleep(Duration::from_millis(50));
}

/// Test that on_process receives changes and can inspect them.
#[test]
fn rhai_operator_processes_changes() {
    let path = tmp("rhai_process");
    let db = TestWorkspace::create(&path, Arc::new(ThreadExecutor), workspace_config()).unwrap();

    // Create input table
    let input_table = db.table("input", Some(TableConfig::default())).unwrap();

    let script = r#"
        fn on_create() {
            this.count = 0;
        }

        fn on_process(changes) {
            for change in changes {
                if change.is_insert() || change.is_update() {
                    this.count += 1;
                }
            }
        }
    "#;

    let config = RhaiOperatorConfig::new(script);

    let runtime_config = OperatorRuntimeConfig {
        subscriptions: vec![Subscription::pattern("input")],
        poll_size: 128,
    };

    db.dispatch_operator::<RhaiOperator>("counter", config, runtime_config)
        .expect("Failed to create Rhai operator");

    // Insert some events
    {
        let table = input_table.get().unwrap();
        let mut guard = table.write();
        guard
            .insert_event(string_event("input", "key1", "value1", 1))
            .unwrap();
        guard
            .insert_event(string_event("input", "key2", "value2", 2))
            .unwrap();
        guard
            .insert_event(string_event("input", "key3", "value3", 3))
            .unwrap();
    }

    // Give operator time to process
    std::thread::sleep(Duration::from_millis(100));
}

/// Test that a Rhai script can emit events to an output table.
#[test]
fn rhai_operator_emits_to_output_table() {
    let path = tmp("rhai_emit");
    let db = TestWorkspace::create(&path, Arc::new(ThreadExecutor), workspace_config()).unwrap();

    // Create input and output tables
    let input_table = db.table("input", Some(TableConfig::default())).unwrap();
    let output_table = db.table("output", Some(TableConfig::default())).unwrap();

    let script = r#"
        fn on_process(changes) {
            for change in changes {
                if change.is_insert() {
                    // Forward to output with modified key
                    let key = change.key();
                    db::emit("output", "processed_" + key, "done");
                }
            }
        }
    "#;

    let config = RhaiOperatorConfig::new(script);

    let runtime_config = OperatorRuntimeConfig {
        subscriptions: vec![Subscription::pattern("input")],
        poll_size: 128,
    };

    db.dispatch_operator::<RhaiOperator>("forwarder", config, runtime_config)
        .expect("Failed to create Rhai operator");

    // Insert an event
    {
        let table = input_table.get().unwrap();
        let mut guard = table.write();
        guard
            .insert_event(string_event("input", "item1", "hello", 1))
            .unwrap();
    }

    // Wait for output to appear
    wait_until_timeout(
        || {
            let table = output_table.get().unwrap();
            let guard = table.read();
            guard
                .get(&PrimaryKey::String("processed_item1".into()))
                .is_some()
        },
        Duration::from_secs(2),
    );

    // Verify output
    let table = output_table.get().unwrap();
    let guard = table.read();
    let cell = guard
        .get(&PrimaryKey::String("processed_item1".into()))
        .unwrap();
    assert!(!cell.is_tombstone());
}
