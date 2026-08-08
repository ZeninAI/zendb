//! End-to-end workspace table, state, durability, and deletion lifecycle.

mod common;

use std::sync::Arc;

use bincode::{Decode, Encode};
use zendb_it::TestPeerIdentity;
use zendb_storage::{ReadBackend, StateConfig, TableConfig, WriteBackend};
use zendb_types::{Op, Path, PrimaryKey, Value};
use zendb_workspace::{Workspace, WorkspaceConfig};

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
struct Greeting {
    text: String,
}

#[test]
fn workspace_data_survives_reopen_and_lifecycle_deletions() {
    common::init_logging();
    let temp = tempfile::tempdir().expect("failed to create workspace directory");
    let root = temp.path();
    let identity = Arc::new(TestPeerIdentity::generate("test-installation"));

    let workspace = Workspace::create(root, identity.clone(), WorkspaceConfig::default())
        .expect("failed to create workspace");
    let workspace_id = workspace.workspace_id();
    workspace
        .tables()
        .upsert("users", TableConfig::default())
        .expect("failed to create users table");
    let users = workspace
        .tables()
        .get("users")
        .expect("failed to open users table");
    users
        .insert(
            PrimaryKey::String("alice".to_owned()),
            Path::new(),
            Op::Upsert {
                value: Value::String("Alice".to_owned()),
            },
        )
        .expect("failed to insert user");

    workspace
        .states()
        .upsert("greetings", StateConfig::default())
        .expect("failed to create greetings state");
    let greetings = workspace
        .states()
        .get::<String, Greeting>("greetings")
        .expect("failed to open greetings state");
    greetings
        .write()
        .expect("application state is writable")
        .put(
            "hello".to_owned(),
            Greeting {
                text: "Hello, world!".to_owned(),
            },
        )
        .expect("failed to write greeting");

    workspace
        .persist(zendb_workspace::Barrier::Sync)
        .expect("failed to sync workspace");
    drop(users);
    drop(greetings);
    drop(workspace);

    let workspace = Workspace::open(root, identity.clone(), WorkspaceConfig::default())
        .expect("failed to reopen workspace");
    assert_eq!(workspace.workspace_id(), workspace_id);
    assert!(workspace.tables().list().contains(&"users".to_owned()));
    assert!(workspace.states().list().contains(&"greetings".to_owned()));

    let users = workspace
        .tables()
        .get("users")
        .expect("durable users table is present");
    assert_eq!(
        users
            .read()
            .get(&PrimaryKey::String("alice".to_owned()))
            .and_then(|cell| cell.value.clone()),
        Some(Value::String("Alice".to_owned()))
    );
    let greetings = workspace
        .states()
        .get::<String, Greeting>("greetings")
        .expect("durable greetings state is present");
    assert_eq!(
        greetings
            .read()
            .get(&"hello".to_owned())
            .expect("durable greeting is present")
            .into_owned(),
        Greeting {
            text: "Hello, world!".to_owned(),
        }
    );

    drop(users);
    drop(greetings);
    workspace
        .tables()
        .delete("users")
        .expect("failed to delete users table");
    workspace
        .states()
        .delete("greetings")
        .expect("failed to delete greetings state");
    workspace
        .persist(zendb_workspace::Barrier::Sync)
        .expect("failed to sync deletions");
    assert!(!root.join("tables").join("users").exists());
    assert!(!root.join("states").join("greetings").exists());
    drop(workspace);

    let workspace = Workspace::open(root, identity, WorkspaceConfig::default())
        .expect("failed to reopen workspace after deletion");
    assert!(!workspace.tables().contains("users"));
    assert!(!workspace.states().contains("greetings"));
}
