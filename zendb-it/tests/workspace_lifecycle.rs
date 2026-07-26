//! End-to-end test exercising workspace create → table/state ops → cleanup.
//! Validates the iter-0003 identity boundary (LocalPeerIdentity passed in).

use std::fs;

use bincode::{Decode, Encode};
use zendb_storage::{ReadBackend, StateConfig, TableConfig, WriteBackend};
use zendb_types::{LocalPeerIdentity, Op, Path, PrimaryKey, Value};
use zendb_workspace::{Workspace, WorkspaceConfig};

/// A test value type used for state storage.
#[derive(Debug, Clone, PartialEq, Encode, Decode)]
struct Greeting {
    text: String,
}

#[test]
fn workspace_create_tables_states_and_cleanup() {
    // Scoped so the workspace is fully dropped before temp dir cleanup.
    let root = {
        let temp = tempfile::tempdir().expect("failed to create temp dir");
        let root = temp.path().to_path_buf();

        // ---- create workspace ----
        let peer = std::sync::Arc::new(LocalPeerIdentity::generate());
        let ws = Workspace::create(&root, peer.clone(), WorkspaceConfig::default())
            .expect("failed to create workspace");
        assert!(!ws.id().to_string().is_empty());

        // ---- create a table and insert rows ----
        ws.tables()
            .upsert("users", TableConfig::default())
            .expect("failed to create table");
        let table = ws.tables().get("users").expect("failed to open table");

        table
            .insert(
                PrimaryKey::String("alice".into()),
                Path::new(),
                Op::Upsert {
                    value: Value::String("Alice".into()),
                },
            )
            .expect("failed to insert alice");

        table
            .insert(
                PrimaryKey::String("bob".into()),
                Path::new(),
                Op::Upsert {
                    value: Value::String("Bob".into()),
                },
            )
            .expect("failed to insert bob");

        // verify reads
        let guard = table.read();
        let alice_cell = guard
            .get(&PrimaryKey::String("alice".into()))
            .expect("alice not found");
        assert_eq!(alice_cell.value, Some(Value::String("Alice".into())));
        drop(guard);

        // verify table listing
        let tables = ws.tables().list();
        assert!(tables.iter().any(|name| name == "users"));

        // ---- initialize a state and write to it ----
        ws.states()
            .upsert("greetings", StateConfig::default())
            .expect("failed to create state");
        let state = ws
            .states()
            .get::<String, Greeting>("greetings")
            .expect("failed to open state");

        state
            .write()
            .expect("system state write should be refused only for system states")
            .put(
                "hello".to_owned(),
                Greeting {
                    text: "Hello, world!".into(),
                },
            )
            .expect("failed to write greeting");

        // verify state read
        let found = state
            .read()
            .get(&"hello".to_owned())
            .expect("greeting not found")
            .into_owned();
        assert_eq!(found.text, "Hello, world!");

        // verify state listing
        let states = ws.states().list();
        assert!(states.contains(&"greetings".to_owned()));

        // ---- explicit flush before drop ----
        ws.flush().expect("flush failed");

        // ---- verify the workspace left files on disk ----
        assert!(root.exists());
        let entries: Vec<_> = fs::read_dir(&root)
            .expect("failed to read root dir")
            .filter_map(|e| e.ok())
            .collect();
        assert!(!entries.is_empty(), "workspace root should contain files");

        // Drop workspace and state before temp dir is cleaned up.
        drop(ws);
        drop(state);

        root
    };
    // `temp` is now dropped (directory deleted).

    // ---- cleanup verified: temp dir is gone ----
    assert!(!root.exists(), "temp dir should be gone after cleanup");
}
