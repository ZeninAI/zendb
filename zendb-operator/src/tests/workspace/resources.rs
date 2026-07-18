use super::support::*;
use crate::StateConfig;
use std::sync::Arc;
use zendb_storage::backend::_traits::WriteBackend;
use zendb_storage::ReadBackend;

#[test]
fn states_preserve_first_opened_key_and_value_types() {
    let path = tmp("typed_state");
    let db = TestWorkspace::create(&path, Arc::new(ThreadExecutor), workspace_config()).unwrap();

    assert!(!db.contains_state("index"));
    let index = db
        .state::<Vec<u8>, Vec<u8>>("index", Some(StateConfig::default()))
        .unwrap();
    assert!(db.contains_state("index"));
    assert!(db.is_state_open("index"));

    index
        .get()
        .unwrap()
        .write()
        .put(b"users".to_vec(), 42_u64.to_le_bytes().to_vec())
        .unwrap();
    assert_eq!(
        index
            .get()
            .unwrap()
            .read()
            .get(&b"users".to_vec())
            .map(|value| value.into_owned()),
        Some(42_u64.to_le_bytes().to_vec())
    );

    assert!(db.state::<u64, u64>("index", None).is_err());
}

#[test]
fn typed_states_reopen_from_catalog() {
    let path = tmp("typed_state_reopen");
    {
        let db =
            TestWorkspace::create(&path, Arc::new(ThreadExecutor), workspace_config()).unwrap();
        db.state::<Vec<u8>, Vec<u8>>("index", Some(StateConfig::default()))
            .unwrap()
            .get()
            .unwrap()
            .write()
            .put(b"users".to_vec(), 42_u64.to_le_bytes().to_vec())
            .unwrap();
    }

    let db = TestWorkspace::open(&path, Arc::new(ThreadExecutor), workspace_config()).unwrap();
    assert!(db.contains_state("index"));
    assert!(!db.is_state_open("index"));

    assert_eq!(
        db.state::<Vec<u8>, Vec<u8>>("index", None)
            .unwrap()
            .get()
            .unwrap()
            .read()
            .get(&b"users".to_vec())
            .map(|value| value.into_owned()),
        Some(42_u64.to_le_bytes().to_vec())
    );
}
