use super::support::*;
use crate::{DatabaseConfig, Subscription, TableConfig};
use std::sync::Arc;
use zendb_storage::core::traits::Backend;

#[test]
fn states_preserve_first_opened_key_and_value_types() {
    let path = tmp("typed_state");
    let (tracker, _) = new_tracker("typed_state");
    let db =
        TestDatabase::create(&path, Arc::new(ThreadExecutor), DatabaseConfig::default()).unwrap();
    db.table("users", Some(TableConfig::default())).unwrap();
    db.dispatch_operator::<CountingOperator>(
        "counter",
        counting_config(tracker, false),
        counting_runtime_config(Subscription::pattern("users")),
    )
    .unwrap();

    wait_until(|| db.state::<Vec<u8>, Vec<u8>>("index", None).is_ok());
    let index = db.state::<Vec<u8>, Vec<u8>>("index", None).unwrap();
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
    let (tracker, _) = new_tracker("typed_state_reopen");
    {
        let db = TestDatabase::create(&path, Arc::new(ThreadExecutor), DatabaseConfig::default())
            .unwrap();
        db.table("users", Some(TableConfig::default())).unwrap();
        db.dispatch_operator::<CountingOperator>(
            "counter",
            counting_config(tracker.clone(), false),
            counting_runtime_config(Subscription::pattern("users")),
        )
        .unwrap();
        wait_until(|| db.state::<Vec<u8>, Vec<u8>>("index", None).is_ok());
        db.state::<Vec<u8>, Vec<u8>>("index", None)
            .unwrap()
            .get()
            .unwrap()
            .write()
            .put(b"users".to_vec(), 42_u64.to_le_bytes().to_vec())
            .unwrap();
    }

    let db =
        TestDatabase::open(&path, Arc::new(ThreadExecutor), DatabaseConfig::default()).unwrap();
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
