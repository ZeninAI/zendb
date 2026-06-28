//! Integration tests for the document indexing pipeline.

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use zendb_engine::{Database, DatabaseConfig, OperatorPhase};
use zendb_storage::core::traits::Backend;
use zendb_types::{Event, Op, Path as ValuePath, PrimaryKey};

use crate::executor::ThreadExecutor;
use crate::operators::{
    archiver_config, doc_event, doc_operators::OperatorInstance, hlc, indexer_config, wait_until,
};

type TestDatabase = Database<OperatorInstance>;

// ---------------------------------------------------------------------------
// Temp dir helpers
// ---------------------------------------------------------------------------

fn tmp_dir(name: &str) -> PathBuf {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let n = SEQ.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join("zendb_testing").join(format!(
        "{name}_{}_{}",
        std::process::id(),
        n
    ));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

struct TmpDir(PathBuf);

impl Drop for TmpDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

impl std::ops::Deref for TmpDir {
    type Target = std::path::Path;
    fn deref(&self) -> &std::path::Path {
        &self.0
    }
}

fn tmp(name: &str) -> TmpDir {
    TmpDir(tmp_dir(name))
}

// -----------------------------------------------------------------------
// Full pipeline test
// -----------------------------------------------------------------------

#[test]
fn document_indexing_pipeline() {
    let path = tmp("doc_pipeline");

    // --- Phase 1: create, index, archive ---
    let db =
        TestDatabase::create(&path, Arc::new(ThreadExecutor), DatabaseConfig::default()).unwrap();

    let documents = db
        .table("documents", Some(zendb_engine::TableConfig::default()))
        .unwrap();
    db.operator("indexer", Some(indexer_config())).unwrap();

    documents
        .get()
        .unwrap()
        .write()
        .insert_event(doc_event("d1", "The quick brown fox", 100))
        .unwrap();
    documents
        .get()
        .unwrap()
        .write()
        .insert_event(doc_event("d2", "jumps over the lazy dog", 110))
        .unwrap();
    documents
        .get()
        .unwrap()
        .write()
        .insert_event(doc_event("d3", "The quick brown fox jumps again", 120))
        .unwrap();

    wait_until(
        || {
            db.state::<String, u64>("doc_stats", None)
                .ok()
                .and_then(|s| {
                    s.get().ok().map(|state| {
                        let r = state.read();
                        r.get(&"d1".to_owned()).is_some()
                            && r.get(&"d2".to_owned()).is_some()
                            && r.get(&"d3".to_owned()).is_some()
                    })
                })
                .unwrap_or(false)
        },
        Duration::from_secs(5),
    );

    // Verify the inverted index.
    let index = db.state::<String, HashSet<String>>("index", None).unwrap();
    let index_handle = index.get().unwrap();
    let index_read = index_handle.read();

    let quick_docs: HashSet<String> = index_read
        .get(&"quick".to_owned())
        .map(|v| v.into_owned())
        .unwrap_or_default();
    assert!(quick_docs.contains("d1"));
    assert!(quick_docs.contains("d3"));
    assert!(!quick_docs.contains("d2"));

    let fox_docs: HashSet<String> = index_read
        .get(&"fox".to_owned())
        .map(|v| v.into_owned())
        .unwrap_or_default();
    assert!(fox_docs.contains("d1"));
    assert!(fox_docs.contains("d3"));

    let dog_docs: HashSet<String> = index_read
        .get(&"dog".to_owned())
        .map(|v| v.into_owned())
        .unwrap_or_default();
    assert!(dog_docs.contains("d2"));

    drop(index_read);
    drop(index_handle);

    // Verify doc stats.
    let stats = db.state::<String, u64>("doc_stats", None).unwrap();
    let stats_handle = stats.get().unwrap();
    let stats_read = stats_handle.read();
    assert_eq!(
        stats_read.get(&"d1".to_owned()).map(|v| v.into_owned()),
        Some(4)
    );
    assert_eq!(
        stats_read.get(&"d2".to_owned()).map(|v| v.into_owned()),
        Some(5)
    );
    assert_eq!(
        stats_read.get(&"d3".to_owned()).map(|v| v.into_owned()),
        Some(6)
    );
    drop(stats_read);
    drop(stats_handle);

    // Register the archiver.
    db.operator("archiver", Some(archiver_config(3))).unwrap();

    wait_until(
        || db.operator_phase("archiver") == Some(OperatorPhase::Finished),
        Duration::from_secs(10),
    );

    // Verify reports were written.
    let reports = db.table("reports", None).unwrap();
    let reports_handle = reports.get().unwrap();
    let reports_read = reports_handle.read();
    let report1 = reports_read
        .get(&PrimaryKey::String("report_1".into()))
        .map(|c| c.into_owned().value);
    assert!(report1.is_some(), "report_1 should exist");
    drop(reports_read);
    drop(reports_handle);

    // --- Phase 2: reopen and verify persistence ---
    drop(db);
    drop(documents);
    drop(reports);

    let db =
        TestDatabase::open(&path, Arc::new(ThreadExecutor), DatabaseConfig::default()).unwrap();

    assert_eq!(db.operator_phase("indexer"), Some(OperatorPhase::Active));
    assert_eq!(db.operator_phase("archiver"), Some(OperatorPhase::Finished));

    let index = db.state::<String, HashSet<String>>("index", None).unwrap();
    let index_handle2 = index.get().unwrap();
    let index_read = index_handle2.read();
    let quick_docs: HashSet<String> = index_read
        .get(&"quick".to_owned())
        .map(|v| v.into_owned())
        .unwrap_or_default();
    assert!(!quick_docs.is_empty(), "index should survive reopen");
    drop(index_read);
    drop(index_handle2);

    let stats = db.state::<String, u64>("doc_stats", None).unwrap();
    let stats_handle2 = stats.get().unwrap();
    let stats_read = stats_handle2.read();
    assert_eq!(
        stats_read.get(&"d2".to_owned()).map(|v| v.into_owned()),
        Some(5)
    );
    drop(stats_read);
    drop(stats_handle2);

    let reports = db.table("reports", None).unwrap();
    let reports_handle2 = reports.get().unwrap();
    let reports_read = reports_handle2.read();
    assert!(reports_read
        .get(&PrimaryKey::String("report_3".into()))
        .is_some());
    drop(reports_read);
    drop(reports_handle2);

    // --- Phase 3: verify cleanup ---
    let documents = db.table("documents", None).unwrap();
    let docs_handle = documents.get().unwrap();
    let docs_read = docs_handle.read();
    let mut archiver_consumer = docs_read.consumer("archiver").unwrap();
    assert!(
        archiver_consumer.next().is_none(),
        "archiver consumer should have no pending records after retirement"
    );
    drop(archiver_consumer);
    drop(docs_read);
    drop(docs_handle);

    db.register_timer("archiver", 1, vec![]).unwrap();
    db.cancel_timer("archiver", 1).unwrap();

    // --- Phase 4: incremental indexing after reopen ---
    db.operator("indexer", Some(indexer_config())).unwrap();
    let documents = db.table("documents", None).unwrap();
    documents
        .get()
        .unwrap()
        .write()
        .insert_event(doc_event("d4", "quick fox", 200))
        .unwrap();

    wait_until(
        || {
            db.state::<String, HashSet<String>>("index", None)
                .ok()
                .and_then(|s| {
                    s.get().ok().map(|state| {
                        state
                            .read()
                            .get(&"quick".to_owned())
                            .map_or(false, |docs| docs.contains("d4"))
                    })
                })
                .unwrap_or(false)
        },
        Duration::from_secs(5),
    );

    let index = db.state::<String, HashSet<String>>("index", None).unwrap();
    let index_h3 = index.get().unwrap();
    let index_read = index_h3.read();
    let quick_docs: HashSet<String> = index_read
        .get(&"quick".to_owned())
        .map(|v| v.into_owned())
        .unwrap_or_default();
    assert!(quick_docs.contains("d4"));
    assert!(quick_docs.contains("d1"));
    assert!(quick_docs.contains("d3"));
    drop(index_read);
    drop(index_h3);
}

// -----------------------------------------------------------------------
// Delete handling
// -----------------------------------------------------------------------

#[test]
fn document_delete_removes_from_index() {
    let path = tmp("doc_delete");

    let db =
        TestDatabase::create(&path, Arc::new(ThreadExecutor), DatabaseConfig::default()).unwrap();

    let documents = db
        .table("documents", Some(zendb_engine::TableConfig::default()))
        .unwrap();
    db.operator("indexer", Some(indexer_config())).unwrap();

    documents
        .get()
        .unwrap()
        .write()
        .insert_event(doc_event("d1", "hello world", 100))
        .unwrap();

    wait_until(
        || {
            db.state::<String, HashSet<String>>("index", None)
                .ok()
                .and_then(|s| {
                    s.get()
                        .ok()
                        .map(|state| state.read().get(&"hello".to_owned()).is_some())
                })
                .unwrap_or(false)
        },
        Duration::from_secs(5),
    );

    documents
        .get()
        .unwrap()
        .write()
        .insert_event(Event {
            table_id: "documents".into(),
            primary_key: PrimaryKey::String("d1".into()),
            path: ValuePath::new(),
            op: Op::Delete,
            hlc: hlc(200),
            sync: false,
            signature: Vec::new(),
        })
        .unwrap();

    wait_until(
        || {
            db.state::<String, HashSet<String>>("index", None)
                .ok()
                .and_then(|s| {
                    s.get()
                        .ok()
                        .map(|state| state.read().get(&"hello".to_owned()).is_none())
                })
                .unwrap_or(false)
        },
        Duration::from_secs(5),
    );

    let index = db.state::<String, HashSet<String>>("index", None).unwrap();
    let index_h = index.get().unwrap();
    let index_read = index_h.read();
    let hello_docs = index_read.get(&"hello".to_owned()).map(|v| v.into_owned());
    assert!(
        hello_docs.map_or(true, |docs| !docs.contains("d1")),
        "hello should not map to d1 after delete"
    );
    drop(index_read);
    drop(index_h);

    let stats = db.state::<String, u64>("doc_stats", None).unwrap();
    let stats_h = stats.get().unwrap();
    let stats_read = stats_h.read();
    assert!(stats_read.get(&"d1".to_owned()).is_none());
    drop(stats_read);
    drop(stats_h);
}

// -----------------------------------------------------------------------
// Multiple operators on same table
// -----------------------------------------------------------------------

#[test]
fn multiple_operators_share_table_cleanly() {
    let path = tmp("multi_ops");

    let db =
        TestDatabase::create(&path, Arc::new(ThreadExecutor), DatabaseConfig::default()).unwrap();

    let documents = db
        .table("documents", Some(zendb_engine::TableConfig::default()))
        .unwrap();

    db.operator("indexer_a", Some(indexer_config())).unwrap();
    db.operator("indexer_b", Some(indexer_config())).unwrap();

    documents
        .get()
        .unwrap()
        .write()
        .insert_event(doc_event("shared", "alpha beta", 100))
        .unwrap();

    wait_until(
        || {
            db.state::<String, HashSet<String>>("index", None)
                .ok()
                .and_then(|s| {
                    s.get().ok().map(|state| {
                        state
                            .read()
                            .get(&"alpha".to_owned())
                            .map(|docs| docs.contains("shared"))
                            .unwrap_or(false)
                    })
                })
                .unwrap_or(false)
        },
        Duration::from_secs(5),
    );

    let index = db.state::<String, HashSet<String>>("index", None).unwrap();
    let index_h = index.get().unwrap();
    let index_read = index_h.read();
    let alpha_docs: HashSet<String> = index_read
        .get(&"alpha".to_owned())
        .map(|v| v.into_owned())
        .unwrap_or_default();
    assert_eq!(alpha_docs.len(), 1);
    assert!(alpha_docs.contains("shared"));
    drop(index_read);
    drop(index_h);

    assert_eq!(db.operator_phase("indexer_a"), Some(OperatorPhase::Active));
    assert_eq!(db.operator_phase("indexer_b"), Some(OperatorPhase::Active));
}

// -----------------------------------------------------------------------
// Timer eviction on retirement
// -----------------------------------------------------------------------

#[test]
fn timers_are_evicted_on_retirement() {
    let path = tmp("timer_evict");

    let db =
        TestDatabase::create(&path, Arc::new(ThreadExecutor), DatabaseConfig::default()).unwrap();

    db.table("documents", Some(zendb_engine::TableConfig::default()))
        .unwrap();
    db.table("reports", Some(zendb_engine::TableConfig::default()))
        .unwrap();

    db.operator("archiver", Some(archiver_config(1))).unwrap();

    wait_until(
        || db.operator_phase("archiver") == Some(OperatorPhase::Finished),
        Duration::from_secs(10),
    );

    // Verify the report was written.
    let reports = db.table("reports", None).unwrap();
    let reports_h = reports.get().unwrap();
    let reports_read = reports_h.read();
    assert!(
        reports_read
            .get(&PrimaryKey::String("report_1".into()))
            .is_some(),
        "timer should have fired and written report_1"
    );
    drop(reports_read);
    drop(reports_h);

    // Verify no timers remain.
    db.register_timer("archiver", 42, vec![1, 2, 3]).unwrap();
    db.cancel_timer("archiver", 42).unwrap();

    // Verify consumer cleanup.
    let documents = db.table("documents", None).unwrap();
    documents
        .get()
        .unwrap()
        .write()
        .insert_event(doc_event("post", "retirement test", 500))
        .unwrap();

    let docs_h = documents.get().unwrap();
    let docs_read = docs_h.read();
    let mut archiver_consumer = docs_read.consumer("archiver").unwrap();
    assert!(
        archiver_consumer.next().is_none(),
        "archiver consumer should have no pending records after retirement"
    );
    drop(archiver_consumer);
    drop(docs_read);
    drop(docs_h);
}
