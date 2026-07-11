//! Integration tests for the document indexing pipeline.

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use zendb_engine::operator::prelude::{
    FullTextIndexConfig, FullTextIndexFacet, FullTextIndexOperator, MerkleTreeConfig,
    MerkleTreeFacet, MerkleTreeOperator,
};
use zendb_engine::{OperatorPhase, Workspace, WorkspaceConfig};
use zendb_storage::core::traits::Backend;
use zendb_types::{
    device_id, init_device_id, Event, Op, Path as ValuePath, PrimaryKey, WorkspaceId,
};

use crate::executor::ThreadExecutor;
use crate::operators::{
    archiver_config, archiver_runtime_config, doc_event, doc_operators::OperatorInstance, hlc,
    indexer_config, indexer_runtime_config, wait_until, ArchiverOp, IndexerOp,
};

type TestWorkspace = Workspace<OperatorInstance>;

fn workspace_config() -> WorkspaceConfig {
    init_device_id();
    WorkspaceConfig {
        workspace_id: WorkspaceId::from("test-workspace"),
        device_id: device_id(),
        graceful_shutdown_max_duration: Duration::from_millis(100),
    }
}

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
    let db = TestWorkspace::create(&path, Arc::new(ThreadExecutor), workspace_config()).unwrap();

    let documents = db
        .table("documents", Some(zendb_engine::TableConfig::default()))
        .unwrap();
    db.dispatch_operator::<IndexerOp>("indexer", indexer_config(), indexer_runtime_config())
        .unwrap();

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
    db.dispatch_operator::<ArchiverOp>("archiver", archiver_config(3), archiver_runtime_config())
        .unwrap();

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

    let db = TestWorkspace::open(&path, Arc::new(ThreadExecutor), workspace_config()).unwrap();

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

    // --- Phase 4: incremental indexing after reopen ---
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

    let db = TestWorkspace::create(&path, Arc::new(ThreadExecutor), workspace_config()).unwrap();

    let documents = db
        .table("documents", Some(zendb_engine::TableConfig::default()))
        .unwrap();
    db.dispatch_operator::<IndexerOp>("indexer", indexer_config(), indexer_runtime_config())
        .unwrap();

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

    let db = TestWorkspace::create(&path, Arc::new(ThreadExecutor), workspace_config()).unwrap();

    let documents = db
        .table("documents", Some(zendb_engine::TableConfig::default()))
        .unwrap();

    db.dispatch_operator::<IndexerOp>("indexer_a", indexer_config(), indexer_runtime_config())
        .unwrap();
    db.dispatch_operator::<IndexerOp>("indexer_b", indexer_config(), indexer_runtime_config())
        .unwrap();

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

    let db = TestWorkspace::create(&path, Arc::new(ThreadExecutor), workspace_config()).unwrap();

    db.table("documents", Some(zendb_engine::TableConfig::default()))
        .unwrap();
    db.table("reports", Some(zendb_engine::TableConfig::default()))
        .unwrap();

    db.dispatch_operator::<ArchiverOp>("archiver", archiver_config(1), archiver_runtime_config())
        .unwrap();

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

// -----------------------------------------------------------------------
// Merkle tree facet test
// -----------------------------------------------------------------------

#[test]
fn merkle_tree_facet_provides_root() {
    let path = tmp("merkle_facet");

    let db = TestWorkspace::create(&path, Arc::new(ThreadExecutor), workspace_config()).unwrap();

    let documents = db
        .table("documents", Some(zendb_engine::TableConfig::default()))
        .unwrap();

    // Insert documents BEFORE registering the operator so that rebuild_table
    // sees them and the consumer starts after these offsets (no double-process).
    documents
        .get()
        .unwrap()
        .write()
        .insert_event(doc_event("d1", "hello world", 100))
        .unwrap();
    documents
        .get()
        .unwrap()
        .write()
        .insert_event(doc_event("d2", "foo bar", 110))
        .unwrap();

    // Register the MerkleTree operator on the "documents" table.
    let merkle_config = MerkleTreeConfig {
        state: "merkle-state".to_owned(),
        leaf_bits: 4, // small for test
    };
    let merkle_runtime = zendb_engine::OperatorRuntimeConfig {
        subscriptions: vec![zendb_engine::Subscription::pattern("documents")],
        poll_size: 128,
    };
    db.dispatch_operator::<MerkleTreeOperator>("merkle", merkle_config, merkle_runtime)
        .unwrap();

    // Wait until the merkle operator has processed the initial rebuild.
    wait_until(
        || {
            db.facet::<MerkleTreeFacet>("merkle")
                .ok()
                .and_then(|f| f.root("documents").ok().flatten())
                .map(|root| root.entries == 2)
                .unwrap_or(false)
        },
        Duration::from_secs(5),
    );

    // Query the facet and verify root.
    let facet = db.facet::<MerkleTreeFacet>("merkle").unwrap();
    let root = facet.root("documents").unwrap().expect("root should exist");
    assert_eq!(root.entries, 2);
    assert_eq!(root.leaf_bits, 4);
    assert_ne!(root.hash, [0u8; 32], "root hash should be non-zero");

    // Verify individual entry lookup.
    let entry = facet
        .entry("documents", &PrimaryKey::String("d1".into()))
        .unwrap()
        .expect("entry d1 should exist");
    assert_ne!(entry.hash, [0u8; 32]);

    // Insert another document and verify the root updates via process().
    documents
        .get()
        .unwrap()
        .write()
        .insert_event(doc_event("d3", "baz qux", 120))
        .unwrap();

    wait_until(
        || {
            facet
                .root("documents")
                .ok()
                .flatten()
                .map(|r| r.entries == 3)
                .unwrap_or(false)
        },
        Duration::from_secs(5),
    );

    let updated_root = facet.root("documents").unwrap().unwrap();
    assert_eq!(updated_root.entries, 3);
    assert_ne!(
        updated_root.hash, root.hash,
        "hash should change after insert"
    );
}

// -----------------------------------------------------------------------
// Facet unavailable when operator is not running
// -----------------------------------------------------------------------

#[test]
fn facet_unavailable_after_operator_cancellation() {
    let path = tmp("facet_cancel");

    let db = TestWorkspace::create(&path, Arc::new(ThreadExecutor), workspace_config()).unwrap();

    let _documents = db
        .table("documents", Some(zendb_engine::TableConfig::default()))
        .unwrap();

    let merkle_config = MerkleTreeConfig {
        state: "merkle-state".to_owned(),
        leaf_bits: 4,
    };
    let merkle_runtime = zendb_engine::OperatorRuntimeConfig {
        subscriptions: vec![zendb_engine::Subscription::pattern("documents")],
        poll_size: 128,
    };
    db.dispatch_operator::<MerkleTreeOperator>("merkle", merkle_config, merkle_runtime)
        .unwrap();

    // Wait for the operator to start and publish the facet.
    wait_until(
        || db.facet::<MerkleTreeFacet>("merkle").is_ok(),
        Duration::from_secs(5),
    );

    // Facet should be available.
    assert!(db.facet::<MerkleTreeFacet>("merkle").is_ok());

    // Cancel the operator.
    db.cancel_operator("merkle").unwrap();

    // Wait for cancellation to complete.
    wait_until(
        || db.operator_phase("merkle") == Some(OperatorPhase::Cancelled),
        Duration::from_secs(5),
    );

    // Facet should no longer be available.
    assert!(
        db.facet::<MerkleTreeFacet>("merkle").is_err(),
        "facet should not be available after operator cancellation"
    );
}

// -----------------------------------------------------------------------
// Full-text index facet test
// -----------------------------------------------------------------------

#[test]
fn full_text_index_search() {
    let path = tmp("fti_search");

    let db = TestWorkspace::create(&path, Arc::new(ThreadExecutor), workspace_config()).unwrap();

    let documents = db
        .table("documents", Some(zendb_engine::TableConfig::default()))
        .unwrap();

    // Insert documents before operator so rebuild picks them up cleanly.
    documents
        .get()
        .unwrap()
        .write()
        .insert_event(doc_event(
            "d1",
            "The quick brown fox jumps over the lazy dog",
            100,
        ))
        .unwrap();
    documents
        .get()
        .unwrap()
        .write()
        .insert_event(doc_event("d2", "A quick brown cat sleeps on the mat", 110))
        .unwrap();
    documents
        .get()
        .unwrap()
        .write()
        .insert_event(doc_event("d3", "The lazy fox does nothing", 120))
        .unwrap();

    // Register FTI operator.
    let fti_config = FullTextIndexConfig {
        state: "fti-state".to_owned(),
        min_token_len: 2,
    };
    let fti_runtime = zendb_engine::OperatorRuntimeConfig {
        subscriptions: vec![zendb_engine::Subscription::pattern("documents")],
        poll_size: 128,
    };
    db.dispatch_operator::<FullTextIndexOperator>("fti", fti_config, fti_runtime)
        .unwrap();

    // Wait for the facet to become available and index to be built.
    wait_until(
        || {
            db.facet::<FullTextIndexFacet>("fti")
                .ok()
                .and_then(|f| {
                    f.tokens_for_entry("documents", &PrimaryKey::String("d1".into()))
                        .ok()
                })
                .map(|tokens| !tokens.is_empty())
                .unwrap_or(false)
        },
        Duration::from_secs(5),
    );

    let facet = db.facet::<FullTextIndexFacet>("fti").unwrap();

    // Search for "quick brown" — should match d1 and d2.
    let results = facet.search("documents", "quick brown", 10).unwrap();
    assert!(results.len() >= 2);
    let keys: Vec<&PrimaryKey> = results.iter().map(|h| &h.key).collect();
    assert!(keys.contains(&&PrimaryKey::String("d1".into())));
    assert!(keys.contains(&&PrimaryKey::String("d2".into())));

    // Search for "lazy fox" — should match d1 and d3.
    let results = facet.search("documents", "lazy fox", 10).unwrap();
    assert!(results.len() >= 2);
    let keys: Vec<&PrimaryKey> = results.iter().map(|h| &h.key).collect();
    assert!(keys.contains(&&PrimaryKey::String("d1".into())));
    assert!(keys.contains(&&PrimaryKey::String("d3".into())));

    // Search for "cat" — should only match d2.
    let results = facet.search("documents", "cat", 10).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].key, PrimaryKey::String("d2".into()));

    // Verify top-k limiting works.
    let results = facet.search("documents", "the", 1).unwrap();
    assert_eq!(results.len(), 1);

    // Search remains table-scoped.
    let results = facet.search("documents", "quick", 10).unwrap();
    assert!(results.len() >= 2);

    // Verify incremental update: insert a new document.
    documents
        .get()
        .unwrap()
        .write()
        .insert_event(doc_event("d4", "quick silver fox", 130))
        .unwrap();

    wait_until(
        || {
            facet
                .search("documents", "quick fox", 10)
                .ok()
                .map(|r| r.iter().any(|h| h.key == PrimaryKey::String("d4".into())))
                .unwrap_or(false)
        },
        Duration::from_secs(5),
    );

    // d4 should now appear in "quick fox" results with score 1.0 (both tokens match).
    let results = facet.search("documents", "quick fox", 10).unwrap();
    let d4_hit = results
        .iter()
        .find(|h| h.key == PrimaryKey::String("d4".into()));
    assert!(d4_hit.is_some());
    assert_eq!(d4_hit.unwrap().matched_tokens, 2);

    // Verify update: change d1's content (removes "dog", adds "deer").
    documents
        .get()
        .unwrap()
        .write()
        .insert_event(doc_event(
            "d1",
            "The quick brown fox jumps over the lazy deer",
            200,
        ))
        .unwrap();

    wait_until(
        || {
            facet
                .search("documents", "dog", 10)
                .ok()
                .map(|r| r.is_empty())
                .unwrap_or(false)
        },
        Duration::from_secs(5),
    );

    // "dog" should no longer match anything.
    let results = facet.search("documents", "dog", 10).unwrap();
    assert!(results.is_empty());

    // "deer" should match d1.
    let results = facet.search("documents", "deer", 10).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].key, PrimaryKey::String("d1".into()));
}
