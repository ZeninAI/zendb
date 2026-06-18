//! KeyDir, SkipList, Topic, LMDB, and BPlusTree storage benchmarks.
//!
//! The suite keeps the workload shape constant within each scenario and
//! labels each backend by the write mode it actually implements:
//!
//! - LMDB is measured as one write transaction per item and as one batch
//!   transaction as an external baseline.
//! - BPlusTree is measured for direct `put` loops, plus its meaningful
//!   `bulk_put_sorted` bottom-up load path.
//!
//! Two scenarios cover both axes:
//!
//! - **fresh** — N unique keys, no overwrites. No dead bytes, no
//!   compaction triggers. This isolates the steady-state encode cost.
//! - **churn** — N puts spread across N/4 unique keys (4× overwrite
//!   ratio). This generates dead bytes and exercises `maybe_compact` in
//!   write paths.
//!
//! SkipList also has sorted bulk paths. Its unsorted bulk methods collect
//! and sort before delegating to the sorted finger-walk implementation, so
//! the bulk benchmarks include both direct sorted input and reversed input
//! that pays the sort cost.
//!
//! LMDB sits next to the in-tree backends as a baseline:
//!
//! - KeyDir/SkipList direct writes <-> LMDB one-txn-per-op.
//! - LMDB single batch transaction remains a reference point for commit
//!   amortization outside the in-tree backends.
//!
//! ## Running
//!
//! ```sh
//! cargo test -p zendb-storage --release benchmark -- --nocapture
//! ```
//!
//! Use `--release` — debug builds run bincode encode/decode 5-10× slower
//! and obscure the storage-layer differences this file is trying to
//! measure.

use std::{
    fmt, io,
    path::Path,
    time::{Duration, Instant},
};

use lmdb::{Database, Environment, EnvironmentFlags, Transaction, WriteFlags};
use tempfile::TempDir;

use crate::{
    core::{
        backend::{Backend, FileBackedBackend},
        btree::{BPlusTree, BPlusTreeConfig},
        keydir::{KeyDir, KeyDirConfig},
        skiplist::{SkipList, SkipListCapacity, SkipListConfig},
        topic::{Topic, TopicConfig},
    },
    utils::serdes::{deserialize_from, serialize_to_vec},
};

// ---------------------------------------------------------------------------
// Workload knobs — kept identical across every scenario so the numbers
// are directly comparable. KeyDir, SkipList, and LMDB use `u64` keys.
// BPlusTree uses fixed-width `[u8; 8]` big-endian keys so its serialized
// key bytes preserve numeric order without adding Vec allocation or a
// length prefix. Values are `u64` for every backend.
// ---------------------------------------------------------------------------

/// Number of `put` operations in each write scenario.
const N: u64 = 10_000;

/// For the "churn" scenario: number of distinct keys. With `N` puts
/// across `N / CHURN_FACTOR` keys, each key is overwritten ~4 times on
/// average — enough to trigger `maybe_compact` repeatedly in direct
/// write paths.
const CHURN_FACTOR: u64 = 4;

/// LMDB map size — larger than the worst-case dataset so no resize event
/// shows up in the measured region.
const LMDB_MAP_SIZE: usize = 512 * 1024 * 1024;

fn keydir_config() -> KeyDirConfig {
    // Use defaults: 1 MiB initial capacity (so the bulk-load triggers
    // mmap growth), compaction_ratio 0.5 (so overwrites in the churn
    // scenario trigger maybe_compact in the direct path).
    KeyDirConfig::default()
}

fn skiplist_config() -> SkipListConfig {
    SkipListConfig::default()
}

fn bounded_skiplist_config() -> SkipListConfig {
    SkipListConfig {
        capacity: SkipListCapacity::Bounded {
            max_entries: N as usize,
        },
    }
}

fn btree_config() -> BPlusTreeConfig {
    BPlusTreeConfig::default()
}

fn topic_config() -> TopicConfig {
    TopicConfig::default()
}

// ---------------------------------------------------------------------------
// Result type — uniform format makes the output easy to scan.
// ---------------------------------------------------------------------------

struct BenchResult {
    name: &'static str,
    op_count: u64,
    duration: Duration,
}

impl BenchResult {
    fn new(name: &'static str, op_count: u64, duration: Duration) -> Self {
        BenchResult {
            name,
            op_count,
            duration,
        }
    }

    fn print(&self) {
        println!("{}", self);
    }
}

impl fmt::Display for BenchResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let secs = self.duration.as_secs_f64();
        let ops_per_sec = if secs > 0.0 {
            self.op_count as f64 / secs
        } else {
            f64::INFINITY
        };
        write!(
            f,
            "{:<40} {:>9} ops in {:>10.2?}  ->  {:>14.0} ops/sec",
            self.name, self.op_count, self.duration, ops_per_sec
        )
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Time the closure and return its elapsed wall-clock duration.
fn timed<F: FnOnce() -> io::Result<()>>(f: F) -> io::Result<Duration> {
    let t0 = Instant::now();
    f()?;
    Ok(t0.elapsed())
}

fn lmdb_open(path: &Path) -> (Environment, Database) {
    let env = Environment::new()
        .set_flags(EnvironmentFlags::empty())
        .set_max_dbs(1)
        .set_map_size(LMDB_MAP_SIZE)
        .open(path)
        .expect("lmdb open");
    let db = env.open_db(None).expect("lmdb open_db");
    (env, db)
}

/// Pre-build the key / value sequence each scenario consumes. The
/// "fresh" sequence uses every key index `0..N` exactly once; the
/// "churn" sequence uses `k % (N / CHURN_FACTOR)` so each unique key
/// is hit ~CHURN_FACTOR times.
///
/// Pre-building puts the loop iteration outside the timed region so
/// the bench measures the storage backend, not modulo arithmetic.
fn fresh_keys() -> Vec<u64> {
    (0..N).collect()
}

fn churn_keys() -> Vec<u64> {
    let domain = N / CHURN_FACTOR;
    (0..N).map(|k| k % domain).collect()
}

fn kv_payload(keys: &[u64]) -> Vec<(u64, u64)> {
    keys.iter().map(|&k| (k, k.wrapping_mul(7))).collect()
}

fn reversed_kv_payload(keys: &[u64]) -> Vec<(u64, u64)> {
    keys.iter().rev().map(|&k| (k, k.wrapping_mul(7))).collect()
}

/// Pre-encode every (key, value) pair for LMDB. Lets the LMDB scenario
/// match the KeyDir scenarios on what's inside vs. outside the timed
/// region.
fn lmdb_payload(keys: &[u64]) -> Vec<(Vec<u8>, Vec<u8>)> {
    keys.iter()
        .map(|&k| {
            (
                serialize_to_vec(&k).unwrap(),
                serialize_to_vec(&k.wrapping_mul(7)).unwrap(),
            )
        })
        .collect()
}

// ---------------------------------------------------------------------------
// KeyDir — fresh (no overwrites)
// ---------------------------------------------------------------------------

#[test]
#[ignore = "benchmark test; run explicitly with --ignored benchmark -- --nocapture"]
fn keydir_writes_fresh_direct() -> io::Result<()> {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("keydir.bin");
    let mut kd = KeyDir::<u64, u64>::create(&path, keydir_config())?;
    let keys = fresh_keys();

    let elapsed = timed(|| {
        for &k in &keys {
            kd.put(k, k.wrapping_mul(7))?;
        }
        kd.flush()
    })?;

    BenchResult::new("KeyDir fresh writes (direct put)", N, elapsed).print();
    Ok(())
}

// ---------------------------------------------------------------------------
// KeyDir — churn (4× overwrite ratio, exercises maybe_compact)
// ---------------------------------------------------------------------------

#[test]
#[ignore = "benchmark test; run explicitly with --ignored benchmark -- --nocapture"]
fn keydir_writes_churn_direct() -> io::Result<()> {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("keydir.bin");
    let mut kd = KeyDir::<u64, u64>::create(&path, keydir_config())?;
    let keys = churn_keys();

    let elapsed = timed(|| {
        for &k in &keys {
            kd.put(k, k.wrapping_mul(7))?;
        }
        kd.flush()
    })?;

    BenchResult::new("KeyDir churn writes (direct put)", N, elapsed).print();
    Ok(())
}

// ---------------------------------------------------------------------------
// KeyDir — reads
// ---------------------------------------------------------------------------

#[test]
#[ignore = "benchmark test; run explicitly with --ignored benchmark -- --nocapture"]
fn keydir_reads() -> io::Result<()> {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("keydir.bin");
    let mut kd = KeyDir::<u64, u64>::create(&path, keydir_config())?;

    // Seed outside the timed region because we measure read throughput.
    kd.bulk_put(kv_payload(&fresh_keys()))?;
    kd.flush()?;

    let elapsed = timed(|| {
        for k in 0..N {
            let v: u64 = *kd.get(&k).expect("key must exist");
            assert_eq!(v, k.wrapping_mul(7));
        }
        Ok(())
    })?;

    BenchResult::new("KeyDir reads", N, elapsed).print();
    Ok(())
}

// ---------------------------------------------------------------------------
// Topic - append and consumer reads
// ---------------------------------------------------------------------------

#[test]
#[ignore = "benchmark test; run explicitly with --ignored benchmark -- --nocapture"]
fn topic_writes_fresh_append() -> io::Result<()> {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("topic");
    let mut topic = Topic::<u64>::create(&path, topic_config())?;
    let keys = fresh_keys();

    let elapsed = timed(|| {
        for &k in &keys {
            topic.append(&k.wrapping_mul(7))?;
        }
        topic.flush()
    })?;

    BenchResult::new("Topic fresh writes (append)", N, elapsed).print();
    Ok(())
}

#[test]
#[ignore = "benchmark test; run explicitly with --ignored benchmark -- --nocapture"]
fn topic_reads() -> io::Result<()> {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("topic");
    let mut topic = Topic::<u64>::create(&path, topic_config())?;

    // Register before seeding because a new consumer starts at the current tail.
    drop(topic.consumer("bench-reader")?);

    for k in 0..N {
        topic.append(&k.wrapping_mul(7))?;
    }
    topic.flush()?;

    let mut consumer = topic.consumer("bench-reader")?;
    let elapsed = timed(|| {
        for k in 0..N {
            let value = consumer.next().expect("record must exist")?;
            assert_eq!(value, k.wrapping_mul(7));
        }
        Ok(())
    })?;

    BenchResult::new("Topic reads", N, elapsed).print();
    Ok(())
}

// ---------------------------------------------------------------------------
// SkipList — fresh (no overwrites)
// ---------------------------------------------------------------------------

#[test]
#[ignore = "benchmark test; run explicitly with --ignored benchmark -- --nocapture"]
fn skiplist_writes_fresh_direct() -> io::Result<()> {
    let mut ol = SkipList::<u64, u64>::new(skiplist_config());
    let keys = fresh_keys();

    let elapsed = timed(|| {
        for &k in &keys {
            ol.put(k, k.wrapping_mul(7))?;
        }
        ol.flush()
    })?;

    BenchResult::new("SkipList fresh writes (direct put)", N, elapsed).print();
    Ok(())
}

#[test]
#[ignore = "benchmark test; run explicitly with --ignored benchmark -- --nocapture"]
fn skiplist_writes_fresh_bounded_direct() -> io::Result<()> {
    let mut list = SkipList::<u64, u64>::new(bounded_skiplist_config());
    let keys = fresh_keys();

    let elapsed = timed(|| {
        for &key in &keys {
            list.put(key, key.wrapping_mul(7))?;
        }
        list.flush()
    })?;

    BenchResult::new("SkipList bounded fresh writes (direct put)", N, elapsed).print();
    Ok(())
}

// ---------------------------------------------------------------------------
// SkipList — churn (4× overwrite ratio, exercises maybe_compact)
// ---------------------------------------------------------------------------

#[test]
#[ignore = "benchmark test; run explicitly with --ignored benchmark -- --nocapture"]
fn skiplist_writes_churn_direct() -> io::Result<()> {
    let mut ol = SkipList::<u64, u64>::new(skiplist_config());
    let keys = churn_keys();

    let elapsed = timed(|| {
        for &k in &keys {
            ol.put(k, k.wrapping_mul(7))?;
        }
        ol.flush()
    })?;

    BenchResult::new("SkipList churn writes (direct put)", N, elapsed).print();
    Ok(())
}

// ---------------------------------------------------------------------------
// SkipList — sorted bulk paths
// ---------------------------------------------------------------------------

#[test]
#[ignore = "benchmark test; run explicitly with --ignored benchmark -- --nocapture"]
fn skiplist_bulk_put_sorted_fresh() -> io::Result<()> {
    let mut ol = SkipList::<u64, u64>::new(skiplist_config());
    let items = kv_payload(&fresh_keys());

    let elapsed = timed(|| {
        ol.bulk_put_sorted(items)?;
        ol.flush()
    })?;

    BenchResult::new("SkipList fresh bulk_put_sorted", N, elapsed).print();
    Ok(())
}

#[test]
#[ignore = "benchmark test; run explicitly with --ignored benchmark -- --nocapture"]
fn skiplist_bulk_put_unsorted_fresh() -> io::Result<()> {
    let mut ol = SkipList::<u64, u64>::new(skiplist_config());
    let items = reversed_kv_payload(&fresh_keys());

    let elapsed = timed(|| {
        ol.bulk_put(items)?;
        ol.flush()
    })?;

    BenchResult::new("SkipList fresh bulk_put (sort first)", N, elapsed).print();
    Ok(())
}

#[test]
#[ignore = "benchmark test; run explicitly with --ignored benchmark -- --nocapture"]
fn skiplist_bulk_put_sorted_churn() -> io::Result<()> {
    let mut ol = SkipList::<u64, u64>::new(skiplist_config());
    let mut items = kv_payload(&churn_keys());
    items.sort_by(|(a, _), (b, _)| a.cmp(b));

    let elapsed = timed(|| {
        ol.bulk_put_sorted(items)?;
        ol.flush()
    })?;

    BenchResult::new("SkipList churn bulk_put_sorted", N, elapsed).print();
    Ok(())
}

#[test]
#[ignore = "benchmark test; run explicitly with --ignored benchmark -- --nocapture"]
fn skiplist_bulk_put_unsorted_churn() -> io::Result<()> {
    let mut ol = SkipList::<u64, u64>::new(skiplist_config());
    let items = reversed_kv_payload(&churn_keys());

    let elapsed = timed(|| {
        ol.bulk_put(items)?;
        ol.flush()
    })?;

    BenchResult::new("SkipList churn bulk_put (sort first)", N, elapsed).print();
    Ok(())
}

// ---------------------------------------------------------------------------
// SkipList — reads
// ---------------------------------------------------------------------------

#[test]
#[ignore = "benchmark test; run explicitly with --ignored benchmark -- --nocapture"]
fn skiplist_reads() -> io::Result<()> {
    let mut ol = SkipList::<u64, u64>::new(skiplist_config());

    ol.bulk_put_sorted(kv_payload(&fresh_keys()))?;
    ol.flush()?;

    let elapsed = timed(|| {
        for k in 0..N {
            let v: u64 = *ol.get(&k).expect("key must exist");
            assert_eq!(v, k.wrapping_mul(7));
        }
        Ok(())
    })?;

    BenchResult::new("SkipList reads", N, elapsed).print();
    Ok(())
}

// ---------------------------------------------------------------------------
// LMDB — apples-to-apples baselines
// ---------------------------------------------------------------------------

#[test]
#[ignore = "benchmark test; run explicitly with --ignored benchmark -- --nocapture"]
fn lmdb_writes_fresh_per_op() {
    let dir = TempDir::new().unwrap();
    let (env, db) = lmdb_open(dir.path());
    let payload = lmdb_payload(&fresh_keys());

    let t0 = Instant::now();
    for (key, val) in &payload {
        let mut txn = env.begin_rw_txn().expect("lmdb begin_rw_txn");
        txn.put(db, key, val, WriteFlags::empty())
            .expect("lmdb put");
        txn.commit().expect("lmdb commit");
    }
    let elapsed = t0.elapsed();

    BenchResult::new("LMDB   fresh writes (txn per op)", N, elapsed).print();
}

#[test]
#[ignore = "benchmark test; run explicitly with --ignored benchmark -- --nocapture"]
fn lmdb_writes_fresh_batch() {
    let dir = TempDir::new().unwrap();
    let (env, db) = lmdb_open(dir.path());
    let payload = lmdb_payload(&fresh_keys());

    let t0 = Instant::now();
    let mut txn = env.begin_rw_txn().expect("lmdb begin_rw_txn");
    for (key, val) in &payload {
        txn.put(db, key, val, WriteFlags::empty())
            .expect("lmdb put");
    }
    txn.commit().expect("lmdb commit");
    let elapsed = t0.elapsed();

    BenchResult::new("LMDB   fresh writes (single batch txn)", N, elapsed).print();
}

#[test]
#[ignore = "benchmark test; run explicitly with --ignored benchmark -- --nocapture"]
fn lmdb_reads() {
    let dir = TempDir::new().unwrap();
    let (env, db) = lmdb_open(dir.path());

    // Seed.
    {
        let mut txn = env.begin_rw_txn().expect("lmdb begin_rw_txn");
        for k in 0..N {
            let key = serialize_to_vec(&k).unwrap();
            let val = serialize_to_vec(&k.wrapping_mul(7)).unwrap();
            txn.put(db, &key, &val, WriteFlags::empty())
                .expect("lmdb put");
        }
        txn.commit().expect("lmdb commit");
    }

    let t0 = Instant::now();
    {
        let txn = env.begin_ro_txn().expect("lmdb begin_ro_txn");
        for k in 0..N {
            let key = serialize_to_vec(&k).unwrap();
            let raw = txn.get(db, &key).expect("lmdb get");
            let v: u64 = deserialize_from(raw).expect("decode");
            assert_eq!(v, k.wrapping_mul(7));
        }
    }
    let elapsed = t0.elapsed();

    BenchResult::new("LMDB   reads", N, elapsed).print();
}

// ---------------------------------------------------------------------------
// BPlusTree — fresh / churn / bulk / reads / range
//
// BPlusTree orders entries by lexicographic bincode key bytes (not
// `K::Ord`). Using `u64` directly with bincode's little-endian encoding
// would NOT preserve numeric order — sorted bulk-load and range scans
// would be misordered. So btree benchmarks use fixed-width `[u8; 8]`
// keys filled with big-endian byte representations of `u64`, which are
// lex-order preserving and avoid extra heap allocation. The value side
// is still `u64` for parity with the other backends' value payload.
// ---------------------------------------------------------------------------

/// Encode a u64 as big-endian bytes. Lex order on the returned bytes
/// matches numeric order on the input.
#[inline]
fn be_key(k: u64) -> [u8; 8] {
    k.to_be_bytes()
}

fn btree_fresh_keys() -> Vec<[u8; 8]> {
    (0..N).map(be_key).collect()
}

fn btree_churn_keys() -> Vec<[u8; 8]> {
    let domain = N / CHURN_FACTOR;
    (0..N).map(|k| be_key(k % domain)).collect()
}

fn btree_kv_payload(keys: &[[u8; 8]]) -> Vec<([u8; 8], u64)> {
    keys.iter()
        .enumerate()
        .map(|(i, k)| (*k, (i as u64).wrapping_mul(7)))
        .collect()
}

fn btree_reversed_kv_payload(keys: &[[u8; 8]]) -> Vec<([u8; 8], u64)> {
    keys.iter()
        .enumerate()
        .rev()
        .map(|(i, k)| (*k, (i as u64).wrapping_mul(7)))
        .collect()
}

#[test]
#[ignore = "benchmark test; run explicitly with --ignored benchmark -- --nocapture"]
fn btree_writes_fresh_direct() -> io::Result<()> {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("btree.bin");
    let mut t = BPlusTree::<[u8; 8], u64>::create(&path, btree_config())?;
    let keys = btree_fresh_keys();

    let elapsed = timed(|| {
        for (i, k) in keys.iter().enumerate() {
            t.put(*k, (i as u64).wrapping_mul(7))?;
        }
        t.flush()
    })?;

    BenchResult::new("BTree  fresh writes (direct put)", N, elapsed).print();
    Ok(())
}

#[test]
#[ignore = "benchmark test; run explicitly with --ignored benchmark -- --nocapture"]
fn btree_writes_churn_direct() -> io::Result<()> {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("btree.bin");
    let mut t = BPlusTree::<[u8; 8], u64>::create(&path, btree_config())?;
    let keys = btree_churn_keys();

    let elapsed = timed(|| {
        for (i, k) in keys.iter().enumerate() {
            t.put(*k, (i as u64).wrapping_mul(7))?;
        }
        t.flush()
    })?;

    BenchResult::new("BTree  churn writes (direct put)", N, elapsed).print();
    Ok(())
}

#[test]
#[ignore = "benchmark test; run explicitly with --ignored benchmark -- --nocapture"]
fn btree_bulk_put_sorted_fresh() -> io::Result<()> {
    // Headline: bottom-up bulk-load on an empty tree. Big-endian keys
    // are already in lex sort order — perfect for bulk_put_sorted.
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("btree.bin");
    let mut t = BPlusTree::<[u8; 8], u64>::create(&path, btree_config())?;
    let items = btree_kv_payload(&btree_fresh_keys());

    let elapsed = timed(|| {
        t.bulk_put_sorted(items)?;
        t.flush()
    })?;

    BenchResult::new("BTree  fresh bulk_put_sorted (bottom-up)", N, elapsed).print();
    Ok(())
}

#[test]
#[ignore = "benchmark test; run explicitly with --ignored benchmark -- --nocapture"]
fn btree_bulk_put_unsorted_fresh() -> io::Result<()> {
    // Reversed input exercises the unsorted bulk_put fallback. For
    // BPlusTree this is intentionally just the direct put loop; only
    // sorted input on an empty tree can use the bottom-up build.
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("btree.bin");
    let mut t = BPlusTree::<[u8; 8], u64>::create(&path, btree_config())?;
    let items = btree_reversed_kv_payload(&btree_fresh_keys());

    let elapsed = timed(|| {
        t.bulk_put(items)?;
        t.flush()
    })?;

    BenchResult::new("BTree  fresh bulk_put (put loop)", N, elapsed).print();
    Ok(())
}

#[test]
#[ignore = "benchmark test; run explicitly with --ignored benchmark -- --nocapture"]
fn btree_reads() -> io::Result<()> {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("btree.bin");
    let mut t = BPlusTree::<[u8; 8], u64>::create(&path, btree_config())?;

    let keys = btree_fresh_keys();
    t.bulk_put_sorted(btree_kv_payload(&keys))?;
    t.flush()?;

    let elapsed = timed(|| {
        for (i, k) in keys.iter().enumerate() {
            let v: u64 = *t.get(k).expect("key must exist");
            assert_eq!(v, (i as u64).wrapping_mul(7));
        }
        Ok(())
    })?;

    BenchResult::new("BTree  reads", N, elapsed).print();
    Ok(())
}

#[test]
#[ignore = "benchmark test; run explicitly with --ignored benchmark -- --nocapture"]
fn btree_range_scan() -> io::Result<()> {
    use crate::core::backend::OrderedBackend;
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("btree.bin");
    let mut t = BPlusTree::<[u8; 8], u64>::create(&path, btree_config())?;
    let keys = btree_fresh_keys();
    t.bulk_put_sorted(btree_kv_payload(&keys))?;
    t.flush()?;

    let lo = be_key(N / 4);
    let hi = be_key((N * 3) / 4);
    let span = (N * 3) / 4 - N / 4;

    let elapsed = timed(|| {
        let count = t.range(&lo, &hi).count();
        assert_eq!(count as u64, span);
        Ok(())
    })?;

    BenchResult::new("BTree  range scan (50% of keys)", span, elapsed).print();
    Ok(())
}

#[test]
#[ignore = "benchmark test; run explicitly with --ignored benchmark -- --nocapture"]
fn btree_range_rev_scan() -> io::Result<()> {
    use crate::core::backend::OrderedBackend;
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("btree.bin");
    let mut t = BPlusTree::<[u8; 8], u64>::create(&path, btree_config())?;
    let keys = btree_fresh_keys();
    t.bulk_put_sorted(btree_kv_payload(&keys))?;
    t.flush()?;

    let lo = be_key(N / 4);
    let hi = be_key((N * 3) / 4);
    let span = (N * 3) / 4 - N / 4;

    let elapsed = timed(|| {
        let count = t.range_rev(&lo, &hi).count();
        assert_eq!(count as u64, span);
        Ok(())
    })?;

    BenchResult::new("BTree  range_rev scan (50% of keys)", span, elapsed).print();
    Ok(())
}
