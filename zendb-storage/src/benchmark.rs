//! KeyDir and OrderLog bulk-write benchmarks.
//!
//! The headline comparison is **write-through vs `open_tx` / `close_tx`
//! bulk-write mode** — same backend, same workload, two write modes.
//! The tx-mode win comes from amortizing the per-op overheads that the
//! no-tx path pays on every call:
//!
//! - mmap `grow` (file `set_len` + remap) — at most one per batch in
//!   tx mode, vs. one every time the no-tx path crosses the current
//!   capacity boundary.
//! - `maybe_compact` (dead-byte ratio check + possibly a full sweep)
//!   — at most one per batch in tx mode, vs. one per record in no-tx.
//!
//! Two scenarios cover both axes:
//!
//! - **fresh** — N unique keys, no overwrites. No dead bytes, no
//!   compaction triggers. This isolates the steady-state encode cost;
//!   the tx path pays an extra staging→mmap memcpy at commit, so it is
//!   *slightly slower* than no-tx in this scenario. Worth measuring
//!   because it sets the floor for the tx path's overhead.
//! - **churn** — N puts spread across N/4 unique keys (4× overwrite
//!   ratio). This generates dead bytes and exercises `maybe_compact`
//!   in the no-tx path. The tx path defers all of it to commit and
//!   wins.
//!
//! OrderLog also has sorted bulk paths. Its unsorted bulk methods collect
//! and sort before delegating to the sorted finger-walk implementation, so
//! the bulk benchmarks include both direct sorted input and reversed input
//! that pays the sort cost.
//!
//! LMDB sits next to the in-tree backends as a baseline:
//!
//! - KeyDir no-tx ↔ LMDB one-txn-per-op (both pay a per-record commit).
//! - KeyDir open_tx ↔ LMDB single batch txn (both amortize commit).
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
    fmt,
    io,
    path::Path,
    time::{Duration, Instant},
};

use lmdb::{Database, Environment, EnvironmentFlags, Transaction, WriteFlags};
use tempfile::TempDir;

use crate::{
    core::{
        backend::Backend,
        keydir::{KeyDir, KeyDirConfig},
        orderlog::{OrderLog, OrderLogConfig},
    },
    utils::serdes::{deserialize_from, serialize_to_vec},
};

// ---------------------------------------------------------------------------
// Workload knobs — kept identical across every scenario so the numbers
// are directly comparable. Keys and values are both `u64`, encoded via
// bincode for every backend (KeyDir uses bincode internally; LMDB sees
// the pre-encoded bytes).
// ---------------------------------------------------------------------------

/// Number of `put` operations in each write scenario.
const N: u64 = 100_000;

/// For the "churn" scenario: number of distinct keys. With `N` puts
/// across `N / CHURN_FACTOR` keys, each key is overwritten ~4 times on
/// average — enough to trigger `maybe_compact` repeatedly in the no-tx
/// path.
const CHURN_FACTOR: u64 = 4;

/// LMDB map size — larger than the worst-case dataset so no resize event
/// shows up in the measured region.
const LMDB_MAP_SIZE: usize = 512 * 1024 * 1024;

fn keydir_config() -> KeyDirConfig {
    // Use defaults: 1 MiB initial capacity (so the bulk-load triggers
    // mmap growth), compaction_ratio 0.5 (so overwrites in the churn
    // scenario trigger maybe_compact in the no-tx path).
    KeyDirConfig::default()
}

fn orderlog_config() -> OrderLogConfig {
    // Match KeyDir's defaults: 1 MiB initial capacity and 0.5 compaction
    // ratio, so fresh/churn measurements line up across both backends.
    OrderLogConfig::default()
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
    keys.iter()
        .rev()
        .map(|&k| (k, k.wrapping_mul(7)))
        .collect()
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
fn keydir_writes_fresh_no_tx() -> io::Result<()> {
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

    BenchResult::new("KeyDir fresh writes (no tx)", N, elapsed).print();
    Ok(())
}

#[test]
fn keydir_writes_fresh_in_tx() -> io::Result<()> {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("keydir.bin");
    let mut kd = KeyDir::<u64, u64>::create(&path, keydir_config())?;
    let keys = fresh_keys();

    let elapsed = timed(|| {
        kd.open_tx()?;
        for &k in &keys {
            kd.put(k, k.wrapping_mul(7))?;
        }
        kd.close_tx()?;
        kd.flush()
    })?;

    BenchResult::new("KeyDir fresh writes (open_tx batch)", N, elapsed).print();
    Ok(())
}

// ---------------------------------------------------------------------------
// KeyDir — churn (4× overwrite ratio, exercises maybe_compact)
// ---------------------------------------------------------------------------

#[test]
fn keydir_writes_churn_no_tx() -> io::Result<()> {
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

    BenchResult::new("KeyDir churn writes (no tx)", N, elapsed).print();
    Ok(())
}

#[test]
fn keydir_writes_churn_in_tx() -> io::Result<()> {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("keydir.bin");
    let mut kd = KeyDir::<u64, u64>::create(&path, keydir_config())?;
    let keys = churn_keys();

    let elapsed = timed(|| {
        kd.open_tx()?;
        for &k in &keys {
            kd.put(k, k.wrapping_mul(7))?;
        }
        kd.close_tx()?;
        kd.flush()
    })?;

    BenchResult::new("KeyDir churn writes (open_tx batch)", N, elapsed).print();
    Ok(())
}

// ---------------------------------------------------------------------------
// KeyDir — reads
// ---------------------------------------------------------------------------

#[test]
fn keydir_reads() -> io::Result<()> {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("keydir.bin");
    let mut kd = KeyDir::<u64, u64>::create(&path, keydir_config())?;

    // Seed outside the timed region. Use the tx path because we measure
    // read throughput, not seed throughput.
    kd.open_tx()?;
    for k in 0..N {
        kd.put(k, k.wrapping_mul(7))?;
    }
    kd.close_tx()?;
    kd.flush()?;

    let elapsed = timed(|| {
        for k in 0..N {
            let v: u64 = kd.get(&k).expect("key must exist");
            assert_eq!(v, k.wrapping_mul(7));
        }
        Ok(())
    })?;

    BenchResult::new("KeyDir reads", N, elapsed).print();
    Ok(())
}

// ---------------------------------------------------------------------------
// OrderLog — fresh (no overwrites)
// ---------------------------------------------------------------------------

#[test]
fn orderlog_writes_fresh_no_tx() -> io::Result<()> {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("orderlog.bin");
    let mut ol = OrderLog::<u64, u64>::create(&path, orderlog_config())?;
    let keys = fresh_keys();

    let elapsed = timed(|| {
        for &k in &keys {
            ol.put(k, k.wrapping_mul(7))?;
        }
        ol.flush()
    })?;

    BenchResult::new("OrderLog fresh writes (no tx)", N, elapsed).print();
    Ok(())
}

#[test]
fn orderlog_writes_fresh_in_tx() -> io::Result<()> {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("orderlog.bin");
    let mut ol = OrderLog::<u64, u64>::create(&path, orderlog_config())?;
    let keys = fresh_keys();

    let elapsed = timed(|| {
        ol.open_tx()?;
        for &k in &keys {
            ol.put(k, k.wrapping_mul(7))?;
        }
        ol.close_tx()?;
        ol.flush()
    })?;

    BenchResult::new("OrderLog fresh writes (open_tx batch)", N, elapsed).print();
    Ok(())
}

// ---------------------------------------------------------------------------
// OrderLog — churn (4× overwrite ratio, exercises maybe_compact)
// ---------------------------------------------------------------------------

#[test]
fn orderlog_writes_churn_no_tx() -> io::Result<()> {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("orderlog.bin");
    let mut ol = OrderLog::<u64, u64>::create(&path, orderlog_config())?;
    let keys = churn_keys();

    let elapsed = timed(|| {
        for &k in &keys {
            ol.put(k, k.wrapping_mul(7))?;
        }
        ol.flush()
    })?;

    BenchResult::new("OrderLog churn writes (no tx)", N, elapsed).print();
    Ok(())
}

#[test]
fn orderlog_writes_churn_in_tx() -> io::Result<()> {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("orderlog.bin");
    let mut ol = OrderLog::<u64, u64>::create(&path, orderlog_config())?;
    let keys = churn_keys();

    let elapsed = timed(|| {
        ol.open_tx()?;
        for &k in &keys {
            ol.put(k, k.wrapping_mul(7))?;
        }
        ol.close_tx()?;
        ol.flush()
    })?;

    BenchResult::new("OrderLog churn writes (open_tx batch)", N, elapsed).print();
    Ok(())
}

// ---------------------------------------------------------------------------
// OrderLog — sorted bulk paths
// ---------------------------------------------------------------------------

#[test]
fn orderlog_bulk_put_sorted_fresh() -> io::Result<()> {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("orderlog.bin");
    let mut ol = OrderLog::<u64, u64>::create(&path, orderlog_config())?;
    let items = kv_payload(&fresh_keys());

    let elapsed = timed(|| {
        ol.bulk_put_sorted(items)?;
        ol.flush()
    })?;

    BenchResult::new("OrderLog fresh bulk_put_sorted", N, elapsed).print();
    Ok(())
}

#[test]
fn orderlog_bulk_put_unsorted_fresh() -> io::Result<()> {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("orderlog.bin");
    let mut ol = OrderLog::<u64, u64>::create(&path, orderlog_config())?;
    let items = reversed_kv_payload(&fresh_keys());

    let elapsed = timed(|| {
        ol.bulk_put(items)?;
        ol.flush()
    })?;

    BenchResult::new("OrderLog fresh bulk_put (sort first)", N, elapsed).print();
    Ok(())
}

#[test]
fn orderlog_bulk_put_sorted_churn() -> io::Result<()> {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("orderlog.bin");
    let mut ol = OrderLog::<u64, u64>::create(&path, orderlog_config())?;
    let mut items = kv_payload(&churn_keys());
    items.sort_by(|(a, _), (b, _)| a.cmp(b));

    let elapsed = timed(|| {
        ol.bulk_put_sorted(items)?;
        ol.flush()
    })?;

    BenchResult::new("OrderLog churn bulk_put_sorted", N, elapsed).print();
    Ok(())
}

#[test]
fn orderlog_bulk_put_unsorted_churn() -> io::Result<()> {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("orderlog.bin");
    let mut ol = OrderLog::<u64, u64>::create(&path, orderlog_config())?;
    let items = reversed_kv_payload(&churn_keys());

    let elapsed = timed(|| {
        ol.bulk_put(items)?;
        ol.flush()
    })?;

    BenchResult::new("OrderLog churn bulk_put (sort first)", N, elapsed).print();
    Ok(())
}

// ---------------------------------------------------------------------------
// OrderLog — reads
// ---------------------------------------------------------------------------

#[test]
fn orderlog_reads() -> io::Result<()> {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("orderlog.bin");
    let mut ol = OrderLog::<u64, u64>::create(&path, orderlog_config())?;

    ol.bulk_put_sorted(kv_payload(&fresh_keys()))?;
    ol.flush()?;

    let elapsed = timed(|| {
        for k in 0..N {
            let v: u64 = ol.get(&k).expect("key must exist");
            assert_eq!(v, k.wrapping_mul(7));
        }
        Ok(())
    })?;

    BenchResult::new("OrderLog reads", N, elapsed).print();
    Ok(())
}

// ---------------------------------------------------------------------------
// LMDB — apples-to-apples baselines
// ---------------------------------------------------------------------------

#[test]
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
