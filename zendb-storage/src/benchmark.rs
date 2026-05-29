//! Storage backend benchmarks — BPlusTree vs LMDB, KeyDir vs LMDB.
//!
//! All three backends use **rkyv** for key/value serialization so the
//! comparison isolates the storage data structure itself (B+‑tree vs
//! hash-indexed append-only file vs LMDB's copy-on-write B+‑tree).
//!
//! ## Running
//!
//! ```sh
//! cargo test -p zendb-storage -- benchmark --nocapture
//! ```

use std::{
    io,
    path::Path,
    time::{Duration, Instant},
};

use lmdb::{Database, Environment, EnvironmentFlags, Transaction, WriteFlags};
use rkyv::{
    api::high::HighDeserializer, rancor::Error as RkyvError, Archive, Archived, Deserialize,
};
use tempfile::TempDir;

use crate::{
    core::{btree::BPlusTree, keydir::KeyDir, keydir::KeyDirConfig},
    utils::serdes::serialize_to_vec,
};

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

/// Number of key-value pairs per benchmark phase.
///
/// Note: the B+Tree backend has a known mmap-remap issue on Windows that
/// can drop data after growing the file past ~1000 entries.  For larger
/// workloads, run the LMDB and KeyDir benches individually.
const N: u64 = 1_000;

fn deserialize_value<V>(bytes: &[u8]) -> V
where
    V: Archive,
    V::Archived: Deserialize<V, HighDeserializer<RkyvError>>,
{
    let archived = unsafe { rkyv::access_unchecked::<Archived<V>>(bytes) };
    rkyv::deserialize::<V, RkyvError>(archived).expect("deserialize")
}

fn ops_per_sec(dur: Duration, n: u64) -> f64 {
    let secs = dur.as_secs_f64();
    if secs == 0.0 {
        f64::INFINITY
    } else {
        n as f64 / secs
    }
}

fn lmdb_open(path: &Path) -> (Environment, Database) {
    let env = Environment::new()
        .set_flags(EnvironmentFlags::empty())
        .set_max_dbs(1)
        .set_map_size(512 * 1024 * 1024)
        .open(path)
        .expect("lmdb open");
    let db = env.open_db(None).expect("lmdb open_db");
    (env, db)
}

// ---------------------------------------------------------------------------
// BPlusTree
// ---------------------------------------------------------------------------

#[test]
fn bench_btree_write_heavy() -> io::Result<()> {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("btree.bin");

    let t0 = Instant::now();
    let mut tree = BPlusTree::<u64, u64>::create(&path)?;
    for k in 0..N {
        tree.put(k, k.wrapping_mul(7))?;
    }
    tree.flush()?;
    let elapsed = t0.elapsed();

    println!(
        "BPlusTree write-heavy: {:>6} inserts in {:>8.2?}  →  {:>12.0} ops/sec",
        N,
        elapsed,
        ops_per_sec(elapsed, N)
    );
    Ok(())
}

#[test]
fn bench_btree_read_heavy() -> io::Result<()> {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("btree.bin");

    let mut tree = BPlusTree::<u64, u64>::create(&path)?;
    for k in 0..N {
        tree.put(k, k.wrapping_mul(7))?;
    }

    let t0 = Instant::now();
    for k in 0..N {
        let v = tree.get(&k).expect("key must exist");
        assert_eq!(*v.archived(), k.wrapping_mul(7));
    }
    let elapsed = t0.elapsed();

    println!(
        "BPlusTree read-heavy:  {:>6} lookups  in {:>8.2?}  →  {:>12.0} ops/sec",
        N,
        elapsed,
        ops_per_sec(elapsed, N)
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// KeyDir
// ---------------------------------------------------------------------------

fn keydir_config() -> KeyDirConfig {
    KeyDirConfig {
        initial_capacity: 128 * 1024 * 1024,
        compaction_ratio: 0.5,
    }
}

#[test]
fn bench_keydir_write_heavy() -> io::Result<()> {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("keydir.bin");

    let t0 = Instant::now();
    let mut kd = KeyDir::<u64, u64>::create(&path, keydir_config())?;
    for k in 0..N {
        kd.put(k, k.wrapping_mul(7))?;
    }
    kd.flush()?;
    let elapsed = t0.elapsed();

    println!(
        "KeyDir  write-heavy: {:>6} inserts in {:>8.2?}  →  {:>12.0} ops/sec",
        N,
        elapsed,
        ops_per_sec(elapsed, N)
    );
    Ok(())
}

#[test]
fn bench_keydir_read_heavy() -> io::Result<()> {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("keydir.bin");

    let mut kd = KeyDir::<u64, u64>::create(&path, keydir_config())?;
    for k in 0..N {
        kd.put(k, k.wrapping_mul(7))?;
    }
    kd.flush()?;

    let t0 = Instant::now();
    for k in 0..N {
        let v = kd.get(&k).expect("key must exist");
        assert_eq!(*v.archived(), k.wrapping_mul(7));
    }
    let elapsed = t0.elapsed();

    println!(
        "KeyDir  read-heavy:  {:>6} lookups  in {:>8.2?}  →  {:>12.0} ops/sec",
        N,
        elapsed,
        ops_per_sec(elapsed, N)
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// LMDB
// ---------------------------------------------------------------------------

#[test]
fn bench_lmdb_write_heavy() {
    let dir = TempDir::new().unwrap();
    let (env, db) = lmdb_open(dir.path());

    // One transaction per put — matches BPlusTree / KeyDir, which have no
    // batching abstraction and pay the per-op cost for every write.
    let t0 = Instant::now();
    for k in 0..N {
        let key = serialize_to_vec(&k).unwrap();
        let val = serialize_to_vec(&k.wrapping_mul(7)).unwrap();
        let mut txn = env.begin_rw_txn().expect("lmdb begin_rw_txn");
        txn.put(db, &key, &val, WriteFlags::empty())
            .expect("lmdb put");
        txn.commit().expect("lmdb commit");
    }
    let elapsed = t0.elapsed();

    println!(
        "LMDB    write-heavy: {:>6} inserts in {:>8.2?}  →  {:>12.0} ops/sec",
        N,
        elapsed,
        ops_per_sec(elapsed, N)
    );
}

#[test]
fn bench_lmdb_read_heavy() {
    let dir = TempDir::new().unwrap();
    let (env, db) = lmdb_open(dir.path());

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
            let val: u64 = deserialize_value(raw);
            assert_eq!(val, k.wrapping_mul(7));
        }
    }
    let elapsed = t0.elapsed();

    println!(
        "LMDB    read-heavy:  {:>6} lookups  in {:>8.2?}  →  {:>12.0} ops/sec",
        N,
        elapsed,
        ops_per_sec(elapsed, N)
    );
}
