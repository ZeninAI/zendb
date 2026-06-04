//! Table — one logical table backed by WAL + delta buffer + storage.

use std::{fs, io, path::Path};

use bincode::{config, decode_from_slice, encode_to_vec};
use rkyv::Archived;
use zendb_storage::{
    core::{
        backend::Backend,
        btree::{BPlusTree, BPlusTreeConfig, BTreeRange},
        keydir::KeyDir,
        skiplist::SkipList,
        wal::{Wal, WalConfig},
    },
    utils::serdes::ValueRef,
};
use zendb_types::{Cell, Delta, PrimaryKey};

use crate::config::{TableConfig, TableKind};

const FLUSH_THRESHOLD: usize = 10_000;

// ---------------------------------------------------------------------------
// Storage — concrete enum wrapping the two backend kinds and implementing
// `Backend<Vec<u8>, Vec<u8>>` so engine code can program against the trait
// without `dyn` (the trait is not object-safe).
//
// Iterator methods unify the two backends' concrete iterator types via a
// small `Either` enum.
// ---------------------------------------------------------------------------

type BytesKeyDir = KeyDir<Vec<u8>, Vec<u8>>;
type BytesBPlusTree = BPlusTree<Vec<u8>, Vec<u8>>;

pub enum Storage {
    Ordered(BytesBPlusTree),
    Unordered(BytesKeyDir),
}

/// Two-variant iterator: yields `Option<A::Item>` from whichever variant
/// is active. Used to unify the concrete iterator types each backend
/// returns under a single `impl Iterator` return.
enum Either<A, B> {
    Left(A),
    Right(B),
}

impl<A, B, Item> Iterator for Either<A, B>
where
    A: Iterator<Item = Item>,
    B: Iterator<Item = Item>,
{
    type Item = Item;
    fn next(&mut self) -> Option<Item> {
        match self {
            Either::Left(a) => a.next(),
            Either::Right(b) => b.next(),
        }
    }
}

impl Backend<Vec<u8>, Vec<u8>> for Storage {
    fn get(&self, key: &Vec<u8>) -> Option<ValueRef<'_, Vec<u8>>> {
        match self {
            Storage::Ordered(t) => t.get(key),
            Storage::Unordered(k) => k.get(key),
        }
    }

    fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> io::Result<()> {
        match self {
            Storage::Ordered(t) => t.put(key, value),
            Storage::Unordered(k) => k.put(key, value),
        }
    }

    fn delete(&mut self, key: &Vec<u8>) -> io::Result<bool> {
        match self {
            Storage::Ordered(t) => t.delete(key),
            Storage::Unordered(k) => k.delete(key),
        }
    }

    fn update<F>(&mut self, key: &Vec<u8>, f: F) -> io::Result<bool>
    where
        F: FnOnce(Vec<u8>) -> Vec<u8>,
    {
        match self {
            Storage::Ordered(t) => Backend::update(t, key, f),
            Storage::Unordered(k) => Backend::update(k, key, f),
        }
    }

    fn update_in_place<F>(&mut self, key: &Vec<u8>, f: F) -> io::Result<bool>
    where
        F: FnOnce(&mut Archived<Vec<u8>>),
    {
        match self {
            Storage::Ordered(t) => Backend::update_in_place(t, key, f),
            Storage::Unordered(k) => Backend::update_in_place(k, key, f),
        }
    }

    fn keys(&self) -> impl Iterator<Item = Vec<u8>> + '_ {
        match self {
            Storage::Ordered(t) => Either::Left(Backend::keys(t)),
            Storage::Unordered(k) => Either::Right(Backend::keys(k)),
        }
    }

    fn values(&self) -> impl Iterator<Item = ValueRef<'_, Vec<u8>>> + '_ {
        match self {
            Storage::Ordered(t) => Either::Left(Backend::values(t)),
            Storage::Unordered(k) => Either::Right(Backend::values(k)),
        }
    }

    fn entries(&self) -> impl Iterator<Item = (Vec<u8>, ValueRef<'_, Vec<u8>>)> + '_ {
        match self {
            Storage::Ordered(t) => Either::Left(Backend::entries(t)),
            Storage::Unordered(k) => Either::Right(Backend::entries(k)),
        }
    }

    fn len(&self) -> usize {
        match self {
            Storage::Ordered(t) => t.len(),
            Storage::Unordered(k) => k.len(),
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            Storage::Ordered(t) => t.is_empty(),
            Storage::Unordered(k) => k.is_empty(),
        }
    }

    fn flush(&self) -> io::Result<()> {
        match self {
            Storage::Ordered(t) => t.flush(),
            Storage::Unordered(k) => k.flush(),
        }
    }
}

impl Storage {
    /// Range scan over `[start, end)`. Only the `Ordered` (BPlusTree) variant
    /// supports range queries; calling this on `Unordered` panics — KeyDir's
    /// hash index has no usable key ordering, so the request is meaningless.
    pub fn range<'a>(&'a self, start: &Vec<u8>, end: &Vec<u8>) -> BTreeRange<'a, Vec<u8>, Vec<u8>> {
        match self {
            Storage::Ordered(t) => t.range(start, end),
            Storage::Unordered(_) => {
                panic!("Unordered backend (KeyDir) does not support range queries")
            }
        }
    }
}

pub struct Table {
    name: String,
    sync_enabled: bool,
    wal: Wal,
    buffer: SkipList<Vec<u8>, Vec<Delta>>,
    backend: Storage,
    buffered_count: usize,
}

impl Table {
    /// Open an existing table (or create if missing) with the given config.
    pub fn open(base: &Path, config: &TableConfig) -> io::Result<Table> {
        let path = base.join(&config.name);
        fs::create_dir_all(&path)?;

        let wal_path = path.join("wal");
        let buffer = Self::recover_wal(&wal_path)?;

        let wal = Wal::create(&wal_path, config.wal.clone())?;

        let backend: Storage = match &config.kind {
            TableKind::Ordered => {
                let tree_path = path.join("tree");
                let tree: BytesBPlusTree = if tree_path.exists() {
                    BPlusTree::open(&tree_path, BPlusTreeConfig::default())?
                } else {
                    BPlusTree::create(&tree_path, BPlusTreeConfig::default())?
                };
                Storage::Ordered(tree)
            }
            TableKind::Unordered(kd_config) => {
                let kd_path = path.join("data");
                let kd: BytesKeyDir = if kd_path.exists() {
                    KeyDir::open(&kd_path, kd_config.clone())?
                } else {
                    KeyDir::create(&kd_path, kd_config.clone())?
                };
                Storage::Unordered(kd)
            }
        };

        let count = buffer.iter().map(|(_, deltas)| deltas.len()).sum();

        Ok(Table {
            name: config.name.clone(),
            sync_enabled: config.sync_enabled,
            wal,
            buffer,
            backend,
            buffered_count: count,
        })
    }

    /// Apply a delta: write to WAL, buffer in memory, optionally flush.
    pub fn apply(&mut self, delta: Delta) -> io::Result<()> {
        let buf = encode_to_vec(&delta, config::standard())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
        self.wal.append(&buf)?;

        let key = primary_key_bytes(&delta.primary_key);
        let existing = self.buffer.get(&key).cloned();
        self.buffer.remove(&key);
        self.buffered_count += 1;

        match existing {
            Some(mut deltas) => {
                deltas.push(delta);
                self.buffer.insert(key, deltas);
            }
            None => {
                self.buffer.insert(key, vec![delta]);
            }
        }

        if self.buffered_count >= FLUSH_THRESHOLD {
            self.flush()?;
        }

        Ok(())
    }

    /// Flush buffered deltas to the backend.
    pub fn flush(&mut self) -> io::Result<()> {
        if self.buffered_count == 0 {
            return Ok(());
        }

        let entries: Vec<(Vec<u8>, Vec<Delta>)> = self
            .buffer
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        self.buffer = SkipList::new();
        self.buffered_count = 0;

        for (key, deltas) in entries {
            let current_bytes: Option<Vec<u8>> =
                self.backend.get(&key).map(|v| v.as_slice().to_vec());
            let mut cell = decode_cell(current_bytes.as_deref()).unwrap_or_else(|| {
                Cell::dummy(zendb_types::Value::Atom(zendb_types::Atom(
                    zendb_types::AtomValue::Null,
                )))
            });

            for delta in &deltas {
                cell.apply(delta);
            }

            let buf = encode_to_vec(&cell, config::standard())
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
            self.backend.put(key, buf)?;
        }

        self.backend.flush()?;
        self.wal.sync()?;

        Ok(())
    }

    /// Get the current value for a key, merging buffered deltas on top.
    pub fn get(&self, key: &[u8]) -> Option<Cell> {
        let key_vec = key.to_vec();
        let current_bytes: Option<Vec<u8>> =
            self.backend.get(&key_vec).map(|v| v.as_slice().to_vec());
        let mut cell = decode_cell(current_bytes.as_deref()).unwrap_or_else(|| {
            Cell::dummy(zendb_types::Value::Atom(zendb_types::Atom(
                zendb_types::AtomValue::Null,
            )))
        });

        let mut modified = false;
        if let Some(deltas) = self.buffer.get(&key_vec) {
            for delta in deltas {
                cell.apply(delta);
                modified = true;
            }
        }

        if modified || !cell.is_dummy() {
            Some(cell)
        } else {
            None
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn sync_enabled(&self) -> bool {
        self.sync_enabled
    }

    /// Borrow the backend for read-only operations (range scans, iteration, etc.).
    pub fn backend(&self) -> &Storage {
        &self.backend
    }

    pub fn sync(&mut self) -> io::Result<()> {
        self.backend.flush()?;
        self.wal.sync()
    }

    // --- internal ---

    fn recover_wal(path: &Path) -> io::Result<SkipList<Vec<u8>, Vec<Delta>>> {
        let mut buffer: SkipList<Vec<u8>, Vec<Delta>> = SkipList::new();
        if !path.exists() {
            return Ok(buffer);
        }

        let wal = match Wal::open(path, WalConfig::default()) {
            Ok(w) => w,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(buffer),
            Err(e) => return Err(e),
        };

        for entry in wal.into_iter() {
            let bytes = entry?.data;
            let (delta, _) = decode_from_slice(&bytes, config::standard())
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;

            let key = primary_key_bytes(&delta.primary_key);
            let existing = buffer.get(&key).cloned();
            buffer.remove(&key);
            match existing {
                Some(mut deltas) => {
                    deltas.push(delta);
                    buffer.insert(key, deltas);
                }
                None => {
                    buffer.insert(key, vec![delta]);
                }
            }
        }

        let _ = fs::remove_file(path);
        Ok(buffer)
    }
}

fn primary_key_bytes(pk: &PrimaryKey) -> Vec<u8> {
    encode_to_vec(pk, config::standard()).expect("primary key encode")
}

fn decode_cell(bytes: Option<&[u8]>) -> Option<Cell> {
    bytes.and_then(|b| {
        decode_from_slice(b, config::standard())
            .ok()
            .map(|(c, _)| c)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use zendb_types::{AtomOp, AtomValue, Hlc, Op, TypeOp};

    fn tmp_dir(name: &str) -> PathBuf {
        let p = std::env::temp_dir().join(format!("zendb_engine_{}", name));
        let _ = fs::remove_dir_all(&p);
        p
    }

    fn make_delta(key: &str, val: &str, hlc_ms: u64) -> (Delta, Vec<u8>) {
        let pk = zendb_types::PrimaryKey::Atom(zendb_types::AtomValue::String(key.into()));
        let pk_bytes = encode_to_vec(&pk, config::standard()).unwrap();
        let d = Delta {
            table_id: "test".into(),
            primary_key: pk,
            path: zendb_types::Path::new(),
            op: Op::Type(TypeOp::Atom(AtomOp::Set(AtomValue::String(val.into())))),
            hlc: Hlc::with_device_id(hlc_ms, 0, [1u8; 8]).unwrap(),
            sync: false,
            signature: vec![],
        };
        (d, pk_bytes)
    }

    #[test]
    fn ordered_table() {
        let dir = tmp_dir("ordered");
        let cfg = TableConfig::ordered("test");
        let mut table = Table::open(&dir, &cfg).unwrap();
        let (d1, k1) = make_delta("k1", "v1", 100);
        table.apply(d1).unwrap();
        table.flush().unwrap();
        assert!(table.get(&k1).is_some());
    }

    #[test]
    fn unordered_table() {
        let dir = tmp_dir("unordered");
        let cfg = TableConfig::unordered("test");
        let mut table = Table::open(&dir, &cfg).unwrap();
        let (d1, k1) = make_delta("k1", "v1", 100);
        table.apply(d1).unwrap();
        table.flush().unwrap();
        assert!(table.get(&k1).is_some());
    }

    #[test]
    fn reopen_persists() {
        let dir = tmp_dir("persist");
        let cfg = TableConfig::ordered("test");
        {
            let mut table = Table::open(&dir, &cfg).unwrap();
            let (d, _k) = make_delta("k", "val", 100);
            table.apply(d).unwrap();
            table.flush().unwrap();
        }
        let table = Table::open(&dir, &cfg).unwrap();
        let (_, k) = make_delta("k", "", 0);
        assert!(table.get(&k).is_some());
    }

    #[test]
    fn ordered_backend_supports_range() {
        let dir = tmp_dir("range_ordered");
        let cfg = TableConfig::ordered("test");
        let mut table = Table::open(&dir, &cfg).unwrap();
        let mut keys = Vec::new();
        for i in 0u32..10 {
            let (d, k) = make_delta(&format!("k{:02}", i), "v", 100 + i as u64);
            keys.push(k);
            table.apply(d).unwrap();
        }
        table.flush().unwrap();
        // keys[i] are the encoded PrimaryKey bytes; query the encoded range.
        let count = table.backend().range(&keys[2], &keys[7]).count();
        assert_eq!(count, 5);
    }

    #[test]
    #[should_panic(expected = "does not support range queries")]
    fn unordered_backend_panics_on_range() {
        let dir = tmp_dir("range_unordered");
        let cfg = TableConfig::unordered("test");
        let mut table = Table::open(&dir, &cfg).unwrap();
        let (d, _) = make_delta("k", "v", 100);
        table.apply(d).unwrap();
        table.flush().unwrap();
        // Should panic with `unimplemented!`.
        let _ = table
            .backend()
            .range(&b"a".to_vec(), &b"z".to_vec())
            .count();
    }
}
