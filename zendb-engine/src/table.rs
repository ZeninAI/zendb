//! Table — one logical table backed by WAL + delta buffer + storage.

use std::{fs, io, path::Path};

use zendb_storage::core::{
    btree::BPlusTree,
    keydir::KeyDir,
    skiplist::SkipList,
    wal::{Wal, WalConfig},
};
use zendb_types::{Cell, Delta, PrimaryKey, TypedValue};

use crate::config::{TableConfig, TableKind};

const FLUSH_THRESHOLD: usize = 10_000;

/// Polymorphic storage backend.
enum Backend {
    Ordered(BPlusTree),
    Unordered(KeyDir),
}

impl Backend {
    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        match self {
            Backend::Ordered(tree) => tree.get(key),
            Backend::Unordered(kd) => kd.get(key),
        }
    }

    fn put(&mut self, key: &[u8], value: &[u8]) -> io::Result<()> {
        match self {
            Backend::Ordered(tree) => tree.insert(key, value),
            Backend::Unordered(kd) => kd.put(key, value),
        }
    }

    fn flush(&self) -> io::Result<()> {
        match self {
            Backend::Ordered(tree) => tree.flush(),
            Backend::Unordered(kd) => kd.flush(),
        }
    }
}

pub struct Table {
    name: String,
    sync_enabled: bool,
    wal: Wal,
    buffer: SkipList<Vec<u8>, Vec<Delta>>,
    backend: Backend,
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

        let backend = match &config.kind {
            TableKind::Ordered => {
                let tree_path = path.join("tree");
                let tree = if tree_path.exists() {
                    BPlusTree::open(&tree_path)?
                } else {
                    BPlusTree::create(&tree_path)?
                };
                Backend::Ordered(tree)
            }
            TableKind::Unordered(kd_config) => {
                let kd_path = path.join("data");
                let kd = if kd_path.exists() {
                    KeyDir::open(&kd_path, kd_config.clone())?
                } else {
                    KeyDir::create(&kd_path, kd_config.clone())?
                };
                Backend::Unordered(kd)
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
        let mut buf = Vec::new();
        delta
            .encode(&mut buf)
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
            let current_bytes = self.backend.get(&key);
            let mut cell = decode_cell(current_bytes.as_deref()).unwrap_or_else(|| {
                Cell::dummy(zendb_types::Value::Atom(zendb_types::AtomValue::Null))
            });

            for delta in &deltas {
                cell.apply_delta(delta);
            }

            let mut buf = Vec::new();
            cell.encode(&mut buf)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
            self.backend.put(&key, &buf)?;
        }

        self.backend.flush()?;
        self.wal.sync()?;

        Ok(())
    }

    /// Get the current value for a key, merging buffered deltas on top.
    pub fn get(&self, key: &[u8]) -> Option<Cell> {
        let current_bytes = self.backend.get(key);
        let mut cell = decode_cell(current_bytes.as_deref())
            .unwrap_or_else(|| Cell::dummy(zendb_types::Value::Atom(zendb_types::AtomValue::Null)));

        let mut modified = false;
        if let Some(deltas) = self.buffer.get(&key.to_vec()) {
            for delta in deltas {
                cell.apply_delta(delta);
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
            let (delta, _) = Delta::decode(&bytes)
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
    let mut buf = Vec::new();
    pk.0.encode(&mut buf).expect("primary key encode");
    buf
}

fn decode_cell(bytes: Option<&[u8]>) -> Option<Cell> {
    bytes.and_then(|b| Cell::decode(b).ok().map(|(c, _)| c))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use zendb_types::{AtomOp, AtomValue, Hlc, Op};

    fn tmp_dir(name: &str) -> PathBuf {
        let p = std::env::temp_dir().join(format!("zendb_engine_{}", name));
        let _ = fs::remove_dir_all(&p);
        p
    }

    fn make_delta(key: &str, val: &str, hlc_ms: u64) -> (Delta, Vec<u8>) {
        let pk = PrimaryKey(zendb_types::AtomValue::String(key.into()));
        let pk_bytes = {
            let mut buf = Vec::new();
            pk.0.encode(&mut buf).unwrap();
            buf
        };
        let d = Delta {
            table_id: zendb_types::TableId("test".into()),
            primary_key: pk,
            path: zendb_types::Path::new(),
            op: Op::Atom(AtomOp::Set(AtomValue::String(val.into()))),
            hlc: Hlc::new(hlc_ms, 0, 1).unwrap(),
            sync: false,
            signature: zendb_types::Signature(vec![]),
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
}
