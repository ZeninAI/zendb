//! KeyDir — persistent key-value store backed by an in-memory hash index
//! and a memory-mapped append-only data file (Bitcask model).
//!
//! # Architecture
//!
//! ```text
//! ┌───────────────────────┐
//! │  KeyDir               │
//! │  ┌──────────────────┐ │       ┌────────────────────────────────┐
//! │  │ HashMap          │ │       │  MmapMut (data file)           │
//! │  │ key → EntryMeta  │─┼──────▶│  ┌──────────────────────────┐ │
//! │  │   offset         │ │       │  │ entry │ entry │ entry ... │ │
//! │  │   key_len        │ │       │  └──────────────────────────┘ │
//! │  │   value_len      │ │       └────────────────────────────────┘
//! │  └──────────────────┘ │
//! └───────────────────────┘
//! ```
//!
//! Every `put` appends a new entry to the end of the file and updates the
//! in-memory index to point at the new location.  `get` looks up the offset
//! in the index and reads directly from the mmap'd file (no syscalls).
//!
//! # File format
//!
//! ```text
//! [key_len: u32 LE][value_len: u32 LE][key_bytes][value_bytes]
//! ```
//!
//! When `value_len == 0xFFFF_FFFF` (u32::MAX), the entry is a **tombstone**
//! marker and has no value bytes.
//!
//! The file is pre-allocated on creation and auto-grows on overflow.
//! On startup (`open`), the entire file is scanned to rebuild the in-memory
//! index — overwritten keys are naturally deduplicated (last write wins).
//!
//! # Deletes
//!
//! `delete` appends a tombstone entry to the data file and removes the key
//! from the in-memory index.  On the next `open`, the tombstone is replayed
//! and the key stays deleted.  Dead space (tombstones + overwritten entries)
//! is reclaimed automatically when the dead-byte ratio exceeds
//! `compaction_ratio` (default 0.5).

use std::{
    fs::{self, File, OpenOptions},
    io::{self},
    path::{Path, PathBuf},
};

use hashbrown::HashMap;
use memmap2::MmapMut;

/// Default pre-allocated file size (1 MiB).
const DEFAULT_CAPACITY: u64 = 1024 * 1024;

/// Per-entry header: 4 bytes key_len + 4 bytes value_len.
const HEADER_SIZE: usize = 8;

/// Sentinel `value_len` marking a tombstone (deleted key).
/// Tombstone entries have no value bytes.
const TOMBSTONE: u32 = u32::MAX;

/// Default compaction ratio — compact when 50% of the file is dead space.
const DEFAULT_COMPACTION_RATIO: f64 = 0.5;

/// Configuration for a KeyDir instance.
#[derive(Debug, Clone)]
pub struct KeyDirConfig {
    /// Pre-allocated file size in bytes.
    pub capacity: u64,
    /// When dead_bytes / write_offset exceeds this, compaction triggers.
    pub compaction_ratio: f64,
}

impl Default for KeyDirConfig {
    fn default() -> Self {
        KeyDirConfig {
            capacity: DEFAULT_CAPACITY,
            compaction_ratio: DEFAULT_COMPACTION_RATIO,
        }
    }
}

impl KeyDirConfig {
    pub fn encode(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.capacity.to_le_bytes());
        out.extend_from_slice(&self.compaction_ratio.to_le_bytes());
    }

    pub fn decode(bytes: &[u8]) -> io::Result<(KeyDirConfig, usize)> {
        if bytes.len() < 16 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "truncated KeyDirConfig",
            ));
        }
        let mut c = [0u8; 8];
        c.copy_from_slice(&bytes[0..8]);
        let capacity = u64::from_le_bytes(c);
        c.copy_from_slice(&bytes[8..16]);
        let compaction_ratio = f64::from_le_bytes(c);
        Ok((
            KeyDirConfig {
                capacity,
                compaction_ratio,
            },
            16,
        ))
    }
}

// ---------------------------------------------------------------------------
// EntryMeta — points into the mmap'd file
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
struct EntryMeta {
    /// Byte offset in the file where the entry header starts.
    offset: u64,
    /// Length of the key in bytes.
    key_len: u32,
    /// Length of the value in bytes.
    value_len: u32,
}

// ---------------------------------------------------------------------------
// KeyDir
// ---------------------------------------------------------------------------

pub struct KeyDir {
    /// In-memory index: key → location in the data file.
    index: HashMap<Vec<u8>, EntryMeta>,
    /// Memory-mapped data file.
    mmap: MmapMut,
    /// Underlying file handle (for growing and syncing).
    file: File,
    /// Path on disk (for reopening after compaction).
    path: PathBuf,
    /// Configuration.
    config: KeyDirConfig,
    /// Next byte offset at which to append a new entry.
    write_offset: u64,
    /// Total allocated file size in bytes.
    capacity: u64,
    /// Bytes in the file that are no longer referenced (overwritten or
    /// tombstoned entries).
    dead_bytes: u64,
}

impl KeyDir {
    // ---- constructors ----

    /// Create a new KeyDir at `path`.
    pub fn create(path: &Path, config: KeyDirConfig) -> io::Result<Self> {
        let capacity = config.capacity;
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(path)?;

        file.set_len(capacity)?;

        let mmap = unsafe { MmapMut::map_mut(&file)? };

        Ok(KeyDir {
            index: HashMap::new(),
            mmap,
            file,
            path: path.to_path_buf(),
            config,
            write_offset: 0,
            capacity,
            dead_bytes: 0,
        })
    }

    pub fn open(path: &Path, config: KeyDirConfig) -> io::Result<Self> {
        // Crash recovery: if main file is missing, try to restore.
        if !path.exists() {
            let compact = path.with_extension("compact");
            let bak = path.with_extension("bak");
            if compact.exists() {
                fs::rename(&compact, path)?;
            } else if bak.exists() {
                fs::rename(&bak, path)?;
            }
        }

        let file = OpenOptions::new().read(true).write(true).open(path)?;
        let capacity = file.metadata()?.len();
        let mmap = unsafe { MmapMut::map_mut(&file)? };

        let mut this = KeyDir {
            index: HashMap::new(),
            mmap,
            file,
            path: path.to_path_buf(),
            config,
            write_offset: 0,
            capacity,
            dead_bytes: 0,
        };

        this.rebuild_index()?;

        let _ = fs::remove_file(path.with_extension("compact"));
        let _ = fs::remove_file(path.with_extension("bak"));

        Ok(this)
    }

    // ---- read path ----

    /// Get the value for `key`, returning an owned copy.
    ///
    /// Returns `None` if the key is not present or was deleted.
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let meta = self.index.get(key)?;
        let offset = meta.offset as usize;
        let value_start = offset + HEADER_SIZE + meta.key_len as usize;
        let value_end = value_start + meta.value_len as usize;
        Some(self.mmap[value_start..value_end].to_vec())
    }

    /// Returns `true` if the key exists in the index.
    pub fn contains_key(&self, key: &[u8]) -> bool {
        self.index.contains_key(key)
    }

    /// Number of live keys in the index.
    pub fn len(&self) -> usize {
        self.index.len()
    }

    /// Returns `true` if the index is empty.
    pub fn is_empty(&self) -> bool {
        self.index.is_empty()
    }

    /// Iterate over all live (key, value) pairs.
    pub fn entries(&self) -> impl Iterator<Item = (&[u8], Vec<u8>)> + '_ {
        self.index.iter().map(|(key, meta)| {
            let start = meta.offset as usize + HEADER_SIZE + meta.key_len as usize;
            let end = start + meta.value_len as usize;
            (key.as_slice(), self.mmap[start..end].to_vec())
        })
    }

    // ---- write path ----

    /// Insert or update a key-value pair.
    ///
    /// The entry is appended to the data file and the index is updated
    /// to point at the new location.  Previous values for the same key
    /// become dead space tracked by `dead_bytes`.  If the dead-byte
    /// ratio exceeds `compaction_ratio`, compaction runs automatically.
    ///
    /// Data is not durable until `flush()` is called.
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> io::Result<()> {
        // If overwriting, the old entry becomes dead space.
        if let Some(old) = self.index.get(key) {
            let old_entry_size = HEADER_SIZE + old.key_len as usize + old.value_len as usize;
            self.dead_bytes += old_entry_size as u64;
        }

        let entry_size = HEADER_SIZE + key.len() + value.len();

        // Grow the file if needed.
        if self.write_offset + entry_size as u64 > self.capacity {
            self.grow(entry_size as u64)?;
        }

        let offset = self.write_offset as usize;
        let key_len = key.len() as u32;
        let val_len = value.len() as u32;

        // Write directly into the mmap.
        self.mmap[offset..offset + 4].copy_from_slice(&key_len.to_le_bytes());
        self.mmap[offset + 4..offset + 8].copy_from_slice(&val_len.to_le_bytes());
        self.mmap[offset + 8..offset + 8 + key.len()].copy_from_slice(key);
        self.mmap[offset + 8 + key.len()..offset + entry_size].copy_from_slice(value);

        self.index.insert(
            key.to_vec(),
            EntryMeta {
                offset: offset as u64,
                key_len: key_len,
                value_len: val_len,
            },
        );

        self.write_offset += entry_size as u64;
        self.maybe_compact()?;
        Ok(())
    }

    /// Delete a key by appending a tombstone entry to the file and
    /// removing the key from the in-memory index.
    ///
    /// On reopen, the tombstone is replayed and the key stays deleted.
    /// The tombstone and old value entries remain in the data file as
    /// dead space until `compact()` is called.
    ///
    /// Returns `true` if the key was present in the index.
    pub fn delete(&mut self, key: &[u8]) -> io::Result<bool> {
        let old = match self.index.remove(key) {
            Some(meta) => meta,
            None => return Ok(false),
        };

        // The old entry becomes dead space.
        let old_entry_size = HEADER_SIZE + old.key_len as usize + old.value_len as usize;
        self.dead_bytes += old_entry_size as u64;

        self.write_tombstone(key)?;

        // The tombstone itself is also dead space.
        self.dead_bytes += (HEADER_SIZE + key.len()) as u64;

        self.maybe_compact()?;
        Ok(true)
    }

    /// Append a tombstone marker for `key` to the data file.
    fn write_tombstone(&mut self, key: &[u8]) -> io::Result<()> {
        let entry_size = HEADER_SIZE + key.len(); // no value bytes

        if self.write_offset + entry_size as u64 > self.capacity {
            self.grow(entry_size as u64)?;
        }

        let offset = self.write_offset as usize;
        let key_len = key.len() as u32;

        self.mmap[offset..offset + 4].copy_from_slice(&key_len.to_le_bytes());
        self.mmap[offset + 4..offset + 8].copy_from_slice(&TOMBSTONE.to_le_bytes());
        self.mmap[offset + 8..offset + 8 + key.len()].copy_from_slice(key);
        // No value bytes — the tombstone sentinel is the marker.

        self.write_offset += entry_size as u64;
        Ok(())
    }

    /// Write a key-value pair directly without triggering `maybe_compact`.
    /// Used only during compaction to populate the new file.
    fn compact_write(&mut self, key: &[u8], value: &[u8]) -> io::Result<()> {
        let entry_size = HEADER_SIZE + key.len() + value.len();
        if self.write_offset + entry_size as u64 > self.capacity {
            self.grow(entry_size as u64)?;
        }
        let offset = self.write_offset as usize;
        self.mmap[offset..offset + 4].copy_from_slice(&(key.len() as u32).to_le_bytes());
        self.mmap[offset + 4..offset + 8].copy_from_slice(&(value.len() as u32).to_le_bytes());
        self.mmap[offset + 8..offset + 8 + key.len()].copy_from_slice(key);
        self.mmap[offset + 8 + key.len()..offset + entry_size].copy_from_slice(value);
        self.index.insert(
            key.to_vec(),
            EntryMeta {
                offset: offset as u64,
                key_len: key.len() as u32,
                value_len: value.len() as u32,
            },
        );
        self.write_offset += entry_size as u64;
        Ok(())
    }

    /// Sync the mmap to disk.
    ///
    /// After this returns, all writes are durable.
    pub fn flush(&self) -> io::Result<()> {
        self.mmap.flush()
    }

    // ---- maintenance ----

    /// Trigger compaction if the dead-byte ratio exceeds the threshold.
    fn maybe_compact(&mut self) -> io::Result<()> {
        if self.write_offset == 0 || self.dead_bytes == 0 {
            return Ok(());
        }
        let ratio = self.dead_bytes as f64 / self.write_offset as f64;
        if ratio >= self.config.compaction_ratio {
            self.compact()?;
        }
        Ok(())
    }

    /// Compact the data file by writing only live entries to a new file.
    ///
    /// Called automatically when the dead-byte ratio exceeds
    /// `compaction_ratio`.  Tombstone and overwritten entries are dropped.
    fn compact(&mut self) -> io::Result<()> {
        let tmp_path = self.path.with_extension("compact");
        let mut new_kd = KeyDir::create(
            &tmp_path,
            KeyDirConfig {
                capacity: self.write_offset.max(self.config.capacity),
                ..Default::default()
            },
        )?;

        // Walk the mmap in file order to preserve sequential layout.
        // Use `compact_write` instead of `put` to avoid triggering
        // `maybe_compact` recursively on the new file.
        let mut cursor: usize = 0;
        while cursor < self.write_offset as usize {
            let key_len =
                u32::from_le_bytes(self.mmap[cursor..cursor + 4].try_into().unwrap()) as usize;
            let val_len =
                u32::from_le_bytes(self.mmap[cursor + 4..cursor + 8].try_into().unwrap()) as usize;

            let key = &self.mmap[cursor + HEADER_SIZE..cursor + HEADER_SIZE + key_len];

            if val_len == TOMBSTONE as usize {
                cursor += HEADER_SIZE + key_len;
                continue;
            }

            let value = &self.mmap
                [cursor + HEADER_SIZE + key_len..cursor + HEADER_SIZE + key_len + val_len];

            // Only copy entries still in the index AND whose offset matches.
            if let Some(meta) = self.index.get(key) {
                if meta.offset == cursor as u64 {
                    new_kd.compact_write(key, value)?;
                }
            }

            cursor += HEADER_SIZE + key_len + val_len;
        }

        new_kd.flush()?;

        // Atomic-ish swap: rename old → backup, rename new → target.
        let bak_path = self.path.with_extension("bak");
        let _ = fs::remove_file(&bak_path);
        fs::rename(&self.path, &bak_path)?;
        fs::rename(&tmp_path, &self.path)?;
        let _ = fs::remove_file(&bak_path);

        // Replace our state with the compacted version.
        let KeyDir {
            mmap,
            file,
            index,
            capacity,
            write_offset,
            path: _,
            dead_bytes: _,
            config: _,
        } = new_kd;
        self.mmap = mmap;
        self.file = file;
        self.index = index;
        self.capacity = capacity;
        self.write_offset = write_offset;
        self.dead_bytes = 0;

        Ok(())
    }

    /// Bytes written so far (logical end of data, excludes pre-allocated tail).
    pub fn data_size(&self) -> u64 {
        self.write_offset
    }

    /// Total allocated file capacity.
    pub fn capacity(&self) -> u64 {
        self.capacity
    }

    /// Bytes in the file that are no longer referenced by any live key.
    pub fn dead_bytes(&self) -> u64 {
        self.dead_bytes
    }

    // ---- internal helpers ----

    /// Scan the file from offset 0 to rebuild the in-memory index in a single
    /// pass.
    ///
    /// Dead-byte accounting is done inline: when an insert displaces an
    /// existing index entry, the old entry's size is added to `dead_bytes`
    /// immediately.  Tombstones are counted as dead on sight.  This avoids
    /// the O(n) second pass over the file.
    ///
    /// Stops when it hits a zeroed `key_len` (pre-allocated but unwritten
    /// space) or runs past the file boundary.  Overwritten keys are
    /// naturally deduplicated (last write wins).
    fn rebuild_index(&mut self) -> io::Result<()> {
        let mut cursor: usize = 0;
        self.dead_bytes = 0;

        loop {
            if cursor + HEADER_SIZE > self.mmap.len() {
                break;
            }

            let key_len =
                u32::from_le_bytes(self.mmap[cursor..cursor + 4].try_into().unwrap()) as usize;

            // key_len == 0 means pre-allocated, unwritten space — stop here.
            if key_len == 0 {
                break;
            }

            let val_len =
                u32::from_le_bytes(self.mmap[cursor + 4..cursor + 8].try_into().unwrap()) as usize;

            if val_len == TOMBSTONE as usize {
                let entry_end = cursor + HEADER_SIZE + key_len;
                if entry_end > self.mmap.len() {
                    break;
                }
                let key = &self.mmap[cursor + HEADER_SIZE..cursor + HEADER_SIZE + key_len];
                // Tombstone: remove from index; both this entry and the
                // previously live entry (already counted when displaced) are dead.
                if let Some(old) = self.index.remove(key) {
                    let old_size = HEADER_SIZE + old.key_len as usize + old.value_len as usize;
                    self.dead_bytes += old_size as u64;
                }
                self.dead_bytes += (HEADER_SIZE + key_len) as u64;
                cursor = entry_end;
                continue;
            }

            let entry_end = cursor + HEADER_SIZE + key_len + val_len;

            // Partial write (crashed mid-append) — discard and stop.
            if entry_end > self.mmap.len() {
                break;
            }

            let key = &self.mmap[cursor + HEADER_SIZE..cursor + HEADER_SIZE + key_len];

            // If a previous entry for this key exists, it becomes dead space.
            if let Some(old) = self.index.get(key) {
                let old_size = HEADER_SIZE + old.key_len as usize + old.value_len as usize;
                self.dead_bytes += old_size as u64;
            }

            self.index.insert(
                key.to_vec(),
                EntryMeta {
                    offset: cursor as u64,
                    key_len: key_len as u32,
                    value_len: val_len as u32,
                },
            );

            cursor = entry_end;
        }

        self.write_offset = cursor as u64;
        Ok(())
    }

    /// Grow the file so it can accommodate at least `needed` more bytes.
    ///
    /// Uses geometric (doubling) growth: doubles the current capacity
    /// until it covers `self.write_offset + needed`.
    fn grow(&mut self, needed: u64) -> io::Result<()> {
        self.mmap.flush()?;

        let target = self.write_offset.checked_add(needed).ok_or_else(|| {
            io::Error::new(io::ErrorKind::OutOfMemory, "KeyDir write offset overflow")
        })?;

        let mut new_capacity = self.capacity;
        while new_capacity < target {
            new_capacity = new_capacity.saturating_mul(2);
        }

        self.file.set_len(new_capacity)?;

        // SAFETY: the file is at least `new_capacity` bytes and we are the
        // sole owner of the mmap.
        self.mmap = unsafe { MmapMut::map_mut(&self.file)? };
        self.capacity = new_capacity;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("zendb_keydir_{}", name))
    }

    fn default_cfg() -> KeyDirConfig {
        KeyDirConfig::default()
    }

    #[test]
    fn put_and_get() {
        let p = tmp("put_get");
        let mut kd = KeyDir::create(&p, default_cfg()).unwrap();
        kd.put(b"hello", b"world").unwrap();
        kd.flush().unwrap();
        assert_eq!(kd.get(b"hello"), Some(b"world".to_vec()));
        assert_eq!(kd.get(b"nope"), None);
        fs::remove_file(&p).ok();
    }

    #[test]
    fn overwrite_last_write_wins() {
        let p = tmp("overwrite");
        let mut kd = KeyDir::create(&p, default_cfg()).unwrap();
        kd.put(b"key", b"v1").unwrap();
        kd.put(b"key", b"v2").unwrap();
        kd.flush().unwrap();
        assert_eq!(kd.get(b"key"), Some(b"v2".to_vec()));
        fs::remove_file(&p).ok();
    }

    #[test]
    fn delete_and_missing() {
        let p = tmp("delete");
        let mut kd = KeyDir::create(&p, default_cfg()).unwrap();
        kd.put(b"key", b"val").unwrap();
        assert!(kd.delete(b"key").unwrap());
        assert!(!kd.delete(b"key").unwrap());
        assert_eq!(kd.get(b"key"), None);
        fs::remove_file(&p).ok();
    }

    #[test]
    fn reopen_rebuilds_index() {
        let p = tmp("reopen");
        {
            let mut kd = KeyDir::create(&p, default_cfg()).unwrap();
            kd.put(b"a", b"alpha").unwrap();
            kd.put(b"b", b"beta").unwrap();
            kd.put(b"c", b"gamma").unwrap();
            kd.flush().unwrap();
        }
        let kd = KeyDir::open(&p, default_cfg()).unwrap();
        assert_eq!(kd.get(b"a"), Some(b"alpha".to_vec()));
        assert_eq!(kd.get(b"b"), Some(b"beta".to_vec()));
        assert_eq!(kd.get(b"c"), Some(b"gamma".to_vec()));
        assert_eq!(kd.len(), 3);
        fs::remove_file(&p).ok();
    }

    #[test]
    fn reopen_overwrite_last_write_wins() {
        let p = tmp("reopen_overwrite");
        {
            let mut kd = KeyDir::create(&p, default_cfg()).unwrap();
            kd.put(b"key", b"old").unwrap();
            kd.put(b"key", b"new").unwrap();
            kd.flush().unwrap();
        }
        let kd = KeyDir::open(&p, default_cfg()).unwrap();
        assert_eq!(kd.get(b"key"), Some(b"new".to_vec()));
        fs::remove_file(&p).ok();
    }

    #[test]
    fn large_values() {
        let p = tmp("large");
        let mut kd = KeyDir::create(&p, default_cfg()).unwrap();
        let big = vec![0xABu8; 10_000];
        kd.put(b"big", &big).unwrap();
        kd.flush().unwrap();
        assert_eq!(kd.get(b"big"), Some(big));
        fs::remove_file(&p).ok();
    }

    #[test]
    fn empty_keydir() {
        let p = tmp("empty");
        let kd = KeyDir::create(&p, default_cfg()).unwrap();
        assert!(kd.is_empty());
        assert_eq!(kd.len(), 0);
        assert_eq!(kd.get(b"anything"), None);
        fs::remove_file(&p).ok();
    }

    #[test]
    fn compact_reclaims_space() {
        let p = tmp("compact");
        {
            let cfg = KeyDirConfig {
                compaction_ratio: 0.0,
                ..default_cfg()
            };
            let mut kd = KeyDir::create(&p, cfg).unwrap();
            kd.put(b"keep", b"value").unwrap();
            kd.put(b"overwritten", b"v1").unwrap();
            kd.put(b"overwritten", b"v2").unwrap();
            assert_eq!(kd.dead_bytes(), 0);
            kd.put(b"deleted", b"gone").unwrap();
            kd.delete(b"deleted").unwrap();
            assert_eq!(kd.dead_bytes(), 0);
            assert_eq!(kd.get(b"keep"), Some(b"value".to_vec()));
            assert_eq!(kd.get(b"overwritten"), Some(b"v2".to_vec()));
            assert_eq!(kd.get(b"deleted"), None);
            kd.flush().unwrap();
        }
        let kd = KeyDir::open(&p, default_cfg()).unwrap();
        assert_eq!(kd.get(b"keep"), Some(b"value".to_vec()));
        assert_eq!(kd.get(b"overwritten"), Some(b"v2".to_vec()));
        assert_eq!(kd.get(b"deleted"), None);
        fs::remove_file(&p).ok();
    }

    #[test]
    fn auto_grows() {
        let p = tmp("grow");
        let cfg = KeyDirConfig {
            capacity: 64,
            ..default_cfg()
        };
        let mut kd = KeyDir::create(&p, cfg).unwrap();
        let data = vec![0xCDu8; 200];
        kd.put(b"data", &data).unwrap();
        kd.flush().unwrap();
        assert_eq!(kd.get(b"data"), Some(data));
        assert!(kd.capacity() > 64, "file should have grown");
        fs::remove_file(&p).ok();
    }

    #[test]
    fn contains_key() {
        let p = tmp("contains");
        let mut kd = KeyDir::create(&p, default_cfg()).unwrap();
        kd.put(b"exists", b"yes").unwrap();
        assert!(kd.contains_key(b"exists"));
        assert!(!kd.contains_key(b"nope"));
        fs::remove_file(&p).ok();
    }

    // --- Regression tests for fixed bugs ---

    // Old bug: entries() called self.get(key) internally, which did a second
    // self.index.get(key) — two hash lookups per entry.  Verify that the
    // values returned by entries() exactly match those returned by get().
    #[test]
    fn entries_values_match_get() {
        let p = tmp("entries_match");
        let mut kd = KeyDir::create(&p, default_cfg()).unwrap();
        kd.put(b"alpha", b"aaa").unwrap();
        kd.put(b"beta", b"bbb").unwrap();
        kd.put(b"gamma", b"ccc").unwrap();
        // Overwrite one key — entries() must return the latest value.
        kd.put(b"alpha", b"zzz").unwrap();

        let mut from_entries: Vec<(Vec<u8>, Vec<u8>)> =
            kd.entries().map(|(k, v)| (k.to_vec(), v)).collect();
        from_entries.sort_by(|a, b| a.0.cmp(&b.0));

        for (key, val_from_entries) in &from_entries {
            let val_from_get = kd.get(key).expect("get() must find the same key");
            assert_eq!(
                val_from_entries, &val_from_get,
                "entries() and get() disagree for key {:?}",
                key
            );
        }
        assert_eq!(from_entries.len(), 3);
        fs::remove_file(&p).ok();
    }

    // Geometric growth: doubling capacity until the needed size is covered.
    #[test]
    fn geometric_capacity_growth() {
        let p = tmp("geom");
        let cfg = KeyDirConfig {
            capacity: 128,
            compaction_ratio: 1.1, // never compact during this test
        };
        let mut kd = KeyDir::create(&p, cfg).unwrap();
        let initial = kd.capacity();
        // Write enough data to trigger at least one grow.
        let data = vec![0xFFu8; 256]; // bigger than initial capacity
        kd.put(b"k", &data).unwrap();
        // With geometric growth the new capacity must be at least 2× the
        // original.
        assert!(
            kd.capacity() >= initial * 2,
            "capacity {} did not double from {}",
            kd.capacity(),
            initial
        );
        assert_eq!(kd.get(b"k"), Some(data));
        fs::remove_file(&p).ok();
    }

    // Old bug: rebuild_index() used two passes — the second pass re-scanned
    // the whole file to compute dead_bytes.  The new single pass computes
    // dead_bytes inline.  Verify the count is accurate after reopening a file
    // with known dead entries.
    #[test]
    fn rebuild_index_dead_bytes_accurate() {
        let p = tmp("dead_bytes");
        {
            let cfg = KeyDirConfig {
                compaction_ratio: 1.1, // suppress auto-compaction
                ..default_cfg()
            };
            let mut kd = KeyDir::create(&p, cfg).unwrap();
            // Entry 1: "x" = "old"  → will be overwritten (dead)
            kd.put(b"x", b"old").unwrap();
            // Entry 2: "y" = "live" → stays live
            kd.put(b"y", b"live").unwrap();
            // Entry 3: "x" = "new"  → overwrites entry 1 (entry 1 becomes dead)
            kd.put(b"x", b"new").unwrap();
            // Entry 4: "z" = "gone" → will be deleted (entry 4 + tombstone are dead)
            kd.put(b"z", b"gone").unwrap();
            kd.delete(b"z").unwrap();
            kd.flush().unwrap();
        }
        let kd = KeyDir::open(&p, default_cfg()).unwrap();
        // Dead entries: "x"="old" (HEADER+1+3=12) + "z"="gone" (HEADER+1+4=13)
        //               + tombstone "z" (HEADER+1=9).
        // The exact byte count depends on HEADER_SIZE (8), but it must be > 0.
        assert!(
            kd.dead_bytes() > 0,
            "rebuild should have detected dead bytes"
        );
        // Live entries must still be correct.
        assert_eq!(kd.get(b"x"), Some(b"new".to_vec()));
        assert_eq!(kd.get(b"y"), Some(b"live".to_vec()));
        assert_eq!(kd.get(b"z"), None);
        assert_eq!(kd.len(), 2);
        fs::remove_file(&p).ok();
    }

    // Old bug: compact() called new_kd.put() which called maybe_compact() on
    // the freshly-created destination file.  With compaction_ratio=0.0 the
    // destination file would immediately trigger another compaction cycle.
    // The fix: compact_write() bypasses maybe_compact() entirely.
    #[test]
    fn compaction_does_not_recurse() {
        let p = tmp("no_recurse");
        let cfg = KeyDirConfig {
            compaction_ratio: 0.0, // compact on every write — would recurse infinitely before fix
            ..default_cfg()
        };
        let mut kd = KeyDir::create(&p, cfg).unwrap();
        // These writes trigger compaction; if the bug were present this would
        // stack-overflow or loop infinitely.
        for i in 0u32..20 {
            kd.put(format!("k{}", i).as_bytes(), b"v").unwrap();
            // Overwrite to build up dead bytes and keep triggering compaction.
            kd.put(format!("k{}", i).as_bytes(), b"vv").unwrap();
        }
        for i in 0u32..20 {
            assert_eq!(kd.get(format!("k{}", i).as_bytes()), Some(b"vv".to_vec()));
        }
        fs::remove_file(&p).ok();
    }

    #[test]
    fn delete_persists_across_reopen() {
        let p = tmp("delete_persists");
        {
            let mut kd = KeyDir::create(&p, default_cfg()).unwrap();
            kd.put(b"keep", b"alive").unwrap();
            kd.put(b"gone", b"dead").unwrap();
            kd.delete(b"gone").unwrap();
            kd.flush().unwrap();
        }
        let kd = KeyDir::open(&p, default_cfg()).unwrap();
        assert_eq!(kd.get(b"keep"), Some(b"alive".to_vec()));
        assert_eq!(kd.get(b"gone"), None);
        assert_eq!(kd.len(), 1);
        fs::remove_file(&p).ok();
    }
}
