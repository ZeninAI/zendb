//! KeyDir — persistent key-value store backed by an in-memory hash index
//! and a memory-mapped append-only data file (Bitcask model).
//!
//! # Architecture
//!
//! The in-memory hash index maps each key to its on-disk location
//! (offset + sizes). Every write appends, never mutates in place.
//!
//! # Public surface
//!
//! Aside from the constructors [`KeyDir::create`] and [`KeyDir::open`],
//! every callable is exposed through the [`Backend`](crate::core::backend::Backend)
//! trait — there are no inherent `pub` accessors that duplicate it.
//! Callers should `use crate::core::backend::Backend;` and reach the
//! operations through that trait.
//!
//! # Writing
//!
//! Generic over `K` (key) and `V` (value). Uses bincode 2 for both
//! directions:
//! - **Write**: [`serialize_into`] writes the encoded bytes directly into
//!   the mmap (no intermediate `Vec<u8>` allocation). `write_entry`
//!   measures both halves up front and grows the file at most once per
//!   `put`, even when the new record straddles the previous capacity.
//! - **Read**: [`deserialize_from`] materializes an owned `V` from the
//!   mmap'd bytes. Bincode is not a zero-copy format, so reads allocate.
//! - **Delete**: the tombstone reuses the *already-encoded* key bytes
//!   that live in the doomed entry's slot — no bincode encode, no scratch
//!   `Vec`. Just a `copy_within` of `key_size` bytes into the new slot.
//! - **Compact**: rewrites the file **in place** with a single forward
//!   sweep of `copy_within` calls and an optional `set_len` to release
//!   the physical slack. No tmp file, no `fs::rename`.
//!
//! # Dead bytes & compaction
//!
//! Overwrites and deletes leave "dead" bytes in the append-only file.
//! When the dead-byte ratio crosses `compaction_ratio`, the file is
//! compacted in place — live records slide forward over the dead space,
//! the file is truncated to the new tail (clamped to `initial_capacity`),
//! and the index offsets are rewritten. `compaction_ratio = 0.0` compacts
//! after every write; `1.0` disables automatic compaction.
//!
//! # File format
//!
//! Header (4 bytes):
//! ```text
//! [MAGIC: u32 LE = 0x4452494B ("KIRD")]
//! ```
//!
//! Live entry:
//! ```text
//! [value_size: u32 LE][V bytes][key_size: u32 LE][K bytes]
//! ```
//! Tombstone:
//! ```text
//! [0xFFFF_FFFF: u32][key_size: u32 LE][K bytes]
//! ```
//!
//! A tombstone is a special entry with `value_size = u32::MAX` (no value
//! bytes). It marks a key as deleted. During `rebuild_index`, tombstones
//! remove keys from the in-memory index; during compaction, they are
//! stripped entirely.
//!
//! # Stats / counters
//!
//! [`KeyDirStats`] tracks only the two counters that *can't* be derived
//! from anything else — `data_size` (next write position) and `dead_bytes`
//! (compaction trigger). The live entry count is **not** stored: it's
//! read straight from `index.len()` via [`Backend::size`]. That means
//! there's no separate counter to update on every mutation, and no
//! `refresh_stats()` call to forget to make.

use std::{
    fmt,
    fs::{File, OpenOptions},
    hash::Hash,
    io::{self},
    marker::PhantomData,
    path::Path,
};

use bincode::{Decode, Encode};
use hashbrown::HashMap;
use memmap2::MmapMut;

use crate::core::backend::Backend;
use crate::utils::serdes::{deserialize_from, read_u32_le, serialize_into, serialized_size};

const DEFAULT_INITIAL_CAPACITY: u64 = 1024 * 1024;
const DEFAULT_COMPACTION_RATIO: f64 = 0.5;
/// Magic number identifying a KeyDir file. Stored at offset 0 as `u32` LE.
/// ASCII: `"KIRD"`.
const MAGIC: u32 = 0x4452494B;
/// File header size in bytes: `[MAGIC: u32 LE]`. Entry data follows immediately.
const HEADER_SIZE: usize = 4;
const TOMBSTONE: u32 = u32::MAX;

#[derive(Debug, Clone, Encode, Decode)]
pub struct KeyDirConfig {
    pub initial_capacity: u64,
    /// Auto-compaction threshold. Callers must pass a value in `[0.0, 1.0]`:
    /// `0.0` compacts after every write, `1.0` disables automatic compaction.
    pub compaction_ratio: f64,
}

impl Default for KeyDirConfig {
    fn default() -> Self {
        KeyDirConfig {
            initial_capacity: DEFAULT_INITIAL_CAPACITY,
            compaction_ratio: DEFAULT_COMPACTION_RATIO,
        }
    }
}

/// Append-log accounting. The live entry count is **not** tracked here —
/// it comes straight from `index.len()` via `Backend::size`, so there's
/// no separate counter to keep in sync on every mutation.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct KeyDirStats {
    pub data_size: u64,
    pub dead_bytes: u64,
}

/// Serialized sizes and offset of an entry in the mmap. Kept compact so the
/// in-memory HashMap stays lean.
#[derive(Debug, Clone, Copy)]
struct EntryMeta {
    offset: u64,
    value_size: u32,
    key_size: u32,
}

impl EntryMeta {
    /// Total bytes the entry occupies on disk: two u32 length prefixes (8)
    /// plus the encoded key and value bytes.
    fn on_disk_size(&self) -> u64 {
        8 + self.value_size as u64 + self.key_size as u64
    }
}

pub struct KeyDir<K, V> {
    index: HashMap<K, EntryMeta>,
    mmap: MmapMut,
    file: File,
    config: KeyDirConfig,
    stats: KeyDirStats,
    _phantom: PhantomData<V>,
}

impl<K, V> KeyDir<K, V>
where
    K: Encode + Decode<()> + Hash + Eq + Clone,
    V: Encode + Decode<()> + Clone,
{
    /// Create a fresh KeyDir at `path`, **truncating** any existing
    /// file. Pre-allocates `config.initial_capacity` bytes and stamps
    /// the file header MAGIC at offset 0. The in-memory index is empty.
    pub fn create(path: &Path, config: KeyDirConfig) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(path)?;

        file.set_len(config.initial_capacity)?;
        let mut mmap = unsafe { MmapMut::map_mut(&file)? };

        // Write file header: MAGIC at offset 0.
        mmap[0..4].copy_from_slice(&MAGIC.to_le_bytes());

        Ok(KeyDir {
            index: HashMap::new(),
            mmap,
            file,
            config,
            stats: KeyDirStats {
                data_size: HEADER_SIZE as u64,
                dead_bytes: 0,
            },
            _phantom: PhantomData,
        })
    }

    /// Open an existing KeyDir at `path`. Validates the MAGIC header
    /// (returns `InvalidData` if missing/mismatched) and rebuilds the
    /// in-memory index by scanning every live entry and tombstone in
    /// the file from `HEADER_SIZE` onward.
    pub fn open(path: &Path, config: KeyDirConfig) -> io::Result<Self> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        let mmap = unsafe { MmapMut::map_mut(&file)? };

        // Validate the file header MAGIC.
        let file_magic = u32::from_le_bytes(mmap[0..4].try_into().unwrap());
        if file_magic != MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "not a KeyDir file",
            ));
        }

        let mut this = KeyDir {
            index: HashMap::new(),
            mmap,
            file,
            config,
            stats: KeyDirStats::default(),
            _phantom: PhantomData,
        };

        this.rebuild_index()?;
        Ok(this)
    }

    // -----------------------------------------------------------------------
    // Private helpers — everything below this point is implementation
    // detail. The Backend trait impl at the bottom of the file is what
    // callers reach through.
    // -----------------------------------------------------------------------

    /// Raw value bytes for the entry pointed at by `meta` — borrowed
    /// directly from the mmap, **not** including the leading length prefix.
    fn value_bytes(&self, meta: &EntryMeta) -> &[u8] {
        let start = meta.offset as usize + 4;
        let end = start + meta.value_size as usize;
        &self.mmap[start..end]
    }

    /// Append `[vlen u32][V][klen u32][K]` at `stats.data_size`. Measures
    /// both halves up front and `grow`s the file **at most once** — the
    /// previous implementation could grow twice when the value pushed
    /// past the boundary and then the key did too.
    fn write_entry(&mut self, key: &K, value: &V) -> io::Result<(u64, u32, u32)> {
        let offset = self.stats.data_size as usize;
        let v_size = serialized_size(value)?;
        let k_size = serialized_size(key)?;
        let total = 8 + v_size + k_size;
        let end = offset
            .checked_add(total)
            .ok_or_else(|| io::Error::new(io::ErrorKind::OutOfMemory, "KeyDir offset overflow"))?;
        if end > self.mmap.len() {
            self.grow(end as u64)?;
        }

        // vlen prefix
        self.mmap[offset..offset + 4].copy_from_slice(&(v_size as u32).to_le_bytes());
        // value bytes
        let v_off = offset + 4;
        let written = serialize_into(value, &mut self.mmap[v_off..v_off + v_size])?;
        debug_assert_eq!(written, v_size);
        // klen prefix
        let k_len_off = v_off + v_size;
        self.mmap[k_len_off..k_len_off + 4].copy_from_slice(&(k_size as u32).to_le_bytes());
        // key bytes
        let k_off = k_len_off + 4;
        let written = serialize_into(key, &mut self.mmap[k_off..k_off + k_size])?;
        debug_assert_eq!(written, k_size);

        self.stats.data_size = end as u64;
        Ok((offset as u64, v_size as u32, k_size as u32))
    }

    /// Append a tombstone for the entry described by `old`, reusing the
    /// already-encoded key bytes already living in the doomed entry's
    /// slot — no bincode encode, no scratch `Vec<u8>`. A single
    /// `copy_within` moves the key into the tombstone payload.
    fn write_tombstone_for(&mut self, old: EntryMeta) -> io::Result<()> {
        let new_offset = self.stats.data_size as usize;
        let key_size = old.key_size as usize;
        let total = 8 + key_size;
        let end = new_offset
            .checked_add(total)
            .ok_or_else(|| io::Error::new(io::ErrorKind::OutOfMemory, "KeyDir offset overflow"))?;
        if end > self.mmap.len() {
            self.grow(end as u64)?;
        }
        // `grow` may remap, but file contents are preserved so the
        // source bytes are still where `old` says they are.
        let key_src_start = old.offset as usize + 4 + old.value_size as usize + 4;

        self.mmap[new_offset..new_offset + 4].copy_from_slice(&TOMBSTONE.to_le_bytes());
        self.mmap[new_offset + 4..new_offset + 8].copy_from_slice(&(key_size as u32).to_le_bytes());
        // src lives strictly before dst (old entry is inside data_size,
        // new_offset is past data_size), so the regions never overlap.
        self.mmap
            .copy_within(key_src_start..key_src_start + key_size, new_offset + 8);

        self.stats.data_size = end as u64;
        self.stats.dead_bytes += 8 + key_size as u64;
        Ok(())
    }

    /// Auto-compaction guard. Reads the configured threshold and, if the
    /// dead-byte ratio is over it, calls the [`Backend::compact`]
    /// implementation. `compact` lives on the trait — no separate
    /// inherent copy of the in-place algorithm.
    fn maybe_compact(&mut self) -> io::Result<()> {
        let threshold = self.config.compaction_ratio;
        if threshold >= 1.0 {
            return Ok(());
        }
        if threshold == 0.0
            || self.stats.dead_bytes as f64 / self.stats.data_size as f64 >= threshold
        {
            self.compact()?;
        }
        Ok(())
    }

    /// Grow the backing file to at least `desired` bytes
    /// (`max(mmap.len() * 2, desired)`). Does **not** msync — `set_len`
    /// + remap don't need prior writes to be durable; the unified page
    /// cache hands them to the new mapping. Saves an msync syscall on
    /// every growth round.
    fn grow(&mut self, desired: u64) -> io::Result<()> {
        let new_capacity = ((self.mmap.len() as u64) * 2).max(desired);
        self.file.set_len(new_capacity)?;
        self.mmap = unsafe { MmapMut::map_mut(&self.file)? };
        Ok(())
    }

    /// Rebuild the in-memory index by scanning the file from `HEADER_SIZE`.
    /// Replays all entries: live entries overwrite prior index entries,
    /// tombstones remove them. Accumulates dead_bytes from each.
    fn rebuild_index(&mut self) -> io::Result<()> {
        let mut cursor: usize = HEADER_SIZE;
        self.stats.dead_bytes = 0;

        while let Some(value_size) = read_u32_le(&self.mmap, cursor) {
            // 0 marks the (zero-filled) tail of the pre-allocated file
            // or the sentinel written by a prior `compact_in_place`.
            if value_size == 0 {
                break;
            }

            let is_tombstone = value_size == TOMBSTONE;
            let key_size_off = if is_tombstone {
                cursor + 4
            } else {
                cursor + 4 + value_size as usize
            };
            let Some(key_size) = read_u32_le(&self.mmap, key_size_off) else {
                break;
            };
            let key_start = key_size_off + 4;
            let entry_end = key_start + key_size as usize;

            let key: K = deserialize_from(&self.mmap[key_start..entry_end])?;

            if is_tombstone {
                if let Some(old) = self.index.remove(&key) {
                    self.stats.dead_bytes += old.on_disk_size();
                }
                self.stats.dead_bytes += 8 + key_size as u64;
            } else {
                let meta = EntryMeta {
                    offset: cursor as u64,
                    value_size,
                    key_size,
                };
                if let Some(old) = self.index.get_mut(&key) {
                    self.stats.dead_bytes += old.on_disk_size();
                    *old = meta;
                } else {
                    self.index.insert(key, meta);
                }
            }
            cursor = entry_end;
        }

        self.stats.data_size = cursor as u64;
        Ok(())
    }
}

impl<K, V> Drop for KeyDir<K, V> {
    fn drop(&mut self) {
        let _ = self.mmap.flush();
    }
}

impl<K, V> fmt::Debug for KeyDir<K, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.stats, f)
    }
}

// ---------------------------------------------------------------------------
// Backend impl — the entirety of KeyDir's public surface besides
// `create`/`open` lives in this block. All the implementations live here
// directly rather than delegating to inherent methods.
// ---------------------------------------------------------------------------

impl<K, V> crate::core::backend::Backend<K, V> for KeyDir<K, V>
where
    K: Encode + Decode<()> + Hash + Eq + Clone,
    V: Encode + Decode<()> + Clone,
{
    type Stats = KeyDirStats;

    /// One HashMap lookup, then materialize the value by decoding the
    /// mmap slice the meta points at.
    fn get(&self, key: &K) -> Option<V> {
        let meta = self.index.get(key)?;
        Some(deserialize_from(self.value_bytes(meta)).expect("valid encoded value"))
    }

    /// HashMap probe — no mmap access, no deserialization. Faster than
    /// `get(key).is_some()`.
    fn contains(&self, key: &K) -> bool {
        self.index.contains_key(key)
    }

    /// Append the new record, then update the index. The hot path takes a
    /// single HashMap lookup via `get_mut` — overwrites update in place;
    /// fresh inserts fall through to a single `insert` call. Either way
    /// only one hash is computed per `put`.
    fn put(&mut self, key: K, value: V) -> io::Result<()> {
        let (offset, value_size, key_size) = self.write_entry(&key, &value)?;
        if let Some(old) = self.index.get_mut(&key) {
            self.stats.dead_bytes += old.on_disk_size();
            old.offset = offset;
            old.value_size = value_size;
            old.key_size = key_size;
        } else {
            self.index.insert(
                key,
                EntryMeta {
                    offset,
                    value_size,
                    key_size,
                },
            );
        }
        self.maybe_compact()
    }

    /// Remove from the index, then write a tombstone using the
    /// already-encoded key bytes that still live in the deleted entry's
    /// slot — see [`write_tombstone_for`](KeyDir::write_tombstone_for).
    /// No bincode encode, no scratch `Vec<u8>`.
    fn delete(&mut self, key: &K) -> io::Result<bool> {
        let Some(old) = self.index.remove(key) else {
            return Ok(false);
        };
        self.stats.dead_bytes += old.on_disk_size();
        self.write_tombstone_for(old)?;
        self.maybe_compact()?;
        Ok(true)
    }

    /// Unified read-modify-write / insert / delete primitive. Writes at
    /// most one new log record. Existing entries are located once before
    /// the closure runs, then the selected branch updates the index.
    fn update<F>(&mut self, key: &K, f: F) -> io::Result<()>
    where
        F: FnOnce(Option<V>) -> Option<V>,
    {
        let existing = self.index.get(key).copied();
        let current: Option<V> = match &existing {
            Some(meta) => Some(deserialize_from(self.value_bytes(meta))?),
            None => None,
        };

        match (existing, f(current)) {
            (Some(old), Some(new_v)) => {
                self.stats.dead_bytes += old.on_disk_size();
                let (offset, value_size, key_size) = self.write_entry(key, &new_v)?;
                *self.index.get_mut(key).unwrap() = EntryMeta {
                    offset,
                    value_size,
                    key_size,
                };
                self.maybe_compact()?;
            }
            (None, Some(new_v)) => {
                let (offset, value_size, key_size) = self.write_entry(key, &new_v)?;
                self.index.insert(
                    key.clone(),
                    EntryMeta {
                        offset,
                        value_size,
                        key_size,
                    },
                );
                self.maybe_compact()?;
            }
            (Some(old), None) => {
                self.stats.dead_bytes += old.on_disk_size();
                self.index.remove(key);
                self.write_tombstone_for(old)?;
                self.maybe_compact()?;
            }
            (None, None) => {}
        }
        Ok(())
    }

    /// Drop every live entry. Resets `data_size` to `HEADER_SIZE` and
    /// zeroes the post-header sentinel so a subsequent open sees an
    /// empty log. The backing file is not truncated.
    fn clear(&mut self) -> io::Result<()> {
        self.index.clear();
        self.stats.data_size = HEADER_SIZE as u64;
        self.stats.dead_bytes = 0;
        // Re-stamp MAGIC at offset 0 (defensive in case the header was
        // overwritten by a prior write that read it back during recovery).
        self.mmap[0..4].copy_from_slice(&MAGIC.to_le_bytes());
        // Zero the byte right after the header so `rebuild_index` knows
        // the log is empty on next open.
        if self.mmap.len() >= HEADER_SIZE + 4 {
            self.mmap[HEADER_SIZE..HEADER_SIZE + 4].copy_from_slice(&0u32.to_le_bytes());
        }
        Ok(())
    }

    /// In-place compaction. Sweeps the mmap forward in offset order,
    /// `copy_within`-ing each live record into its packed position. No
    /// tmp file, no `fs::rename`, one optional `set_len` + remap at the
    /// end to release the physical slack.
    ///
    /// Correctness: entries are sorted by ascending current offset, and
    /// the write cursor (`dst`) is always ≤ `src` at each step, so the
    /// forward shift never destructively overlaps a record we haven't
    /// moved yet (`copy_within` itself uses memmove semantics).
    fn compact(&mut self) -> io::Result<()> {
        // Collect mutable references to every entry so we can update
        // offsets in-place — no drain/reinsert, no key clones.
        let mut snapshot: Vec<(&K, &mut EntryMeta)> = self.index.iter_mut().collect();
        snapshot.sort_by_key(|(_, m)| m.offset);

        let mut cursor: usize = HEADER_SIZE;
        for (_, meta) in snapshot.iter_mut() {
            let src = meta.offset as usize;
            let len = meta.on_disk_size() as usize;
            if src != cursor {
                self.mmap.copy_within(src..src + len, cursor);
            }
            meta.offset = cursor as u64;
            cursor += len;
        }

        // 4-byte sentinel: tells `rebuild_index` to stop here on next open
        // (even if we don't shrink the file).
        if cursor + 4 <= self.mmap.len() {
            self.mmap[cursor..cursor + 4].copy_from_slice(&0u32.to_le_bytes());
        }

        self.stats.data_size = cursor as u64;
        self.stats.dead_bytes = 0;

        // Release the physical slack past the new tail. Keep at least
        // `initial_capacity` so the next batch of writes doesn't
        // immediately re-grow the file.
        //
        // Windows note: `set_len` refuses to shrink a file while a
        // mapped section is open. Swap in a small anonymous mmap as a
        // placeholder so the file's mapped section is dropped before
        // the truncation, then remap onto the resized file.
        let new_capacity = (cursor as u64).max(self.config.initial_capacity);
        self.mmap = MmapMut::map_anon(1)?;
        self.file.set_len(new_capacity)?;
        self.mmap = unsafe { MmapMut::map_mut(&self.file)? };
        Ok(())
    }

    fn keys(&self) -> impl Iterator<Item = K> + '_ {
        self.index.keys().cloned()
    }

    fn values(&self) -> impl Iterator<Item = V> + '_ {
        self.index
            .values()
            .map(|meta| deserialize_from(self.value_bytes(meta)).expect("valid encoded value"))
    }

    fn entries(&self) -> impl Iterator<Item = (K, V)> + '_ {
        self.index.iter().map(|(k, meta)| {
            let v: V = deserialize_from(self.value_bytes(meta)).expect("valid encoded value");
            (k.clone(), v)
        })
    }

    /// Live entry count. Read straight off the in-memory HashMap — no
    /// separate counter to keep in sync on every mutation.
    fn size(&self) -> usize {
        self.index.len()
    }

    fn is_empty(&self) -> bool {
        self.index.is_empty()
    }

    fn stats(&self) -> &Self::Stats {
        &self.stats
    }

    /// Schedule mmap writeback asynchronously. Returns once the OS has
    /// accepted the request; use [`sync`](Self::sync) to wait for it.
    fn flush(&self) -> io::Result<()> {
        self.mmap.flush_async()
    }

    /// Block until pending mmap writes have been flushed by the OS.
    /// This does not provide crash recovery or log repair.
    fn sync(&self) -> io::Result<()> {
        self.mmap.flush()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::backend::Backend;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    type TestKey = Vec<u8>;

    #[derive(Encode, Decode, Debug, Clone, PartialEq)]
    struct TestVal {
        name: String,
        count: u32,
        tags: Vec<u32>,
    }

    fn k(s: &str) -> TestKey {
        s.as_bytes().to_vec()
    }

    fn v(name: &str, count: u32) -> TestVal {
        TestVal {
            name: name.into(),
            count,
            tags: vec![count, count * 2, count * 3],
        }
    }

    /// Each test gets a fresh, isolated path.
    fn tmp_path(label: &str) -> PathBuf {
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let n = SEQ.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join("zendb_keydir_tests");
        fs::create_dir_all(&dir).unwrap();
        let p = dir.join(format!("{}_{}_{}.kd", label, std::process::id(), n));
        let _ = fs::remove_file(&p);
        let _ = fs::remove_file(p.with_extension("compact"));
        let _ = fs::remove_file(p.with_extension("bak"));
        p
    }

    fn create(path: &Path) -> KeyDir<TestKey, TestVal> {
        KeyDir::create(path, KeyDirConfig::default()).unwrap()
    }

    fn create_with(path: &Path, cfg: KeyDirConfig) -> KeyDir<TestKey, TestVal> {
        KeyDir::create(path, cfg).unwrap()
    }

    fn open(path: &Path) -> KeyDir<TestKey, TestVal> {
        KeyDir::open(path, KeyDirConfig::default()).unwrap()
    }

    // ---- basic put / get ----

    #[test]
    fn put_then_get() {
        let p = tmp_path("put_get");
        let mut kd = create(&p);
        kd.put(k("alice"), v("alice", 1)).unwrap();
        assert_eq!(kd.get(&k("alice")), Some(v("alice", 1)));
    }

    #[test]
    fn get_missing_returns_none() {
        let p = tmp_path("missing");
        let kd = create(&p);
        assert!(kd.get(&k("ghost")).is_none());
    }

    #[test]
    fn put_multiple_keys() {
        let p = tmp_path("multi");
        let mut kd = create(&p);
        kd.put(k("a"), v("alpha", 1)).unwrap();
        kd.put(k("b"), v("beta", 2)).unwrap();
        kd.put(k("c"), v("gamma", 3)).unwrap();
        assert_eq!(kd.size(), 3);
        assert_eq!(kd.get(&k("a")), Some(v("alpha", 1)));
        assert_eq!(kd.get(&k("b")), Some(v("beta", 2)));
        assert_eq!(kd.get(&k("c")), Some(v("gamma", 3)));
    }

    // ---- overwrite & dead-bytes accounting ----

    #[test]
    fn overwrite_replaces_value() {
        let p = tmp_path("overwrite");
        let mut kd = create(&p);
        kd.put(k("a"), v("first", 1)).unwrap();
        kd.put(k("a"), v("second", 2)).unwrap();
        assert_eq!(kd.size(), 1);
        assert_eq!(kd.get(&k("a")), Some(v("second", 2)));
    }

    #[test]
    fn overwrite_accumulates_dead_bytes() {
        let p = tmp_path("dead_bytes");
        let mut kd = create_with(
            &p,
            KeyDirConfig {
                initial_capacity: 1 << 20,
                compaction_ratio: 0.99,
            },
        );
        kd.put(k("a"), v("first", 1)).unwrap();
        assert_eq!(kd.stats().dead_bytes, 0);
        kd.put(k("a"), v("second", 2)).unwrap();
        assert!(kd.stats().dead_bytes > 0);
    }

    // ---- delete & tombstones ----

    #[test]
    fn delete_removes_key() {
        let p = tmp_path("delete");
        let mut kd = create(&p);
        kd.put(k("a"), v("x", 1)).unwrap();
        assert!(kd.contains(&k("a")));
        assert!(kd.delete(&k("a")).unwrap());
        assert!(!kd.contains(&k("a")));
        assert!(kd.get(&k("a")).is_none());
        assert_eq!(kd.size(), 0);
    }

    #[test]
    fn delete_missing_returns_false() {
        let p = tmp_path("delete_missing");
        let mut kd = create(&p);
        assert!(!kd.delete(&k("ghost")).unwrap());
    }

    #[test]
    fn delete_then_reinsert() {
        let p = tmp_path("delete_reinsert");
        let mut kd = create(&p);
        kd.put(k("a"), v("first", 1)).unwrap();
        kd.delete(&k("a")).unwrap();
        kd.put(k("a"), v("second", 2)).unwrap();
        assert_eq!(kd.get(&k("a")), Some(v("second", 2)));
    }

    // ---- introspection ----

    #[test]
    fn len_and_is_empty() {
        let p = tmp_path("len");
        let mut kd = create(&p);
        assert_eq!(kd.size(), 0);
        assert!(kd.is_empty());
        kd.put(k("a"), v("a", 1)).unwrap();
        kd.put(k("b"), v("b", 2)).unwrap();
        assert_eq!(kd.size(), 2);
        assert!(!kd.is_empty());
    }

    #[test]
    fn entries_yields_live_pairs_only() {
        let p = tmp_path("entries");
        let mut kd = create(&p);
        kd.put(k("a"), v("av", 1)).unwrap();
        kd.put(k("b"), v("bv", 2)).unwrap();
        kd.put(k("c"), v("cv", 3)).unwrap();
        kd.delete(&k("b")).unwrap();

        let mut collected: std::collections::HashMap<TestKey, u32> =
            kd.entries().map(|(key, val)| (key, val.count)).collect();
        assert_eq!(collected.len(), 2);
        assert_eq!(collected.remove(&k("a")), Some(1));
        assert_eq!(collected.remove(&k("c")), Some(3));
    }

    // ---- persistence: reopen ----

    #[test]
    fn reopen_preserves_live_entries() {
        let p = tmp_path("persist_live");
        {
            let mut kd = create(&p);
            kd.put(k("a"), v("alpha", 1)).unwrap();
            kd.put(k("b"), v("beta", 2)).unwrap();
            kd.flush().unwrap();
        }
        let kd = open(&p);
        assert_eq!(kd.size(), 2);
        assert_eq!(kd.get(&k("a")), Some(v("alpha", 1)));
        assert_eq!(kd.get(&k("b")), Some(v("beta", 2)));
    }

    #[test]
    fn reopen_replays_tombstones() {
        let p = tmp_path("persist_tombstone");
        {
            let mut kd = create(&p);
            kd.put(k("a"), v("a", 1)).unwrap();
            kd.put(k("b"), v("b", 2)).unwrap();
            kd.delete(&k("a")).unwrap();
            kd.flush().unwrap();
        }
        let kd = open(&p);
        assert_eq!(kd.size(), 1);
        assert!(!kd.contains(&k("a")));
        assert!(kd.contains(&k("b")));
    }

    #[test]
    fn reopen_last_write_wins() {
        let p = tmp_path("persist_overwrite");
        {
            let mut kd = create(&p);
            kd.put(k("a"), v("v1", 1)).unwrap();
            kd.put(k("a"), v("v2", 2)).unwrap();
            kd.put(k("a"), v("v3", 3)).unwrap();
            kd.flush().unwrap();
        }
        let kd = open(&p);
        assert_eq!(kd.size(), 1);
        assert_eq!(kd.get(&k("a")), Some(v("v3", 3)));
    }

    #[test]
    fn reopen_accumulates_dead_bytes_from_overwrites() {
        let p = tmp_path("persist_dead");
        {
            let mut kd = create_with(
                &p,
                KeyDirConfig {
                    initial_capacity: 1 << 20,
                    compaction_ratio: 0.99,
                },
            );
            kd.put(k("a"), v("v1", 1)).unwrap();
            kd.put(k("a"), v("v2", 2)).unwrap();
            kd.flush().unwrap();
        }
        let kd = KeyDir::<TestKey, TestVal>::open(
            &p,
            KeyDirConfig {
                initial_capacity: 1 << 20,
                compaction_ratio: 0.99,
            },
        )
        .unwrap();
        assert!(
            kd.stats().dead_bytes > 0,
            "rebuild should have seen the overwrite as dead"
        );
    }

    // ---- growth ----

    #[test]
    fn grows_when_initial_capacity_exceeded() {
        let p = tmp_path("grow");
        let cfg = KeyDirConfig {
            initial_capacity: 256,
            compaction_ratio: 1.0,
        };
        let mut kd = create_with(&p, cfg);
        let initial_len = kd.mmap.len();

        for i in 0..50u32 {
            kd.put(
                format!("key_{:04}", i).into_bytes(),
                v("payload-grows-the-file", i),
            )
            .unwrap();
        }
        assert!(
            kd.mmap.len() > initial_len,
            "mmap length should have grown beyond {}",
            initial_len
        );
        for i in 0..50u32 {
            let key = format!("key_{:04}", i).into_bytes();
            assert_eq!(kd.get(&key), Some(v("payload-grows-the-file", i)));
        }
    }

    // ---- compaction (in place) ----

    #[test]
    fn compaction_zeros_dead_bytes_and_preserves_data() {
        let p = tmp_path("compact_basic");
        let cfg = KeyDirConfig {
            initial_capacity: 8 * 1024,
            compaction_ratio: 0.3,
        };
        let mut kd = create_with(&p, cfg);

        for round in 0..20u32 {
            for i in 0..10u32 {
                kd.put(format!("k{}", i).into_bytes(), v("payload", round * 10 + i))
                    .unwrap();
            }
        }

        assert_eq!(kd.size(), 10);
        assert!(
            (kd.stats().dead_bytes as f64) / (kd.stats().data_size as f64) < 0.3,
            "post-compact dead_bytes ratio too high: {} / {}",
            kd.stats().dead_bytes,
            kd.stats().data_size
        );
        for i in 0..10u32 {
            let got = kd.get(&format!("k{}", i).into_bytes()).unwrap();
            assert_eq!(got.name, "payload");
            assert_eq!(got.count, 19 * 10 + i);
        }
    }

    #[test]
    fn compaction_drops_tombstoned_keys() {
        let p = tmp_path("compact_tomb");
        let cfg = KeyDirConfig {
            initial_capacity: 4 * 1024,
            compaction_ratio: 0.3,
        };
        let mut kd = create_with(&p, cfg);

        for i in 0..50u32 {
            kd.put(format!("k{}", i).into_bytes(), v("p", i)).unwrap();
        }
        for i in 0..40u32 {
            kd.delete(&format!("k{}", i).into_bytes()).unwrap();
        }
        for i in 40..50u32 {
            kd.put(format!("k{}", i).into_bytes(), v("p2", i)).unwrap();
        }

        for i in 0..40u32 {
            assert!(!kd.contains(&format!("k{}", i).into_bytes()));
        }
        for i in 40..50u32 {
            assert!(kd.contains(&format!("k{}", i).into_bytes()));
        }
    }

    #[test]
    fn compaction_survives_reopen() {
        let p = tmp_path("compact_reopen");
        let cfg = KeyDirConfig {
            initial_capacity: 8 * 1024,
            compaction_ratio: 0.3,
        };
        {
            let mut kd = create_with(&p, cfg.clone());
            for round in 0..20u32 {
                for i in 0..10u32 {
                    kd.put(format!("k{}", i).into_bytes(), v("p", round * 10 + i))
                        .unwrap();
                }
            }
            kd.flush().unwrap();
        }
        let kd: KeyDir<TestKey, TestVal> = KeyDir::open(&p, cfg).unwrap();
        assert_eq!(kd.size(), 10);
        for i in 0..10u32 {
            let got = kd.get(&format!("k{}", i).into_bytes()).unwrap();
            assert_eq!(got.count, 19 * 10 + i);
        }
    }

    #[test]
    fn zero_ratio_compacts_on_first_write_after_reopen() {
        let p = tmp_path("compact_zero_after_reopen");
        {
            let mut kd = create_with(
                &p,
                KeyDirConfig {
                    initial_capacity: 8 * 1024,
                    compaction_ratio: 1.0,
                },
            );
            for i in 0..20u32 {
                kd.put(k("a"), v("payload", i)).unwrap();
            }
            assert!(kd.stats().dead_bytes > 0);
            kd.sync().unwrap();
        }

        let mut kd: KeyDir<TestKey, TestVal> = KeyDir::open(
            &p,
            KeyDirConfig {
                initial_capacity: 8 * 1024,
                compaction_ratio: 0.0,
            },
        )
        .unwrap();
        assert!(kd.stats().dead_bytes > 0);
        kd.put(k("b"), v("new", 1)).unwrap();

        assert_eq!(kd.stats().dead_bytes, 0);
        assert_eq!(kd.size(), 2);
        assert_eq!(kd.get(&k("a")).unwrap().count, 19);
    }

    /// Locks in the in-place compaction contract: after many overwrites
    /// have grown the file well past `initial_capacity`, calling
    /// `compact` releases the dead bytes by truncating the mmap (rather
    /// than provisioning a `.compact` tmp file and renaming it in).
    /// Verifies the file length never *grows* across compact, the live
    /// entry survives, and `dead_bytes` is zero afterwards.
    #[test]
    fn in_place_compact_shrinks_file_to_live_size() {
        let p = tmp_path("compact_shrinks_file");
        let cfg = KeyDirConfig {
            initial_capacity: 4 * 1024,
            compaction_ratio: 1.0,
        };
        let mut kd = create_with(&p, cfg);
        // Repeated overwrites of one key push the file past initial_capacity
        // and leave nothing but dead bytes behind the latest record.
        for i in 0..2000u32 {
            kd.put(k("hot"), v("payload", i)).unwrap();
        }
        let pre_len = kd.mmap.len();
        kd.compact().unwrap();
        let post_len = kd.mmap.len();
        assert!(
            post_len <= pre_len,
            "compact should not grow the file ({} -> {})",
            pre_len,
            post_len
        );
        assert_eq!(kd.size(), 1);
        assert_eq!(kd.get(&k("hot")).unwrap().count, 1999);
        assert_eq!(kd.stats().dead_bytes, 0);
    }

    // ---- variable-length values round-trip ----

    #[test]
    fn variable_length_value_round_trips() {
        let p = tmp_path("var_len");
        let mut kd = create(&p);
        let big = TestVal {
            name: "a moderately long string".into(),
            count: 0xDEAD_BEEF,
            tags: (0..32).collect(),
        };
        kd.put(k("big"), big.clone()).unwrap();
        assert_eq!(kd.get(&k("big")), Some(big));
    }

    // ---- stress ----

    #[test]
    fn many_entries_round_trip() {
        let p = tmp_path("many");
        let mut kd = create(&p);
        let n = 500u32;
        for i in 0..n {
            kd.put(
                format!("key_{:05}", i).into_bytes(),
                v(&format!("val_{}", i), i),
            )
            .unwrap();
        }
        assert_eq!(kd.size(), n as usize);
        for i in 0..n {
            let key = format!("key_{:05}", i).into_bytes();
            let got = kd.get(&key).unwrap();
            assert_eq!(got.name, format!("val_{}", i));
            assert_eq!(got.count, i);
        }
    }

    #[test]
    fn many_entries_survive_reopen() {
        let p = tmp_path("many_reopen");
        let n = 200u32;
        {
            let mut kd = create(&p);
            for i in 0..n {
                kd.put(
                    format!("key_{:04}", i).into_bytes(),
                    v(&format!("val_{}", i), i),
                )
                .unwrap();
            }
            kd.flush().unwrap();
        }
        let kd = open(&p);
        assert_eq!(kd.size(), n as usize);
        for i in 0..n {
            let key = format!("key_{:04}", i).into_bytes();
            let got = kd.get(&key).unwrap();
            assert_eq!(got.count, i);
        }
    }

    // ---- empty / fresh ----

    #[test]
    fn fresh_keydir_has_zero_data() {
        let p = tmp_path("fresh");
        let kd = create(&p);
        assert_eq!(kd.size(), 0);
        assert_eq!(kd.stats().data_size, HEADER_SIZE as u64);
        assert_eq!(kd.stats().dead_bytes, 0);
        assert!(kd.is_empty());
    }

    #[test]
    fn open_fresh_file_is_empty() {
        let p = tmp_path("open_fresh");
        drop(create(&p));
        let kd = open(&p);
        assert_eq!(kd.size(), 0);
        assert!(kd.is_empty());
    }

    // ---- owned iteration ----

    #[test]
    fn keys_yields_owned() {
        let p = tmp_path("keys_owned");
        let mut kd = create(&p);
        kd.put(k("a"), v("x", 1)).unwrap();
        kd.put(k("b"), v("y", 2)).unwrap();
        let mut keys: Vec<TestKey> = kd.keys().collect();
        keys.sort();
        assert_eq!(keys, vec![k("a"), k("b")]);
    }

    #[test]
    fn values_yields_owned() {
        let p = tmp_path("values_owned");
        let mut kd = create(&p);
        kd.put(k("a"), v("av", 1)).unwrap();
        kd.put(k("b"), v("bv", 2)).unwrap();
        let mut vals: Vec<u32> = kd.values().map(|v| v.count).collect();
        vals.sort();
        assert_eq!(vals, vec![1, 2]);
    }

    // ---- update ----

    #[test]
    fn update_modifies_existing() {
        let p = tmp_path("update_exists");
        let mut kd = create(&p);
        kd.put(k("a"), v("initial", 10)).unwrap();
        kd.update(&k("a"), |opt| {
            let mut v = opt.unwrap();
            v.count += 5;
            v.tags.push(999);
            Some(v)
        })
        .unwrap();
        let got = kd.get(&k("a")).unwrap();
        assert_eq!(got.count, 15);
        assert_eq!(got.tags.len(), 4);
        assert_eq!(got.tags[3], 999);
    }

    #[test]
    fn update_inserts_when_absent_and_returns_some() {
        let p = tmp_path("update_insert");
        let mut kd = create(&p);
        kd.update(&k("new"), |opt| {
            assert!(opt.is_none());
            Some(v("inserted", 1))
        })
        .unwrap();
        assert_eq!(kd.get(&k("new")), Some(v("inserted", 1)));
    }

    #[test]
    fn update_absent_returning_none_is_noop() {
        let p = tmp_path("update_missing");
        let mut kd = create(&p);
        kd.update(&k("ghost"), |opt| {
            assert!(opt.is_none());
            None
        })
        .unwrap();
        assert!(kd.get(&k("ghost")).is_none());
        assert_eq!(kd.size(), 0);
    }

    #[test]
    fn update_returning_none_deletes_existing() {
        let p = tmp_path("update_delete");
        let mut kd = create(&p);
        kd.put(k("a"), v("init", 1)).unwrap();
        kd.update(&k("a"), |opt| {
            assert!(opt.is_some());
            None
        })
        .unwrap();
        assert!(kd.get(&k("a")).is_none());
        assert_eq!(kd.size(), 0);
    }

    #[test]
    fn stats_track_data_size_and_dead_bytes() {
        let p = tmp_path("stats");
        let mut kd = create_with(
            &p,
            KeyDirConfig {
                initial_capacity: 8 * 1024,
                compaction_ratio: 1.0,
            },
        );
        assert_eq!(
            *kd.stats(),
            KeyDirStats {
                data_size: HEADER_SIZE as u64,
                dead_bytes: 0,
            }
        );
        assert_eq!(kd.size(), 0);

        kd.put(k("a"), v("first", 1)).unwrap();
        assert_eq!(kd.size(), 1);
        assert!(kd.stats().data_size > HEADER_SIZE as u64);
        assert_eq!(kd.stats().dead_bytes, 0);

        kd.put(k("a"), v("second", 2)).unwrap();
        assert_eq!(kd.size(), 1);
        assert!(kd.stats().dead_bytes > 0);
    }

    // ---- clear ----

    #[test]
    fn clear_removes_all_entries() {
        let p = tmp_path("clear");
        let mut kd = create(&p);
        kd.put(k("a"), v("av", 1)).unwrap();
        kd.put(k("b"), v("bv", 2)).unwrap();
        assert_eq!(kd.size(), 2);

        kd.clear().unwrap();
        assert_eq!(kd.size(), 0);
        assert!(kd.is_empty());
        assert!(kd.get(&k("a")).is_none());
        assert_eq!(kd.stats().dead_bytes, 0);
        assert_eq!(kd.stats().data_size, HEADER_SIZE as u64);
    }

    #[test]
    fn clear_then_put_reuses_file() {
        let p = tmp_path("clear_reuse");
        let mut kd = create(&p);
        kd.put(k("a"), v("av", 1)).unwrap();
        kd.clear().unwrap();
        kd.put(k("z"), v("zv", 99)).unwrap();
        assert_eq!(kd.size(), 1);
        assert_eq!(kd.get(&k("z")), Some(v("zv", 99)));
        assert!(kd.get(&k("a")).is_none());
    }

    #[test]
    fn clear_survives_reopen() {
        let p = tmp_path("clear_reopen");
        {
            let mut kd = create(&p);
            kd.put(k("a"), v("av", 1)).unwrap();
            kd.put(k("b"), v("bv", 2)).unwrap();
            kd.clear().unwrap();
            kd.flush().unwrap();
        }
        let kd = open(&p);
        assert_eq!(kd.size(), 0);
        assert!(kd.is_empty());
    }

    // ---- put_if_absent / replace ----

    #[test]
    fn put_if_absent_inserts_when_missing() {
        let p = tmp_path("pia_insert");
        let mut kd = create(&p);
        let inserted = Backend::put_if_absent(&mut kd, k("a"), v("first", 1)).unwrap();
        assert!(inserted);
        assert_eq!(kd.get(&k("a")), Some(v("first", 1)));
    }

    #[test]
    fn put_if_absent_noop_when_present() {
        let p = tmp_path("pia_noop");
        let mut kd = create(&p);
        kd.put(k("a"), v("first", 1)).unwrap();
        let inserted = Backend::put_if_absent(&mut kd, k("a"), v("second", 2)).unwrap();
        assert!(!inserted);
        assert_eq!(kd.get(&k("a")), Some(v("first", 1)));
    }

    #[test]
    fn replace_returns_previous_value() {
        let p = tmp_path("replace");
        let mut kd = create(&p);
        kd.put(k("a"), v("first", 1)).unwrap();
        let prev = Backend::replace(&mut kd, k("a"), v("second", 2)).unwrap();
        assert_eq!(prev, Some(v("first", 1)));
        assert_eq!(kd.get(&k("a")), Some(v("second", 2)));
    }

    #[test]
    fn replace_returns_none_when_absent() {
        let p = tmp_path("replace_absent");
        let mut kd = create(&p);
        let prev = Backend::replace(&mut kd, k("a"), v("new", 99)).unwrap();
        assert!(prev.is_none());
        assert_eq!(kd.get(&k("a")), Some(v("new", 99)));
    }

    // ---- bulk_put / bulk_put_sorted / bulk_delete ----

    #[test]
    fn bulk_put_inserts_all_items() {
        let p = tmp_path("bulk_put");
        let mut kd = create(&p);
        let items: Vec<(TestKey, TestVal)> = (0..50u32)
            .map(|i| (format!("k{:04}", i).into_bytes(), v("p", i)))
            .collect();
        Backend::bulk_put(&mut kd, items).unwrap();
        assert_eq!(kd.size(), 50);
        for i in 0..50u32 {
            assert_eq!(kd.get(&format!("k{:04}", i).into_bytes()), Some(v("p", i)));
        }
    }

    #[test]
    fn bulk_put_sorted_inserts_all_items() {
        let p = tmp_path("bulk_put_sorted");
        let mut kd = create(&p);
        let items: Vec<(TestKey, TestVal)> = (0..50u32)
            .map(|i| (format!("k{:04}", i).into_bytes(), v("p", i)))
            .collect();
        Backend::bulk_put_sorted(&mut kd, items).unwrap();
        assert_eq!(kd.size(), 50);
    }

    #[test]
    fn bulk_delete_returns_removed_count() {
        let p = tmp_path("bulk_delete");
        let mut kd = create(&p);
        for i in 0..10u32 {
            kd.put(format!("k{:02}", i).into_bytes(), v("p", i))
                .unwrap();
        }
        let keys: Vec<TestKey> = (0..5u32)
            .map(|i| format!("k{:02}", i).into_bytes())
            .collect();
        // Add a couple of ghost keys to verify only existing ones are counted.
        let ghosts: Vec<TestKey> = vec![k("ghost1"), k("ghost2")];
        let all: Vec<&TestKey> = keys.iter().chain(ghosts.iter()).collect();
        let n = Backend::bulk_delete(&mut kd, all).unwrap();
        assert_eq!(n, 5);
        assert_eq!(kd.size(), 5);
    }

    // ---- sync / flush ----

    #[test]
    fn sync_persists_writes() {
        let p = tmp_path("sync_persist");
        {
            let mut kd = create(&p);
            kd.put(k("a"), v("alpha", 1)).unwrap();
            kd.sync().unwrap();
        }
        let kd = open(&p);
        assert_eq!(kd.get(&k("a")), Some(v("alpha", 1)));
    }

    #[test]
    fn flush_returns_ok_on_empty_keydir() {
        let p = tmp_path("flush_empty");
        let kd = create(&p);
        kd.flush().unwrap();
        kd.sync().unwrap();
    }
}
