//! KeyDir — persistent key-value store backed by an in-memory hash index
//! and a memory-mapped append-only data file (Bitcask model).
//!
//! # Architecture
//!
//! ```text
//! ┌───────────────┐     ┌──────────────────────────────────┐
//! │  HashMap<K,   │     │  append-only mmap data file      │
//! │   EntryMeta>  │────▶│  [entry] [entry] [tombstone] …  │
//! │  (in memory)  │     │  live entries + dead gaps        │
//! └───────────────┘     └──────────────────────────────────┘
//! ```
//!
//! The in-memory hash index maps each key to its on-disk location
//! (offset + sizes). Every write appends, never mutates in place
//! (except `update_in_place`, which is opt-in and size-stable).
//!
//! # Writing
//!
//! Generic over `K` (key) and `V` (value). Uses rkyv for true zero-copy I/O:
//! - **Write**: `serialize_into` writes directly into the mmap buffer — no
//!   intermediate allocation.
//! - **Read**: `get` returns a [`ValueRef`] borrowing the archived bytes
//!   directly from the mmap — no allocation, no copy.
//! - **Compact**: rewrites the file by `memcpy`-ing each live entry's raw
//!   bytes — no deserialize/re-serialize round-trip.
//!
//! # Dead bytes & compaction
//!
//! Overwrites and deletes leave "dead" bytes in the append-only file.
//! When the dead-byte ratio exceeds `compaction_ratio`, the file is
//! rewritten to a `.compact` file containing only live entries. The
//! atomic rename protocol (main → .bak, .compact → main, remove .bak)
//! ensures crash-safety.
//!
//! # File format
//!
//! Live entry:
//! ```text
//! [value_size: u32 LE][archived V bytes (value_size)][key_size: u32 LE][archived K bytes (key_size)]
//! ```
//! Tombstone:
//! ```text
//! [0xFFFF_FFFF: u32][key_size: u32 LE][archived K bytes (key_size)]
//! ```
//!
//! A tombstone is a special entry with value_size = `u32::MAX` (no value
//! bytes). It marks a key as deleted. During `rebuild_index`, tombstones
//! remove keys from the in-memory index; during compaction, they are
//! stripped entirely.

use std::{
    fs::{self, File, OpenOptions},
    hash::Hash,
    io::{self},
    marker::PhantomData,
    path::{Path, PathBuf},
};

use hashbrown::HashMap;
use memmap2::MmapMut;
use rkyv::{
    api::high::{HighDeserializer, HighSerializer},
    rancor::Error as RkyvError,
    ser::{allocator::ArenaHandle, writer::Buffer},
    Archive, Archived, Deserialize, Portable, Serialize,
};

use crate::utils::serdes::{
    read_u32_le, serialize_into, serialized_size, CountingWriter, ValueRef,
};

const DEFAULT_INITIAL_CAPACITY: u64 = 1024 * 1024;
const DEFAULT_COMPACTION_RATIO: f64 = 0.5;
const TOMBSTONE: u32 = u32::MAX;

#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
pub struct KeyDirConfig {
    pub initial_capacity: u64,
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
    /// plus the archived key and value bytes.
    fn on_disk_size(&self) -> u64 {
        8 + self.value_size as u64 + self.key_size as u64
    }
}

pub struct KeyDir<K, V> {
    index: HashMap<K, EntryMeta>,
    mmap: MmapMut,
    file: File,
    path: PathBuf,
    config: KeyDirConfig,
    write_offset: u64,
    capacity: u64,
    dead_bytes: u64,
    _phantom: PhantomData<V>,
}

impl<K, V> KeyDir<K, V>
where
    K: Hash + Eq + Clone + Archive,
    for<'buf, 'a> K: Serialize<HighSerializer<Buffer<'buf>, ArenaHandle<'a>, RkyvError>>,
    for<'a> K: Serialize<HighSerializer<CountingWriter, ArenaHandle<'a>, RkyvError>>,
    <K as Archive>::Archived: Portable + Deserialize<K, HighDeserializer<RkyvError>> + 'static,
    V: Archive,
    for<'buf, 'a> V: Serialize<HighSerializer<Buffer<'buf>, ArenaHandle<'a>, RkyvError>>,
    for<'a> V: Serialize<HighSerializer<CountingWriter, ArenaHandle<'a>, RkyvError>>,
    <V as Archive>::Archived: Portable + Deserialize<V, HighDeserializer<RkyvError>> + 'static,
{
    pub fn create(path: &Path, config: KeyDirConfig) -> io::Result<Self> {
        let capacity = config.initial_capacity;
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
            _phantom: PhantomData,
        })
    }

    pub fn open(path: &Path, config: KeyDirConfig) -> io::Result<Self> {
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
            _phantom: PhantomData,
        };

        this.rebuild_index()?;

        let _ = fs::remove_file(path.with_extension("compact"));
        let _ = fs::remove_file(path.with_extension("bak"));

        Ok(this)
    }

    // ---- read (zero-copy) ----

    /// Look up `key`. Returns a [`ValueRef`] borrowing the archived value
    /// directly from the memory-mapped file — no allocation, no copy.
    pub fn get(&self, key: &K) -> Option<ValueRef<'_, V>> {
        let meta = self.index.get(key)?;
        Some(self.value_ref(meta))
    }

    /// Iterate all entries. Yields owned keys (cloned from the in-memory
    /// HashMap) and zero-copy [`ValueRef`]s into the mmap. Order is
    /// HashMap order.
    pub fn entries(&self) -> impl Iterator<Item = (K, ValueRef<'_, V>)> + '_ {
        self.index
            .iter()
            .map(|(k, meta)| (k.clone(), self.value_ref(meta)))
    }

    /// Iterate all keys (cloned from the in-memory index). Order is
    /// HashMap order.
    pub fn keys(&self) -> impl Iterator<Item = K> + '_ {
        self.index.keys().cloned()
    }

    /// Iterate all values. Yields zero-copy [`ValueRef`]s into the mmap.
    /// Order is HashMap order.
    pub fn values(&self) -> impl Iterator<Item = ValueRef<'_, V>> + '_ {
        self.index.values().map(|meta| self.value_ref(meta))
    }

    pub fn contains_key(&self, key: &K) -> bool {
        self.index.contains_key(key)
    }
    pub fn len(&self) -> usize {
        self.index.len()
    }
    pub fn is_empty(&self) -> bool {
        self.index.is_empty()
    }

    /// Construct a `ValueRef` for the entry pointed at by `meta`.
    fn value_ref(&self, meta: &EntryMeta) -> ValueRef<'_, V> {
        let start = meta.offset as usize + 4;
        let end = start + meta.value_size as usize;
        ValueRef::from_bytes(&self.mmap[start..end])
    }

    // ---- write (direct-to-mmap) ----

    /// Insert or overwrite. Both `key` and `value` are consumed —
    /// `value` is serialized directly into the mmap (no intermediate
    /// allocation), `key` is moved into the in-memory index.
    pub fn put(&mut self, key: K, value: V) -> io::Result<()> {
        if let Some(old) = self.index.get(&key) {
            self.dead_bytes += old.on_disk_size();
        }

        let (offset, value_size, key_size) = self.write_entry(&key, &value)?;
        self.index.insert(
            key,
            EntryMeta {
                offset,
                value_size,
                key_size,
            },
        );
        self.maybe_compact()?;
        Ok(())
    }

    /// Serialize `key` and `value` into the mmap at the current write
    /// offset. The entry layout is:
    ///
    /// ```text
    /// [value_size: u32 LE][archived V][key_size: u32 LE][archived K]
    /// ```
    ///
    /// Updates `self.write_offset` to point past the new entry. Returns
    /// the metadata (`offset`, `value_size`, `key_size`) needed to index
    /// the entry. The caller is responsible for inserting the metadata
    /// into `self.index` and calling `maybe_compact()`.
    fn write_entry(&mut self, key: &K, value: &V) -> io::Result<(u64, u32, u32)> {
        let offset = self.write_offset as usize;

        // Layout: [value_size: u32][archived value][key_size: u32][archived key]
        let v_end = self.serialize_into_mmap(value, offset + 4)?;
        let value_size = (v_end - (offset + 4)) as u32;
        self.mmap[offset..offset + 4].copy_from_slice(&value_size.to_le_bytes());

        let k_end = self.serialize_into_mmap(key, v_end + 4)?;
        let key_size = (k_end - (v_end + 4)) as u32;
        self.mmap[v_end..v_end + 4].copy_from_slice(&key_size.to_le_bytes());

        self.write_offset = k_end as u64;
        Ok((offset as u64, value_size, key_size))
    }

    /// Read-modify-write. Deserializes the value, passes it to `f`,
    /// and serializes the result back. Returns `true` if the key existed,
    /// `false` otherwise — `f` is never called when the key is absent.
    pub fn update<F>(&mut self, key: &K, f: F) -> io::Result<bool>
    where
        F: FnOnce(V) -> V,
    {
        // Single lookup — get metadata and account for dead bytes upfront.
        let Some(meta) = self.index.get(key).copied() else {
            return Ok(false);
        };
        self.dead_bytes += meta.on_disk_size();

        // Deserialize current value from mmap.
        let current = self.value_ref(&meta).to_owned();
        let new_v = f(current);

        // Write the new entry (serializes &K — no clone needed).
        let (offset, value_size, key_size) = self.write_entry(key, &new_v)?;

        // Update the index entry in place — no key clone.
        *self.index.get_mut(key).expect("key just found") = EntryMeta {
            offset,
            value_size,
            key_size,
        };
        self.maybe_compact()?;
        Ok(true)
    }

    /// In-place archived update. Mutates the value's bytes in the mmap
    /// without deserializing — the closure operates directly on the
    /// archived form. Returns whether the key existed.
    ///
    /// # Safety contract
    /// The closure must not change the byte length of the archive (no
    /// growing `ArchivedVec`s, no `ArchivedString` length changes, etc.).
    /// Size-stable mutations only — see the trait docs for details.
    pub fn update_in_place<F>(&mut self, key: &K, f: F) -> io::Result<bool>
    where
        F: FnOnce(&mut Archived<V>),
    {
        let Some(meta) = self.index.get(key).copied() else {
            return Ok(false);
        };
        let start = meta.offset as usize + 4;
        let end = start + meta.value_size as usize;
        // SAFETY: bytes are a valid archive of V (maintained by `put`).
        // The caller's closure must not change the byte length — documented
        // contract above. `unseal_unchecked` is sound under the same
        // assumption: the closure only mutates in-place fields, never
        // restructures relative pointers.
        let sealed =
            unsafe { rkyv::access_unchecked_mut::<Archived<V>>(&mut self.mmap[start..end]) };
        let archived: &mut Archived<V> = unsafe { sealed.unseal_unchecked() };
        f(archived);
        Ok(true)
    }

    pub fn delete(&mut self, key: &K) -> io::Result<bool> {
        let Some(old) = self.index.remove(key) else {
            return Ok(false);
        };
        self.dead_bytes += old.on_disk_size();
        self.write_tombstone(key)?;
        self.maybe_compact()?;
        Ok(true)
    }

    /// Write a tombstone entry for `key` at the current write offset. A
    /// tombstone is a marker entry with `value_size = u32::MAX` and no
    /// value bytes — just the key. Records the dead bytes it consumes.
    fn write_tombstone(&mut self, key: &K) -> io::Result<()> {
        let offset = self.write_offset as usize;
        let k_end = self.serialize_into_mmap(key, offset + 8)?;
        let key_size = (k_end - (offset + 8)) as u32;

        self.mmap[offset..offset + 4].copy_from_slice(&TOMBSTONE.to_le_bytes());
        self.mmap[offset + 4..offset + 8].copy_from_slice(&key_size.to_le_bytes());

        self.write_offset = k_end as u64;
        self.dead_bytes += 8 + key_size as u64;
        Ok(())
    }

    /// Serialize `value` directly into `self.mmap` starting at byte `pos`.
    ///
    /// Two-phase: first measures the archive size via [`CountingWriter`]
    /// (no allocation), then writes in one pass via [`Buffer`]. If the
    /// measured size exceeds `self.capacity`, the file is grown before
    /// writing. Returns the byte offset just past the written archive.
    ///
    /// This is the core write primitive — every entry (live or tombstone)
    /// calls it for its key and (for live entries) value.
    fn serialize_into_mmap<T>(&mut self, value: &T, pos: usize) -> io::Result<usize>
    where
        T: for<'buf, 'a> Serialize<HighSerializer<Buffer<'buf>, ArenaHandle<'a>, RkyvError>>
            + for<'a> Serialize<HighSerializer<CountingWriter, ArenaHandle<'a>, RkyvError>>,
    {
        let size = serialized_size(value)?;
        let end = pos
            .checked_add(size)
            .ok_or_else(|| io::Error::new(io::ErrorKind::OutOfMemory, "KeyDir offset overflow"))?;
        if (end as u64) > self.capacity {
            self.grow(end as u64)?;
        }
        let written = serialize_into(value, &mut self.mmap[pos..end])?;
        debug_assert_eq!(written, size);
        Ok(end)
    }

    pub fn flush(&self) -> io::Result<()> {
        self.mmap.flush()
    }

    /// Trigger compaction if the dead-byte ratio meets or exceeds
    /// `compaction_ratio`. Called after every mutation (`put`, `update`,
    /// `delete`). Idempotent: does nothing if dead_bytes is zero or below
    /// the threshold.
    fn maybe_compact(&mut self) -> io::Result<()> {
        if self.write_offset == 0 || self.dead_bytes == 0 {
            return Ok(());
        }
        if self.dead_bytes as f64 / self.write_offset as f64 >= self.config.compaction_ratio {
            self.compact()?;
        }
        Ok(())
    }

    /// Rewrite the file, keeping only live entries. Uses a three-phase
    /// atomic rename protocol for crash safety:
    ///
    /// 1. Write a `.compact` file with only live entries (`memcpy` from
    ///    the old mmap — no deserialization).
    /// 2. Rename old main file to `.bak`.
    /// 3. Rename `.compact` to the main path.
    /// 4. Remove `.bak`.
    ///
    /// After compaction: dead_bytes = 0, write_offset = total live bytes,
    /// and `self.mmap`/`self.file`/`self.index` are replaced with the
    /// compacted versions.
    fn compact(&mut self) -> io::Result<()> {
        let tmp_path = self.path.with_extension("compact");

        let live_size: u64 = self.index.values().map(EntryMeta::on_disk_size).sum();
        let new_capacity = live_size.max(self.config.initial_capacity);

        let mut new_index: HashMap<K, EntryMeta> = HashMap::with_capacity(self.index.len());
        let write_offset = {
            let new_file = OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                .truncate(true)
                .open(&tmp_path)?;
            new_file.set_len(new_capacity)?;
            let mut new_mmap = unsafe { MmapMut::map_mut(&new_file)? };

            let mut cursor: usize = 0;
            for (key, meta) in self.index.iter() {
                let entry_size = meta.on_disk_size() as usize;
                let src_start = meta.offset as usize;
                new_mmap[cursor..cursor + entry_size]
                    .copy_from_slice(&self.mmap[src_start..src_start + entry_size]);
                new_index.insert(
                    key.clone(),
                    EntryMeta {
                        offset: cursor as u64,
                        ..*meta
                    },
                );
                cursor += entry_size;
            }
            new_mmap.flush()?;
            cursor as u64
            // new_mmap and new_file dropped here so the rename below can
            // proceed even on platforms with mandatory locking.
        };

        let bak_path = self.path.with_extension("bak");
        let _ = fs::remove_file(&bak_path);
        fs::rename(&self.path, &bak_path)?;
        fs::rename(&tmp_path, &self.path)?;
        let _ = fs::remove_file(&bak_path);

        let file = OpenOptions::new().read(true).write(true).open(&self.path)?;
        let mmap = unsafe { MmapMut::map_mut(&file)? };

        self.mmap = mmap;
        self.file = file;
        self.index = new_index;
        self.capacity = new_capacity;
        self.write_offset = write_offset;
        self.dead_bytes = 0;
        Ok(())
    }

    pub fn data_size(&self) -> u64 {
        self.write_offset
    }
    pub fn capacity(&self) -> u64 {
        self.capacity
    }
    pub fn dead_bytes(&self) -> u64 {
        self.dead_bytes
    }

    // ---- rebuild ----

    /// Rebuild the in-memory index by scanning the file from byte 0.
    /// Replays all entries in order: live entries overwrite prior index
    /// entries, tombstones remove them. Accumulates dead_bytes from entries
    /// that get overwritten or tombstoned. Sets `write_offset` to the end
    /// of the last valid entry.
    ///
    /// Called once at startup (`open`). Makes no assumptions about the
    /// file's contents — the index is built purely from what's on disk.
    fn rebuild_index(&mut self) -> io::Result<()> {
        let mut cursor: usize = 0;
        self.dead_bytes = 0;

        while let Some(value_size) = read_u32_le(&self.mmap, cursor) {
            // 0 marks the (zero-filled) tail of the pre-allocated file.
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
            if entry_end > self.mmap.len() {
                break;
            }

            let key: K = unsafe {
                rkyv::from_bytes_unchecked::<K, RkyvError>(&self.mmap[key_start..entry_end])
            }
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "corrupt KeyDir entry"))?;

            if is_tombstone {
                if let Some(old) = self.index.remove(&key) {
                    self.dead_bytes += old.on_disk_size();
                }
                self.dead_bytes += 8 + key_size as u64;
            } else {
                if let Some(old) = self.index.get(&key) {
                    self.dead_bytes += old.on_disk_size();
                }
                self.index.insert(
                    key,
                    EntryMeta {
                        offset: cursor as u64,
                        value_size,
                        key_size,
                    },
                );
            }
            cursor = entry_end;
        }

        self.write_offset = cursor as u64;
        Ok(())
    }

    /// Grow the backing file to at least `desired` bytes in a single shot.
    ///
    /// Picks `max(capacity * 2, desired)` so amortized growth stays
    /// exponential even when a write skips several doubling rounds.
    /// Caller is responsible for only invoking this when an actual grow
    /// is required (`desired > self.capacity`).
    fn grow(&mut self, desired: u64) -> io::Result<()> {
        self.mmap.flush()?;
        let doubled = self.capacity.checked_mul(2).ok_or_else(|| {
            io::Error::new(io::ErrorKind::OutOfMemory, "KeyDir capacity overflow")
        })?;
        let new_capacity = doubled.max(desired);
        self.file.set_len(new_capacity)?;
        self.mmap = unsafe { MmapMut::map_mut(&self.file)? };
        self.capacity = new_capacity;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Backend impl — unordered backend.
//
// KeyDir implements `Backend` only (not `OrderedBackend`) — its in-memory
// hash index has no usable key ordering. Iteration order is arbitrary
// (HashMap iteration order). Every method is a thin delegation to the
// identically-named inherent method.
// ---------------------------------------------------------------------------

impl<K, V> crate::core::backend::Backend<K, V> for KeyDir<K, V>
where
    K: Hash + Eq + Clone + Archive,
    for<'buf, 'a> K: Serialize<HighSerializer<Buffer<'buf>, ArenaHandle<'a>, RkyvError>>,
    for<'a> K: Serialize<HighSerializer<CountingWriter, ArenaHandle<'a>, RkyvError>>,
    <K as Archive>::Archived: Portable + Deserialize<K, HighDeserializer<RkyvError>> + 'static,
    V: Archive,
    for<'buf, 'a> V: Serialize<HighSerializer<Buffer<'buf>, ArenaHandle<'a>, RkyvError>>,
    for<'a> V: Serialize<HighSerializer<CountingWriter, ArenaHandle<'a>, RkyvError>>,
    <V as Archive>::Archived: Portable + Deserialize<V, HighDeserializer<RkyvError>> + 'static,
{
    fn get(&self, key: &K) -> Option<ValueRef<'_, V>> {
        KeyDir::get(self, key)
    }

    fn contains(&self, key: &K) -> bool {
        KeyDir::contains_key(self, key)
    }

    fn put(&mut self, key: K, value: V) -> io::Result<()> {
        KeyDir::put(self, key, value)
    }

    fn delete(&mut self, key: &K) -> io::Result<bool> {
        KeyDir::delete(self, key)
    }

    fn update<F>(&mut self, key: &K, f: F) -> io::Result<bool>
    where
        F: FnOnce(V) -> V,
    {
        KeyDir::update(self, key, f)
    }

    fn update_in_place<F>(&mut self, key: &K, f: F) -> io::Result<bool>
    where
        F: FnOnce(&mut Archived<V>),
    {
        KeyDir::update_in_place(self, key, f)
    }

    fn keys(&self) -> impl Iterator<Item = K> + '_ {
        KeyDir::keys(self)
    }

    fn values(&self) -> impl Iterator<Item = ValueRef<'_, V>> + '_ {
        KeyDir::values(self)
    }

    fn entries(&self) -> impl Iterator<Item = (K, ValueRef<'_, V>)> + '_ {
        KeyDir::entries(self)
    }

    fn len(&self) -> usize {
        KeyDir::len(self)
    }

    fn is_empty(&self) -> bool {
        KeyDir::is_empty(self)
    }

    fn flush(&self) -> io::Result<()> {
        KeyDir::flush(self)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    type TestKey = Vec<u8>;

    #[derive(Archive, Serialize, Deserialize, Debug, Clone, PartialEq)]
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

    fn assert_val_eq(archived: &ArchivedTestVal, expected: &TestVal) {
        assert_eq!(archived.name.as_str(), expected.name);
        assert_eq!(archived.count.to_native(), expected.count);
        let tags: Vec<u32> = archived.tags.iter().map(|t| t.to_native()).collect();
        assert_eq!(tags, expected.tags);
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
        assert_val_eq(&*kd.get(&k("alice")).unwrap(), &v("alice", 1));
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
        assert_eq!(kd.len(), 3);
        assert_val_eq(&*kd.get(&k("a")).unwrap(), &v("alpha", 1));
        assert_val_eq(&*kd.get(&k("b")).unwrap(), &v("beta", 2));
        assert_val_eq(&*kd.get(&k("c")).unwrap(), &v("gamma", 3));
    }

    // ---- overwrite & dead-bytes accounting ----

    #[test]
    fn overwrite_replaces_value() {
        let p = tmp_path("overwrite");
        let mut kd = create(&p);
        kd.put(k("a"), v("first", 1)).unwrap();
        kd.put(k("a"), v("second", 2)).unwrap();
        assert_eq!(kd.len(), 1);
        assert_val_eq(&*kd.get(&k("a")).unwrap(), &v("second", 2));
    }

    #[test]
    fn overwrite_accumulates_dead_bytes() {
        let p = tmp_path("dead_bytes");
        // High compaction_ratio so we observe dead_bytes without auto-compaction kicking in.
        let mut kd = create_with(
            &p,
            KeyDirConfig {
                initial_capacity: 1 << 20,
                compaction_ratio: 0.99,
            },
        );
        kd.put(k("a"), v("first", 1)).unwrap();
        assert_eq!(kd.dead_bytes(), 0);
        kd.put(k("a"), v("second", 2)).unwrap();
        assert!(kd.dead_bytes() > 0);
    }

    // ---- delete & tombstones ----

    #[test]
    fn delete_removes_key() {
        let p = tmp_path("delete");
        let mut kd = create(&p);
        kd.put(k("a"), v("x", 1)).unwrap();
        assert!(kd.contains_key(&k("a")));
        assert!(kd.delete(&k("a")).unwrap());
        assert!(!kd.contains_key(&k("a")));
        assert!(kd.get(&k("a")).is_none());
        assert_eq!(kd.len(), 0);
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
        assert_val_eq(&*kd.get(&k("a")).unwrap(), &v("second", 2));
    }

    // ---- introspection ----

    #[test]
    fn len_and_is_empty() {
        let p = tmp_path("len");
        let mut kd = create(&p);
        assert_eq!(kd.len(), 0);
        assert!(kd.is_empty());
        kd.put(k("a"), v("a", 1)).unwrap();
        kd.put(k("b"), v("b", 2)).unwrap();
        assert_eq!(kd.len(), 2);
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

        // entries() now yields (&Archived<K>, &Archived<V>); ArchivedVec<u8>
        // derefs to [u8], so we convert via as_slice().to_vec().
        let mut collected: std::collections::HashMap<TestKey, u32> = kd
            .entries()
            .map(|(key, val)| (key.as_slice().to_vec(), val.count.to_native()))
            .collect();
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
        assert_eq!(kd.len(), 2);
        assert_val_eq(&*kd.get(&k("a")).unwrap(), &v("alpha", 1));
        assert_val_eq(&*kd.get(&k("b")).unwrap(), &v("beta", 2));
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
        assert_eq!(kd.len(), 1);
        assert!(!kd.contains_key(&k("a")));
        assert!(kd.contains_key(&k("b")));
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
        assert_eq!(kd.len(), 1);
        assert_val_eq(&*kd.get(&k("a")).unwrap(), &v("v3", 3));
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
            kd.dead_bytes() > 0,
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
        let initial_capacity = kd.capacity();

        for i in 0..50u32 {
            kd.put(
                format!("key_{:04}", i).into_bytes(),
                v("payload-grows-the-file", i),
            )
            .unwrap();
        }
        assert!(
            kd.capacity() > initial_capacity,
            "capacity should have grown beyond {}",
            initial_capacity
        );
        for i in 0..50u32 {
            let key = format!("key_{:04}", i).into_bytes();
            assert_val_eq(&*kd.get(&key).unwrap(), &v("payload-grows-the-file", i));
        }
    }

    // ---- compaction ----

    #[test]
    fn compaction_zeros_dead_bytes_and_preserves_data() {
        let p = tmp_path("compact_basic");
        let cfg = KeyDirConfig {
            initial_capacity: 8 * 1024,
            compaction_ratio: 0.3,
        };
        let mut kd = create_with(&p, cfg);

        // 20 rounds of overwriting 10 keys → tons of dead bytes → compaction fires.
        for round in 0..20u32 {
            for i in 0..10u32 {
                kd.put(format!("k{}", i).into_bytes(), v("payload", round * 10 + i))
                    .unwrap();
            }
        }

        assert_eq!(kd.len(), 10);
        // After compaction, dead_bytes should be small relative to file size.
        // (May not be exactly zero if a write happened after the last compact.)
        assert!(
            (kd.dead_bytes() as f64) / (kd.data_size() as f64) < 0.3,
            "post-compact dead_bytes ratio too high: {} / {}",
            kd.dead_bytes(),
            kd.data_size()
        );
        for i in 0..10u32 {
            let archived = kd.get(&format!("k{}", i).into_bytes()).unwrap();
            assert_eq!(archived.name.as_str(), "payload");
            assert_eq!(archived.count.to_native(), 19 * 10 + i);
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
        // delete most of them, then keep churning so compaction fires
        for i in 0..40u32 {
            kd.delete(&format!("k{}", i).into_bytes()).unwrap();
        }
        // Force a few more writes to push the dead-byte ratio over the threshold.
        for i in 40..50u32 {
            kd.put(format!("k{}", i).into_bytes(), v("p2", i)).unwrap();
        }

        for i in 0..40u32 {
            assert!(!kd.contains_key(&format!("k{}", i).into_bytes()));
        }
        for i in 40..50u32 {
            assert!(kd.contains_key(&format!("k{}", i).into_bytes()));
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
        assert_eq!(kd.len(), 10);
        for i in 0..10u32 {
            let archived = kd.get(&format!("k{}", i).into_bytes()).unwrap();
            assert_eq!(archived.count.to_native(), 19 * 10 + i);
        }
    }

    // ---- crash-recovery rename paths ----

    #[test]
    fn open_recovers_from_compact_extension() {
        // Simulates a crash *after* compaction wrote the new file and the
        // first rename succeeded (main → bak) but the second (compact → main)
        // didn't: we observe `<path>.compact` exists, `<path>` doesn't.
        let p = tmp_path("recover_compact");
        {
            let mut kd = create(&p);
            kd.put(k("a"), v("recovered_compact", 7)).unwrap();
            kd.flush().unwrap();
        }
        let compact = p.with_extension("compact");
        fs::rename(&p, &compact).unwrap();
        assert!(!p.exists() && compact.exists());

        let kd = open(&p);
        assert!(
            p.exists() && !compact.exists(),
            "main should have been restored"
        );
        assert_val_eq(&*kd.get(&k("a")).unwrap(), &v("recovered_compact", 7));
    }

    #[test]
    fn open_recovers_from_bak_extension() {
        // Simulates a crash between the two renames in compact(), with no
        // .compact file present — falls back to .bak.
        let p = tmp_path("recover_bak");
        {
            let mut kd = create(&p);
            kd.put(k("b"), v("recovered_bak", 11)).unwrap();
            kd.flush().unwrap();
        }
        let bak = p.with_extension("bak");
        fs::rename(&p, &bak).unwrap();
        assert!(!p.exists() && bak.exists());

        let kd = open(&p);
        assert!(
            p.exists() && !bak.exists(),
            "main should have been restored"
        );
        assert_val_eq(&*kd.get(&k("b")).unwrap(), &v("recovered_bak", 11));
    }

    #[test]
    fn open_prefers_compact_over_bak() {
        // If both stale files exist, .compact (the newer, intended-final file)
        // wins over .bak (the safety copy of the pre-compaction state).
        let p = tmp_path("recover_both");
        // Build a "compact" candidate with the new data.
        let compact = p.with_extension("compact");
        {
            let mut kd = create(&compact);
            kd.put(k("k"), v("from_compact", 1)).unwrap();
            kd.flush().unwrap();
        }
        // Build a "bak" candidate with the old data.
        let bak = p.with_extension("bak");
        {
            let mut kd = create(&bak);
            kd.put(k("k"), v("from_bak", 99)).unwrap();
            kd.flush().unwrap();
        }
        assert!(!p.exists());

        let kd = open(&p);
        assert_val_eq(&*kd.get(&k("k")).unwrap(), &v("from_compact", 1));
    }

    // ---- zero-copy properties ----

    #[test]
    fn get_returns_stable_reference_into_mmap() {
        // Two successive `get`s on the same key must yield the *same* address
        // — proving no allocation/copy occurred. If `get` was deserializing
        // into an owned value, each call would produce a fresh address.
        let p = tmp_path("zero_copy_ptr");
        let mut kd = create(&p);
        kd.put(k("a"), v("hello", 1)).unwrap();
        let p1 = kd.get(&k("a")).unwrap().archived() as *const ArchivedTestVal as usize;
        let p2 = kd.get(&k("a")).unwrap().archived() as *const ArchivedTestVal as usize;
        assert_eq!(p1, p2);
    }

    #[test]
    fn variable_length_value_round_trips() {
        // Exercises rkyv's *relative pointers* inside V (`String`, `Vec`).
        // The previous buggy `get` cast bytes at offset 0 instead of at the
        // root position (end of buffer) — variable-length payloads would
        // either crash or silently return garbage. This test would fail
        // catastrophically under the old code.
        let p = tmp_path("rel_ptrs");
        let mut kd = create(&p);
        let big = TestVal {
            name: "a moderately long string that lives via a relative pointer".into(),
            count: 0xDEAD_BEEF,
            tags: (0..32).collect(),
        };
        let big_clone = big.clone();
        kd.put(k("big"), big).unwrap();
        assert_val_eq(&*kd.get(&k("big")).unwrap(), &big_clone);
    }

    #[test]
    fn deserialize_via_rkyv_high_api_works() {
        // Sanity-check: from the archived reference we can also deserialize
        // into an owned `TestVal` using the regular rkyv API.
        let p = tmp_path("deser");
        let mut kd = create(&p);
        let original = v("round-trip", 42);
        kd.put(k("a"), original.clone()).unwrap();

        let archived = kd.get(&k("a")).unwrap();
        let restored: TestVal =
            rkyv::deserialize::<TestVal, rkyv::rancor::Error>(&*archived).unwrap();
        assert_eq!(restored, original);
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
        assert_eq!(kd.len(), n as usize);
        for i in 0..n {
            let key = format!("key_{:05}", i).into_bytes();
            let archived = kd.get(&key).unwrap();
            assert_eq!(archived.name.as_str(), format!("val_{}", i));
            assert_eq!(archived.count.to_native(), i);
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
        assert_eq!(kd.len(), n as usize);
        for i in 0..n {
            let key = format!("key_{:04}", i).into_bytes();
            let archived = kd.get(&key).unwrap();
            assert_eq!(archived.count.to_native(), i);
        }
    }

    // ---- empty / fresh ----

    #[test]
    fn fresh_keydir_has_zero_data() {
        let p = tmp_path("fresh");
        let kd = create(&p);
        assert_eq!(kd.len(), 0);
        assert_eq!(kd.data_size(), 0);
        assert_eq!(kd.dead_bytes(), 0);
        assert!(kd.is_empty());
    }

    #[test]
    fn open_fresh_file_is_empty() {
        let p = tmp_path("open_fresh");
        // Create then immediately drop without writing anything.
        drop(create(&p));
        let kd = open(&p);
        assert_eq!(kd.len(), 0);
        assert!(kd.is_empty());
    }

    // ---- new API surface: ValueRef, owned-key iteration, update ----------

    #[test]
    fn value_ref_derefs_to_archived() {
        // ValueRef should be transparently usable wherever &Archived<V> is.
        let p = tmp_path("vr_deref");
        let mut kd = create(&p);
        kd.put(k("a"), v("hello", 7)).unwrap();
        let vr = kd.get(&k("a")).unwrap();
        // Field access through Deref.
        assert_eq!(vr.name.as_str(), "hello");
        assert_eq!(vr.count.to_native(), 7);
    }

    #[test]
    fn value_ref_to_owned_round_trips() {
        let p = tmp_path("vr_owned");
        let mut kd = create(&p);
        let original = v("round", 42);
        kd.put(k("a"), original.clone()).unwrap();
        let owned: TestVal = kd.get(&k("a")).unwrap().to_owned();
        assert_eq!(owned, original);
    }

    #[test]
    fn keys_yields_owned() {
        let p = tmp_path("keys_owned");
        let mut kd = create(&p);
        kd.put(k("a"), v("x", 1)).unwrap();
        kd.put(k("b"), v("y", 2)).unwrap();
        // Collect the iterator — yields owned Vec<u8>, no borrow held.
        let mut keys: Vec<TestKey> = kd.keys().collect();
        keys.sort();
        assert_eq!(keys, vec![k("a"), k("b")]);
    }

    #[test]
    fn entries_yields_owned_keys_and_value_refs() {
        let p = tmp_path("entries_owned");
        let mut kd = create(&p);
        kd.put(k("a"), v("av", 1)).unwrap();
        kd.put(k("b"), v("bv", 2)).unwrap();
        // Collect into a HashMap keyed by owned K — proves keys are detached
        // from kd's borrow (ValueRefs still borrow, dropped at end of map).
        let collected: std::collections::HashMap<TestKey, u32> = kd
            .entries()
            .map(|(key, vr)| (key, vr.count.to_native()))
            .collect();
        assert_eq!(collected.len(), 2);
        assert_eq!(collected.get(&k("a")), Some(&1));
        assert_eq!(collected.get(&k("b")), Some(&2));
    }

    #[test]
    fn update_modifies_existing() {
        let p = tmp_path("update_exists");
        let mut kd = create(&p);
        kd.put(k("a"), v("initial", 10)).unwrap();
        let existed = kd
            .update(&k("a"), |mut v| {
                v.count += 5;
                v.tags.push(999);
                v
            })
            .unwrap();
        assert!(existed);
        let archived = kd.get(&k("a")).unwrap();
        assert_eq!(archived.count.to_native(), 15);
        assert_eq!(archived.tags.len(), 4);
        assert_eq!(archived.tags[3].to_native(), 999);
    }

    #[test]
    fn update_missing_returns_false() {
        // `update` only modifies existing keys — no implicit insert.
        let p = tmp_path("update_missing");
        let mut kd = create(&p);
        let call_count = std::cell::Cell::new(0);
        let existed = kd
            .update(&k("new"), |v| {
                call_count.set(call_count.get() + 1);
                v
            })
            .unwrap();
        assert!(!existed);
        assert_eq!(
            call_count.get(),
            0,
            "f should not be called when key is absent"
        );
        assert!(kd.get(&k("new")).is_none());
    }

    #[test]
    fn update_in_place_mutates_fixed_width_field() {
        // Increment a u32 counter directly in the archive — no deserialize.
        let p = tmp_path("uip_count");
        let mut kd = create(&p);
        kd.put(k("a"), v("counter", 10)).unwrap();

        let existed = kd
            .update_in_place(&k("a"), |archived| {
                let new = archived.count.to_native() + 5;
                archived.count = new.into();
            })
            .unwrap();
        assert!(existed);

        let archived = kd.get(&k("a")).unwrap();
        assert_eq!(archived.count.to_native(), 15);
        // Name and tags unchanged.
        assert_eq!(archived.name.as_str(), "counter");
        assert_eq!(archived.tags.len(), 3);
    }

    #[test]
    fn update_in_place_returns_false_for_missing() {
        let p = tmp_path("uip_missing");
        let mut kd = create(&p);
        let mut called = false;
        let existed = kd
            .update_in_place(&k("ghost"), |_| {
                called = true;
            })
            .unwrap();
        assert!(!existed);
        assert!(!called, "closure must not run for absent keys");
    }

    #[test]
    fn update_in_place_persists_after_reopen() {
        // Mutation through update_in_place writes directly into the mmap,
        // so it must survive reopen.
        let p = tmp_path("uip_persist");
        {
            let mut kd = create(&p);
            kd.put(k("a"), v("counter", 100)).unwrap();
            kd.update_in_place(&k("a"), |archived| {
                archived.count = 999u32.into();
            })
            .unwrap();
            kd.flush().unwrap();
        }
        let kd = open(&p);
        let archived = kd.get(&k("a")).unwrap();
        assert_eq!(archived.count.to_native(), 999);
    }
}
