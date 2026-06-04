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
//!   the mmap (no intermediate `Vec<u8>` allocation). `write_entry_into`
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
//!
use std::{
    borrow::Cow,
    fmt,
    fs::{File, OpenOptions},
    hash::Hash,
    io::{self},
    marker::PhantomData,
    path::Path,
};

use bincode::{Decode, Encode};
use hashbrown::hash_map::Entry;
use hashbrown::HashMap;
use memmap2::MmapMut;

use crate::core::backend::Backend;
use crate::utils::serdes::{deserialize_from, read_u32_le, with_two_scratches};

const DEFAULT_INITIAL_CAPACITY: u64 = 1024 * 1024;
const DEFAULT_COMPACTION_RATIO: f64 = 0.5;
/// Magic number identifying a KeyDir file. Stored at offset 0 as `u32` LE.
/// ASCII: `"KIRD"`.
const MAGIC: u32 = 0x4452494B;
/// File header size in bytes: `[MAGIC: u32 LE]`. Entry data follows immediately.
const HEADER_SIZE: usize = 4;
const TOMBSTONE: u32 = u32::MAX;
/// Replay terminator written at the live tail by `create`, every
/// record write, `clear`, and `compact`. Distinct from `value_size == 0`
/// (which is a legitimate empty-value encoding) so a `V` whose bincode
/// representation is 0 bytes (e.g. the unit type) round-trips correctly.
const SENTINEL: u32 = u32::MAX - 1;

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
#[derive(Debug, Clone, Default, PartialEq, Eq, Encode, Decode)]
pub struct KeyDirStats {
    pub data_size: u64,
    pub dead_bytes: u64,
}

/// Offset and total on-disk size of an entry in the mmap. Kept compact so
/// the in-memory HashMap stays lean. The individual `value_size` and
/// `key_size` live on disk and are re-read from the mmap when the
/// tombstone writer needs to locate the encoded key inside the record.
#[derive(Debug, Clone)]
struct EntryMeta {
    offset: u64,
    /// Total bytes the entry occupies on disk: two u32 length prefixes
    /// (8) plus the encoded key and value bytes.
    record_size: u32,
}

// ---------------------------------------------------------------------------
// Field-level free functions
//
// These take individual field borrows rather than `&self` / `&mut self`,
// so Backend methods that hold a HashMap `Entry` / `RawEntryMut` open
// across a read-and-then-write sequence can call them with
// `&mut self.mmap`, `&mut self.file`, `&mut self.stats` directly — the
// disjoint-field borrows don't conflict with the entry's borrow on
// `self.index`.
// ---------------------------------------------------------------------------

/// Borrow the raw value bytes for `meta`. The 4-byte length prefix is
/// excluded. `value_size` is read from the live record's own header on
/// the same cache line as the value bytes themselves — essentially free.
fn value_bytes_in<'a>(mmap: &'a MmapMut, meta: &EntryMeta) -> &'a [u8] {
    let offset = meta.offset as usize;
    let value_size =
        read_u32_le(mmap, offset).expect("live record header within mmap bounds") as usize;
    let start = offset + 4;
    &mmap[start..start + value_size]
}

/// Grow the file to at least `desired` bytes (`max(current * 2, desired)`)
/// and remap.
fn grow_into(mmap: &mut MmapMut, file: &mut File, desired: u64) -> io::Result<()> {
    let new_capacity = ((mmap.len() as u64) * 2).max(desired);
    file.set_len(new_capacity)?;
    *mmap = unsafe { MmapMut::map_mut(&*file)? };
    Ok(())
}

/// Append `[vlen u32][V][klen u32][K]` at the current write tail —
/// at `stats.data_size`. Writes the trailing `SENTINEL` so a
/// subsequent `replay_wal` terminates correctly. Returns the new
/// record's `EntryMeta`.
fn write_entry_into<K, V>(
    mmap: &mut MmapMut,
    file: &mut File,
    stats: &mut KeyDirStats,
    key: &K,
    value: &V,
) -> io::Result<EntryMeta>
where
    K: Encode,
    V: Encode,
{
    // Encode key + value once into pooled scratch buffers; the slice
    // lengths give the on-disk sizes for free.
    with_two_scratches(key, value, |kb, vb| {
        let k_size = kb.len();
        let v_size = vb.len();
        let total = 8 + v_size + k_size;

        let offset = stats.data_size as usize;
        let end = offset + total;
        // Grow with room for the trailing SENTINEL too.
        if end + 4 > mmap.len() {
            grow_into(mmap, file, (end + 4) as u64)?;
        }

        mmap[offset..offset + 4].copy_from_slice(&(v_size as u32).to_le_bytes());
        let v_off = offset + 4;
        mmap[v_off..v_off + v_size].copy_from_slice(vb);
        let k_len_off = v_off + v_size;
        mmap[k_len_off..k_len_off + 4].copy_from_slice(&(k_size as u32).to_le_bytes());
        let k_off = k_len_off + 4;
        mmap[k_off..k_off + k_size].copy_from_slice(kb);

        mmap[end..end + 4].copy_from_slice(&SENTINEL.to_le_bytes());
        stats.data_size = end as u64;
        Ok(EntryMeta {
            offset: offset as u64,
            record_size: total as u32,
        })
    })
}

/// Append a tombstone for the live record described by `old`, reusing
/// the already-encoded key bytes that still live in the doomed
/// entry's slot — no bincode encode, no scratch `Vec<u8>`. A single
/// `copy_within` moves the key into the tombstone payload.
fn write_tombstone_into(
    mmap: &mut MmapMut,
    file: &mut File,
    stats: &mut KeyDirStats,
    old: &EntryMeta,
) -> io::Result<()> {
    // value_size sits in the first 4 bytes of the live record;
    // key_size falls out of `record_size - 8 - value_size`. We only
    // need value_size to locate where the key bytes start.
    let value_size = read_u32_le(mmap, old.offset as usize)
        .expect("live record header within mmap bounds") as usize;
    let key_size = old.record_size as usize - 8 - value_size;
    let total = 8 + key_size;

    let new_offset = stats.data_size as usize;
    let end = new_offset + total;
    // Grow with room for the trailing SENTINEL too.
    if end + 4 > mmap.len() {
        grow_into(mmap, file, (end + 4) as u64)?;
    }
    let key_src_start = old.offset as usize + 8 + value_size;

    mmap[new_offset..new_offset + 4].copy_from_slice(&TOMBSTONE.to_le_bytes());
    mmap[new_offset + 4..new_offset + 8].copy_from_slice(&(key_size as u32).to_le_bytes());
    mmap.copy_within(key_src_start..key_src_start + key_size, new_offset + 8);

    mmap[end..end + 4].copy_from_slice(&SENTINEL.to_le_bytes());
    stats.data_size = end as u64;
    stats.dead_bytes += 8 + key_size as u64;
    Ok(())
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

        // Write file header: MAGIC at offset 0, then SENTINEL at the
        // live tail so `replay_wal` on a fresh file terminates
        // immediately at `HEADER_SIZE`.
        mmap[0..4].copy_from_slice(&MAGIC.to_le_bytes());
        mmap[HEADER_SIZE..HEADER_SIZE + 4].copy_from_slice(&SENTINEL.to_le_bytes());

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
    // callers reach through. The mmap-touching primitives all live as
    // field-level free functions (`value_bytes_in`, `write_entry_into`,
    // `write_tombstone_into`) so callers can hold a `HashMap::Entry`
    // open across them without aliasing self.
    // -----------------------------------------------------------------------

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
    /// Rebuild the in-memory index by scanning the file from `HEADER_SIZE`.
    /// Replays all entries: live entries overwrite prior index entries,
    /// tombstones remove them. Accumulates dead_bytes from each.
    ///
    /// Live-entry inserts route through the `Entry` API so the cold
    /// path hashes once instead of twice.
    fn rebuild_index(&mut self) -> io::Result<()> {
        let mut cursor: usize = HEADER_SIZE;
        self.stats.dead_bytes = 0;

        while let Some(value_size) = read_u32_le(&self.mmap, cursor) {
            // SENTINEL marks the live tail. value_size == 0 is a
            // legitimate empty-value encoding (e.g. V = ()) and is NOT
            // a stop condition.
            if value_size == SENTINEL {
                break;
            }

            let is_tombstone = value_size == TOMBSTONE;
            let key_size_off = if is_tombstone {
                cursor + 4
            } else {
                cursor + 4 + value_size as usize
            };
            let key_size =
                read_u32_le(&self.mmap, key_size_off).expect("key_size header within mmap bounds");
            let key_start = key_size_off + 4;
            let entry_end = key_start + key_size as usize;

            let key: K = deserialize_from(&self.mmap[key_start..entry_end])?;

            if is_tombstone {
                if let Some(old) = self.index.remove(&key) {
                    self.stats.dead_bytes += old.record_size as u64;
                }
                self.stats.dead_bytes += 8 + key_size as u64;
            } else {
                let record_size = 8 + value_size + key_size;
                let meta = EntryMeta {
                    offset: cursor as u64,
                    record_size,
                };
                match self.index.entry(key) {
                    Entry::Occupied(mut e) => {
                        self.stats.dead_bytes += e.get().record_size as u64;
                        *e.get_mut() = meta;
                    }
                    Entry::Vacant(e) => {
                        e.insert(meta);
                    }
                }
            }
            cursor = entry_end;
        }

        self.stats.data_size = cursor as u64;
        Ok(())
    }
}

impl<K, V> Drop for KeyDir<K, V> {
    /// Schedule a final writeback without blocking. We don't promise
    /// crash recovery in this layer, so a sync `flush()` here would
    /// just stall shutdown for a guarantee we don't make. Callers that
    /// need durability should call [`Backend::sync`] explicitly before
    /// dropping.
    fn drop(&mut self) {
        let _ = self.mmap.flush_async();
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
    type Config = KeyDirConfig;

    /// One HashMap lookup, then materialize the value by decoding the
    /// mmap slice the meta points at. Returns `Cow::Owned` since the
    /// value is freshly decoded each call.
    fn get(&self, key: &K) -> Option<Cow<'_, V>> {
        let meta = self.index.get(key)?;
        let v: V = deserialize_from(value_bytes_in(&self.mmap, meta)).expect("valid encoded value");
        Some(Cow::Owned(v))
    }

    /// HashMap probe — no mmap access, no deserialization. Faster than
    /// `get(key).is_some()`.
    fn contains(&self, key: &K) -> bool {
        self.index.contains_key(key)
    }

    /// Append the new record, then update the index. Routes the index
    /// touch through hashbrown's `Entry` API — one hash whether the key
    /// is fresh or being overwritten. (The previous `get_mut` / `insert`
    /// pair hashed twice on the cold path.)
    fn put(&mut self, key: K, value: V) -> io::Result<()> {
        let new_meta = write_entry_into(
            &mut self.mmap,
            &mut self.file,
            &mut self.stats,
            &key,
            &value,
        )?;
        match self.index.entry(key) {
            Entry::Occupied(mut e) => {
                self.stats.dead_bytes += e.get().record_size as u64;
                *e.get_mut() = new_meta;
            }
            Entry::Vacant(e) => {
                e.insert(new_meta);
            }
        }
        self.maybe_compact()
    }

    /// Remove from the index, then write a tombstone using the
    /// already-encoded key bytes that still live in the deleted entry's
    /// slot — see [`write_tombstone_into`]. No bincode encode, no
    /// scratch `Vec<u8>`.
    fn delete(&mut self, key: &K) -> io::Result<bool> {
        let Some(old) = self.index.remove(key) else {
            return Ok(false);
        };
        self.stats.dead_bytes += old.record_size as u64;
        write_tombstone_into(&mut self.mmap, &mut self.file, &mut self.stats, &old)?;
        self.maybe_compact()?;
        Ok(true)
    }

    /// Insert iff `key` is absent. One hash per call: probes via
    /// `Entry`; the Vacant arm writes the record and fills the slot in
    /// the same lookup. The default trait impl does `contains` + `put`
    /// — two hashes — which is why we override.
    fn put_if_absent(&mut self, key: K, value: V) -> io::Result<bool> {
        let inserted = match self.index.entry(key) {
            Entry::Occupied(_) => false,
            Entry::Vacant(e) => {
                // The entry holds K; pass it as &K to write_entry_into
                // via VacantEntry::key().
                let key_ref: &K = e.key();
                let meta = write_entry_into(
                    &mut self.mmap,
                    &mut self.file,
                    &mut self.stats,
                    key_ref,
                    &value,
                )?;
                e.insert(meta);
                true
            }
        };

        if inserted {
            self.maybe_compact()?;
        }
        Ok(inserted)
    }

    /// Insert or overwrite, returning the previous value (if any).
    /// One hash per call: the same `Entry` slot is reused for the
    /// optional value read and for the placement. The default trait
    /// impl does `get` + `put` — two hashes plus a redundant decode on
    /// the missing-key path — which is why we override.
    ///
    /// The returned `Cow` is always `Owned`: KeyDir decodes the prior
    /// value out of the mmap and immediately overwrites the slot, so
    /// there's nothing borrowable left behind.
    fn replace(&mut self, key: K, value: V) -> io::Result<Option<Cow<'_, V>>> {
        let prev = match self.index.entry(key) {
            Entry::Occupied(mut e) => {
                let (prev_val, old_size) = {
                    let old_meta = e.get();
                    let bytes = value_bytes_in(&self.mmap, old_meta);
                    let prev_val: V = deserialize_from(bytes)?;
                    (prev_val, old_meta.record_size as u64)
                };
                let key_ref: &K = e.key();
                let meta = write_entry_into(
                    &mut self.mmap,
                    &mut self.file,
                    &mut self.stats,
                    key_ref,
                    &value,
                )?;
                self.stats.dead_bytes += old_size;
                *e.get_mut() = meta;
                Some(prev_val)
            }
            Entry::Vacant(e) => {
                let key_ref: &K = e.key();
                let meta = write_entry_into(
                    &mut self.mmap,
                    &mut self.file,
                    &mut self.stats,
                    key_ref,
                    &value,
                )?;
                e.insert(meta);
                None
            }
        };

        self.maybe_compact()?;
        Ok(prev.map(Cow::Owned))
    }

    /// Unified read-modify-write / insert / delete primitive. Writes at
    /// most one new log record.
    ///
    /// Hashes the key exactly **once** per call. The slot is located via
    /// `raw_entry_mut().from_key(key)` and held open across the value
    /// read, the closure invocation, and the slot mutation. The
    /// disjoint-field borrows on `self.mmap` / `self.file` / `self.stats`
    /// don't conflict with the entry's borrow on `self.index`.
    fn update<F>(&mut self, key: &K, f: F) -> io::Result<()>
    where
        F: FnOnce(Option<V>) -> Option<V>,
    {
        use hashbrown::hash_map::RawEntryMut;

        let mutated = match self.index.raw_entry_mut().from_key(key) {
            RawEntryMut::Occupied(mut e) => {
                let current: V = {
                    let bytes = value_bytes_in(&self.mmap, e.get());
                    deserialize_from(bytes)?
                };
                match f(Some(current)) {
                    Some(new_v) => {
                        let old_size = e.get().record_size as u64;
                        let meta = write_entry_into(
                            &mut self.mmap,
                            &mut self.file,
                            &mut self.stats,
                            key,
                            &new_v,
                        )?;
                        self.stats.dead_bytes += old_size;
                        *e.get_mut() = meta;
                        true
                    }
                    None => {
                        self.stats.dead_bytes += e.get().record_size as u64;
                        write_tombstone_into(
                            &mut self.mmap,
                            &mut self.file,
                            &mut self.stats,
                            e.get(),
                        )?;
                        e.remove();
                        true
                    }
                }
            }
            RawEntryMut::Vacant(e) => match f(None) {
                Some(new_v) => {
                    let meta = write_entry_into(
                        &mut self.mmap,
                        &mut self.file,
                        &mut self.stats,
                        key,
                        &new_v,
                    )?;
                    e.insert(key.clone(), meta);
                    true
                }
                None => false,
            },
        };

        if mutated {
            self.maybe_compact()?;
        }
        Ok(())
    }

    /// Drop every live entry. Resets `data_size` to `HEADER_SIZE` and
    /// zeroes the post-header sentinel so a subsequent open sees an
    /// empty log. The backing file is not truncated.
    ///
    fn clear(&mut self) -> io::Result<()> {
        self.index.clear();
        self.stats.data_size = HEADER_SIZE as u64;
        self.stats.dead_bytes = 0;
        self.mmap[0..4].copy_from_slice(&MAGIC.to_le_bytes());
        // SENTINEL at the live tail so a subsequent reopen replays as
        // empty. initial_capacity is always >> HEADER_SIZE + 4 so no
        // length check is needed.
        self.mmap[HEADER_SIZE..HEADER_SIZE + 4].copy_from_slice(&SENTINEL.to_le_bytes());
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
        // offsets in-place — no drain/reinsert, no key clones. We only
        // need `&mut EntryMeta`; the K borrow would just bloat the
        // ref-tuple (2 pointers → 1) without being used by the sort or
        // the relocation loop.
        let mut snapshot: Vec<&mut EntryMeta> = self.index.values_mut().collect();
        snapshot.sort_unstable_by_key(|m| m.offset);

        let mut cursor: usize = HEADER_SIZE;
        for meta in snapshot.iter_mut() {
            let src = meta.offset as usize;
            let len = meta.record_size as usize;
            if src != cursor {
                self.mmap.copy_within(src..src + len, cursor);
            }
            meta.offset = cursor as u64;
            cursor += len;
        }

        self.mmap[cursor..cursor + 4].copy_from_slice(&SENTINEL.to_le_bytes());

        self.stats.data_size = cursor as u64;
        self.stats.dead_bytes = 0;

        // Release the physical slack past the new tail. Keep at least
        // `initial_capacity` so the next writes don't immediately
        // re-grow the file.
        //
        // Windows note: `set_len` refuses to shrink a file while a
        // mapped section is open. Swap in a small anonymous mmap as a
        // placeholder so the file's mapped section is dropped before
        // the truncation, then remap onto the resized file.
        let new_capacity = ((cursor + 4) as u64).max(self.config.initial_capacity);
        self.mmap = MmapMut::map_anon(1)?;
        self.file.set_len(new_capacity)?;
        self.mmap = unsafe { MmapMut::map_mut(&self.file)? };
        Ok(())
    }

    fn keys<'a>(&'a self) -> impl Iterator<Item = Cow<'a, K>> + 'a
    where
        K: 'a,
    {
        self.index.keys().map(Cow::Borrowed)
    }

    fn values<'a>(&'a self) -> impl Iterator<Item = Cow<'a, V>> + 'a
    where
        V: 'a,
    {
        self.index.values().map(|meta| {
            let v: V =
                deserialize_from(value_bytes_in(&self.mmap, meta)).expect("valid encoded value");
            Cow::Owned(v)
        })
    }

    fn entries<'a>(&'a self) -> impl Iterator<Item = (Cow<'a, K>, Cow<'a, V>)> + 'a
    where
        K: 'a,
        V: 'a,
    {
        self.index.iter().map(|(k, meta)| {
            let v: V =
                deserialize_from(value_bytes_in(&self.mmap, meta)).expect("valid encoded value");
            (Cow::Borrowed(k), Cow::Owned(v))
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

    fn config(&self) -> &Self::Config {
        &self.config
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
    use std::borrow::Cow;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    /// Test helper — read through the Cow-returning Backend::get API
    /// and immediately materialize to an owned value for easy compare.
    fn get<K, V>(kd: &KeyDir<K, V>, key: &K) -> Option<V>
    where
        K: Encode + Decode<()> + Hash + Eq + Clone,
        V: Encode + Decode<()> + Clone,
    {
        Backend::get(kd, key).map(Cow::into_owned)
    }

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
        assert_eq!(get(&kd, &k("alice")), Some(v("alice", 1)));
    }

    #[test]
    fn get_missing_returns_none() {
        let p = tmp_path("missing");
        let kd = create(&p);
        assert!(get(&kd, &k("ghost")).is_none());
    }

    #[test]
    fn put_multiple_keys() {
        let p = tmp_path("multi");
        let mut kd = create(&p);
        kd.put(k("a"), v("alpha", 1)).unwrap();
        kd.put(k("b"), v("beta", 2)).unwrap();
        kd.put(k("c"), v("gamma", 3)).unwrap();
        assert_eq!(kd.size(), 3);
        assert_eq!(get(&kd, &k("a")), Some(v("alpha", 1)));
        assert_eq!(get(&kd, &k("b")), Some(v("beta", 2)));
        assert_eq!(get(&kd, &k("c")), Some(v("gamma", 3)));
    }

    // ---- overwrite & dead-bytes accounting ----

    #[test]
    fn overwrite_replaces_value() {
        let p = tmp_path("overwrite");
        let mut kd = create(&p);
        kd.put(k("a"), v("first", 1)).unwrap();
        kd.put(k("a"), v("second", 2)).unwrap();
        assert_eq!(kd.size(), 1);
        assert_eq!(get(&kd, &k("a")), Some(v("second", 2)));
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
        assert!(get(&kd, &k("a")).is_none());
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
        assert_eq!(get(&kd, &k("a")), Some(v("second", 2)));
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

        let mut collected: std::collections::HashMap<TestKey, u32> = kd
            .entries()
            .map(|(key, val)| (key.into_owned(), val.count))
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
        assert_eq!(kd.size(), 2);
        assert_eq!(get(&kd, &k("a")), Some(v("alpha", 1)));
        assert_eq!(get(&kd, &k("b")), Some(v("beta", 2)));
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
        assert_eq!(get(&kd, &k("a")), Some(v("v3", 3)));
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
            assert_eq!(get(&kd, &key), Some(v("payload-grows-the-file", i)));
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
            let got = get(&kd, &format!("k{}", i).into_bytes()).unwrap();
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
            let got = get(&kd, &format!("k{}", i).into_bytes()).unwrap();
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
        assert_eq!(get(&kd, &k("a")).unwrap().count, 19);
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
        assert_eq!(get(&kd, &k("hot")).unwrap().count, 1999);
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
        assert_eq!(get(&kd, &k("big")), Some(big));
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
            let got = get(&kd, &key).unwrap();
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
            let got = get(&kd, &key).unwrap();
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
        let mut keys: Vec<TestKey> = kd.keys().map(Cow::into_owned).collect();
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
        let got = get(&kd, &k("a")).unwrap();
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
        assert_eq!(get(&kd, &k("new")), Some(v("inserted", 1)));
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
        assert!(get(&kd, &k("ghost")).is_none());
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
        assert!(get(&kd, &k("a")).is_none());
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
        assert!(get(&kd, &k("a")).is_none());
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
        assert_eq!(get(&kd, &k("z")), Some(v("zv", 99)));
        assert!(get(&kd, &k("a")).is_none());
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
        assert_eq!(get(&kd, &k("a")), Some(v("first", 1)));
    }

    #[test]
    fn put_if_absent_noop_when_present() {
        let p = tmp_path("pia_noop");
        let mut kd = create(&p);
        kd.put(k("a"), v("first", 1)).unwrap();
        let inserted = Backend::put_if_absent(&mut kd, k("a"), v("second", 2)).unwrap();
        assert!(!inserted);
        assert_eq!(get(&kd, &k("a")), Some(v("first", 1)));
    }

    #[test]
    fn replace_returns_previous_value() {
        let p = tmp_path("replace");
        let mut kd = create(&p);
        kd.put(k("a"), v("first", 1)).unwrap();
        let prev = Backend::replace(&mut kd, k("a"), v("second", 2))
            .unwrap()
            .map(Cow::into_owned);
        assert_eq!(prev, Some(v("first", 1)));
        assert_eq!(get(&kd, &k("a")), Some(v("second", 2)));
    }

    #[test]
    fn replace_returns_none_when_absent() {
        let p = tmp_path("replace_absent");
        let mut kd = create(&p);
        let prev = Backend::replace(&mut kd, k("a"), v("new", 99)).unwrap();
        assert!(prev.is_none());
        assert_eq!(get(&kd, &k("a")), Some(v("new", 99)));
    }

    // ---- bulk_put / bulk_delete ----

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
            assert_eq!(
                get(&kd, &format!("k{:04}", i).into_bytes()),
                Some(v("p", i))
            );
        }
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
        assert_eq!(get(&kd, &k("a")), Some(v("alpha", 1)));
    }

    #[test]
    fn flush_returns_ok_on_empty_keydir() {
        let p = tmp_path("flush_empty");
        let kd = create(&p);
        kd.flush().unwrap();
        kd.sync().unwrap();
    }

    // ---- replace / put_if_absent: persistence checks for the overrides ----

    #[test]
    fn replace_inserts_when_absent_and_persists() {
        // The Backend override of `replace` exercises the Vacant->Insert
        // branch — write the record, fill the slot, return None. Reopen
        // to confirm the new record is durable.
        let p = tmp_path("replace_absent_persist");
        {
            let mut kd = create(&p);
            let prev = Backend::replace(&mut kd, k("fresh"), v("inserted", 7)).unwrap();
            assert!(prev.is_none());
            kd.sync().unwrap();
        }
        let kd = open(&p);
        assert_eq!(get(&kd, &k("fresh")), Some(v("inserted", 7)));
    }

    #[test]
    fn replace_returns_old_value_and_accounts_dead_bytes() {
        // The Occupied branch of the override must return the decoded
        // old value AND record the old record's bytes as dead.
        let p = tmp_path("replace_old_dead");
        let mut kd = create_with(
            &p,
            KeyDirConfig {
                initial_capacity: 1 << 20,
                compaction_ratio: 0.99,
            },
        );
        kd.put(k("a"), v("v1", 1)).unwrap();
        assert_eq!(kd.stats().dead_bytes, 0);
        let prev = Backend::replace(&mut kd, k("a"), v("v2", 2))
            .unwrap()
            .map(Cow::into_owned);
        assert_eq!(prev, Some(v("v1", 1)));
        assert_eq!(get(&kd, &k("a")), Some(v("v2", 2)));
        assert!(kd.stats().dead_bytes > 0);
    }

    #[test]
    fn put_if_absent_does_not_touch_disk_when_present() {
        // The Occupied branch of the override must NOT call write_entry —
        // no record is appended, data_size doesn't move.
        let p = tmp_path("pia_no_disk_write");
        let mut kd = create_with(
            &p,
            KeyDirConfig {
                initial_capacity: 1 << 20,
                compaction_ratio: 0.99,
            },
        );
        kd.put(k("a"), v("first", 1)).unwrap();
        let size_before = kd.stats().data_size;
        let dead_before = kd.stats().dead_bytes;
        let inserted = Backend::put_if_absent(&mut kd, k("a"), v("second", 2)).unwrap();
        assert!(!inserted);
        assert_eq!(get(&kd, &k("a")), Some(v("first", 1)));
        // No write happened.
        assert_eq!(kd.stats().data_size, size_before);
        assert_eq!(kd.stats().dead_bytes, dead_before);
    }

    /// KeyDir borrows keys from the in-memory index but always decodes
    /// values from mmap, so `get`/`values` must return `Cow::Owned` and
    /// `keys`/`entries` must borrow the key side.
    #[test]
    fn retrieval_cow_variants() {
        let p = tmp_path("cow_variants");
        let mut kd = create(&p);
        kd.put(k("a"), v("alpha", 1)).unwrap();
        kd.put(k("b"), v("beta", 2)).unwrap();

        assert!(matches!(Backend::get(&kd, &k("a")), Some(Cow::Owned(_))));
        assert!(Backend::keys(&kd).all(|c| matches!(c, Cow::Borrowed(_))));
        assert!(Backend::values(&kd).all(|c| matches!(c, Cow::Owned(_))));
        assert!(Backend::entries(&kd)
            .all(|(k, v)| { matches!(k, Cow::Borrowed(_)) && matches!(v, Cow::Owned(_)) }));
    }

    /// Read paths must round-trip cleanly through `Cow::into_owned()`.
    #[test]
    fn into_owned_yields_decoded_values() {
        let p = tmp_path("cow_into_owned");
        let mut kd = create(&p);
        kd.put(k("a"), v("alpha", 1)).unwrap();

        let owned: TestVal = Backend::get(&kd, &k("a")).unwrap().into_owned();
        assert_eq!(owned, v("alpha", 1));

        let owned_entries: Vec<(TestKey, TestVal)> = Backend::entries(&kd)
            .map(|(k, v)| (k.into_owned(), v.into_owned()))
            .collect();
        assert_eq!(owned_entries, vec![(k("a"), v("alpha", 1))]);
    }

    #[test]
    fn put_if_absent_inserts_and_persists() {
        let p = tmp_path("pia_persist");
        {
            let mut kd = create(&p);
            assert!(Backend::put_if_absent(&mut kd, k("only"), v("once", 1)).unwrap());
            assert!(!Backend::put_if_absent(&mut kd, k("only"), v("twice", 2)).unwrap());
            kd.sync().unwrap();
        }
        let kd = open(&p);
        assert_eq!(get(&kd, &k("only")), Some(v("once", 1)));
    }

    /// A `V` whose bincode encoding is 0 bytes (the unit type here)
    /// used to be silently truncated by the old `value_size == 0`
    /// replay terminator. `SENTINEL = u32::MAX - 1` separates the
    /// empty-value case from the live-tail case.
    #[test]
    fn zero_byte_value_round_trips() {
        let p = tmp_path("zero_byte_value");
        {
            let mut kd: KeyDir<TestKey, ()> = KeyDir::create(&p, KeyDirConfig::default()).unwrap();
            kd.put(k("alpha"), ()).unwrap();
            kd.put(k("beta"), ()).unwrap();
            kd.put(k("gamma"), ()).unwrap();
            kd.delete(&k("beta")).unwrap();
            kd.sync().unwrap();
        }
        let kd: KeyDir<TestKey, ()> = KeyDir::open(&p, KeyDirConfig::default()).unwrap();
        assert_eq!(kd.size(), 2);
        assert_eq!(
            Backend::get(&kd, &k("alpha")).map(Cow::into_owned),
            Some(())
        );
        assert!(Backend::get(&kd, &k("beta")).is_none());
        assert_eq!(
            Backend::get(&kd, &k("gamma")).map(Cow::into_owned),
            Some(())
        );
    }
}
