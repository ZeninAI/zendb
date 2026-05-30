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
//!
//! # Bulk-write mode (`open_tx` / `close_tx`)
//!
//! [`KeyDir`] overrides the trait's transactional bracket to route writes
//! through an in-memory staging buffer while a "transaction" is open. The
//! goal is **write throughput, not atomicity**: there is no isolation,
//! no rollback, no crash semantics beyond what the underlying mmap
//! provides. It's a batching window — call `open_tx`, dump many writes,
//! call `close_tx`, pay the per-batch costs once.
//!
//! ## What changes during a tx
//!
//! - `put` / `delete` encode records into a [`crate::utils::reusables::PooledBuf`]
//!   (a recycled `Vec<u8>`) instead of the mmap. No `grow`, no remap,
//!   no auto-compaction during the tx.
//! - The in-memory index stores **virtual offsets** that span both the
//!   committed mmap region and the staged region. Reads check one
//!   branch: if `meta.offset >= tx_base`, materialize from staging; else
//!   from the mmap. This gives read-your-own-writes for free.
//!
//! ## What `close_tx` actually does
//!
//! 1. `grow` the mmap once to fit `tx_base + staging.len()`.
//! 2. One `copy_from_slice` from staging into `mmap[tx_base..]`.
//! 3. Advance `stats.data_size`. **No index walk** — every
//!    `EntryMeta.offset` written during the tx already addresses the
//!    correct byte of the post-commit mmap, because `tx_base + staging_off
//!    == eventual mmap offset`.
//! 4. Run one `maybe_compact` against the post-commit state.
//! 5. The `PooledBuf` is returned to the thread-local pool on drop, so
//!    the next `open_tx` on any KeyDir in this thread can reuse its
//!    capacity without a new allocation.
//!
//! ## Restrictions
//!
//! - `open_tx` while a tx is already open returns
//!   [`io::ErrorKind::AlreadyExists`]. No nesting, no savepoints.
//! - `compact` / `clear` mid-tx is undefined behaviour for the index
//!   (their staged offsets would point at relocated bytes). A
//!   `debug_assert` catches it in tests; release builds trust the caller.
//! - The caller is responsible for the staging buffer's size — there is
//!   no soft cap. Stuffing gigabytes into one tx will double-buffer
//!   that much RAM until commit.

use std::{
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
use crate::utils::reusables::PooledBuf;
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

/// Active bulk-write window. Holds the staging buffer and the snapshot
/// of `stats.data_size` taken at `open_tx` — together they let every
/// `EntryMeta.offset` written during the tx address the eventual mmap
/// location directly (see the "Bulk-write mode" section of the module
/// docs).
struct TxState {
    staging: PooledBuf,
    tx_base: u64,
}

// ---------------------------------------------------------------------------
// Field-level free functions
//
// These mirror the inherent helpers (`value_bytes`, `grow`, `write_entry`,
// `write_tombstone_for`) but take individual field borrows instead of
// `&self` / `&mut self`. Methods that need to hold a HashMap `Entry` /
// `RawEntryMut` open across a read-and-then-write sequence destructure
// `self` and call these directly, so the entry's borrow on `self.index`
// doesn't conflict with the disjoint borrows on mmap / tx / stats / file.
// ---------------------------------------------------------------------------

/// Borrow the raw value bytes for `meta` — from staging if the entry
/// was written during the current tx (virtual offset >= `tx_base`),
/// otherwise from the mmap. The 4-byte length prefix is excluded.
fn value_bytes_in<'a>(mmap: &'a MmapMut, tx: Option<&'a TxState>, meta: &EntryMeta) -> &'a [u8] {
    if let Some(tx) = tx {
        if meta.offset >= tx.tx_base {
            let local = (meta.offset - tx.tx_base) as usize;
            let start = local + 4;
            let end = start + meta.value_size as usize;
            return &tx.staging[start..end];
        }
    }
    let start = meta.offset as usize + 4;
    let end = start + meta.value_size as usize;
    &mmap[start..end]
}

/// Grow the file to at least `desired` bytes (`max(current * 2, desired)`)
/// and remap. Matches the inherent [`KeyDir::grow`] semantics.
fn grow_into(mmap: &mut MmapMut, file: &mut File, desired: u64) -> io::Result<()> {
    let new_capacity = ((mmap.len() as u64) * 2).max(desired);
    file.set_len(new_capacity)?;
    *mmap = unsafe { MmapMut::map_mut(&*file)? };
    Ok(())
}

/// Append `[vlen u32][V][klen u32][K]` at the current write tail —
/// either `stats.data_size` (mmap path) or the end of `tx.staging`
/// (tx path). Returns `(virtual_offset, value_size, key_size)`. Mirrors
/// [`KeyDir::write_entry`].
fn write_entry_into<K, V>(
    mmap: &mut MmapMut,
    file: &mut File,
    tx: &mut Option<TxState>,
    stats: &mut KeyDirStats,
    key: &K,
    value: &V,
) -> io::Result<(u64, u32, u32)>
where
    K: Encode,
    V: Encode,
{
    let v_size = serialized_size(value)?;
    let k_size = serialized_size(key)?;
    let total = 8 + v_size + k_size;

    if let Some(tx_state) = tx.as_mut() {
        let local = tx_state.staging.len();
        let virtual_offset = tx_state
            .tx_base
            .checked_add(local as u64)
            .ok_or_else(|| io::Error::new(io::ErrorKind::OutOfMemory, "KeyDir offset overflow"))?;
        tx_state.staging.resize(local + total, 0);
        let dst = &mut tx_state.staging[local..local + total];
        dst[0..4].copy_from_slice(&(v_size as u32).to_le_bytes());
        let written = serialize_into(value, &mut dst[4..4 + v_size])?;
        debug_assert_eq!(written, v_size);
        let k_len_off = 4 + v_size;
        dst[k_len_off..k_len_off + 4].copy_from_slice(&(k_size as u32).to_le_bytes());
        let written = serialize_into(key, &mut dst[k_len_off + 4..total])?;
        debug_assert_eq!(written, k_size);
        return Ok((virtual_offset, v_size as u32, k_size as u32));
    }

    let offset = stats.data_size as usize;
    let end = offset
        .checked_add(total)
        .ok_or_else(|| io::Error::new(io::ErrorKind::OutOfMemory, "KeyDir offset overflow"))?;
    if end > mmap.len() {
        grow_into(mmap, file, end as u64)?;
    }

    mmap[offset..offset + 4].copy_from_slice(&(v_size as u32).to_le_bytes());
    let v_off = offset + 4;
    let written = serialize_into(value, &mut mmap[v_off..v_off + v_size])?;
    debug_assert_eq!(written, v_size);
    let k_len_off = v_off + v_size;
    mmap[k_len_off..k_len_off + 4].copy_from_slice(&(k_size as u32).to_le_bytes());
    let k_off = k_len_off + 4;
    let written = serialize_into(key, &mut mmap[k_off..k_off + k_size])?;
    debug_assert_eq!(written, k_size);

    stats.data_size = end as u64;
    Ok((offset as u64, v_size as u32, k_size as u32))
}

/// Append a tombstone for `old`, reusing the already-encoded key bytes
/// from wherever `old` lives (mmap or staging). Mirrors
/// [`KeyDir::write_tombstone_for`].
fn write_tombstone_into(
    mmap: &mut MmapMut,
    file: &mut File,
    tx: &mut Option<TxState>,
    stats: &mut KeyDirStats,
    old: EntryMeta,
) -> io::Result<()> {
    let key_size = old.key_size as usize;
    let total = 8 + key_size;
    let key_offset_within_entry = 4 + old.value_size as usize + 4;

    if let Some(tx_state) = tx.as_mut() {
        let new_local = tx_state.staging.len();
        tx_state.staging.resize(new_local + total, 0);
        tx_state.staging[new_local..new_local + 4].copy_from_slice(&TOMBSTONE.to_le_bytes());
        tx_state.staging[new_local + 4..new_local + 8]
            .copy_from_slice(&(key_size as u32).to_le_bytes());

        if old.offset < tx_state.tx_base {
            let src = old.offset as usize + key_offset_within_entry;
            tx_state.staging[new_local + 8..new_local + 8 + key_size]
                .copy_from_slice(&mmap[src..src + key_size]);
        } else {
            let local = (old.offset - tx_state.tx_base) as usize;
            let src = local + key_offset_within_entry;
            tx_state
                .staging
                .copy_within(src..src + key_size, new_local + 8);
        }

        stats.dead_bytes += 8 + key_size as u64;
        return Ok(());
    }

    let new_offset = stats.data_size as usize;
    let end = new_offset
        .checked_add(total)
        .ok_or_else(|| io::Error::new(io::ErrorKind::OutOfMemory, "KeyDir offset overflow"))?;
    if end > mmap.len() {
        grow_into(mmap, file, end as u64)?;
    }
    let key_src_start = old.offset as usize + key_offset_within_entry;

    mmap[new_offset..new_offset + 4].copy_from_slice(&TOMBSTONE.to_le_bytes());
    mmap[new_offset + 4..new_offset + 8].copy_from_slice(&(key_size as u32).to_le_bytes());
    mmap.copy_within(key_src_start..key_src_start + key_size, new_offset + 8);

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
    /// `Some` while a bulk-write window is open. Writes encode into
    /// `tx.staging` instead of `mmap`; reads consult both via the
    /// virtual-offset branch in `value_bytes`.
    tx: Option<TxState>,
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
            tx: None,
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
            tx: None,
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
    /// directly from either the staging buffer (during a tx, if the
    /// entry lives past `tx_base`) or the mmap. The leading length
    /// prefix is excluded.
    fn value_bytes(&self, meta: &EntryMeta) -> &[u8] {
        value_bytes_in(&self.mmap, self.tx.as_ref(), meta)
    }

    /// Append `[vlen u32][V][klen u32][K]` at the current write tail —
    /// either `stats.data_size` (mmap path) or the end of the staging
    /// buffer (tx path). Measures both halves up front so the mmap path
    /// `grow`s **at most once** per call.
    ///
    /// Returns `(virtual_offset, value_size, key_size)`. Outside of a tx
    /// `virtual_offset == mmap byte offset`; inside a tx it is
    /// `tx_base + staging_offset`, which becomes the correct mmap offset
    /// after `close_tx` copies staging into `mmap[tx_base..]`.
    fn write_entry(&mut self, key: &K, value: &V) -> io::Result<(u64, u32, u32)> {
        write_entry_into(
            &mut self.mmap,
            &mut self.file,
            &mut self.tx,
            &mut self.stats,
            key,
            value,
        )
    }

    /// Append a tombstone for the entry described by `old`, reusing the
    /// already-encoded key bytes already living in the doomed entry's
    /// slot — no bincode encode, no scratch `Vec<u8>`. A single
    /// `copy_within` moves the key into the tombstone payload.
    ///
    /// In tx mode the tombstone is appended to the staging buffer, and
    /// the source key bytes are read from either mmap (if `old` was
    /// committed) or staging (if `old` was written earlier in the same
    /// tx).
    fn write_tombstone_for(&mut self, old: EntryMeta) -> io::Result<()> {
        write_tombstone_into(
            &mut self.mmap,
            &mut self.file,
            &mut self.tx,
            &mut self.stats,
            old,
        )
    }

    /// Auto-compaction guard. Reads the configured threshold and, if the
    /// dead-byte ratio is over it, calls the [`Backend::compact`]
    /// implementation. `compact` lives on the trait — no separate
    /// inherent copy of the in-place algorithm.
    ///
    /// Suppressed while a tx is open: mid-tx compaction would relocate
    /// committed bytes that staged offsets implicitly depend on (the
    /// virtual-offset scheme assumes `mmap[..tx_base]` is stable until
    /// commit). `close_tx` runs `maybe_compact` once after the staging
    /// flush, so deferred dead-byte ratios still trigger.
    fn maybe_compact(&mut self) -> io::Result<()> {
        if self.tx.is_some() {
            return Ok(());
        }
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
        grow_into(&mut self.mmap, &mut self.file, desired)
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
                match self.index.entry(key) {
                    Entry::Occupied(mut e) => {
                        self.stats.dead_bytes += e.get().on_disk_size();
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

    /// Open a bulk-write window. Subsequent `put` / `delete` calls stage
    /// their records into an in-memory pooled buffer; the mmap is left
    /// alone until [`close_tx`] copies the staging buffer back in.
    /// Returns [`io::ErrorKind::AlreadyExists`] if a tx is already open
    /// — nesting is not supported.
    ///
    /// See the "Bulk-write mode" section of the module docs for the
    /// detailed semantics.
    fn open_tx(&mut self) -> io::Result<()> {
        if self.tx.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "KeyDir tx already open",
            ));
        }
        self.tx = Some(TxState {
            staging: PooledBuf::acquire(),
            tx_base: self.stats.data_size,
        });
        Ok(())
    }

    /// Commit the staged writes. Grows the mmap once to fit
    /// `tx_base + staging.len()`, performs one `copy_from_slice` from
    /// staging into `mmap[tx_base..]`, advances `stats.data_size`, then
    /// runs `maybe_compact` once. The staging buffer is returned to the
    /// thread-local pool on drop.
    ///
    /// Idempotent: calling without a prior `open_tx` returns `Ok(())`.
    fn close_tx(&mut self) -> io::Result<()> {
        let Some(tx) = self.tx.take() else {
            return Ok(());
        };
        let staged_len = tx.staging.len();
        if staged_len > 0 {
            let end = tx.tx_base.checked_add(staged_len as u64).ok_or_else(|| {
                io::Error::new(io::ErrorKind::OutOfMemory, "KeyDir offset overflow")
            })?;
            if end > self.mmap.len() as u64 {
                self.grow(end)?;
            }
            let start = tx.tx_base as usize;
            self.mmap[start..start + staged_len].copy_from_slice(&tx.staging);
            self.stats.data_size = end;
        }
        // `tx` (and its `PooledBuf`) drops here, returning the staging
        // buffer to the thread-local pool.
        drop(tx);
        self.maybe_compact()
    }

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

    /// Append the new record, then update the index. Routes the index
    /// touch through hashbrown's `Entry` API — one hash whether the key
    /// is fresh or being overwritten. (The previous `get_mut` / `insert`
    /// pair hashed twice on the cold path.)
    fn put(&mut self, key: K, value: V) -> io::Result<()> {
        let (offset, value_size, key_size) = self.write_entry(&key, &value)?;
        let new_meta = EntryMeta {
            offset,
            value_size,
            key_size,
        };
        match self.index.entry(key) {
            Entry::Occupied(mut e) => {
                self.stats.dead_bytes += e.get().on_disk_size();
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

    /// Insert iff `key` is absent. One hash per call: probes via
    /// `Entry`; the Vacant arm writes the record and fills the slot in
    /// the same lookup. The default trait impl does `contains` + `put`
    /// — two hashes — which is why we override.
    fn put_if_absent(&mut self, key: K, value: V) -> io::Result<bool> {
        let inserted;
        {
            let Self {
                index,
                mmap,
                file,
                tx,
                stats,
                ..
            } = self;

            match index.entry(key) {
                Entry::Occupied(_) => {
                    inserted = false;
                }
                Entry::Vacant(e) => {
                    // The entry holds K; pass it as &K to write_entry_into
                    // via VacantEntry::key().
                    let key_ref: &K = e.key();
                    let (offset, value_size, key_size) =
                        write_entry_into(mmap, file, tx, stats, key_ref, &value)?;
                    e.insert(EntryMeta {
                        offset,
                        value_size,
                        key_size,
                    });
                    inserted = true;
                }
            }
        }

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
    fn replace(&mut self, key: K, value: V) -> io::Result<Option<V>> {
        let prev;
        {
            let Self {
                index,
                mmap,
                file,
                tx,
                stats,
                ..
            } = self;

            match index.entry(key) {
                Entry::Occupied(mut e) => {
                    let old_meta: EntryMeta = *e.get();
                    let prev_val: V = {
                        let bytes = value_bytes_in(mmap, tx.as_ref(), &old_meta);
                        deserialize_from(bytes)?
                    };
                    let key_ref: &K = e.key();
                    let (offset, value_size, key_size) =
                        write_entry_into(mmap, file, tx, stats, key_ref, &value)?;
                    stats.dead_bytes += old_meta.on_disk_size();
                    *e.get_mut() = EntryMeta {
                        offset,
                        value_size,
                        key_size,
                    };
                    prev = Some(prev_val);
                }
                Entry::Vacant(e) => {
                    let key_ref: &K = e.key();
                    let (offset, value_size, key_size) =
                        write_entry_into(mmap, file, tx, stats, key_ref, &value)?;
                    e.insert(EntryMeta {
                        offset,
                        value_size,
                        key_size,
                    });
                    prev = None;
                }
            }
        }

        self.maybe_compact()?;
        Ok(prev)
    }

    /// Insert every item. Auto-wraps the loop in [`open_tx`] /
    /// [`close_tx`] when no tx is already open, so callers get the
    /// bulk-write amortization (one mmap grow, one deferred
    /// `maybe_compact`) without having to manage the tx themselves.
    ///
    /// If a tx is already open, the items are simply appended into the
    /// caller's existing staging buffer — no nested tx, no early
    /// commit. The caller's `close_tx` still controls when the batch
    /// becomes visible in the mmap.
    ///
    /// Pre-reserves the index from the iterator's `size_hint().0` so
    /// the cold path avoids HashMap rehashes during the load.
    fn bulk_put<I>(&mut self, items: I) -> io::Result<()>
    where
        I: IntoIterator<Item = (K, V)>,
    {
        let iter = items.into_iter();
        let (lower, _) = iter.size_hint();
        if lower > 0 {
            self.index.reserve(lower);
        }

        let opened_here = self.tx.is_none();
        if opened_here {
            self.open_tx()?;
        }

        let mut first_err: Option<io::Error> = None;
        for (k, v) in iter {
            if let Err(e) = self.put(k, v) {
                first_err = Some(e);
                break;
            }
        }

        if opened_here {
            // Always close the auto-opened tx so the backend isn't left
            // stuck in tx mode — even if one of the puts failed. The
            // close commits whatever was staged successfully.
            if let Err(e) = self.close_tx() {
                if first_err.is_none() {
                    first_err = Some(e);
                }
            }
        }

        match first_err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    /// Remove every key the iterator yields, auto-wrapped in
    /// [`open_tx`] / [`close_tx`] when no tx is already open. Returns
    /// the number of keys that were actually present (and removed).
    ///
    /// Same error semantics as [`bulk_put`]: a failing `delete` stops
    /// the loop, the auto-opened tx still closes, and the first error
    /// is returned.
    fn bulk_delete<'a, I>(&mut self, keys: I) -> io::Result<usize>
    where
        I: IntoIterator<Item = &'a K>,
        K: 'a,
    {
        let opened_here = self.tx.is_none();
        if opened_here {
            self.open_tx()?;
        }

        let mut n: usize = 0;
        let mut first_err: Option<io::Error> = None;
        for k in keys {
            match self.delete(k) {
                Ok(true) => n += 1,
                Ok(false) => {}
                Err(e) => {
                    first_err = Some(e);
                    break;
                }
            }
        }

        if opened_here {
            if let Err(e) = self.close_tx() {
                if first_err.is_none() {
                    first_err = Some(e);
                }
            }
        }

        match first_err {
            Some(e) => Err(e),
            None => Ok(n),
        }
    }

    /// Unified read-modify-write / insert / delete primitive. Writes at
    /// most one new log record.
    ///
    /// Hashes the key exactly **once** per call. The slot is located via
    /// `raw_entry_mut().from_key(key)` and held open across the value
    /// read, the closure invocation, and the slot mutation — `self` is
    /// destructured into field-level borrows so the disjoint
    /// `mmap` / `file` / `tx` / `stats` writes don't conflict with the
    /// entry's borrow on `self.index`.
    fn update<F>(&mut self, key: &K, f: F) -> io::Result<()>
    where
        F: FnOnce(Option<V>) -> Option<V>,
    {
        use hashbrown::hash_map::RawEntryMut;

        // Did the closure mutate state? Used to decide whether to run
        // maybe_compact after the destructure goes out of scope.
        let mutated;

        {
            let Self {
                index,
                mmap,
                file,
                tx,
                stats,
                ..
            } = self;

            match index.raw_entry_mut().from_key(key) {
                RawEntryMut::Occupied(mut e) => {
                    // Copy out the meta (it's `Copy`) so we can drop the
                    // immutable borrow before doing the mutable write.
                    let old_meta: EntryMeta = *e.get();
                    let current: V = {
                        let bytes = value_bytes_in(mmap, tx.as_ref(), &old_meta);
                        deserialize_from(bytes)?
                    };
                    match f(Some(current)) {
                        Some(new_v) => {
                            let (offset, value_size, key_size) =
                                write_entry_into(mmap, file, tx, stats, key, &new_v)?;
                            stats.dead_bytes += old_meta.on_disk_size();
                            *e.get_mut() = EntryMeta {
                                offset,
                                value_size,
                                key_size,
                            };
                            mutated = true;
                        }
                        None => {
                            stats.dead_bytes += old_meta.on_disk_size();
                            e.remove();
                            write_tombstone_into(mmap, file, tx, stats, old_meta)?;
                            mutated = true;
                        }
                    }
                }
                RawEntryMut::Vacant(e) => match f(None) {
                    Some(new_v) => {
                        let (offset, value_size, key_size) =
                            write_entry_into(mmap, file, tx, stats, key, &new_v)?;
                        e.insert(
                            key.clone(),
                            EntryMeta {
                                offset,
                                value_size,
                                key_size,
                            },
                        );
                        mutated = true;
                    }
                    None => {
                        mutated = false;
                    }
                },
            }
        }

        if mutated {
            self.maybe_compact()?;
        }
        Ok(())
    }

    /// Drop every live entry. Resets `data_size` to `HEADER_SIZE` and
    /// zeroes the post-header sentinel so a subsequent open sees an
    /// empty log. The backing file is not truncated.
    ///
    /// Calling this mid-tx would invalidate every staged offset; doing
    /// so is undefined behaviour at the index level. Tests catch it via
    /// `debug_assert`.
    fn clear(&mut self) -> io::Result<()> {
        debug_assert!(self.tx.is_none(), "clear() called inside an open tx");
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
    ///
    /// Calling this mid-tx would relocate committed bytes that staged
    /// offsets depend on. `maybe_compact` already short-circuits during
    /// a tx; this assert catches direct calls.
    fn compact(&mut self) -> io::Result<()> {
        debug_assert!(self.tx.is_none(), "compact() called inside an open tx");
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
    ///
    /// Called mid-tx, this only flushes the committed mmap region —
    /// staged writes live in the heap-backed [`PooledBuf`] and are not
    /// yet part of the file. They become durable only after
    /// [`close_tx`](Self::close_tx) followed by `flush` / `sync`.
    fn flush(&self) -> io::Result<()> {
        self.mmap.flush_async()
    }

    /// Block until pending mmap writes have been flushed by the OS.
    /// This does not provide crash recovery or log repair.
    ///
    /// Same mid-tx caveat as [`flush`](Self::flush): only the
    /// committed mmap region is synced; staged writes are not touched.
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
            assert_eq!(kd.get(&format!("k{:04}", i).into_bytes()), Some(v("p", i)));
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
        assert_eq!(kd.get(&k("a")), Some(v("alpha", 1)));
    }

    #[test]
    fn flush_returns_ok_on_empty_keydir() {
        let p = tmp_path("flush_empty");
        let kd = create(&p);
        kd.flush().unwrap();
        kd.sync().unwrap();
    }

    // ---- bulk-write mode (open_tx / close_tx) ----

    #[test]
    fn tx_bulk_put_visible_after_close_and_reopen() {
        let p = tmp_path("tx_bulk_put");
        {
            let mut kd = create(&p);
            kd.open_tx().unwrap();
            for i in 0..1000u32 {
                kd.put(format!("k{:04}", i).into_bytes(), v("payload", i))
                    .unwrap();
            }
            kd.close_tx().unwrap();
            kd.sync().unwrap();
        }
        let kd = open(&p);
        assert_eq!(kd.size(), 1000);
        for i in 0..1000u32 {
            assert_eq!(
                kd.get(&format!("k{:04}", i).into_bytes()),
                Some(v("payload", i))
            );
        }
    }

    #[test]
    fn tx_read_your_own_writes() {
        let p = tmp_path("tx_ryow");
        let mut kd = create(&p);
        kd.open_tx().unwrap();
        kd.put(k("staged"), v("fresh", 42)).unwrap();
        // Reading during the tx returns the staged value.
        assert_eq!(kd.get(&k("staged")), Some(v("fresh", 42)));
        assert_eq!(kd.size(), 1);
        kd.close_tx().unwrap();
        assert_eq!(kd.get(&k("staged")), Some(v("fresh", 42)));
    }

    #[test]
    fn tx_delete_inside_tx_survives_reopen() {
        let p = tmp_path("tx_delete_inside");
        {
            let mut kd = create(&p);
            kd.put(k("doomed"), v("v", 1)).unwrap();
            kd.put(k("keeper"), v("v", 2)).unwrap();
            kd.open_tx().unwrap();
            assert!(kd.delete(&k("doomed")).unwrap());
            // Mid-tx the delete is already visible.
            assert!(!kd.contains(&k("doomed")));
            assert!(kd.contains(&k("keeper")));
            kd.close_tx().unwrap();
            kd.sync().unwrap();
        }
        let kd = open(&p);
        assert!(!kd.contains(&k("doomed")));
        assert!(kd.contains(&k("keeper")));
    }

    #[test]
    fn tx_overwrite_within_tx_only() {
        let p = tmp_path("tx_overwrite_within");
        let mut kd = create(&p);
        kd.open_tx().unwrap();
        kd.put(k("a"), v("first", 1)).unwrap();
        kd.put(k("a"), v("second", 2)).unwrap();
        assert_eq!(kd.get(&k("a")), Some(v("second", 2)));
        kd.close_tx().unwrap();
        assert_eq!(kd.get(&k("a")), Some(v("second", 2)));
    }

    #[test]
    fn tx_overwrite_committed_entry_inside_tx() {
        // Committed entry is overwritten by a staged write: covers the
        // cross-region path where `old` lives in mmap but the new entry
        // (and any later tombstone source) lives in staging.
        let p = tmp_path("tx_overwrite_cross");
        let mut kd = create(&p);
        kd.put(k("a"), v("mmap_v", 1)).unwrap();
        kd.open_tx().unwrap();
        kd.put(k("a"), v("staged_v", 2)).unwrap();
        assert_eq!(kd.get(&k("a")), Some(v("staged_v", 2)));
        kd.close_tx().unwrap();
        assert_eq!(kd.get(&k("a")), Some(v("staged_v", 2)));
        assert!(kd.stats().dead_bytes > 0);
    }

    #[test]
    fn tx_delete_committed_entry_inside_tx() {
        // Tombstone written into staging sources its key bytes from
        // mmap (the old entry was committed pre-tx). Exercises the
        // mmap→staging branch of `write_tombstone_for`.
        let p = tmp_path("tx_delete_cross");
        let mut kd = create(&p);
        kd.put(k("doomed"), v("mmap_v", 1)).unwrap();
        kd.open_tx().unwrap();
        assert!(kd.delete(&k("doomed")).unwrap());
        assert!(!kd.contains(&k("doomed")));
        kd.close_tx().unwrap();
        assert!(!kd.contains(&k("doomed")));
    }

    #[test]
    fn tx_delete_staged_entry_inside_tx() {
        // Tombstone written into staging sources its key bytes from
        // staging itself (the entry was put earlier in the same tx).
        // Exercises the staging→staging copy_within branch.
        let p = tmp_path("tx_delete_staged");
        let mut kd = create(&p);
        kd.open_tx().unwrap();
        kd.put(k("doomed"), v("staged_v", 1)).unwrap();
        assert!(kd.delete(&k("doomed")).unwrap());
        assert!(!kd.contains(&k("doomed")));
        kd.close_tx().unwrap();
        assert!(!kd.contains(&k("doomed")));
    }

    #[test]
    fn nested_open_tx_errors() {
        let p = tmp_path("tx_nested");
        let mut kd = create(&p);
        kd.open_tx().unwrap();
        let err = kd.open_tx().unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::AlreadyExists);
        kd.close_tx().unwrap();
    }

    #[test]
    fn close_tx_without_open_is_noop() {
        let p = tmp_path("tx_close_noop");
        let mut kd = create(&p);
        // Calling close_tx with no tx open is idempotent.
        kd.close_tx().unwrap();
        kd.close_tx().unwrap();
        kd.put(k("a"), v("v", 1)).unwrap();
        kd.close_tx().unwrap();
    }

    #[test]
    fn compaction_does_not_fire_during_tx() {
        // With compaction_ratio = 0.0 every non-tx mutation would trigger
        // compact. Inside a tx mutations accumulate dead bytes without
        // ever compacting. After close_tx, the deferred maybe_compact
        // fires and reclaims them in one pass.
        let p = tmp_path("tx_compact_deferred");
        let cfg = KeyDirConfig {
            initial_capacity: 16 * 1024,
            compaction_ratio: 0.0,
        };
        let mut kd = create_with(&p, cfg);
        kd.put(k("hot"), v("seed", 0)).unwrap();
        // After the seeded put compaction has already run, so dead_bytes
        // is 0 going into the tx.
        assert_eq!(kd.stats().dead_bytes, 0);
        kd.open_tx().unwrap();
        for i in 1..20u32 {
            kd.put(k("hot"), v("staged", i)).unwrap();
        }
        // Dead bytes have accumulated (each overwrite kills the prior
        // entry) but no compaction has fired — the index still points
        // into staging via virtual offsets.
        assert!(
            kd.stats().dead_bytes > 0,
            "expected accumulated dead bytes mid-tx; got {}",
            kd.stats().dead_bytes
        );
        kd.close_tx().unwrap();
        // Post-commit compaction reclaims everything.
        assert_eq!(kd.stats().dead_bytes, 0);
        assert_eq!(kd.get(&k("hot")), Some(v("staged", 19)));
    }

    #[test]
    fn tx_pooled_buffer_is_recycled() {
        // Two consecutive txs should reuse the staging buffer from the
        // thread-local pool. We can't observe the pool directly without
        // adding test hooks, but we can verify the second tx's writes
        // round-trip correctly — proving the recycled buffer was cleared.
        let p = tmp_path("tx_recycle");
        let mut kd = create(&p);
        kd.open_tx().unwrap();
        for i in 0..50u32 {
            kd.put(format!("a{:02}", i).into_bytes(), v("first", i))
                .unwrap();
        }
        kd.close_tx().unwrap();
        kd.open_tx().unwrap();
        for i in 0..50u32 {
            kd.put(format!("b{:02}", i).into_bytes(), v("second", i))
                .unwrap();
        }
        kd.close_tx().unwrap();
        assert_eq!(kd.size(), 100);
        for i in 0..50u32 {
            assert_eq!(
                kd.get(&format!("a{:02}", i).into_bytes()),
                Some(v("first", i))
            );
            assert_eq!(
                kd.get(&format!("b{:02}", i).into_bytes()),
                Some(v("second", i))
            );
        }
    }

    // ---- bulk methods (auto-tx wrapping) ----

    #[test]
    fn bulk_put_auto_opens_and_closes_tx() {
        let p = tmp_path("bulk_put_auto_tx");
        {
            let mut kd = create(&p);
            let items: Vec<(TestKey, TestVal)> = (0..500u32)
                .map(|i| (format!("k{:04}", i).into_bytes(), v("p", i)))
                .collect();
            Backend::bulk_put(&mut kd, items).unwrap();
            // No tx should be lingering after bulk_put returns.
            assert!(
                kd.open_tx().is_ok(),
                "bulk_put left a tx open — open_tx should not error"
            );
            kd.close_tx().unwrap();
            kd.flush().unwrap();
        }
        let kd = open(&p);
        assert_eq!(kd.size(), 500);
        for i in 0..500u32 {
            assert_eq!(kd.get(&format!("k{:04}", i).into_bytes()), Some(v("p", i)));
        }
    }

    #[test]
    fn bulk_put_within_existing_tx_does_not_close_it() {
        // If the caller already opened a tx, bulk_put should join it
        // rather than auto-closing — the caller's close_tx still
        // controls when the batch becomes visible in the mmap.
        let p = tmp_path("bulk_put_within_tx");
        let mut kd = create(&p);
        kd.open_tx().unwrap();
        let items: Vec<(TestKey, TestVal)> = (0..50u32)
            .map(|i| (format!("a{:02}", i).into_bytes(), v("bulk", i)))
            .collect();
        Backend::bulk_put(&mut kd, items).unwrap();
        // Tx is still open — a follow-up put must continue staging.
        kd.put(k("after_bulk"), v("staged", 999)).unwrap();
        // Nested open_tx would error iff the tx is still open. Verify.
        let err = kd.open_tx().unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::AlreadyExists);
        kd.close_tx().unwrap();
        // All entries — both the bulk batch and the trailing put — are
        // committed by the caller's close_tx.
        assert_eq!(kd.size(), 51);
        assert_eq!(kd.get(&k("after_bulk")), Some(v("staged", 999)));
    }

    #[test]
    fn bulk_delete_auto_opens_and_closes_tx() {
        let p = tmp_path("bulk_delete_auto_tx");
        let mut kd = create(&p);
        for i in 0..20u32 {
            kd.put(format!("k{:02}", i).into_bytes(), v("p", i))
                .unwrap();
        }
        let to_remove: Vec<TestKey> = (0..10u32)
            .map(|i| format!("k{:02}", i).into_bytes())
            .collect();
        let ghosts: Vec<TestKey> = vec![k("ghost1"), k("ghost2")];
        let all: Vec<&TestKey> = to_remove.iter().chain(ghosts.iter()).collect();
        let n = Backend::bulk_delete(&mut kd, all).unwrap();
        assert_eq!(n, 10);
        assert_eq!(kd.size(), 10);
        // No tx should be lingering.
        kd.open_tx().unwrap();
        kd.close_tx().unwrap();
    }

    #[test]
    fn bulk_delete_within_existing_tx_does_not_close_it() {
        let p = tmp_path("bulk_delete_within_tx");
        let mut kd = create(&p);
        for i in 0..10u32 {
            kd.put(format!("k{:02}", i).into_bytes(), v("p", i))
                .unwrap();
        }
        kd.open_tx().unwrap();
        let to_remove: Vec<TestKey> = (0..5u32)
            .map(|i| format!("k{:02}", i).into_bytes())
            .collect();
        let n = Backend::bulk_delete(&mut kd, to_remove.iter()).unwrap();
        assert_eq!(n, 5);
        // Tx still open — caller's close_tx commits the deletions.
        let err = kd.open_tx().unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::AlreadyExists);
        kd.close_tx().unwrap();
        assert_eq!(kd.size(), 5);
    }

    #[test]
    fn tx_mixed_committed_and_staged_reads() {
        // Some entries committed before tx, others added inside; all
        // reachable through `get` via the appropriate branch.
        let p = tmp_path("tx_mixed");
        let mut kd = create(&p);
        kd.put(k("c1"), v("committed", 1)).unwrap();
        kd.put(k("c2"), v("committed", 2)).unwrap();
        kd.open_tx().unwrap();
        kd.put(k("s1"), v("staged", 10)).unwrap();
        kd.put(k("s2"), v("staged", 20)).unwrap();
        assert_eq!(kd.get(&k("c1")), Some(v("committed", 1)));
        assert_eq!(kd.get(&k("c2")), Some(v("committed", 2)));
        assert_eq!(kd.get(&k("s1")), Some(v("staged", 10)));
        assert_eq!(kd.get(&k("s2")), Some(v("staged", 20)));
        kd.close_tx().unwrap();
        assert_eq!(kd.size(), 4);
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
        assert_eq!(kd.get(&k("fresh")), Some(v("inserted", 7)));
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
        let prev = Backend::replace(&mut kd, k("a"), v("v2", 2)).unwrap();
        assert_eq!(prev, Some(v("v1", 1)));
        assert_eq!(kd.get(&k("a")), Some(v("v2", 2)));
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
        assert_eq!(kd.get(&k("a")), Some(v("first", 1)));
        // No write happened.
        assert_eq!(kd.stats().data_size, size_before);
        assert_eq!(kd.stats().dead_bytes, dead_before);
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
        assert_eq!(kd.get(&k("only")), Some(v("once", 1)));
    }
}
