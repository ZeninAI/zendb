//! OrderLog — in-memory ordered KV store with a write-ahead log for
//! durability.
//!
//! ## Model
//!
//! Both **keys and values** live in memory inside an arena-backed skip
//! list. The on-disk file is a **pure WAL**: every mutation is appended
//! to the file, and on `open` the WAL is replayed sequentially to
//! reconstruct the in-memory state. The file is never read from after
//! that — `get`/`range`/`first`/`last` all hit the skip list directly,
//! no deserialize-from-mmap roundtrip.
//!
//! That's the difference vs. KeyDir/BPlusTree: those keep the value in
//! the file and the index points at it. OrderLog keeps the value in the
//! index and the file is only there so the next process can see it.
//!
//! ## What's shared with KeyDir
//!
//! Append-only WAL with periodic compaction, last-write-wins replay,
//! tombstones for delete. The on-disk record format is identical to
//! KeyDir for ergonomic reasons:
//!
//! Header (4 bytes):
//! ```text
//! [MAGIC: u32 LE = 0x474F4C4F ("OLOG")]
//! ```
//!
//! Live record:
//! ```text
//! [value_size: u32 LE][V bytes][key_size: u32 LE][K bytes]
//! ```
//! Tombstone:
//! ```text
//! [0xFFFF_FFFF: u32][key_size: u32 LE][K bytes]
//! ```
//!
//! ## What's different
//!
//! - The in-memory index is a skip list keyed by `K`, valued by `V`,
//!   plus the exact WAL record size for dead-byte accounting. Reads
//!   return clones of the in-memory `V`.
//! - `Ord` on `K` lives on [`OrderedBackend`], not on this type's
//!   inherent surface — but the skip list naturally requires it, so the
//!   inherent impl is bounded `K: Ord` too.
//! - Implements both [`Backend`] and [`OrderedBackend`] (range, first,
//!   last, entries_rev).
//!
//! ## Bounds
//! - `K: Encode + Decode<()> + Hash + Eq + Clone + Ord`
//! - `V: Encode + Decode<()> + Clone`
//!
//! Custom skip list and custom WAL — neither `core::skiplist` nor
//! `core::wal` is used. Only [`crate::utils::serdes`] is shared.

use std::{
    fmt,
    fs::{self, File, OpenOptions},
    hash::Hash,
    io::{self},
    path::{Path, PathBuf},
};

use bincode::{Decode, Encode};
use memmap2::MmapMut;

use crate::utils::{
    fast_rand,
    serdes::{deserialize_from, read_u32_le, serialize_into, serialized_size},
};

const DEFAULT_INITIAL_CAPACITY: u64 = 1024 * 1024;
const DEFAULT_COMPACTION_RATIO: f64 = 0.5;
/// Magic number identifying an OrderLog file. ASCII: `"OLOG"`.
const MAGIC: u32 = 0x474F4C4F;
const HEADER_SIZE: usize = 4;
const TOMBSTONE: u32 = u32::MAX;

// ---------------------------------------------------------------------------
// Embedded skip list — in-memory map from K to V.
// ---------------------------------------------------------------------------

const MAX_LEVEL: usize = 16;

/// `next` is sized to `level` slots (one per active skip-list level for
/// this node). The previous layout reserved `MAX_LEVEL` slots in every
/// node — 128 bytes of pointers per node — but most nodes are level 1-3
/// so the tail was permanently `None`. The boxed slice trims that waste
/// (~3-4× less per-node memory on average) and packs more nodes into the
/// CPU cache lines that `search` and `last_idx` walk.
struct SkipNode<K, V> {
    key: K,
    value: V,
    record_size: u64,
    next: Box<[Option<usize>]>,
    level: usize,
}

type Heads = [Option<usize>; MAX_LEVEL];

/// Arena-backed ordered map from `K` to `V`. Indices into `arena` are
/// reused via `free` so insert/delete churn doesn't grow the arena
/// unboundedly. No `unsafe`, no raw pointers.
struct OrderIndex<K: Ord, V> {
    arena: Vec<SkipNode<K, V>>,
    free: Vec<usize>,
    heads: Heads,
    height: usize,
    len: usize,
}

impl<K: Ord, V> OrderIndex<K, V> {
    fn new() -> Self {
        OrderIndex {
            arena: Vec::new(),
            free: Vec::new(),
            heads: [None; MAX_LEVEL],
            height: 0,
            len: 0,
        }
    }

    fn len(&self) -> usize {
        self.len
    }

    fn is_empty(&self) -> bool {
        self.len == 0
    }

    fn random_level(&self) -> usize {
        let mut level = 1;
        while level < MAX_LEVEL && fast_rand() & 1 == 0 {
            level += 1;
        }
        level
    }

    /// Multi-level descent: at each level walk forward until a key
    /// `>= search` is found, drop down a level, repeat. The `prev`
    /// cursor carries across levels, which is what makes lookup
    /// O(log n) expected instead of repeated O(n) sweeps.
    fn search(&self, key: &K) -> (Heads, Option<usize>) {
        let mut update: Heads = [None; MAX_LEVEL];
        let mut found: Option<usize> = None;
        let mut prev: Option<usize> = None;

        for i in (0..self.height).rev() {
            let mut x = match prev {
                Some(p) => self.arena[p].next[i],
                None => self.heads[i],
            };
            while let Some(idx) = x {
                match self.arena[idx].key.cmp(key) {
                    std::cmp::Ordering::Less => {
                        prev = Some(idx);
                        x = self.arena[idx].next[i];
                    }
                    std::cmp::Ordering::Equal => {
                        found = Some(idx);
                        break;
                    }
                    std::cmp::Ordering::Greater => break,
                }
            }
            update[i] = prev;
        }

        (update, found)
    }

    fn get(&self, key: &K) -> Option<&V> {
        if self.is_empty() {
            return None;
        }
        let (_u, found) = self.search(key);
        found.map(|idx| &self.arena[idx].value)
    }

    fn contains(&self, key: &K) -> bool {
        self.get(key).is_some()
    }

    /// Single-search mutable access. Returns a direct mutable reference
    /// to the node so callers can read/overwrite `value` and
    /// `record_size` in-place without a second skip-list descent.
    fn get_mut(&mut self, key: &K) -> Option<&mut SkipNode<K, V>> {
        if self.is_empty() {
            return None;
        }
        let (_update, found) = self.search(key);
        found.map(|idx| &mut self.arena[idx])
    }

    /// Insert or overwrite. Returns the previous value if the key was
    /// already present, `None` otherwise. The returned size is the exact
    /// WAL record size of the overwritten live entry.
    fn insert(&mut self, key: K, value: V, record_size: u64) -> Option<u64> {
        let (update, found) = self.search(&key);
        if let Some(idx) = found {
            self.arena[idx].value = value;
            return Some(std::mem::replace(
                &mut self.arena[idx].record_size,
                record_size,
            ));
        }

        let level = self.random_level();
        if level > self.height {
            self.height = level;
        }

        // `next` length matches `level`. All indexing into `next[i]` in this
        // file is invariantly `i < node.level`: nodes at level i are only
        // reached via forward pointers at level i, which only exist on nodes
        // whose own level >= i.
        let next_slots = vec![None; level].into_boxed_slice();
        let new_idx = if let Some(slot) = self.free.pop() {
            self.arena[slot] = SkipNode {
                key,
                value,
                record_size,
                next: next_slots,
                level,
            };
            slot
        } else {
            let i = self.arena.len();
            self.arena.push(SkipNode {
                key,
                value,
                record_size,
                next: next_slots,
                level,
            });
            i
        };

        for i in 0..level {
            if let Some(prev) = update[i] {
                self.arena[new_idx].next[i] = self.arena[prev].next[i];
                self.arena[prev].next[i] = Some(new_idx);
            } else {
                self.arena[new_idx].next[i] = self.heads[i];
                self.heads[i] = Some(new_idx);
            }
        }

        self.len += 1;
        None
    }

    /// Variant of `remove` that does not require `V: Default` — the
    /// node is unlinked and its slot returned to the free-list. The
    /// stored value is dropped when the slot is overwritten on next
    /// reuse (or when the arena itself drops). Returns the exact WAL
    /// record size of the removed live entry.
    fn remove_drop(&mut self, key: &K) -> Option<u64> {
        if self.is_empty() {
            return None;
        }
        let (update, found) = self.search(key);
        let Some(idx) = found else {
            return None;
        };

        let lvl = self.arena[idx].level;
        for i in 0..lvl {
            if let Some(prev) = update[i] {
                self.arena[prev].next[i] = self.arena[idx].next[i];
            } else {
                self.heads[i] = self.arena[idx].next[i];
            }
        }
        while self.height > 0 && self.heads[self.height - 1].is_none() {
            self.height -= 1;
        }
        let record_size = self.arena[idx].record_size;
        self.free.push(idx);
        self.len -= 1;
        Some(record_size)
    }

    fn clear(&mut self) {
        self.arena.clear();
        self.free.clear();
        self.heads = [None; MAX_LEVEL];
        self.height = 0;
        self.len = 0;
    }

    fn iter(&self) -> OrderIndexIter<'_, K, V> {
        OrderIndexIter {
            arena: &self.arena,
            current: self.heads[0],
        }
    }

    fn range<'a>(&'a self, start: &K, end: &'a K) -> OrderIndexRangeIter<'a, K, V> {
        let first = if self.is_empty() {
            None
        } else {
            let (update, found) = self.search(start);
            found.or_else(|| match update[0] {
                None => self.heads[0],
                Some(p) => self.arena[p].next[0],
            })
        };
        OrderIndexRangeIter {
            arena: &self.arena,
            current: first,
            end,
        }
    }

    fn first_idx(&self) -> Option<usize> {
        self.heads[0]
    }

    /// Right-most node in O(log n) expected: slide right at each level
    /// from top to bottom, never backtracking past the current cursor.
    fn last_idx(&self) -> Option<usize> {
        if self.is_empty() {
            return None;
        }
        let mut idx: Option<usize> = None;
        for i in (0..self.height).rev() {
            let mut cur = match idx {
                Some(p) => self.arena[p].next[i],
                None => self.heads[i],
            };
            while let Some(c) = cur {
                idx = Some(c);
                cur = self.arena[c].next[i];
            }
        }
        idx
    }
}

struct OrderIndexIter<'a, K, V> {
    arena: &'a [SkipNode<K, V>],
    current: Option<usize>,
}

impl<'a, K, V> Iterator for OrderIndexIter<'a, K, V> {
    type Item = (&'a K, &'a V);
    fn next(&mut self) -> Option<Self::Item> {
        let idx = self.current?;
        let node = &self.arena[idx];
        self.current = node.next[0];
        Some((&node.key, &node.value))
    }
}

struct OrderIndexRangeIter<'a, K: Ord, V> {
    arena: &'a [SkipNode<K, V>],
    current: Option<usize>,
    end: &'a K,
}

impl<'a, K: Ord, V> Iterator for OrderIndexRangeIter<'a, K, V> {
    type Item = (&'a K, &'a V);
    fn next(&mut self) -> Option<Self::Item> {
        let idx = self.current?;
        let node = &self.arena[idx];
        if node.key.cmp(self.end) != std::cmp::Ordering::Less {
            return None;
        }
        self.current = node.next[0];
        Some((&node.key, &node.value))
    }
}

// ---------------------------------------------------------------------------
// Free helpers: field-level WAL write helpers avoid `&mut self` conflicts.
// When a caller holds a mutable borrow on `self.index` (e.g. via
// `OrderIndex::get_mut`), it can still write to the WAL by passing
// `&mut self.mmap`, `&mut self.file`, and `&mut self.stats.data_size`
// directly to these functions.
// ---------------------------------------------------------------------------

fn append_into<K: Encode, V: Encode>(
    mmap: &mut MmapMut,
    file: &mut File,
    data_size: &mut u64,
    key: &K,
    value: &V,
) -> io::Result<u64> {
    let offset = *data_size as usize;

    let v_size = serialized_size(value)?;
    let v_start = offset + 4;
    let v_end = v_start + v_size;
    ensure_mmap(mmap, file, v_end)?;
    let written = serialize_into(value, &mut mmap[v_start..v_end])?;
    debug_assert_eq!(written, v_size);
    mmap[offset..offset + 4].copy_from_slice(&(v_size as u32).to_le_bytes());

    let k_size = serialized_size(key)?;
    let k_start = v_end + 4;
    let k_end = k_start + k_size;
    ensure_mmap(mmap, file, k_end)?;
    let written = serialize_into(key, &mut mmap[k_start..k_end])?;
    debug_assert_eq!(written, k_size);
    mmap[v_end..v_end + 4].copy_from_slice(&(k_size as u32).to_le_bytes());

    *data_size = k_end as u64;
    Ok((k_end - offset) as u64)
}

fn ensure_mmap(mmap: &mut MmapMut, file: &mut File, desired: usize) -> io::Result<()> {
    if desired <= mmap.len() {
        return Ok(());
    }
    mmap.flush()?;
    let new_cap = ((mmap.len() as u64) * 2).max(desired as u64);
    file.set_len(new_cap)?;
    *mmap = unsafe { MmapMut::map_mut(&*file)? };
    Ok(())
}

// ---------------------------------------------------------------------------
// Public OrderLog
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Encode, Decode)]
pub struct OrderLogConfig {
    pub initial_capacity: u64,
    /// Auto-compaction threshold for the WAL. Values in `[0.0, 1.0]`:
    /// `0.0` compacts after every write, `1.0` disables it. Compaction
    /// rewrites the WAL with one record per live entry, in ascending
    /// key order, dropping all overwrites and tombstones.
    pub compaction_ratio: f64,
}

impl Default for OrderLogConfig {
    fn default() -> Self {
        OrderLogConfig {
            initial_capacity: DEFAULT_INITIAL_CAPACITY,
            compaction_ratio: DEFAULT_COMPACTION_RATIO,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct OrderLogStats {
    pub entries: usize,
    pub data_size: u64,
    pub dead_bytes: u64,
}

/// In-memory ordered KV with WAL durability. See module docs.
pub struct OrderLog<K: Ord, V> {
    index: OrderIndex<K, V>,
    mmap: MmapMut,
    file: File,
    path: PathBuf,
    config: OrderLogConfig,
    stats: OrderLogStats,
}

impl<K, V> OrderLog<K, V>
where
    K: Encode + Decode<()> + Hash + Eq + Clone + Ord,
    V: Encode + Decode<()> + Clone,
{
    /// Create a fresh OrderLog at `path`, **truncating** any existing
    /// file. Pre-allocates `config.initial_capacity` bytes and stamps
    /// MAGIC. The in-memory skip list is empty.
    pub fn create(path: &Path, config: OrderLogConfig) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(path)?;

        file.set_len(config.initial_capacity)?;
        let mut mmap = unsafe { MmapMut::map_mut(&file)? };
        mmap[0..4].copy_from_slice(&MAGIC.to_le_bytes());

        Ok(OrderLog {
            index: OrderIndex::new(),
            mmap,
            file,
            path: path.to_path_buf(),
            config,
            stats: OrderLogStats {
                entries: 0,
                data_size: HEADER_SIZE as u64,
                dead_bytes: 0,
            },
        })
    }

    /// Open an existing OrderLog at `path`. Validates MAGIC and replays
    /// the WAL into the in-memory skip list, last-write-wins.
    pub fn open(path: &Path, config: OrderLogConfig) -> io::Result<Self> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        let mmap = unsafe { MmapMut::map_mut(&file)? };

        let file_magic = u32::from_le_bytes(mmap[0..4].try_into().unwrap());
        if file_magic != MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "not an OrderLog file",
            ));
        }

        let mut this = OrderLog {
            index: OrderIndex::new(),
            mmap,
            file,
            path: path.to_path_buf(),
            config,
            stats: OrderLogStats::default(),
        };
        this.replay_wal()?;
        Ok(this)
    }

    // ---- reads (all served from in-memory skip list) ----

    /// Look up `key`. Returns a clone of the in-memory value, or
    /// `None` if absent. No file access.
    pub fn get(&self, key: &K) -> Option<V> {
        self.index.get(key).cloned()
    }

    pub fn contains(&self, key: &K) -> bool {
        self.index.contains(key)
    }

    pub fn size(&self) -> usize {
        self.stats.entries
    }

    pub fn is_empty(&self) -> bool {
        self.stats.entries == 0
    }

    pub fn stats(&self) -> &OrderLogStats {
        &self.stats
    }

    /// Iterate all live entries in **ascending key order**. Yields
    /// owned `(K, V)` clones from the skip list.
    pub fn entries(&self) -> impl Iterator<Item = (K, V)> + '_ {
        self.index.iter().map(|(k, v)| (k.clone(), v.clone()))
    }

    pub fn keys(&self) -> impl Iterator<Item = K> + '_ {
        self.index.iter().map(|(k, _)| k.clone())
    }

    pub fn values(&self) -> impl Iterator<Item = V> + '_ {
        self.index.iter().map(|(_, v)| v.clone())
    }

    pub fn range<'a>(&'a self, start: &K, end: &'a K) -> impl Iterator<Item = (K, V)> + 'a {
        self.index
            .range(start, end)
            .map(|(k, v)| (k.clone(), v.clone()))
    }

    pub fn first(&self) -> Option<(K, V)> {
        let idx = self.index.first_idx()?;
        let node = &self.index.arena[idx];
        Some((node.key.clone(), node.value.clone()))
    }

    pub fn last(&self) -> Option<(K, V)> {
        let idx = self.index.last_idx()?;
        let node = &self.index.arena[idx];
        Some((node.key.clone(), node.value.clone()))
    }

    // ---- writes ----

    /// Insert or overwrite. Appends a record to the WAL, then updates
    /// the in-memory skip list. The owned `value` becomes the live
    /// in-memory value; only its encoded form goes to disk.
    pub fn put(&mut self, key: K, value: V) -> io::Result<()> {
        let entry_size = self.append_entry(&key, &value)?;
        if let Some(old_record_size) = self.index.insert(key, value, entry_size) {
            self.stats.dead_bytes += old_record_size;
        }
        self.maybe_compact()?;
        self.refresh_stats();
        Ok(())
    }

    /// Remove `key`. Appends a tombstone, drops the in-memory entry.
    /// Returns whether the key existed.
    pub fn delete(&mut self, key: &K) -> io::Result<bool> {
        let Some(old_record_size) = self.index.remove_drop(key) else {
            return Ok(false);
        };
        self.stats.dead_bytes += old_record_size;
        self.append_tombstone(key)?;
        self.maybe_compact()?;
        self.refresh_stats();
        Ok(true)
    }

    /// Unified read-modify-write / insert / delete primitive. `f`
    /// receives `Some(current)` when the key exists, `None` otherwise,
    /// and returns the desired post-state:
    ///
    /// - `Some(new)` → write the new value (overwrite or insert)
    /// - `None`      → delete the entry (or no-op when absent)
    pub fn update<F>(&mut self, key: &K, f: F) -> io::Result<()>
    where
        F: FnOnce(Option<V>) -> Option<V>,
    {
        // Single-search mutable access when key exists. `f` is wrapped in
        // `Option` so the compiler sees it's consumed at most once.
        let mut f = Some(f);
        let mut needs_delete = false;
        let mut old_record_for_delete = 0u64;

        {
            if let Some(node) = self.index.get_mut(key) {
                let current = node.value.clone();
                let old_record = node.record_size;
                match f.take().unwrap()(Some(current)) {
                    Some(new_v) => {
                        // Overwrite in-place — no second search, no arena
                        // allocation, no level recomputation.
                        let entry_size = append_into(
                            &mut self.mmap,
                            &mut self.file,
                            &mut self.stats.data_size,
                            key,
                            &new_v,
                        )?;
                        self.stats.dead_bytes += old_record;
                        node.value = new_v;
                        node.record_size = entry_size;
                        self.maybe_compact()?;
                        self.refresh_stats();
                        return Ok(());
                    }
                    None => {
                        needs_delete = true;
                        old_record_for_delete = old_record;
                    }
                }
            }
        } // node borrow ends here — safe for remove_drop below

        if needs_delete {
            self.stats.dead_bytes += old_record_for_delete;
            self.index.remove_drop(key);
            self.append_tombstone(key)?;
            self.maybe_compact()?;
            self.refresh_stats();
            return Ok(());
        }

        // Key absent.
        if let Some(f) = f {
            match f(None) {
                Some(new_v) => {
                    let entry_size = self.append_entry(key, &new_v)?;
                    self.index.insert(key.clone(), new_v, entry_size);
                    self.maybe_compact()?;
                    self.refresh_stats();
                }
                None => {}
            }
        }

        Ok(())
    }

    pub fn clear(&mut self) -> io::Result<()> {
        self.index.clear();
        self.stats.data_size = HEADER_SIZE as u64;
        self.stats.dead_bytes = 0;
        self.mmap[0..4].copy_from_slice(&MAGIC.to_le_bytes());
        if self.mmap.len() >= HEADER_SIZE + 4 {
            self.mmap[HEADER_SIZE..HEADER_SIZE + 4].copy_from_slice(&0u32.to_le_bytes());
        }
        self.refresh_stats();
        Ok(())
    }

    // ---- WAL writes ----

    /// Append `[vlen: u32][V][klen: u32][K]` at `stats.data_size`. Returns
    /// the total bytes written for this record.
    fn append_entry(&mut self, key: &K, value: &V) -> io::Result<u64> {
        let offset = self.stats.data_size as usize;

        let v_end = self.serialize_into_mmap(value, offset + 4)?;
        let value_size = (v_end - (offset + 4)) as u32;
        self.mmap[offset..offset + 4].copy_from_slice(&value_size.to_le_bytes());

        let k_end = self.serialize_into_mmap(key, v_end + 4)?;
        let key_size = (k_end - (v_end + 4)) as u32;
        self.mmap[v_end..v_end + 4].copy_from_slice(&key_size.to_le_bytes());

        let written = (k_end - offset) as u64;
        self.stats.data_size = k_end as u64;
        Ok(written)
    }

    /// Append `[TOMBSTONE: u32][klen: u32][K]` at `stats.data_size`.
    fn append_tombstone(&mut self, key: &K) -> io::Result<()> {
        let offset = self.stats.data_size as usize;
        let k_end = self.serialize_into_mmap(key, offset + 8)?;
        let key_size = (k_end - (offset + 8)) as u32;

        self.mmap[offset..offset + 4].copy_from_slice(&TOMBSTONE.to_le_bytes());
        self.mmap[offset + 4..offset + 8].copy_from_slice(&key_size.to_le_bytes());

        self.stats.data_size = k_end as u64;
        self.stats.dead_bytes += 8 + key_size as u64;
        Ok(())
    }

    fn serialize_into_mmap<T: Encode>(&mut self, value: &T, pos: usize) -> io::Result<usize> {
        let size = serialized_size(value)?;
        let end = pos.checked_add(size).ok_or_else(|| {
            io::Error::new(io::ErrorKind::OutOfMemory, "OrderLog offset overflow")
        })?;
        if end > self.mmap.len() {
            self.grow(end as u64)?;
        }
        let written = serialize_into(value, &mut self.mmap[pos..end])?;
        debug_assert_eq!(written, size);
        Ok(end)
    }

    pub fn flush(&self) -> io::Result<()> {
        self.mmap.flush_async()
    }

    pub fn sync(&self) -> io::Result<()> {
        self.mmap.flush()
    }

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

    /// Rewrite the WAL with one record per live entry, in **ascending
    /// key order** (a side benefit of the skip-list iteration). Uses
    /// tmp-file rename — no atomicity beyond what the OS provides.
    pub fn compact(&mut self) -> io::Result<()> {
        let tmp_path = self.path.with_extension("compact");

        // Snapshot live (K, V) pairs in ascending order, cloning out of
        // the skip list. We need owned copies to write to the new file
        // and to keep around if anything fails partway through.
        let snapshot: Vec<(K, V)> = self
            .index
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        let new_capacity = self.config.initial_capacity.max({
            // Re-encode-sized estimate: actual size depends on bincode,
            // but we have to allocate something. Start with the current
            // data_size as an upper bound (it can't be larger than
            // what live entries produce; dead bytes only inflate it).
            self.stats.data_size
        });

        let new_file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(&tmp_path)?;
        new_file.set_len(new_capacity)?;
        let new_mmap = unsafe { MmapMut::map_mut(&new_file)? };

        // Swap the new (empty) mmap in temporarily so we can use the
        // existing append helpers, then write each live entry.
        let old_mmap = std::mem::replace(&mut self.mmap, new_mmap);
        let old_file = std::mem::replace(&mut self.file, new_file);
        let old_stats = self.stats;
        self.stats.data_size = HEADER_SIZE as u64;
        self.stats.dead_bytes = 0;

        // Stamp MAGIC on the new file.
        self.mmap[0..4].copy_from_slice(&MAGIC.to_le_bytes());

        let result: io::Result<()> = (|| {
            for (k, v) in &snapshot {
                self.append_entry(k, v)?;
            }
            self.mmap.flush()?;
            Ok(())
        })();

        if let Err(e) = result {
            // Roll back: restore the old mapping. The tmp file is
            // dropped (its handle is in `self.file` which we'll swap
            // back), and we delete it best-effort.
            self.mmap = old_mmap;
            self.file = old_file;
            self.stats = old_stats;
            let _ = fs::remove_file(&tmp_path);
            return Err(e);
        }

        // Drop the previous mapping/handle before the rename.
        drop(old_mmap);
        drop(old_file);

        // Re-open the tmp file fresh after rename so our handle points
        // at the canonical path.
        fs::rename(&tmp_path, &self.path)?;
        let file = OpenOptions::new().read(true).write(true).open(&self.path)?;
        let mmap = unsafe { MmapMut::map_mut(&file)? };
        self.mmap = mmap;
        self.file = file;
        self.refresh_stats();

        Ok(())
    }

    pub fn data_size(&self) -> u64 {
        self.stats.data_size
    }

    pub fn dead_bytes(&self) -> u64 {
        self.stats.dead_bytes
    }

    fn refresh_stats(&mut self) {
        self.stats.entries = self.index.len();
    }

    fn grow(&mut self, desired: u64) -> io::Result<()> {
        self.mmap.flush()?;
        let new_capacity = ((self.mmap.len() as u64) * 2).max(desired);
        self.file.set_len(new_capacity)?;
        self.mmap = unsafe { MmapMut::map_mut(&self.file)? };
        Ok(())
    }

    // ---- WAL replay ----

    /// Walk the WAL from `HEADER_SIZE` and reconstruct the skip list.
    /// Live records insert/overwrite, tombstones remove. Each record
    /// that is later overwritten or tombstoned contributes to
    /// `dead_bytes` so the first post-open compaction reclaims it.
    fn replay_wal(&mut self) -> io::Result<()> {
        let mut cursor: usize = HEADER_SIZE;
        self.stats.dead_bytes = 0;

        while let Some(value_size) = read_u32_le(&self.mmap, cursor) {
            if value_size == 0 {
                // Zero-filled tail of the pre-allocated file.
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
                if let Some(old_record_size) = self.index.remove_drop(&key) {
                    self.stats.dead_bytes += old_record_size;
                }
                self.stats.dead_bytes += 8 + key_size as u64;
            } else {
                let value_start = cursor + 4;
                let value: V =
                    deserialize_from(&self.mmap[value_start..value_start + value_size as usize])?;
                let record_size = self.estimate_record_size(value_size, key_size);
                if let Some(old_record_size) = self.index.insert(key, value, record_size) {
                    self.stats.dead_bytes += old_record_size;
                }
            }
            cursor = entry_end;
        }

        self.stats.data_size = cursor as u64;
        self.refresh_stats();
        Ok(())
    }

    /// Bytes occupied on disk by a record with the given encoded
    /// `value_size` and `key_size`: 4 (vlen prefix) + V + 4 (klen
    /// prefix) + K = 8 + V + K.
    fn estimate_record_size(&self, value_size: u32, key_size: u32) -> u64 {
        8 + value_size as u64 + key_size as u64
    }
}

impl<K: Ord, V> Drop for OrderLog<K, V> {
    fn drop(&mut self) {
        let _ = self.mmap.flush();
    }
}

impl<K: Ord, V> fmt::Debug for OrderLog<K, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.stats, f)
    }
}

// ---------------------------------------------------------------------------
// Backend / OrderedBackend impls
// ---------------------------------------------------------------------------

impl<K, V> crate::core::backend::Backend<K, V> for OrderLog<K, V>
where
    K: Encode + Decode<()> + Hash + Eq + Clone + Ord,
    V: Encode + Decode<()> + Clone,
{
    type Stats = OrderLogStats;

    fn get(&self, key: &K) -> Option<V> {
        OrderLog::get(self, key)
    }

    fn contains(&self, key: &K) -> bool {
        OrderLog::contains(self, key)
    }

    fn put(&mut self, key: K, value: V) -> io::Result<()> {
        OrderLog::put(self, key, value)
    }

    fn delete(&mut self, key: &K) -> io::Result<bool> {
        OrderLog::delete(self, key)
    }

    fn update<F>(&mut self, key: &K, f: F) -> io::Result<()>
    where
        F: FnOnce(Option<V>) -> Option<V>,
    {
        OrderLog::update(self, key, f)
    }

    fn clear(&mut self) -> io::Result<()> {
        OrderLog::clear(self)
    }

    fn compact(&mut self) -> io::Result<()> {
        OrderLog::compact(self)
    }

    fn keys(&self) -> impl Iterator<Item = K> + '_ {
        OrderLog::keys(self)
    }

    fn values(&self) -> impl Iterator<Item = V> + '_ {
        OrderLog::values(self)
    }

    fn entries(&self) -> impl Iterator<Item = (K, V)> + '_ {
        OrderLog::entries(self)
    }

    fn size(&self) -> usize {
        OrderLog::size(self)
    }

    fn is_empty(&self) -> bool {
        OrderLog::is_empty(self)
    }

    fn stats(&self) -> &Self::Stats {
        OrderLog::stats(self)
    }

    fn flush(&self) -> io::Result<()> {
        OrderLog::flush(self)
    }

    fn sync(&self) -> io::Result<()> {
        OrderLog::sync(self)
    }
}

impl<K, V> crate::core::backend::OrderedBackend<K, V> for OrderLog<K, V>
where
    K: Encode + Decode<()> + Hash + Eq + Clone + Ord,
    V: Encode + Decode<()> + Clone,
{
    fn range(&self, start: &K, end: &K) -> impl Iterator<Item = (K, V)> + '_ {
        // Trait `end` lifetime is short, so materialize eagerly.
        let v: Vec<(K, V)> = self
            .index
            .range(start, end)
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        v.into_iter()
    }

    fn first(&self) -> Option<(K, V)> {
        OrderLog::first(self)
    }

    fn last(&self) -> Option<(K, V)> {
        OrderLog::last(self)
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

    #[derive(Encode, Decode, Debug, Clone, PartialEq, Default)]
    struct TestVal {
        name: String,
        count: u32,
    }

    fn k(s: &str) -> TestKey {
        s.as_bytes().to_vec()
    }

    fn v(name: &str, count: u32) -> TestVal {
        TestVal {
            name: name.into(),
            count,
        }
    }

    fn tmp_path(label: &str) -> PathBuf {
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let n = SEQ.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join("zendb_orderlog_tests");
        fs::create_dir_all(&dir).unwrap();
        let p = dir.join(format!("{}_{}_{}.olog", label, std::process::id(), n));
        let _ = fs::remove_file(&p);
        let _ = fs::remove_file(p.with_extension("compact"));
        p
    }

    fn create(path: &Path) -> OrderLog<TestKey, TestVal> {
        OrderLog::create(path, OrderLogConfig::default()).unwrap()
    }

    fn open(path: &Path) -> OrderLog<TestKey, TestVal> {
        OrderLog::open(path, OrderLogConfig::default()).unwrap()
    }

    #[test]
    fn put_then_get() {
        let p = tmp_path("put_get");
        let mut ol = create(&p);
        ol.put(k("alice"), v("alice", 1)).unwrap();
        assert_eq!(ol.get(&k("alice")), Some(v("alice", 1)));
    }

    #[test]
    fn missing_returns_none() {
        let p = tmp_path("missing");
        let ol = create(&p);
        assert!(ol.get(&k("ghost")).is_none());
    }

    #[test]
    fn entries_are_ordered() {
        let p = tmp_path("ordered");
        let mut ol = create(&p);
        for s in ["delta", "alpha", "echo", "bravo", "charlie"] {
            ol.put(k(s), v(s, 0)).unwrap();
        }
        let got: Vec<TestKey> = ol.keys().collect();
        assert_eq!(
            got,
            vec![k("alpha"), k("bravo"), k("charlie"), k("delta"), k("echo"),]
        );
    }

    #[test]
    fn range_returns_subrange() {
        let p = tmp_path("range");
        let mut ol = create(&p);
        for s in ["a", "b", "c", "d", "e", "f"] {
            ol.put(k(s), v(s, 0)).unwrap();
        }
        let got: Vec<TestKey> = ol.range(&k("b"), &k("e")).map(|(k, _)| k).collect();
        assert_eq!(got, vec![k("b"), k("c"), k("d")]);
    }

    #[test]
    fn first_last_endpoints() {
        let p = tmp_path("endpoints");
        let mut ol = create(&p);
        for s in ["m", "a", "z", "f"] {
            ol.put(k(s), v(s, 0)).unwrap();
        }
        assert_eq!(ol.first().map(|(k, _)| k), Some(k("a")));
        assert_eq!(ol.last().map(|(k, _)| k), Some(k("z")));
    }

    #[test]
    fn delete_removes_and_persists() {
        let p = tmp_path("delete");
        let mut ol = create(&p);
        ol.put(k("a"), v("a", 1)).unwrap();
        ol.put(k("b"), v("b", 2)).unwrap();
        assert!(ol.delete(&k("a")).unwrap());
        assert!(!ol.delete(&k("a")).unwrap());
        assert!(ol.get(&k("a")).is_none());
        ol.sync().unwrap();
        drop(ol);

        let ol = open(&p);
        assert!(ol.get(&k("a")).is_none());
        assert_eq!(ol.get(&k("b")), Some(v("b", 2)));
    }

    #[test]
    fn overwrite_yields_latest_after_reopen() {
        let p = tmp_path("overwrite");
        {
            let mut ol = create(&p);
            ol.put(k("x"), v("v1", 1)).unwrap();
            ol.put(k("x"), v("v2", 2)).unwrap();
            ol.sync().unwrap();
        }
        let ol = open(&p);
        assert_eq!(ol.get(&k("x")), Some(v("v2", 2)));
    }

    #[test]
    fn update_combines_existing() {
        let p = tmp_path("update_combine");
        let mut ol: OrderLog<TestKey, u32> =
            OrderLog::create(&p, OrderLogConfig::default()).unwrap();
        ol.put(k("counter"), 1u32).unwrap();
        let inc = 5u32;
        ol.update(&k("counter"), move |cur| Some(cur.unwrap_or(0) + inc))
            .unwrap();
        let inc2 = 4u32;
        ol.update(&k("counter"), move |cur| Some(cur.unwrap_or(0) + inc2))
            .unwrap();
        assert_eq!(ol.get(&k("counter")), Some(10u32));
    }

    #[test]
    fn update_inserts_when_absent() {
        let p = tmp_path("update_absent_insert");
        let mut ol: OrderLog<TestKey, u32> =
            OrderLog::create(&p, OrderLogConfig::default()).unwrap();
        let val = 7u32;
        ol.update(&k("fresh"), move |cur| Some(cur.unwrap_or(0) + val))
            .unwrap();
        assert_eq!(ol.get(&k("fresh")), Some(7u32));
    }

    #[test]
    fn update_returning_none_deletes() {
        let p = tmp_path("update_delete");
        let mut ol: OrderLog<TestKey, u32> =
            OrderLog::create(&p, OrderLogConfig::default()).unwrap();
        ol.put(k("x"), 42u32).unwrap();
        ol.update(&k("x"), |_| None).unwrap();
        assert!(ol.get(&k("x")).is_none());
        assert_eq!(ol.size(), 0);
    }

    #[test]
    fn update_absent_returning_none_is_noop() {
        let p = tmp_path("update_noop");
        let mut ol: OrderLog<TestKey, u32> =
            OrderLog::create(&p, OrderLogConfig::default()).unwrap();
        ol.update(&k("ghost"), |_| None).unwrap();
        assert!(ol.get(&k("ghost")).is_none());
        assert_eq!(ol.size(), 0);
    }

    #[test]
    fn stats_track_entries_data_size_and_dead_bytes() {
        let p = tmp_path("stats");
        let mut ol = OrderLog::create(
            &p,
            OrderLogConfig {
                initial_capacity: 8 * 1024,
                compaction_ratio: 1.0,
            },
        )
        .unwrap();
        assert_eq!(
            *ol.stats(),
            OrderLogStats {
                entries: 0,
                data_size: HEADER_SIZE as u64,
                dead_bytes: 0,
            }
        );

        ol.put(k("a"), v("first", 1)).unwrap();
        assert_eq!(ol.stats().entries, 1);
        assert!(ol.stats().data_size > HEADER_SIZE as u64);
        assert_eq!(ol.stats().dead_bytes, 0);

        ol.put(k("a"), v("second", 2)).unwrap();
        assert_eq!(ol.stats().entries, 1);
        assert!(ol.stats().dead_bytes > 0);
    }

    #[test]
    fn dead_bytes_are_exact_across_overwrite_delete_and_reopen() {
        let p = tmp_path("dead_bytes_exact");
        let key = k("x");
        let v1 = v("short", 1);
        let v2 = v("a longer value", 2);
        let rec1 = 8 + serialized_size(&v1).unwrap() as u64 + serialized_size(&key).unwrap() as u64;
        let rec2 = 8 + serialized_size(&v2).unwrap() as u64 + serialized_size(&key).unwrap() as u64;
        let tombstone = 8 + serialized_size(&key).unwrap() as u64;

        {
            let mut ol = OrderLog::create(
                &p,
                OrderLogConfig {
                    initial_capacity: 8 * 1024,
                    compaction_ratio: 1.0,
                },
            )
            .unwrap();
            ol.put(key.clone(), v1.clone()).unwrap();
            assert_eq!(ol.dead_bytes(), 0);
            ol.put(key.clone(), v2.clone()).unwrap();
            assert_eq!(ol.dead_bytes(), rec1);
            ol.delete(&key).unwrap();
            assert_eq!(ol.dead_bytes(), rec1 + rec2 + tombstone);
            ol.sync().unwrap();
        }

        let ol: OrderLog<TestKey, TestVal> = OrderLog::open(
            &p,
            OrderLogConfig {
                initial_capacity: 8 * 1024,
                compaction_ratio: 1.0,
            },
        )
        .unwrap();
        assert_eq!(ol.dead_bytes(), rec1 + rec2 + tombstone);
        assert_eq!(ol.stats().dead_bytes, rec1 + rec2 + tombstone);
        assert!(ol.dead_bytes() < u32::MAX as u64);
    }

    #[test]
    fn update_can_move_owned_captures() {
        let p = tmp_path("update_move");
        let mut ol: OrderLog<TestKey, Vec<u32>> =
            OrderLog::create(&p, OrderLogConfig::default()).unwrap();
        ol.put(k("acc"), vec![1, 2, 3]).unwrap();
        let extras: Vec<u32> = vec![10, 20, 30];
        ol.update(&k("acc"), move |cur| {
            let mut v = cur.unwrap();
            v.extend(extras);
            Some(v)
        })
        .unwrap();
        assert_eq!(ol.get(&k("acc")), Some(vec![1, 2, 3, 10, 20, 30]));
    }

    #[test]
    fn compact_keeps_live_drops_dead() {
        let p = tmp_path("compact");
        let mut ol = create(&p);
        for i in 0..50 {
            ol.put(k(&format!("k{:02}", i)), v("init", i)).unwrap();
        }
        for i in 0..50 {
            ol.put(k(&format!("k{:02}", i)), v("update", i)).unwrap();
        }
        for i in 0..25 {
            ol.delete(&k(&format!("k{:02}", i))).unwrap();
        }
        ol.compact().unwrap();
        assert_eq!(ol.size(), 25);
        for i in 25..50 {
            assert_eq!(ol.get(&k(&format!("k{:02}", i))), Some(v("update", i)));
        }
    }

    #[test]
    fn rebuild_after_reopen() {
        let p = tmp_path("rebuild");
        {
            let mut ol = create(&p);
            for s in ["b", "a", "c"] {
                ol.put(k(s), v(s, 1)).unwrap();
            }
            ol.delete(&k("b")).unwrap();
            ol.put(k("d"), v("d", 1)).unwrap();
            ol.sync().unwrap();
        }
        let ol = open(&p);
        let got: Vec<TestKey> = ol.keys().collect();
        assert_eq!(got, vec![k("a"), k("c"), k("d")]);
    }

    #[test]
    fn clear_resets() {
        let p = tmp_path("clear");
        let mut ol = create(&p);
        for s in ["a", "b", "c"] {
            ol.put(k(s), v(s, 0)).unwrap();
        }
        ol.clear().unwrap();
        assert_eq!(ol.size(), 0);
        assert!(ol.is_empty());
        assert!(ol.get(&k("a")).is_none());
    }

    #[test]
    fn reads_dont_touch_disk_after_open() {
        // Sanity: after open, get/range/first/last should be served
        // from memory. We can't observe page faults from a test, but
        // we can at least verify identical results before and after a
        // reopen — that's the contract that matters.
        let p = tmp_path("memory_reads");
        {
            let mut ol = create(&p);
            for i in 0..20u32 {
                ol.put(format!("k{:02}", i).into_bytes(), v("entry", i))
                    .unwrap();
            }
            ol.sync().unwrap();
        }
        let ol = open(&p);
        assert_eq!(ol.size(), 20);
        let first = ol.first().unwrap();
        let last = ol.last().unwrap();
        assert_eq!(first.0, k("k00"));
        assert_eq!(last.0, k("k19"));
        let middle: Vec<_> = ol.range(&k("k05"), &k("k08")).map(|(k, _)| k).collect();
        assert_eq!(middle, vec![k("k05"), k("k06"), k("k07")]);
    }
}
