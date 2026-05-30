//! BPlusTree - persistent, ordered key-value store backed by a mmap'd B+ tree
//! with suffix truncation and contiguous extents for large values.
//!
//! # Generic over K, V
//!
//! Keys and values are serialized through bincode. Reads return owned decoded
//! values (`Option<V>`), and iteration yields owned `(K, V)` pairs.
//!
//! # Architecture
//!
//! Every internal page holds a `leftmost_child` pointer plus an array of
//! `(child_page, separator_key)` pairs. Binary search on the separator keys
//! routes point-lookups to the correct subtree. All values live in leaves;
//! internal pages carry only keys for navigation.
//!
//! Tree navigation compares serialized key bytes lexicographically, not
//! `K::Ord`. Callers that need semantic ordering must choose key encodings
//! whose bincode byte representation has that ordering, or use a backend such
//! as `OrderLog` whose ordered surface is based on `K::Ord`.
//!
//! # Page layout
//!
//! Pages are 4096-byte slotted pages. Internal pages have a 16-byte header:
//! `type(u8) flags(u8) count(u16 LE) data_off(u32 LE) leftmost_child(u64 LE)`.
//! Leaf pages have a 24-byte header:
//! `type(u8) flags(u8) count(u16 LE) data_off(u32 LE) next_leaf(u64 LE)
//! prev_leaf(u64 LE)`.
//!
//! Slots are a grow-down array of `u16` LE offsets from the slot array; each
//! points to an entry's starting byte within the page. Entries grow up from the
//! bottom. When the two regions collide, the page splits.
//!
//! # Entry formats
//!
//! Leaf, inline:   `[key_len: u16][value_len: u32][key_bytes][value_bytes]`
//! Leaf, extent:   `[key_len: u16][value_len: u32 | OVFL_FLAG][key_bytes][extent_start: u64]`
//! Internal:       `[key_len: u16][child_page: u64][separator_key]`
//!
//! # Extents
//!
//! Values exceeding the per-page inline limit (`MAX_INLINE`) are stored in a
//! contiguous run of pages allocated at the file's tail. The leaf entry stores
//! the first page index of the extent and the true value length; the value bytes
//! occupy bytes `[0..value_len]` of the extent mmap slice.
//!
//! Extent allocation always grows the file. Freed extent pages go onto the
//! single-page freelist so they can be reused as tree pages.
//!
//! # Meta page
//!
//! Page 0 is the meta page:
//! `[magic: u32][root: u64][free_head: u64][pages: u64][entries: u64][rightmost_leaf: u64]`.
//!
//! # Suffix truncation
//!
//! When a leaf or internal page splits, the separator key pushed to the parent
//! is the shortest prefix of the new page's first key that is strictly greater
//! than the old page's last key.

use memmap2::MmapMut;
use std::{
    fmt,
    fs::{self, OpenOptions},
    hash::Hash,
    io,
    marker::PhantomData,
    path::Path,
};

use bincode::{Decode, Encode};

use crate::utils::serdes::{
    deserialize_from, rd_u16, rd_u32, rd_u64, serialize_into, serialize_to_vec, serialized_size,
    with_scratch, wr_u16, wr_u32, wr_u64,
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const PAGE_SIZE: usize = 4096;
/// Header size for internal pages (type, flags, count, data_off, leftmost_child).
const HEADER_SIZE: usize = 16;
/// Header size for leaf pages — adds an 8-byte `prev_leaf` pointer after
/// `next_leaf` so reverse iteration can stream without materializing.
const LEAF_HEADER_SIZE: usize = 24;
const SLOT_SIZE: usize = 2;
/// Magic identifies the on-disk BPlusTree layout.
const MAGIC: u32 = 0x5450425B;
const DEFAULT_COMPACTION_RATIO: f64 = 0.5;
const META_PAGE: u64 = 0;
const PAGE_LEAF: u8 = 1;
const PAGE_INTERNAL: u8 = 2;
const FLAG_ROOT: u8 = 0x01;

// Meta-page field offsets (page 0).
// Layout: magic(4) root(8) free_head(8) pages(8) entries(8) rightmost_leaf(8)
const META_ROOT: usize = 4;
const META_FREE_HEAD: usize = 12;
const META_PAGES: usize = 20;
const META_ENTRIES: usize = 28;
const META_RIGHTMOST_LEAF: usize = 36;

/// High bit of `value_len` marks an extent value (stored in dedicated pages).
const OVFL_FLAG: u32 = 0x8000_0000;

/// Maximum inline entry size: a single entry must fit in a fresh leaf page.
/// = PAGE_SIZE - LEAF_HEADER_SIZE - SLOT_SIZE - 6 (key_len u16 + value_len u32)
const MAX_INLINE: usize = PAGE_SIZE - LEAF_HEADER_SIZE - SLOT_SIZE - 6;

#[inline]
fn page_offset(page: u64) -> usize {
    page as usize * PAGE_SIZE
}

/// Result of splitting a full page. Carries enough information for the
/// caller to insert a new separator-key/child-pointer pair into the parent.
///
/// `separator_key` is a suffix-truncated key (see [`truncated_separator`])
/// that discriminates the left child's key range from the right child's.
struct PageSplit {
    left_page: u64,
    right_page: u64,
    separator_key: Vec<u8>,
}

/// Value variant carried through a leaf split. Avoids copying extent data
/// — large values already living in their own pages are carried by reference
/// (`Extent`), small values are copied inline (`Inline`). This keeps the
/// split path allocation-minimal: only short values materialize as bytes.
enum SplitVal {
    /// Small value stored inline.
    Inline(Vec<u8>),
    /// Large value already living in an extent — carry the pointer as-is.
    Extent { start: u64, value_len: u32 },
}

/// Compute the **shortest prefix** of `right_first` that is strictly greater
/// than `left_last` and ≤ `right_first`. Used as the separator key when
/// splitting a page — stores only the bytes needed to distinguish the two
/// key ranges, maximizing internal-page fanout.
///
/// # Examples
///
/// - `(b"abc", b"abd")` → `b"abd"` (diverges at third byte)
/// - `(b"abc", b"abcd")` → `[b'a', b'b', b'c', 0]` (left is prefix of right;
///   appends 0x00 to create a key `> "abc"` and `≤ "abcd"`)
/// - `(b"hello", b"world")` → `b"w"` (diverges at first byte; one byte suffices)
fn truncated_separator(left_last: &[u8], right_first: &[u8]) -> Vec<u8> {
    let n = left_last.len().min(right_first.len());
    for i in 0..n {
        if right_first[i] > left_last[i] {
            return right_first[..=i].to_vec();
        }
    }
    // left_last is a prefix of right_first (e.g. "abc" / "abcd").
    let mut s = left_last.to_vec();
    s.push(0);
    s
}

// ---------------------------------------------------------------------------
// BPlusTree
// ---------------------------------------------------------------------------

pub struct BPlusTree<K, V> {
    mmap: MmapMut,
    file: fs::File,
    config: BPlusTreeConfig,
    stats: BPlusTreeStats,
    _phantom: PhantomData<(K, V)>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct BPlusTreeStats {
    pub entries: usize,
    pub pages: u64,
    pub free_pages: u64,
    /// Number of pages currently used as leaves. Maintained incrementally
    /// in [`BPlusTree::init_leaf`] and reset in `create` / `clear` /
    /// `compact` so [`BPlusTree::fragmentation_ratio`] can answer in O(1)
    /// without re-walking the leaf chain on every write.
    pub leaf_pages: u64,
    /// Total live entry bytes across all leaves (entry header + key +
    /// payload + slot). Used together with `leaf_pages` to estimate the
    /// packed leaf count.
    pub leaf_entry_bytes: u64,
}

/// Byte cost of a single leaf entry: `[klen u16][vlen u32][key][payload]`
/// (header + key + payload) plus the 2-byte slot pointer. `payload_len` is
/// the inline value length or `8` for an extent pointer.
#[inline]
fn leaf_entry_bytes(key_len: usize, payload_len: usize) -> u64 {
    (6 + key_len + payload_len + SLOT_SIZE) as u64
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct BPlusTreeConfig {
    /// Auto-compaction threshold. The tree estimates reclaimable pages as
    /// freed extent pages plus leaf pages that would disappear if live
    /// entries were packed into fresh leaves. Callers must pass a value
    /// in `[0.0, 1.0]`: `0.0` compacts after every write, `1.0` disables
    /// automatic compaction.
    pub compaction_ratio: f64,
}

impl Default for BPlusTreeConfig {
    fn default() -> Self {
        BPlusTreeConfig {
            compaction_ratio: DEFAULT_COMPACTION_RATIO,
        }
    }
}

/// Byte-level helpers: raw mmap access without `K`/`V` serialization bounds.
impl<K, V> BPlusTree<K, V> {
    /// Read the value slice at leaf page `off`, slot `i`. Handles both
    /// inline and extent layouts and returns a single contiguous slice.
    fn value_slice_at(&self, off: usize, i: usize) -> &[u8] {
        let sp = off + LEAF_HEADER_SIZE + i * SLOT_SIZE;
        let eo = off + rd_u16(&self.mmap[sp..sp + 2]) as usize;
        let kl = rd_u16(&self.mmap[eo..eo + 2]) as usize;
        let raw_vl = rd_u32(&self.mmap[eo + 2..eo + 6]);
        if raw_vl & OVFL_FLAG != 0 {
            let real_len = (raw_vl & !OVFL_FLAG) as usize;
            let extent_start = rd_u64(&self.mmap[eo + 6 + kl..eo + 6 + kl + 8]);
            let xo = page_offset(extent_start);
            &self.mmap[xo..xo + real_len]
        } else {
            let vl = raw_vl as usize;
            &self.mmap[eo + 6 + kl..eo + 6 + kl + vl]
        }
    }

    /// Key slice for slot `i` of leaf `off`. Borrows from `self.mmap`.
    fn key_slice_at(&self, off: usize, i: usize) -> &[u8] {
        let sp = off + LEAF_HEADER_SIZE + i * SLOT_SIZE;
        let eo = off + rd_u16(&self.mmap[sp..sp + 2]) as usize;
        let kl = rd_u16(&self.mmap[eo..eo + 2]) as usize;
        &self.mmap[eo + 6..eo + 6 + kl]
    }
}

impl<K, V> BPlusTree<K, V>
where
    K: Encode + Decode<()> + Hash + Eq + Clone + Ord,
    V: Encode + Decode<()> + Clone,
{
    // -- constructors --

    /// Create a fresh BPlusTree at `path`, **truncating** any existing
    /// file. Pre-allocates 64 pages (256 KiB) — the meta page, an empty
    /// root leaf, and 62 reserved-but-unused pages — so typical small
    /// workloads finish without triggering a single file grow. The
    /// meta page is stamped with MAGIC + initial counters.
    pub fn create(path: &Path, config: BPlusTreeConfig) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(path)?;
        // 2 pages used (meta + root leaf), but pre-allocate 64 pages
        // (256 KiB) so small workloads finish without a single remap.
        // Exponential growth kicks in past that.
        let np = 2u64;
        let initial_pages = 64u64;
        file.set_len(initial_pages * PAGE_SIZE as u64)?;
        let mut mmap = unsafe { MmapMut::map_mut(&file)? };
        let m = page_offset(META_PAGE);
        wr_u32(&mut mmap[m..m + 4], MAGIC);
        wr_u64(&mut mmap[m + META_ROOT..m + META_ROOT + 8], 1); // root page
        wr_u64(&mut mmap[m + META_FREE_HEAD..m + META_FREE_HEAD + 8], 0); // freelist
        wr_u64(&mut mmap[m + META_PAGES..m + META_PAGES + 8], np); // total pages
        wr_u64(&mut mmap[m + META_ENTRIES..m + META_ENTRIES + 8], 0); // entries
        wr_u64(
            &mut mmap[m + META_RIGHTMOST_LEAF..m + META_RIGHTMOST_LEAF + 8],
            1,
        ); // rightmost
        let r = page_offset(1);
        mmap[r] = PAGE_LEAF;
        mmap[r + 1] = FLAG_ROOT;
        wr_u16(&mut mmap[r + 2..r + 4], 0);
        wr_u32(&mut mmap[r + 4..r + 8], PAGE_SIZE as u32);
        wr_u64(&mut mmap[r + 8..r + 16], 0); // next_leaf = 0
        wr_u64(&mut mmap[r + 16..r + 24], 0); // prev_leaf = 0
        Ok(BPlusTree {
            mmap,
            file,
            config,
            stats: BPlusTreeStats {
                entries: 0,
                pages: np,
                free_pages: 0,
                leaf_pages: 1,
                leaf_entry_bytes: 0,
            },
            _phantom: PhantomData,
        })
    }

    /// Open an existing BPlusTree at `path`. Validates the MAGIC header
    /// at offset 0 (returns `InvalidData` if missing/mismatched). No
    /// index rebuild needed — the tree structure lives in the mmap
    /// itself and is read on demand.
    pub fn open(path: &Path, config: BPlusTreeConfig) -> io::Result<Self> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        let mmap = unsafe { MmapMut::map_mut(&file)? };
        if rd_u32(&mmap[0..4]) != MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "not a BPlusTree file (bad magic)",
            ));
        }
        let mut this = BPlusTree {
            mmap,
            file,
            config,
            stats: BPlusTreeStats::default(),
            _phantom: PhantomData,
        };
        this.refresh_stats_from_meta();
        Ok(this)
    }

    // -- public generic API --

    /// Look up `key`. Returns the decoded value, or `None` if absent.
    pub fn get(&self, key: &K) -> Option<V> {
        with_scratch(key, |kb| {
            Ok(self
                .value_slice_for(kb)
                .map(|slice| deserialize_from(slice).expect("valid encoded value")))
        })
        .ok()
        .flatten()
    }

    /// Convenience: existence check without deserializing the value.
    pub fn contains(&self, key: &K) -> bool {
        with_scratch(key, |kb| Ok(self.value_slice_for(kb).is_some())).unwrap_or(false)
    }

    /// Insert or overwrite. Both `key` and `value` are consumed.
    ///
    /// The value's encoding is written **directly into the mmap** — no
    /// intermediate buffer. The key is encoded into a reusable thread-local
    /// scratch buffer (no per-call allocation) used for tree navigation and
    /// then copied into the leaf slot in one pass.
    pub fn put(&mut self, key: K, value: V) -> io::Result<()> {
        let v_size = serialized_size(&value)?;
        with_scratch(&key, |kb| self.insert_typed(kb, &value, v_size).map(|_| ()))?;
        self.maybe_compact()?;
        Ok(())
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
        // Use the thread-local scratch so the read leg, the optional
        // overwrite/insert, and the optional delete all share one encoded
        // key buffer. The whole sequence lives inside a single `with`
        // borrow — that's why `f` runs there too.
        with_scratch(key, |kb| {
            let current: Option<V> = self
                .value_slice_for(kb)
                .map(|slice| deserialize_from(slice))
                .transpose()?;
            let had_value = current.is_some();
            match (had_value, f(current)) {
                (_, Some(new_v)) => {
                    let v_size = serialized_size(&new_v)?;
                    self.insert_typed(kb, &new_v, v_size)?;
                    self.maybe_compact()?;
                }
                (true, None) => {
                    self.delete_bytes(kb)?;
                    self.maybe_compact()?;
                }
                (false, None) => {}
            }
            Ok(())
        })
    }

    /// Remove `key`. Returns true if the key existed.
    pub fn delete(&mut self, key: &K) -> io::Result<bool> {
        let deleted = with_scratch(key, |kb| self.delete_bytes(kb))?;
        if deleted {
            self.maybe_compact()?;
        }
        Ok(deleted)
    }

    /// Remove every entry, resetting the tree to its post-`create` state.
    /// The on-disk file size is left as-is; pages 2..N are now logically
    /// unallocated and future writes reuse them.
    pub fn clear(&mut self) -> io::Result<()> {
        let m = page_offset(META_PAGE);
        wr_u64(&mut self.mmap[m + META_ROOT..m + META_ROOT + 8], 1); // root page = 1
        wr_u64(
            &mut self.mmap[m + META_FREE_HEAD..m + META_FREE_HEAD + 8],
            0,
        ); // freelist = 0
        wr_u64(&mut self.mmap[m + META_PAGES..m + META_PAGES + 8], 2); // pages = 2
        wr_u64(&mut self.mmap[m + META_ENTRIES..m + META_ENTRIES + 8], 0); // entries = 0
        wr_u64(
            &mut self.mmap[m + META_RIGHTMOST_LEAF..m + META_RIGHTMOST_LEAF + 8],
            1,
        ); // rightmost = root leaf
        self.stats = BPlusTreeStats {
            entries: 0,
            pages: 2,
            free_pages: 0,
            leaf_pages: 1,
            leaf_entry_bytes: 0,
        };
        // Re-initialize page 1 as a fresh root leaf.
        let r = page_offset(1);
        self.mmap[r] = PAGE_LEAF;
        self.mmap[r + 1] = FLAG_ROOT;
        wr_u16(&mut self.mmap[r + 2..r + 4], 0);
        wr_u32(&mut self.mmap[r + 4..r + 8], PAGE_SIZE as u32);
        wr_u64(&mut self.mmap[r + 8..r + 16], 0); // next_leaf = 0
        wr_u64(&mut self.mmap[r + 16..r + 24], 0); // prev_leaf = 0
        Ok(())
    }

    /// Rebuild the tree from live entries. Removes empty/underfilled leaves
    /// and drops freed extent pages from the logical page high-water mark.
    ///
    /// The rebuild path snapshots **raw bytes** straight from the existing
    /// leaves — no bincode `Decode` → `Encode` roundtrip. Both keys and
    /// values are copied as already-encoded `&[u8]` and replayed via
    /// [`insert_raw`], which writes them directly into the new leaf slots.
    /// Saves one decode and one encode per live entry vs. the previous
    /// `entries().collect()` round-trip.
    pub fn compact(&mut self) -> io::Result<()> {
        // Phase 1: snapshot raw (key, value) byte pairs. Both must be copied
        // out before `clear()` resets the page space — including extent
        // payloads, since `clear()` truncates the logical page count and
        // any old extent pages stop being reachable.
        let mut snapshot: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(self.stats.entries);
        let mut page = self.first_leaf();
        while page != 0 {
            let off = page_offset(page);
            let count = rd_u16(&self.mmap[off + 2..off + 4]) as usize;
            for i in 0..count {
                let key_bytes = self.key_slice_at(off, i).to_vec();
                let value_bytes = self.value_slice_at(off, i).to_vec();
                snapshot.push((key_bytes, value_bytes));
            }
            page = rd_u64(&self.mmap[off + 8..off + 16]);
        }

        // Phase 2: reset the tree to its post-`create` state.
        self.clear()?;

        // Phase 3: replay the snapshot via the raw-bytes insert path.
        for (k, v) in snapshot {
            self.insert_raw(&k, &v)?;
        }
        Ok(())
    }

    /// Number of live entries from the in-memory stats snapshot.
    pub fn size(&self) -> usize {
        self.stats.entries
    }

    /// Current estimated reclaimable-page ratio used by auto-compaction.
    ///
    /// Reads the incrementally-maintained `leaf_pages` and
    /// `leaf_entry_bytes` counters from `stats` rather than walking the
    /// leaf chain — so this is O(1) on every `put`/`delete`.
    pub fn fragmentation_ratio(&self) -> f64 {
        let allocated = self.pages().saturating_sub(1);
        if allocated == 0 {
            return 0.0;
        }
        let capacity = (PAGE_SIZE - LEAF_HEADER_SIZE) as u64;
        // Approximation: ceil(live_bytes / per_leaf_capacity). Exact
        // bottom-up packing might fit one more or fewer entries per leaf
        // due to entry-boundary alignment, but the heuristic is fine for
        // the auto-compaction trigger.
        let packed_leaf_pages = if self.stats.leaf_entry_bytes == 0 {
            1
        } else {
            (self.stats.leaf_entry_bytes + capacity - 1) / capacity
        };
        let reclaimable_leaf_pages = self.stats.leaf_pages.saturating_sub(packed_leaf_pages);
        let free_pages = self.stats.free_pages;
        (free_pages + reclaimable_leaf_pages) as f64 / allocated as f64
    }

    pub fn stats(&self) -> &BPlusTreeStats {
        &self.stats
    }

    /// Schedule mmap writeback asynchronously. Returns once the OS has
    /// accepted the request; use [`sync`] when the caller wants to wait
    /// for that writeback to complete.
    pub fn flush(&self) -> io::Result<()> {
        self.mmap.flush_async()
    }

    /// Block until pending mmap writes have been flushed by the OS.
    /// This does not provide crash recovery or multi-page atomicity.
    pub fn sync(&self) -> io::Result<()> {
        self.mmap.flush()
    }

    /// Iterate all entries in serialized-key order. Yields owned `(K, V)`
    /// — both are deserialized from the mmap'd bytes per slot.
    pub fn entries(&self) -> impl Iterator<Item = (K, V)> + '_ {
        BTreeIter {
            tree: self,
            page: self.first_leaf(),
            slot: 0,
        }
    }

    /// Iterate all keys in serialized-key order. Yields owned `K`.
    pub fn keys(&self) -> impl Iterator<Item = K> + '_ {
        self.entries().map(|(k, _)| k)
    }

    /// Iterate all values in serialized-key order. Yields owned `V`.
    pub fn values(&self) -> impl Iterator<Item = V> + '_ {
        self.entries().map(|(_, v)| v)
    }

    /// Iterate entries with serialized keys in `[start, end)`.
    pub fn range(&self, start: &K, end: &K) -> BTreeRange<'_, K, V> {
        let Ok(start_bytes) = serialize_to_vec(start) else {
            return BTreeRange {
                tree: self,
                page: 0,
                slot: 0,
                end: Vec::new(),
            };
        };
        let Ok(end_bytes) = serialize_to_vec(end) else {
            return BTreeRange {
                tree: self,
                page: 0,
                slot: 0,
                end: Vec::new(),
            };
        };
        let leaf = self.find_leaf(self.root(), &start_bytes);
        let off = page_offset(leaf);
        let count = rd_u16(&self.mmap[off + 2..off + 4]) as usize;
        let slot = match self.leaf_find_slot(off, count, &start_bytes) {
            Ok(i) | Err(i) => i,
        };
        BTreeRange {
            tree: self,
            page: leaf,
            slot,
            end: end_bytes,
        }
    }

    // -----------------------------------------------------------------------
    // Internal byte-level helpers — operate on serialized key bytes, not
    // typed K/V. This keeps the trait-bound surface minimal for generic
    // code while the byte-level machinery is agnostic to typed values.
    // -----------------------------------------------------------------------

    /// Navigate the tree to the leaf that would contain `key_bytes`, then
    /// return the contiguous encoded value bytes (inline or extent).
    /// Returns `None` if the key is not present. Slice borrows from
    /// `self.mmap`.
    fn value_slice_for(&self, key_bytes: &[u8]) -> Option<&[u8]> {
        let leaf = self.find_leaf(self.root(), key_bytes);
        let off = page_offset(leaf);
        let count = rd_u16(&self.mmap[off + 2..off + 4]) as usize;
        let i = self.leaf_find_slot(off, count, key_bytes).ok()?;
        Some(self.value_slice_at(off, i))
    }

    // -- typed write path (direct-to-mmap) --

    /// Insert or overwrite `(key_bytes, value)`. The value is serialized
    /// **straight into the mmap** — no intermediate buffer. Only the rare
    /// leaf-split fallback materializes the value as bytes (so `leaf_split`
    /// can replay it alongside existing entries).
    ///
    /// Calls the caller-provided `v_size` (pre-measured via `serialized_size`)
    /// to decide inline-vs-extent storage before serializing. The tree is
    /// walked from the root with a path stack; if the leaf splits, the split
    /// cascades back up through [`cascade_split`].
    ///
    /// On overwrite of an existing key: if the old value was an extent, its
    /// pages are freed; if the new value is the same size and still inline,
    /// the write is a cheap in-place overwrite (no slot shuffle).
    fn insert_typed(&mut self, key: &[u8], value: &V, v_size: usize) -> io::Result<bool> {
        let mut path: Vec<(u64, usize)> = Vec::new();
        let mut page = self.root();
        loop {
            let off = page_offset(page);
            if self.mmap[off] == PAGE_LEAF {
                break;
            }
            path.push((page, off));
            page = self.internal_search(off, key);
        }
        let loff = page_offset(page);
        let existed = self
            .leaf_find_slot(loff, rd_u16(&self.mmap[loff + 2..loff + 4]) as usize, key)
            .is_ok();
        let split = self.leaf_insert_typed(loff, page, key, value, v_size)?;
        if !existed {
            self.inc_entries(1);
        }
        if let Some(si) = split {
            self.cascade_split(path, si)?;
        }
        Ok(existed)
    }

    /// Insert raw `(key_bytes, value_bytes)` — both pre-encoded. Used by
    /// [`compact`] to rebuild the tree without a bincode `Decode` →
    /// `Encode` roundtrip on every entry. The caller guarantees no
    /// duplicate keys (compact just cleared the tree and is replaying a
    /// snapshot in ascending order).
    fn insert_raw(&mut self, key: &[u8], value_bytes: &[u8]) -> io::Result<()> {
        let mut path: Vec<(u64, usize)> = Vec::new();
        let mut page = self.root();
        loop {
            let off = page_offset(page);
            if self.mmap[off] == PAGE_LEAF {
                break;
            }
            path.push((page, off));
            page = self.internal_search(off, key);
        }
        let loff = page_offset(page);
        let split = self.leaf_insert_raw(loff, page, key, value_bytes)?;
        self.inc_entries(1);
        if let Some(si) = split {
            self.cascade_split(path, si)?;
        }
        Ok(())
    }

    /// Insert raw `(key, value_bytes)` into the leaf at `off`. Mirrors
    /// [`leaf_insert_typed`]'s `Err(pos)` branch — fresh-insert only, no
    /// overwrite handling — since [`compact`] is the only caller and
    /// guarantees no duplicates.
    fn leaf_insert_raw(
        &mut self,
        off: usize,
        page: u64,
        key: &[u8],
        value_bytes: &[u8],
    ) -> io::Result<Option<PageSplit>> {
        let count = rd_u16(&self.mmap[off + 2..off + 4]) as usize;
        let data_off = rd_u32(&self.mmap[off + 4..off + 8]) as usize;
        let v_len = value_bytes.len();
        let ovfl = key.len() + v_len + 6 > MAX_INLINE;
        let leaf_es = if ovfl {
            6 + key.len() + 8
        } else {
            6 + key.len() + v_len
        };
        let needed = leaf_es + SLOT_SIZE;
        let free_start = LEAF_HEADER_SIZE + count * SLOT_SIZE;

        let pos = match self.leaf_find_slot(off, count, key) {
            Ok(_) => {
                debug_assert!(false, "compact rebuild encountered duplicate key");
                return Ok(None);
            }
            Err(p) => p,
        };

        if free_start + needed > data_off {
            return self.leaf_split(off, page, key, value_bytes);
        }

        let ss = off + LEAF_HEADER_SIZE;
        for j in (pos..count).rev() {
            let v = rd_u16(&self.mmap[ss + j * SLOT_SIZE..ss + j * SLOT_SIZE + 2]);
            wr_u16(
                &mut self.mmap[ss + (j + 1) * SLOT_SIZE..ss + (j + 1) * SLOT_SIZE + 2],
                v,
            );
        }
        let nd = data_off - leaf_es;
        self.write_leaf_entry_raw(off, nd, key, value_bytes, ovfl)?;
        wr_u16(
            &mut self.mmap[ss + pos * SLOT_SIZE..ss + pos * SLOT_SIZE + 2],
            nd as u16,
        );
        wr_u16(&mut self.mmap[off + 2..off + 4], (count + 1) as u16);
        wr_u32(&mut self.mmap[off + 4..off + 8], nd as u32);
        let payload = if ovfl { 8 } else { v_len };
        self.stats.leaf_entry_bytes += leaf_entry_bytes(key.len(), payload);
        Ok(None)
    }

    /// Write a leaf entry from raw bytes (key + value). Mirrors
    /// [`write_leaf_entry_typed`] but the value is already a `&[u8]`, so
    /// we copy it straight in (or, for extents, copy into a fresh extent
    /// and store the pointer).
    fn write_leaf_entry_raw(
        &mut self,
        off: usize,
        nd: usize,
        key: &[u8],
        value_bytes: &[u8],
        is_ovfl: bool,
    ) -> io::Result<()> {
        wr_u16(&mut self.mmap[off + nd..off + nd + 2], key.len() as u16);
        if is_ovfl {
            let extent_start = self.write_extent(value_bytes)?;
            wr_u32(
                &mut self.mmap[off + nd + 2..off + nd + 6],
                value_bytes.len() as u32 | OVFL_FLAG,
            );
            self.mmap[off + nd + 6..off + nd + 6 + key.len()].copy_from_slice(key);
            wr_u64(
                &mut self.mmap[off + nd + 6 + key.len()..off + nd + 6 + key.len() + 8],
                extent_start,
            );
        } else {
            wr_u32(
                &mut self.mmap[off + nd + 2..off + nd + 6],
                value_bytes.len() as u32,
            );
            self.mmap[off + nd + 6..off + nd + 6 + key.len()].copy_from_slice(key);
            self.mmap[off + nd + 6 + key.len()..off + nd + 6 + key.len() + value_bytes.len()]
                .copy_from_slice(value_bytes);
        }
        Ok(())
    }

    /// Remove the entry identified by serialized key bytes. Frees any
    /// extent pages backing the value and calls [`leaf_remove_entry`] to
    /// slide the leaf's slot array and data gap. Returns `true` if the
    /// key existed (and was removed).
    fn delete_bytes(&mut self, key: &[u8]) -> io::Result<bool> {
        let leaf = self.find_leaf(self.root(), key);
        let off = page_offset(leaf);
        let count = rd_u16(&self.mmap[off + 2..off + 4]) as usize;
        let data_off = rd_u32(&self.mmap[off + 4..off + 8]) as usize;
        match self.leaf_find_slot(off, count, key) {
            Ok(i) => {
                let sp = off + LEAF_HEADER_SIZE + i * SLOT_SIZE;
                let eo = off + rd_u16(&self.mmap[sp..sp + 2]) as usize;
                let kl = rd_u16(&self.mmap[eo..eo + 2]) as usize;
                let raw_vl = rd_u32(&self.mmap[eo + 2..eo + 6]);
                if raw_vl & OVFL_FLAG != 0 {
                    let extent_start = rd_u64(&self.mmap[eo + 6 + kl..eo + 6 + kl + 8]);
                    let real_len = raw_vl & !OVFL_FLAG;
                    self.free_extent(extent_start, real_len);
                }
                self.leaf_remove_entry(off, i, count, data_off);
                self.dec_entries();
                Ok(true)
            }
            Err(_) => Ok(false),
        }
    }

    // -- meta accessors — read/write fields of the meta page (page 0) --

    /// Page number of the root node.
    fn root(&self) -> u64 {
        rd_u64(&self.mmap[META_ROOT..META_ROOT + 8])
    }
    /// Update the root page pointer in the meta page.
    fn set_root(&mut self, p: u64) {
        wr_u64(&mut self.mmap[META_ROOT..META_ROOT + 8], p);
    }
    /// Page number of the rightmost leaf (for O(1) `last` / reverse iteration seed).
    fn rightmost_leaf(&self) -> u64 {
        rd_u64(&self.mmap[META_RIGHTMOST_LEAF..META_RIGHTMOST_LEAF + 8])
    }
    fn set_rightmost_leaf(&mut self, p: u64) {
        wr_u64(
            &mut self.mmap[META_RIGHTMOST_LEAF..META_RIGHTMOST_LEAF + 8],
            p,
        );
    }
    /// Increment the global entry counter by `d` (wrapping).
    fn inc_entries(&mut self, d: u64) {
        let c = rd_u64(&self.mmap[META_ENTRIES..META_ENTRIES + 8]);
        let next = c.wrapping_add(d);
        wr_u64(&mut self.mmap[META_ENTRIES..META_ENTRIES + 8], next);
        self.stats.entries = next as usize;
    }
    /// Decrement the global entry counter by 1 (wrapping). Caller must
    /// ensure the counter is ≥ 1 before calling.
    fn dec_entries(&mut self) {
        let c = rd_u64(&self.mmap[META_ENTRIES..META_ENTRIES + 8]);
        let next = c.wrapping_sub(1);
        wr_u64(&mut self.mmap[META_ENTRIES..META_ENTRIES + 8], next);
        self.stats.entries = next as usize;
    }

    // -- traversal — navigate the tree using serialized key bytes --

    /// Walk from the root down the leftmost-child path to the first leaf
    /// page (the one with the smallest key range). Used to seed iteration.
    fn first_leaf(&self) -> u64 {
        let mut p = self.root();
        loop {
            let off = page_offset(p);
            if self.mmap[off] == PAGE_LEAF {
                return p;
            }
            p = rd_u64(&self.mmap[off + 8..off + 16]);
        }
    }

    /// Walk from `root` down to the leaf page that would contain `key`,
    /// routing through internal pages via binary search on separator keys.
    fn find_leaf(&self, root: u64, key: &[u8]) -> u64 {
        let mut p = root;
        loop {
            let off = page_offset(p);
            if self.mmap[off] == PAGE_LEAF {
                return p;
            }
            p = self.internal_search(off, key);
        }
    }

    /// Binary search through the sorted separator keys of an internal page
    /// at byte offset `off`. Returns the child page number that should be
    /// descended to for the given lookup `key`. If `key` sorts before every
    /// separator, returns the `leftmost_child` pointer.
    fn internal_search(&self, off: usize, key: &[u8]) -> u64 {
        let count = rd_u16(&self.mmap[off + 2..off + 4]) as usize;
        let leftmost = rd_u64(&self.mmap[off + 8..off + 16]);
        let mut lo = 0usize;
        let mut hi = count;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let sp = off + HEADER_SIZE + mid * SLOT_SIZE;
            let eo = off + rd_u16(&self.mmap[sp..sp + 2]) as usize;
            let kl = rd_u16(&self.mmap[eo..eo + 2]) as usize;
            if key < &self.mmap[eo + 10..eo + 10 + kl] {
                hi = mid;
            } else {
                lo = mid + 1;
            }
        }
        if lo == 0 {
            leftmost
        } else {
            let sp = off + HEADER_SIZE + (lo - 1) * SLOT_SIZE;
            let eo = off + rd_u16(&self.mmap[sp..sp + 2]) as usize;
            rd_u64(&self.mmap[eo + 2..eo + 10])
        }
    }

    /// Binary search a leaf page's sorted slot array for `key`.
    /// Returns `Ok(slot_index)` on exact match, or `Err(insertion_point)`
    /// where `insertion_point` is the index at which the key should be
    /// inserted to maintain sorted order.
    fn leaf_find_slot(&self, off: usize, count: usize, key: &[u8]) -> Result<usize, usize> {
        let mut lo = 0usize;
        let mut hi = count;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let sp = off + LEAF_HEADER_SIZE + mid * SLOT_SIZE;
            let eo = off + rd_u16(&self.mmap[sp..sp + 2]) as usize;
            let kl = rd_u16(&self.mmap[eo..eo + 2]) as usize;
            match self.mmap[eo + 6..eo + 6 + kl].cmp(key) {
                std::cmp::Ordering::Equal => return Ok(mid),
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
            }
        }
        Err(lo)
    }

    // -- leaf insert (typed: value written directly into mmap) --

    /// Insert `(key, value)` into the leaf page at byte offset `off` (page
    /// number `page`). The value is serialized **straight into the mmap**.
    ///
    /// Returns `Some(PageSplit)` if the leaf must split because the entry
    /// doesn't fit in the page's free space; `None` if insertion succeeded
    /// in-place (either new slot or same-size inline overwrite).
    ///
    /// On overwrite of an existing key: frees any old extent pages, then
    /// attempts a fast-path same-size inline overwrite before falling back
    /// to remove-and-reinsert.
    fn leaf_insert_typed(
        &mut self,
        off: usize,
        page: u64,
        key: &[u8],
        value: &V,
        v_size: usize,
    ) -> io::Result<Option<PageSplit>> {
        let count = rd_u16(&self.mmap[off + 2..off + 4]) as usize;
        let data_off = rd_u32(&self.mmap[off + 4..off + 8]) as usize;

        let ovfl = key.len() + v_size + 6 > MAX_INLINE;
        let leaf_es = if ovfl {
            6 + key.len() + 8
        } else {
            6 + key.len() + v_size
        };
        let needed = leaf_es + SLOT_SIZE;
        let free_start = LEAF_HEADER_SIZE + count * SLOT_SIZE;

        match self.leaf_find_slot(off, count, key) {
            Ok(i) => {
                let sp = off + LEAF_HEADER_SIZE + i * SLOT_SIZE;
                let eo = off + rd_u16(&self.mmap[sp..sp + 2]) as usize;
                let kl = rd_u16(&self.mmap[eo..eo + 2]) as usize;
                let raw_vl = rd_u32(&self.mmap[eo + 2..eo + 6]);
                // Free old extent if present — about to overwrite the slot.
                if raw_vl & OVFL_FLAG != 0 {
                    let old_start = rd_u64(&self.mmap[eo + 6 + kl..eo + 6 + kl + 8]);
                    let old_len = raw_vl & !OVFL_FLAG;
                    self.free_extent(old_start, old_len);
                }
                // Fast path: same-size inline overwrite → serialize new
                // value straight into the existing slot, no slot shuffle.
                if raw_vl & OVFL_FLAG == 0 && !ovfl && raw_vl as usize == v_size {
                    let v_off = eo + 6 + kl;
                    let written = serialize_into(value, &mut self.mmap[v_off..v_off + v_size])?;
                    debug_assert_eq!(written, v_size);
                    return Ok(None);
                }
                // Size changed (or inline/extent flipped): drop the old
                // entry then re-insert. Checking existence before page space
                // prevents full-leaf overwrites from duplicating the key.
                self.leaf_remove_entry(off, i, count, data_off);
                return self.leaf_insert_typed(off, page, key, value, v_size);
            }
            Err(pos) => {
                if free_start + needed > data_off {
                    // Split path: materialize the value as bytes so `leaf_split`
                    // can replay it alongside the existing serialized entries.
                    // This is the only place the typed `put` path allocates a
                    // scratch buffer for the value, and it only happens on split.
                    let mut v_bytes = vec![0u8; v_size];
                    let written = serialize_into(value, &mut v_bytes)?;
                    debug_assert_eq!(written, v_size);
                    return self.leaf_split(off, page, key, &v_bytes);
                }

                let ss = off + LEAF_HEADER_SIZE;
                for j in (pos..count).rev() {
                    let v = rd_u16(&self.mmap[ss + j * SLOT_SIZE..ss + j * SLOT_SIZE + 2]);
                    wr_u16(
                        &mut self.mmap[ss + (j + 1) * SLOT_SIZE..ss + (j + 1) * SLOT_SIZE + 2],
                        v,
                    );
                }
                let nd = data_off - leaf_es;
                self.write_leaf_entry_typed(off, nd, key, value, v_size, ovfl)?;
                wr_u16(
                    &mut self.mmap[ss + pos * SLOT_SIZE..ss + pos * SLOT_SIZE + 2],
                    nd as u16,
                );
                wr_u16(&mut self.mmap[off + 2..off + 4], (count + 1) as u16);
                wr_u32(&mut self.mmap[off + 4..off + 8], nd as u32);
                let payload = if ovfl { 8 } else { v_size };
                self.stats.leaf_entry_bytes += leaf_entry_bytes(key.len(), payload);
                Ok(None)
            }
        }
    }

    /// Write a complete leaf entry (header + key + value) at byte `nd`
    /// within the page. For inline values, the value is serialized directly
    /// into the mmap. For extent values, a contiguous extent is allocated at
    /// the file tail and the value is serialized there; the leaf slot stores
    /// `(key, extent_start)` instead of the value bytes.
    fn write_leaf_entry_typed(
        &mut self,
        off: usize,
        nd: usize,
        key: &[u8],
        value: &V,
        v_size: usize,
        is_ovfl: bool,
    ) -> io::Result<()> {
        wr_u16(&mut self.mmap[off + nd..off + nd + 2], key.len() as u16);
        if is_ovfl {
            let n_pages = ((v_size + PAGE_SIZE - 1) / PAGE_SIZE) as u64;
            let extent_start = self.alloc_extent(n_pages)?;
            let xo = page_offset(extent_start);
            // alloc_extent may have remapped self.mmap; access through self.mmap.
            let written = serialize_into(value, &mut self.mmap[xo..xo + v_size])?;
            debug_assert_eq!(written, v_size);
            wr_u32(
                &mut self.mmap[off + nd + 2..off + nd + 6],
                v_size as u32 | OVFL_FLAG,
            );
            self.mmap[off + nd + 6..off + nd + 6 + key.len()].copy_from_slice(key);
            wr_u64(
                &mut self.mmap[off + nd + 6 + key.len()..off + nd + 6 + key.len() + 8],
                extent_start,
            );
        } else {
            wr_u32(&mut self.mmap[off + nd + 2..off + nd + 6], v_size as u32);
            self.mmap[off + nd + 6..off + nd + 6 + key.len()].copy_from_slice(key);
            let v_off = off + nd + 6 + key.len();
            let written = serialize_into(value, &mut self.mmap[v_off..v_off + v_size])?;
            debug_assert_eq!(written, v_size);
        }
        Ok(())
    }

    /// Write a leaf entry at byte `nd` from a [`SplitVal`]. For
    /// `SplitVal::Extent`, the existing extent is reused as-is (no new
    /// allocation, no data copy) — the leaf entry just records the existing
    /// extent's starting page. For `SplitVal::Inline`, the bytes are copied
    /// into the slot (allocating a new extent if the inline bytes exceed
    /// `MAX_INLINE`).
    fn write_leaf_entry(
        &mut self,
        off: usize,
        nd: usize,
        key: &[u8],
        val: &SplitVal,
        is_ovfl: bool,
    ) -> io::Result<()> {
        wr_u16(&mut self.mmap[off + nd..off + nd + 2], key.len() as u16);
        match val {
            SplitVal::Inline(v) if is_ovfl => {
                let extent_start = self.write_extent(v)?;
                wr_u32(
                    &mut self.mmap[off + nd + 2..off + nd + 6],
                    v.len() as u32 | OVFL_FLAG,
                );
                self.mmap[off + nd + 6..off + nd + 6 + key.len()].copy_from_slice(key);
                wr_u64(
                    &mut self.mmap[off + nd + 6 + key.len()..off + nd + 6 + key.len() + 8],
                    extent_start,
                );
            }
            SplitVal::Inline(v) => {
                wr_u32(&mut self.mmap[off + nd + 2..off + nd + 6], v.len() as u32);
                self.mmap[off + nd + 6..off + nd + 6 + key.len()].copy_from_slice(key);
                self.mmap[off + nd + 6 + key.len()..off + nd + 6 + key.len() + v.len()]
                    .copy_from_slice(v);
            }
            SplitVal::Extent { start, value_len } => {
                wr_u32(
                    &mut self.mmap[off + nd + 2..off + nd + 6],
                    value_len | OVFL_FLAG,
                );
                self.mmap[off + nd + 6..off + nd + 6 + key.len()].copy_from_slice(key);
                wr_u64(
                    &mut self.mmap[off + nd + 6 + key.len()..off + nd + 6 + key.len() + 8],
                    *start,
                );
            }
        }
        Ok(())
    }

    // -- leaf split --
    //
    // Split logic: collect all entries (old + new) into a Vec, compute the
    // midpoint such that each half fits in a 4096-byte page, truncate the
    // separator key, allocate a new right sibling, and redistribute entries.
    // Extents are carried through `SplitVal::Extent` — their page pointers
    // move to the new leaf without copying the value data.

    /// Split a full leaf page. Gathers all existing entries plus the new
    /// `(key, value)` pair, partitions them at a midpoint, allocates a new
    /// right-sibling leaf, and links the two via `next_leaf`.
    ///
    /// Returns a [`PageSplit`] with the left/right page numbers and a
    /// suffix-truncated separator key; the caller is responsible for
    /// inserting or cascading that separator into the parent.
    fn leaf_split(
        &mut self,
        off: usize,
        page: u64,
        key: &[u8],
        value: &[u8],
    ) -> io::Result<Option<PageSplit>> {
        let count = rd_u16(&self.mmap[off + 2..off + 4]) as usize;

        // Collect existing entries without materializing extent data.
        let mut entries: Vec<(Vec<u8>, SplitVal)> = Vec::with_capacity(count + 1);
        for i in 0..count {
            let sp = off + LEAF_HEADER_SIZE + i * SLOT_SIZE;
            let eo = off + rd_u16(&self.mmap[sp..sp + 2]) as usize;
            let kl = rd_u16(&self.mmap[eo..eo + 2]) as usize;
            let raw_vl = rd_u32(&self.mmap[eo + 2..eo + 6]);
            let k = self.mmap[eo + 6..eo + 6 + kl].to_vec();
            if raw_vl & OVFL_FLAG != 0 {
                let extent_start = rd_u64(&self.mmap[eo + 6 + kl..eo + 6 + kl + 8]);
                entries.push((
                    k,
                    SplitVal::Extent {
                        start: extent_start,
                        value_len: raw_vl & !OVFL_FLAG,
                    },
                ));
            } else {
                let vl = raw_vl as usize;
                entries.push((
                    k,
                    SplitVal::Inline(self.mmap[eo + 6 + kl..eo + 6 + kl + vl].to_vec()),
                ));
            }
        }

        let ip = entries
            .binary_search_by(|(k, _)| k.as_slice().cmp(key))
            .unwrap_or_else(|i| i);
        entries.insert(ip, (key.to_vec(), SplitVal::Inline(value.to_vec())));

        let mut used = 0usize;
        let mid = entries
            .iter()
            .position(|(k, v)| {
                let entry_inline = match v {
                    SplitVal::Inline(val) => 6 + k.len() + val.len() <= MAX_INLINE,
                    SplitVal::Extent { .. } => false,
                };
                let payload = match v {
                    SplitVal::Inline(val) if entry_inline => val.len(),
                    _ => 8,
                };
                let sz = 6 + k.len() + payload + SLOT_SIZE;
                if used + sz > PAGE_SIZE - LEAF_HEADER_SIZE {
                    return true;
                }
                used += sz;
                false
            })
            .unwrap_or(entries.len());

        // Net delta to leaf_entry_bytes across a split is exactly the new
        // entry's footprint (existing entries are redistributed, not
        // added). Compute it once here from the inserted (key, value)
        // before we redistribute below.
        let new_entry_payload = match &entries[ip].1 {
            SplitVal::Inline(v) if 6 + key.len() + v.len() <= MAX_INLINE => v.len(),
            _ => 8,
        };
        let new_entry_bytes = leaf_entry_bytes(key.len(), new_entry_payload);

        if mid >= entries.len() {
            wr_u16(&mut self.mmap[off + 2..off + 4], 0);
            wr_u32(&mut self.mmap[off + 4..off + 8], PAGE_SIZE as u32);
            for (k, v) in entries {
                self.leaf_insert_split_entry(off, &k, v)?;
            }
            self.stats.leaf_entry_bytes += new_entry_bytes;
            return Ok(None);
        }
        let mid = mid.max(1);

        let sep = truncated_separator(&entries[mid - 1].0, &entries[mid].0);

        wr_u16(&mut self.mmap[off + 2..off + 4], 0);
        wr_u32(&mut self.mmap[off + 4..off + 8], PAGE_SIZE as u32);
        let right = self.alloc_page()?;
        self.init_leaf(right, false);

        let entries_right: Vec<(Vec<u8>, SplitVal)> = entries.split_off(mid);
        for (k, v) in entries {
            self.leaf_insert_split_entry(off, &k, v)?;
        }
        let ro = page_offset(right);
        for (k, v) in entries_right {
            self.leaf_insert_split_entry(ro, &k, v)?;
        }
        self.stats.leaf_entry_bytes += new_entry_bytes;

        // Link prev/next pointers for the new sibling chain.
        let old_next = rd_u64(&self.mmap[off + 8..off + 16]);
        wr_u64(&mut self.mmap[ro + 8..ro + 16], old_next); // right.next = old.next
        wr_u64(&mut self.mmap[ro + 16..ro + 24], page); // right.prev = left
        wr_u64(&mut self.mmap[off + 8..off + 16], right); // left.next = right
        if old_next != 0 {
            // Fix up the old successor's prev pointer.
            let ono = page_offset(old_next);
            wr_u64(&mut self.mmap[ono + 16..ono + 24], right);
        } else {
            // Old page was the rightmost — new right page takes over.
            self.set_rightmost_leaf(right);
        }

        Ok(Some(PageSplit {
            left_page: page,
            right_page: right,
            separator_key: sep,
        }))
    }

    /// Append a single entry `(key, val)` to a leaf page that is known to
    /// have room. Used during split redistribution — no collision check,
    /// no space check. The caller has already partitioned entries so each
    /// half fits.
    fn leaf_insert_split_entry(&mut self, off: usize, key: &[u8], val: SplitVal) -> io::Result<()> {
        let count = rd_u16(&self.mmap[off + 2..off + 4]) as usize;
        let data_off = rd_u32(&self.mmap[off + 4..off + 8]) as usize;

        let leaf_es = match &val {
            SplitVal::Inline(v) if 6 + key.len() + v.len() <= MAX_INLINE => 6 + key.len() + v.len(),
            _ => 6 + key.len() + 8,
        };
        let is_ovfl = matches!(&val, SplitVal::Extent { .. })
            || matches!(&val, SplitVal::Inline(v) if 6 + key.len() + v.len() > MAX_INLINE);

        let nd = data_off - leaf_es;
        self.write_leaf_entry(off, nd, key, &val, is_ovfl)?;

        let ss = off + LEAF_HEADER_SIZE;
        wr_u16(
            &mut self.mmap[ss + count * SLOT_SIZE..ss + count * SLOT_SIZE + 2],
            nd as u16,
        );
        wr_u16(&mut self.mmap[off + 2..off + 4], (count + 1) as u16);
        wr_u32(&mut self.mmap[off + 4..off + 8], nd as u32);
        Ok(())
    }

    // -- leaf remove --

    /// Remove the entry at slot `pos` from a leaf page. Slides all higher
    /// slots down by one and slides the data gap down over the removed entry
    /// bytes (via `copy_within`). Updates the page's count and data-offset
    /// header fields. The caller is responsible for freeing any extent pages
    /// before calling this.
    fn leaf_remove_entry(&mut self, off: usize, pos: usize, count: usize, data_off: usize) {
        let sp = off + LEAF_HEADER_SIZE + pos * SLOT_SIZE;
        let eo_rel = rd_u16(&self.mmap[sp..sp + 2]) as usize;
        let eo = off + eo_rel;
        let kl = rd_u16(&self.mmap[eo..eo + 2]) as usize;
        let raw_vl = rd_u32(&self.mmap[eo + 2..eo + 6]) as usize;
        let payload = if raw_vl & OVFL_FLAG as usize != 0 {
            8
        } else {
            raw_vl
        };
        let es = 6 + kl + payload;
        // Maintain the incremental leaf_entry_bytes counter used by
        // `fragmentation_ratio`. Same formula as `leaf_entry_bytes`.
        let removed = leaf_entry_bytes(kl, payload);
        self.stats.leaf_entry_bytes = self.stats.leaf_entry_bytes.saturating_sub(removed);
        let ss = off + LEAF_HEADER_SIZE;
        for j in pos + 1..count {
            let v = rd_u16(&self.mmap[ss + j * SLOT_SIZE..ss + j * SLOT_SIZE + 2]);
            wr_u16(
                &mut self.mmap[ss + (j - 1) * SLOT_SIZE..ss + (j - 1) * SLOT_SIZE + 2],
                v,
            );
        }
        let dlen = eo_rel - data_off;
        self.mmap
            .copy_within(off + data_off..off + data_off + dlen, off + data_off + es);
        for j in 0..count - 1 {
            let mut e = rd_u16(&self.mmap[ss + j * SLOT_SIZE..ss + j * SLOT_SIZE + 2]) as usize;
            if e <= eo_rel {
                e += es;
            }
            wr_u16(
                &mut self.mmap[ss + j * SLOT_SIZE..ss + j * SLOT_SIZE + 2],
                e as u16,
            );
        }
        wr_u16(&mut self.mmap[off + 2..off + 4], (count - 1) as u16);
        wr_u32(&mut self.mmap[off + 4..off + 8], (data_off + es) as u32);
    }

    // -- cascade --

    /// Propagate a page split upward through the parent chain. `path` is a
    /// (page_number, page_offset) stack collected during the original
    /// leaf-to-root descent (leaf not included). Each split result is
    /// inserted into the parent page via [`internal_insert`]; if the parent
    /// itself splits, the cascade continues upward.
    ///
    /// If the root splits, a new root page is allocated with the split's
    /// left and right children, and the old root's `FLAG_ROOT` bit is cleared.
    fn cascade_split(&mut self, path: Vec<(u64, usize)>, split: PageSplit) -> io::Result<()> {
        let (mut sep, mut left, mut right) =
            (split.separator_key, split.left_page, split.right_page);
        for (_pg, poff) in path.into_iter().rev() {
            match self.internal_insert(poff, &sep, right)? {
                Some(ps) => {
                    sep = ps.separator_key;
                    left = ps.left_page;
                    right = ps.right_page;
                }
                None => return Ok(()),
            }
        }
        let nr = self.alloc_page()?;
        self.init_internal(nr);
        let ro = page_offset(nr);
        wr_u64(&mut self.mmap[ro + 8..ro + 16], left);
        self.internal_insert(ro, &sep, right)?;
        self.mmap[ro + 1] |= FLAG_ROOT;
        self.mmap[page_offset(left) + 1] &= !FLAG_ROOT;
        self.set_root(nr);
        Ok(())
    }

    /// Insert a `(separator_key, child_page)` pair into an internal page
    /// at byte offset `off`. Binary-searches for the insertion point, shifts
    /// higher slots, and writes the entry into the gap between slot array
    /// and data. Returns `Some(PageSplit)` if the page must split.
    fn internal_insert(
        &mut self,
        off: usize,
        key: &[u8],
        child: u64,
    ) -> io::Result<Option<PageSplit>> {
        let count = rd_u16(&self.mmap[off + 2..off + 4]) as usize;
        let data_off = rd_u32(&self.mmap[off + 4..off + 8]) as usize;
        let es = 10 + key.len();
        let free_start = HEADER_SIZE + count * SLOT_SIZE;
        if free_start + es + SLOT_SIZE > data_off {
            return self.internal_split(off, key, child);
        }
        let mut lo = 0usize;
        let mut hi = count;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let sp = off + HEADER_SIZE + mid * SLOT_SIZE;
            let eo = off + rd_u16(&self.mmap[sp..sp + 2]) as usize;
            let kl = rd_u16(&self.mmap[eo..eo + 2]) as usize;
            if key < &self.mmap[eo + 10..eo + 10 + kl] {
                hi = mid;
            } else {
                lo = mid + 1;
            }
        }
        let pos = lo;
        let ss = off + HEADER_SIZE;
        for j in (pos..count).rev() {
            let v = rd_u16(&self.mmap[ss + j * SLOT_SIZE..ss + j * SLOT_SIZE + 2]);
            wr_u16(
                &mut self.mmap[ss + (j + 1) * SLOT_SIZE..ss + (j + 1) * SLOT_SIZE + 2],
                v,
            );
        }
        let nd = data_off - es;
        wr_u16(&mut self.mmap[off + nd..off + nd + 2], key.len() as u16);
        wr_u64(&mut self.mmap[off + nd + 2..off + nd + 10], child);
        self.mmap[off + nd + 10..off + nd + 10 + key.len()].copy_from_slice(key);
        wr_u16(
            &mut self.mmap[ss + pos * SLOT_SIZE..ss + pos * SLOT_SIZE + 2],
            nd as u16,
        );
        wr_u16(&mut self.mmap[off + 2..off + 4], (count + 1) as u16);
        wr_u32(&mut self.mmap[off + 4..off + 8], nd as u32);
        Ok(None)
    }

    /// Split a full internal page. Gathers all existing `(key, child)`
    /// pairs plus the new one, partitions at the midpoint, allocates a
    /// right sibling, and redistributes entries. The middle key is promoted
    /// to the parent as a separator (suffix-truncated). Returns a
    /// [`PageSplit`] for the caller to cascade upward.
    fn internal_split(
        &mut self,
        off: usize,
        key: &[u8],
        child: u64,
    ) -> io::Result<Option<PageSplit>> {
        let count = rd_u16(&self.mmap[off + 2..off + 4]) as usize;
        let mut keys: Vec<(Vec<u8>, u64)> = Vec::with_capacity(count + 1);
        for i in 0..count {
            let sp = off + HEADER_SIZE + i * SLOT_SIZE;
            let eo = off + rd_u16(&self.mmap[sp..sp + 2]) as usize;
            let kl = rd_u16(&self.mmap[eo..eo + 2]) as usize;
            keys.push((
                self.mmap[eo + 10..eo + 10 + kl].to_vec(),
                rd_u64(&self.mmap[eo + 2..eo + 10]),
            ));
        }
        let ip = keys
            .binary_search_by(|(k, _)| k.as_slice().cmp(key))
            .unwrap_or_else(|i| i);
        keys.insert(ip, (key.to_vec(), child));
        let mut used = 0usize;
        let mid = keys
            .iter()
            .position(|(k, _)| {
                let sz = 10 + k.len() + SLOT_SIZE;
                if used + sz > PAGE_SIZE - HEADER_SIZE {
                    return true;
                }
                used += sz;
                false
            })
            .unwrap_or(keys.len());
        if mid >= keys.len() {
            wr_u16(&mut self.mmap[off + 2..off + 4], 0);
            wr_u32(&mut self.mmap[off + 4..off + 8], PAGE_SIZE as u32);
            for (k, c) in &keys {
                self.internal_insert(off, k, *c)?;
            }
            return Ok(None);
        }
        let mid = mid.max(1);
        let sep = truncated_separator(&keys[mid - 1].0, &keys[mid].0);
        let right = self.alloc_page()?;
        self.init_internal(right);
        let lm = rd_u64(&self.mmap[off + 8..off + 16]);
        wr_u16(&mut self.mmap[off + 2..off + 4], 0);
        wr_u32(&mut self.mmap[off + 4..off + 8], PAGE_SIZE as u32);
        wr_u64(&mut self.mmap[off + 8..off + 16], lm);
        for (k, c) in &keys[..mid] {
            self.internal_insert(off, k, *c)?;
        }
        let ro = page_offset(right);
        wr_u64(&mut self.mmap[ro + 8..ro + 16], keys[mid].1);
        for (k, c) in &keys[mid + 1..] {
            self.internal_insert(ro, k, *c)?;
        }
        Ok(Some(PageSplit {
            left_page: (off / PAGE_SIZE) as u64,
            right_page: right,
            separator_key: sep,
        }))
    }

    // -- extents (contiguous overflow) --

    /// Allocate a fresh contiguous extent and copy `value_bytes` into it.
    /// Returns the first page index of the extent. Always grows the file —
    /// no extent freelist reuse in v1.
    fn write_extent(&mut self, value_bytes: &[u8]) -> io::Result<u64> {
        let n_pages = ((value_bytes.len() + PAGE_SIZE - 1) / PAGE_SIZE) as u64;
        let start = self.alloc_extent(n_pages)?;
        let off = page_offset(start);
        self.mmap[off..off + value_bytes.len()].copy_from_slice(value_bytes);
        Ok(start)
    }

    /// Free all pages of an extent by returning each page to the
    /// single-page freelist. These pages can be reused later for tree
    /// (internal/leaf) pages via [`alloc_page`], but not as part of a
    /// new extent (extents always grow the file).
    fn free_extent(&mut self, start: u64, value_len: u32) {
        let n_pages = ((value_len as usize + PAGE_SIZE - 1) / PAGE_SIZE) as u64;
        for i in 0..n_pages {
            self.free_page(start + i);
        }
    }

    /// Allocate `n_pages` contiguous pages at the file's tail. Always grows
    /// the file — v1 does not reuse freed extent pages for new extents.
    /// Caller receives the first page index of the allocated run.
    ///
    /// Note: `self.mmap` may be invalidated after `grow` re-mmaps the file;
    /// callers must re-derive mmap offsets after calling this.
    fn alloc_extent(&mut self, n_pages: u64) -> io::Result<u64> {
        let start = self.pages();
        self.ensure_capacity((start + n_pages) * PAGE_SIZE as u64)?;
        self.set_pages(start + n_pages);
        Ok(start)
    }

    // -- page allocation --

    /// Head of the single-page freelist. `0` means the list is empty.
    fn free_head(&self) -> u64 {
        rd_u64(&self.mmap[META_FREE_HEAD..META_FREE_HEAD + 8])
    }
    fn set_free_head(&mut self, p: u64) {
        wr_u64(&mut self.mmap[META_FREE_HEAD..META_FREE_HEAD + 8], p);
    }
    /// Total number of pages in the file (including meta page and extents).
    fn pages(&self) -> u64 {
        rd_u64(&self.mmap[META_PAGES..META_PAGES + 8])
    }
    fn set_pages(&mut self, n: u64) {
        wr_u64(&mut self.mmap[META_PAGES..META_PAGES + 8], n);
        self.stats.pages = n;
    }

    fn maybe_compact(&mut self) -> io::Result<()> {
        let threshold = self.config.compaction_ratio;
        if threshold >= 1.0 {
            return Ok(());
        }
        if self.fragmentation_ratio() >= threshold {
            self.compact()?;
        }
        Ok(())
    }

    fn free_list_pages(&self) -> u64 {
        let mut n = 0u64;
        let mut p = self.free_head();
        let max_pages = self.pages();
        while p != 0 && n < max_pages {
            n += 1;
            p = rd_u64(&self.mmap[page_offset(p)..page_offset(p) + 8]);
        }
        n
    }

    /// Walk the leaf chain once to seed `stats.leaf_pages` and
    /// `stats.leaf_entry_bytes` — used by `open` only. After the seed,
    /// these counters are maintained incrementally so neither
    /// `fragmentation_ratio` nor the auto-compaction trigger need to
    /// re-walk the tree.
    fn scan_leaf_stats(&self) -> (u64, u64) {
        let mut leaf_pages = 0u64;
        let mut total_bytes = 0u64;
        let mut page = self.first_leaf();
        while page != 0 {
            leaf_pages += 1;
            let off = page_offset(page);
            let count = rd_u16(&self.mmap[off + 2..off + 4]) as usize;
            for i in 0..count {
                let sp = off + LEAF_HEADER_SIZE + i * SLOT_SIZE;
                let eo = off + rd_u16(&self.mmap[sp..sp + 2]) as usize;
                let kl = rd_u16(&self.mmap[eo..eo + 2]) as usize;
                let raw_vl = rd_u32(&self.mmap[eo + 2..eo + 6]);
                let payload = if raw_vl & OVFL_FLAG != 0 {
                    8
                } else {
                    raw_vl as usize
                };
                total_bytes += leaf_entry_bytes(kl, payload);
            }
            page = rd_u64(&self.mmap[off + 8..off + 16]);
        }
        (leaf_pages.max(1), total_bytes)
    }

    /// Allocate a single free page. Pulls from the freelist (if non-empty)
    /// or extends the page-count high-water mark, growing the file only when
    /// the mark crosses the current mmap length. The returned page is **not**
    /// zeroed — the caller is responsible for initializing it (e.g. via
    /// `init_leaf` / `init_internal`).
    fn alloc_page(&mut self) -> io::Result<u64> {
        let free = self.free_head();
        if free != 0 {
            let next = rd_u64(&self.mmap[page_offset(free)..page_offset(free) + 8]);
            self.set_free_head(next);
            self.stats.free_pages = self.stats.free_pages.saturating_sub(1);
            return Ok(free);
        }
        let p = self.pages();
        self.ensure_capacity((p + 1) * PAGE_SIZE as u64)?;
        self.set_pages(p + 1);
        Ok(p)
    }

    /// Return a single page to the freelist. The page is prepended to the
    /// list by writing the old freelist head into the page's first 8 bytes
    /// and updating the meta page's `free_head`.
    fn free_page(&mut self, page: u64) {
        let head = self.free_head();
        wr_u64(
            &mut self.mmap[page_offset(page)..page_offset(page) + 8],
            head,
        );
        self.set_free_head(page);
        self.stats.free_pages += 1;
    }

    fn refresh_stats_from_meta(&mut self) {
        let (leaf_pages, leaf_entry_bytes) = self.scan_leaf_stats();
        self.stats = BPlusTreeStats {
            entries: rd_u64(&self.mmap[META_ENTRIES..META_ENTRIES + 8]) as usize,
            pages: self.pages(),
            free_pages: self.free_list_pages(),
            leaf_pages,
            leaf_entry_bytes,
        };
    }

    /// Ensure the backing file is at least `target_bytes` long, growing
    /// exponentially (`max(mmap_len * 2, target_bytes)`) so the cost is
    /// amortized O(1) per write. Does **not** flush — the OS page cache
    /// is unified, so the new mapping sees prior writes immediately, and
    /// durability is handled by explicit `flush()` and the `Drop` impl.
    /// Invalidates existing mmap references — callers must re-derive
    /// offsets from `self.mmap` after this returns.
    fn ensure_capacity(&mut self, target_bytes: u64) -> io::Result<()> {
        let current = self.mmap.len() as u64;
        if target_bytes <= current {
            return Ok(());
        }
        let new_size = (current * 2).max(target_bytes);
        self.file.set_len(new_size)?;
        self.mmap = unsafe { MmapMut::map_mut(&self.file)? };
        Ok(())
    }

    /// Initialize a page as a leaf. Zeros out the header and slot array.
    /// If `root` is true, the `FLAG_ROOT` bit is set.
    ///
    /// Increments `stats.leaf_pages` because the caller always passes a
    /// freshly allocated page (this is the only path that promotes a page
    /// into a leaf — `create`/`clear` initialize the root leaf inline and
    /// set the counter directly).
    fn init_leaf(&mut self, page: u64, root: bool) {
        let off = page_offset(page);
        self.mmap[off] = PAGE_LEAF;
        self.mmap[off + 1] = if root { FLAG_ROOT } else { 0 };
        wr_u16(&mut self.mmap[off + 2..off + 4], 0);
        wr_u32(&mut self.mmap[off + 4..off + 8], PAGE_SIZE as u32);
        wr_u64(&mut self.mmap[off + 8..off + 16], 0); // next_leaf = 0
        wr_u64(&mut self.mmap[off + 16..off + 24], 0); // prev_leaf = 0
        self.stats.leaf_pages += 1;
    }

    /// Initialize a page as an internal node. Zeros out the header,
    /// slot array, and `leftmost_child` pointer.
    fn init_internal(&mut self, page: u64) {
        let off = page_offset(page);
        self.mmap[off] = PAGE_INTERNAL;
        self.mmap[off + 1] = 0;
        wr_u16(&mut self.mmap[off + 2..off + 4], 0);
        wr_u32(&mut self.mmap[off + 4..off + 8], PAGE_SIZE as u32);
        wr_u64(&mut self.mmap[off + 8..off + 16], 0);
    }
}

// ---------------------------------------------------------------------------
// Iterators
// ---------------------------------------------------------------------------

/// Full-scan iterator over all entries in the tree. Walks leaf pages
/// left-to-right via `next_leaf` pointers, visiting each slot in order.
/// Yields owned `(K, V)` — both decoded from the mmap'd bytes.
pub struct BTreeIter<'a, K, V> {
    tree: &'a BPlusTree<K, V>,
    page: u64,
    slot: usize,
}

impl<'a, K, V> Iterator for BTreeIter<'a, K, V>
where
    K: Decode<()>,
    V: Decode<()>,
{
    type Item = (K, V);
    fn next(&mut self) -> Option<Self::Item> {
        while self.page != 0 {
            let off = page_offset(self.page);
            let count = rd_u16(&self.tree.mmap[off + 2..off + 4]) as usize;
            if self.slot < count {
                let key_slice = self.tree.key_slice_at(off, self.slot);
                let value_slice = self.tree.value_slice_at(off, self.slot);
                self.slot += 1;
                let k: K = deserialize_from(key_slice).expect("valid encoded key");
                let v: V = deserialize_from(value_slice).expect("valid encoded value");
                return Some((k, v));
            }
            self.page = rd_u64(&self.tree.mmap[off + 8..off + 16]); // next_leaf
            self.slot = 0;
        }
        None
    }
}

/// Reverse iterator — walks the leaf chain backward via `prev_leaf`
/// pointers, yielding entries in descending key order without
/// materializing the entire tree into memory.
pub struct BTreeIterRev<'a, K, V> {
    tree: &'a BPlusTree<K, V>,
    page: u64,
    slot: usize, // 0 = last entry; increment walks backward through slots
}

impl<'a, K, V> Iterator for BTreeIterRev<'a, K, V>
where
    K: Decode<()>,
    V: Decode<()>,
{
    type Item = (K, V);
    fn next(&mut self) -> Option<Self::Item> {
        while self.page != 0 {
            let off = page_offset(self.page);
            let count = rd_u16(&self.tree.mmap[off + 2..off + 4]) as usize;
            if self.slot < count {
                let idx = count - 1 - self.slot;
                let key_slice = self.tree.key_slice_at(off, idx);
                let value_slice = self.tree.value_slice_at(off, idx);
                self.slot += 1;
                let k: K = deserialize_from(key_slice).expect("valid encoded key");
                let v: V = deserialize_from(value_slice).expect("valid encoded value");
                return Some((k, v));
            }
            self.page = rd_u64(&self.tree.mmap[off + 16..off + 24]); // prev_leaf
            self.slot = 0;
        }
        None
    }
}

/// Range-scan iterator for entries with serialized keys in `[start, end)`.
/// Like [`BTreeIter`] but stops when the current key ≥ `end`. Seeded at
/// the leaf and slot where `start` would be inserted (first slot ≥ start).
pub struct BTreeRange<'a, K, V> {
    tree: &'a BPlusTree<K, V>,
    page: u64,
    slot: usize,
    end: Vec<u8>,
}

impl<'a, K, V> Iterator for BTreeRange<'a, K, V>
where
    K: Decode<()>,
    V: Decode<()>,
{
    type Item = (K, V);
    fn next(&mut self) -> Option<Self::Item> {
        while self.page != 0 {
            let off = page_offset(self.page);
            let count = rd_u16(&self.tree.mmap[off + 2..off + 4]) as usize;
            if self.slot < count {
                let key_slice = self.tree.key_slice_at(off, self.slot);
                if key_slice >= self.end.as_slice() {
                    return None;
                }
                let value_slice = self.tree.value_slice_at(off, self.slot);
                self.slot += 1;
                let k: K = deserialize_from(key_slice).expect("valid encoded key");
                let v: V = deserialize_from(value_slice).expect("valid encoded value");
                return Some((k, v));
            }
            self.page = rd_u64(&self.tree.mmap[off + 8..off + 16]);
            self.slot = 0;
        }
        None
    }
}

impl<K, V> Drop for BPlusTree<K, V> {
    fn drop(&mut self) {
        let _ = self.mmap.flush();
    }
}

impl<K, V> fmt::Debug for BPlusTree<K, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.stats, f)
    }
}

use crate::core::backend::{Backend, OrderedBackend};

impl<K, V> Backend<K, V> for BPlusTree<K, V>
where
    K: Encode + Decode<()> + Hash + Eq + Clone + Ord,
    V: Encode + Decode<()> + Clone,
{
    type Stats = BPlusTreeStats;

    fn get(&self, key: &K) -> Option<V> {
        BPlusTree::get(self, key)
    }

    fn contains(&self, key: &K) -> bool {
        BPlusTree::contains(self, key)
    }

    fn put(&mut self, key: K, value: V) -> io::Result<()> {
        BPlusTree::put(self, key, value)
    }

    fn delete(&mut self, key: &K) -> io::Result<bool> {
        BPlusTree::delete(self, key)
    }

    fn update<F>(&mut self, key: &K, f: F) -> io::Result<()>
    where
        F: FnOnce(Option<V>) -> Option<V>,
    {
        BPlusTree::update(self, key, f)
    }

    fn clear(&mut self) -> io::Result<()> {
        BPlusTree::clear(self)
    }

    fn compact(&mut self) -> io::Result<()> {
        BPlusTree::compact(self)
    }

    fn keys(&self) -> impl Iterator<Item = K> + '_ {
        BPlusTree::keys(self)
    }

    fn values(&self) -> impl Iterator<Item = V> + '_ {
        BPlusTree::values(self)
    }

    fn entries(&self) -> impl Iterator<Item = (K, V)> + '_ {
        BPlusTree::entries(self)
    }

    fn size(&self) -> usize {
        BPlusTree::size(self)
    }

    fn stats(&self) -> &Self::Stats {
        BPlusTree::stats(self)
    }

    fn flush(&self) -> io::Result<()> {
        BPlusTree::flush(self)
    }

    fn sync(&self) -> io::Result<()> {
        BPlusTree::sync(self)
    }
}

impl<K, V> OrderedBackend<K, V> for BPlusTree<K, V>
where
    K: Encode + Decode<()> + Hash + Eq + Clone + Ord,
    V: Encode + Decode<()> + Clone,
{
    fn range(&self, start: &K, end: &K) -> impl Iterator<Item = (K, V)> + '_ {
        BPlusTree::range(self, start, end)
    }

    /// Reads the last entry from the rightmost non-empty leaf. Usually O(1),
    /// but may walk left over empty rightmost leaves left behind by deletes.
    fn last(&self) -> Option<(K, V)> {
        if self.size() == 0 {
            return None;
        }
        let mut page = self.rightmost_leaf();
        while page != 0 {
            let off = page_offset(page);
            let count = rd_u16(&self.mmap[off + 2..off + 4]) as usize;
            if count != 0 {
                let last_slot = count - 1;
                let ks = self.key_slice_at(off, last_slot);
                let vs = self.value_slice_at(off, last_slot);
                let k: K = deserialize_from(ks).expect("valid encoded key");
                let v: V = deserialize_from(vs).expect("valid encoded value");
                return Some((k, v));
            }
            page = rd_u64(&self.mmap[off + 16..off + 24]);
        }
        None
    }

    /// Streaming reverse iteration via `prev_leaf` pointers — O(1)
    /// memory, yields entries in descending key order.
    fn entries_rev(&self) -> impl Iterator<Item = (K, V)> + '_
    where
        K: 'static,
        V: 'static,
    {
        let page = if self.size() == 0 {
            0
        } else {
            self.rightmost_leaf()
        };
        BTreeIterRev {
            tree: self,
            page,
            slot: 0,
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::backend::Backend;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    type TestKey = Vec<u8>;
    type TestVal = Vec<u8>;

    fn tmp(label: &str) -> PathBuf {
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let n = SEQ.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join("zendb_btree_tests");
        fs::create_dir_all(&dir).unwrap();
        let p = dir.join(format!("{}_{}_{}.bt", label, std::process::id(), n));
        let _ = fs::remove_file(&p);
        p
    }

    fn create(p: &Path) -> BPlusTree<TestKey, TestVal> {
        BPlusTree::create(p, BPlusTreeConfig::default()).unwrap()
    }

    fn open(p: &Path) -> BPlusTree<TestKey, TestVal> {
        BPlusTree::open(p, BPlusTreeConfig::default()).unwrap()
    }

    fn k(s: &str) -> TestKey {
        s.as_bytes().to_vec()
    }
    fn vbytes(s: &str) -> TestVal {
        s.as_bytes().to_vec()
    }

    /// Helper kept for compatibility with existing tests: `get` already
    /// returns the owned value directly under the new API.
    fn vget(t: &BPlusTree<TestKey, TestVal>, key: &TestKey) -> Option<Vec<u8>> {
        t.get(key)
    }

    // -- basic --

    #[test]
    fn create_and_open() {
        let p = tmp("co");
        create(&p).flush().unwrap();
        open(&p);
    }

    #[test]
    fn insert_and_get() {
        let p = tmp("ig");
        let mut t = create(&p);
        t.put(k("hello"), vbytes("world")).unwrap();
        t.put(k("foo"), vbytes("bar")).unwrap();
        t.flush().unwrap();
        assert_eq!(vget(&t, &k("hello")), Some(vbytes("world")));
        assert_eq!(vget(&t, &k("foo")), Some(vbytes("bar")));
        assert_eq!(vget(&t, &k("nope")), None);
    }

    #[test]
    fn overwrite() {
        let p = tmp("ow");
        let mut t = create(&p);
        t.put(k("k"), vbytes("v1")).unwrap();
        t.put(k("k"), vbytes("v2")).unwrap();
        assert_eq!(vget(&t, &k("k")), Some(vbytes("v2")));
        assert_eq!(t.size(), 1);
    }

    #[test]
    fn delete() {
        let p = tmp("del");
        let mut t = create(&p);
        t.put(k("a"), vbytes("1")).unwrap();
        t.put(k("b"), vbytes("2")).unwrap();
        assert!(t.delete(&k("a")).unwrap());
        assert!(!t.delete(&k("a")).unwrap());
        assert_eq!(vget(&t, &k("a")), None);
        assert_eq!(vget(&t, &k("b")), Some(vbytes("2")));
        assert_eq!(t.size(), 1);
    }

    #[test]
    fn empty_ops() {
        let p = tmp("e");
        let t = create(&p);
        assert!(t.is_empty());
        assert_eq!(vget(&t, &k("x")), None);
        assert!(t.entries().next().is_none());
    }

    #[test]
    fn iterator_yields_ordered() {
        let p = tmp("it");
        let mut t = create(&p);
        t.put(k("c"), vbytes("3")).unwrap();
        t.put(k("a"), vbytes("1")).unwrap();
        t.put(k("b"), vbytes("2")).unwrap();
        let keys: Vec<Vec<u8>> = t.entries().map(|(k, _)| k).collect();
        assert_eq!(keys.len(), 3);
        assert_eq!(keys[0], k("a"));
        assert_eq!(keys[1], k("b"));
        assert_eq!(keys[2], k("c"));
    }

    #[test]
    fn range_query() {
        let p = tmp("rq");
        let mut t = create(&p);
        for i in 0u32..20 {
            let key = format!("k{:04}", i);
            t.put(key.into_bytes(), i.to_le_bytes().to_vec()).unwrap();
        }
        let keys: Vec<String> = t
            .range(&k("k0005"), &k("k0010"))
            .map(|(k, _)| String::from_utf8(k).unwrap())
            .collect();
        assert_eq!(keys, ["k0005", "k0006", "k0007", "k0008", "k0009"]);
    }

    #[test]
    fn range_empty_and_bounds() {
        let p = tmp("rqb");
        let mut t = create(&p);
        t.put(k("b"), vbytes("2")).unwrap();
        t.put(k("d"), vbytes("4")).unwrap();
        assert_eq!(t.range(&k("e"), &k("z")).count(), 0);
        let keys: Vec<Vec<u8>> = t.range(&k("a"), &k("c")).map(|(k, _)| k).collect();
        assert_eq!(keys, vec![k("b")]);
    }

    #[test]
    fn reopen() {
        let p = tmp("ro");
        {
            let mut t = create(&p);
            t.put(k("p"), vbytes("d")).unwrap();
            t.flush().unwrap();
        }
        let t: BPlusTree<TestKey, TestVal> = open(&p);
        assert_eq!(vget(&t, &k("p")), Some(vbytes("d")));
        // No key cache — iter reads keys straight from mmap.
        let keys: Vec<Vec<u8>> = t.entries().map(|(k, _)| k).collect();
        assert_eq!(keys, vec![k("p")]);
    }

    #[test]
    fn split_leaf() {
        let p = tmp("sl");
        let mut t = create(&p);
        for i in 0u32..500 {
            t.put(format!("k{:04}", i).into_bytes(), i.to_le_bytes().to_vec())
                .unwrap();
        }
        for i in 0u32..500 {
            let key = format!("k{:04}", i).into_bytes();
            assert_eq!(vget(&t, &key), Some(i.to_le_bytes().to_vec()));
        }
        assert_eq!(t.size(), 500);
    }

    // -- extents (contiguous overflow) --

    #[test]
    fn extent_value_round_trip() {
        let p = tmp("ext");
        let mut t = create(&p);
        let big = vec![0xABu8; 10_000];
        t.put(k("big"), big.clone()).unwrap();
        t.flush().unwrap();
        assert_eq!(vget(&t, &k("big")), Some(big.clone()));

        // Overwrite with even bigger value — old extent must be freed,
        // new one allocated.
        let bigger = vec![0xCDu8; 15_000];
        t.put(k("big"), bigger.clone()).unwrap();
        assert_eq!(vget(&t, &k("big")), Some(bigger.clone()));

        // Delete frees the extent.
        assert!(t.delete(&k("big")).unwrap());
        assert_eq!(vget(&t, &k("big")), None);
        assert_eq!(t.size(), 0);
    }

    #[test]
    fn very_large_value_50kb() {
        let p = tmp("vlarge");
        let mut t = create(&p);
        let huge = vec![0xEFu8; 50_000];
        t.put(k("huge"), huge.clone()).unwrap();
        t.flush().unwrap();
        assert_eq!(vget(&t, &k("huge")), Some(huge));
    }

    #[test]
    fn extent_survives_split() {
        // 8 000-byte values per entry => every value goes to an extent.
        // Inserting 30 forces several leaf splits; extents must be
        // preserved (pointers carried through `SplitVal::Extent`).
        let p = tmp("ext_split");
        let mut t = create(&p);
        let big = vec![0xBBu8; 8_000];
        for i in 0u32..30 {
            t.put(format!("ovk{:04}", i).into_bytes(), big.clone())
                .unwrap();
        }
        for i in 0u32..30 {
            let key = format!("ovk{:04}", i).into_bytes();
            assert_eq!(vget(&t, &key), Some(big.clone()));
        }
        assert_eq!(t.size(), 30);
        // Range scan across page boundaries.
        let count = t.range(&k("ovk0000"), &k("ovk0030")).count();
        assert_eq!(count, 30);
    }

    #[test]
    fn extent_survives_reopen() {
        let p = tmp("ext_reopen");
        let big = vec![0xAAu8; 8_000];
        {
            let mut t = create(&p);
            t.put(k("a"), big.clone()).unwrap();
            t.put(k("b"), vbytes("inline")).unwrap();
            t.flush().unwrap();
        }
        let t: BPlusTree<TestKey, TestVal> = open(&p);
        assert_eq!(vget(&t, &k("a")), Some(big));
        assert_eq!(vget(&t, &k("b")), Some(vbytes("inline")));
    }

    // -- iteration after reopen --

    #[test]
    fn iter_after_reopen() {
        let p = tmp("iter_reopen");
        {
            let mut t = create(&p);
            for i in 0..50u32 {
                t.put(format!("k{:03}", i).into_bytes(), vbytes("v"))
                    .unwrap();
            }
            t.flush().unwrap();
        }
        let t: BPlusTree<TestKey, TestVal> = open(&p);
        let keys: Vec<Vec<u8>> = t.entries().map(|(k, _)| k).collect();
        assert_eq!(keys.len(), 50);
        for (i, k) in keys.iter().enumerate() {
            assert_eq!(k, &format!("k{:03}", i).into_bytes());
        }
    }

    #[test]
    fn delete_drops_from_iteration() {
        let p = tmp("del_iter");
        let mut t = create(&p);
        t.put(k("a"), vbytes("1")).unwrap();
        t.put(k("b"), vbytes("2")).unwrap();
        assert_eq!(t.entries().count(), 2);
        t.delete(&k("a")).unwrap();
        let remaining: Vec<Vec<u8>> = t.entries().map(|(k, _)| k).collect();
        assert_eq!(remaining, vec![k("b")]);
    }

    // -- separator helpers --

    #[test]
    fn truncated_separator_basic() {
        assert_eq!(truncated_separator(b"abc", b"abd"), b"abd".to_vec());
        assert_eq!(
            truncated_separator(b"abc", b"abcd"),
            vec![b'a', b'b', b'c', 0]
        );
        assert_eq!(truncated_separator(b"hello", b"world"), b"w".to_vec());
        assert_eq!(truncated_separator(b"key_a", b"key_b"), b"key_b".to_vec());
    }

    // -- extent overwrite transitions --

    #[test]
    fn overwrite_inline_with_extent() {
        let p = tmp("ow_inl2ext");
        let mut t = create(&p);
        t.put(k("k"), vbytes("small")).unwrap();
        let big = vec![0x42u8; 9_000];
        t.put(k("k"), big.clone()).unwrap();
        assert_eq!(vget(&t, &k("k")), Some(big));
        assert_eq!(t.size(), 1);
    }

    #[test]
    fn overwrite_extent_with_inline() {
        let p = tmp("ow_ext2inl");
        let mut t = create(&p);
        let big = vec![0x77u8; 9_000];
        t.put(k("k"), big).unwrap();
        t.put(k("k"), vbytes("tiny")).unwrap();
        assert_eq!(vget(&t, &k("k")), Some(vbytes("tiny")));
        assert_eq!(t.size(), 1);
    }

    #[test]
    fn overwrite_extent_with_different_size_extent() {
        let p = tmp("ow_ext2ext");
        let mut t = create(&p);
        let a = vec![0xAAu8; 7_000];
        let b = vec![0xBBu8; 12_000];
        t.put(k("k"), a).unwrap();
        t.put(k("k"), b.clone()).unwrap();
        assert_eq!(vget(&t, &k("k")), Some(b));
        assert_eq!(t.size(), 1);
    }

    #[test]
    fn overwrite_full_leaf_does_not_duplicate_key() {
        let p = tmp("ow_full_leaf");
        let mut t = BPlusTree::create(
            &p,
            BPlusTreeConfig {
                compaction_ratio: 1.0,
            },
        )
        .unwrap();

        for i in 0..120u32 {
            t.put(format!("k{:04}", i).into_bytes(), vec![0x11; 64])
                .unwrap();
        }
        let before = t.size();
        let key = k("k0005");
        let replacement = vec![0x22; 3_000];
        t.put(key.clone(), replacement.clone()).unwrap();

        assert_eq!(t.size(), before);
        assert_eq!(vget(&t, &key), Some(replacement));
        assert_eq!(t.entries().filter(|(found, _)| found == &key).count(), 1);
    }

    // -- clear --

    #[test]
    fn clear_removes_all_entries() {
        let p = tmp("clear");
        let mut t = create(&p);
        t.put(k("a"), vbytes("av")).unwrap();
        t.put(k("b"), vbytes("bv")).unwrap();
        t.put(k("c"), vbytes("cv")).unwrap();
        assert_eq!(t.size(), 3);

        t.clear().unwrap();
        assert_eq!(t.size(), 0);
        assert!(t.is_empty());
        assert_eq!(vget(&t, &k("a")), None);
        assert_eq!(vget(&t, &k("b")), None);
        assert!(t.entries().next().is_none());
    }

    #[test]
    fn clear_then_put_reuses_file() {
        let p = tmp("clear_reuse");
        let mut t = create(&p);
        for i in 0u32..100 {
            t.put(format!("k{:04}", i).into_bytes(), vbytes("v"))
                .unwrap();
        }
        t.clear().unwrap();
        t.put(k("z"), vbytes("zv")).unwrap();
        assert_eq!(t.size(), 1);
        assert_eq!(vget(&t, &k("z")), Some(vbytes("zv")));
        assert_eq!(vget(&t, &k("k0050")), None);
    }

    #[test]
    fn clear_survives_reopen() {
        let p = tmp("clear_reopen");
        {
            let mut t = create(&p);
            t.put(k("a"), vbytes("av")).unwrap();
            t.put(k("b"), vbytes("bv")).unwrap();
            t.clear().unwrap();
            t.flush().unwrap();
        }
        let t: BPlusTree<TestKey, TestVal> = open(&p);
        assert_eq!(t.size(), 0);
        assert!(t.is_empty());
    }

    // -- delete-counter sanity --

    #[test]
    fn delete_counter_never_wraps() {
        let p = tmp("nowrap");
        let mut t = create(&p);
        for i in 0u32..100 {
            t.put(format!("k{:04}", i).into_bytes(), vbytes("v"))
                .unwrap();
        }
        assert_eq!(t.size(), 100);
        for i in 0u32..100 {
            assert!(t.delete(&format!("k{:04}", i).into_bytes()).unwrap());
        }
        assert_eq!(t.size(), 0);
    }

    // -- put_if_absent / replace --

    #[test]
    fn put_if_absent_inserts_when_missing() {
        use crate::core::backend::Backend;
        let p = tmp("pia_insert");
        let mut t = create(&p);
        assert!(Backend::put_if_absent(&mut t, k("a"), vbytes("first")).unwrap());
        assert_eq!(vget(&t, &k("a")), Some(vbytes("first")));
    }

    #[test]
    fn put_if_absent_noop_when_present() {
        use crate::core::backend::Backend;
        let p = tmp("pia_noop");
        let mut t = create(&p);
        t.put(k("a"), vbytes("first")).unwrap();
        assert!(!Backend::put_if_absent(&mut t, k("a"), vbytes("second")).unwrap());
        assert_eq!(vget(&t, &k("a")), Some(vbytes("first")));
    }

    #[test]
    fn replace_returns_previous_value() {
        use crate::core::backend::Backend;
        let p = tmp("replace");
        let mut t = create(&p);
        t.put(k("a"), vbytes("first")).unwrap();
        let prev = Backend::replace(&mut t, k("a"), vbytes("second")).unwrap();
        assert_eq!(prev, Some(vbytes("first")));
        assert_eq!(vget(&t, &k("a")), Some(vbytes("second")));
    }

    #[test]
    fn replace_returns_none_when_absent() {
        use crate::core::backend::Backend;
        let p = tmp("replace_absent");
        let mut t = create(&p);
        let prev = Backend::replace(&mut t, k("a"), vbytes("new")).unwrap();
        assert!(prev.is_none());
        assert_eq!(vget(&t, &k("a")), Some(vbytes("new")));
    }

    // -- bulk_put / bulk_put_sorted / bulk_delete --

    #[test]
    fn bulk_put_inserts_all_items() {
        use crate::core::backend::Backend;
        let p = tmp("bulk_put");
        let mut t = create(&p);
        let items: Vec<(TestKey, TestVal)> = (0u32..50)
            .map(|i| (format!("k{:04}", i).into_bytes(), vbytes("v")))
            .collect();
        Backend::bulk_put(&mut t, items).unwrap();
        assert_eq!(t.size(), 50);
    }

    #[test]
    fn bulk_put_sorted_inserts_all_items() {
        use crate::core::backend::Backend;
        let p = tmp("bulk_put_sorted");
        let mut t = create(&p);
        let items: Vec<(TestKey, TestVal)> = (0u32..50)
            .map(|i| (format!("k{:04}", i).into_bytes(), vbytes("v")))
            .collect();
        Backend::bulk_put_sorted(&mut t, items).unwrap();
        assert_eq!(t.size(), 50);
    }

    #[test]
    fn bulk_delete_returns_removed_count() {
        use crate::core::backend::Backend;
        let p = tmp("bulk_delete");
        let mut t = create(&p);
        for i in 0u32..10 {
            t.put(format!("k{:02}", i).into_bytes(), vbytes("v"))
                .unwrap();
        }
        let keys: Vec<TestKey> = (0u32..5)
            .map(|i| format!("k{:02}", i).into_bytes())
            .collect();
        let ghosts: Vec<TestKey> = vec![k("ghost1"), k("ghost2")];
        let all: Vec<&TestKey> = keys.iter().chain(ghosts.iter()).collect();
        let n = Backend::bulk_delete(&mut t, all).unwrap();
        assert_eq!(n, 5);
        assert_eq!(t.size(), 5);
    }

    // -- OrderedBackend::first / last --

    #[test]
    fn first_and_last_walk_leaf_chain() {
        use crate::core::backend::OrderedBackend;
        let p = tmp("first_last");
        let mut t = create(&p);
        for i in 0u32..30 {
            t.put(format!("k{:04}", i).into_bytes(), vbytes("v"))
                .unwrap();
        }
        let (fk, _) = OrderedBackend::first(&t).unwrap();
        let (lk, _) = OrderedBackend::last(&t).unwrap();
        assert_eq!(fk, k("k0000"));
        assert_eq!(lk, k("k0029"));
    }

    #[test]
    fn last_skips_empty_rightmost_leaves_after_deletes() {
        use crate::core::backend::OrderedBackend;
        let p = tmp("last_after_delete");
        let mut t = BPlusTree::create(
            &p,
            BPlusTreeConfig {
                compaction_ratio: 1.0,
            },
        )
        .unwrap();

        for i in 0u32..600 {
            t.put(format!("k{:04}", i).into_bytes(), vbytes("v"))
                .unwrap();
        }
        for i in 500u32..600 {
            t.delete(&format!("k{:04}", i).into_bytes()).unwrap();
        }

        let (lk, _) = OrderedBackend::last(&t).unwrap();
        assert_eq!(lk, k("k0499"));
        let rev_first = OrderedBackend::entries_rev(&t).next().unwrap().0;
        assert_eq!(rev_first, k("k0499"));
    }

    #[test]
    fn first_and_last_on_empty_tree() {
        use crate::core::backend::OrderedBackend;
        let p = tmp("first_last_empty");
        let t = create(&p);
        assert!(OrderedBackend::first(&t).is_none());
        assert!(OrderedBackend::last(&t).is_none());
    }

    // -- OrderedBackend::entries_rev --

    #[test]
    fn entries_rev_yields_descending_order() {
        use crate::core::backend::OrderedBackend;
        let p = tmp("entries_rev");
        let mut t = create(&p);
        for i in 0u32..10 {
            t.put(format!("k{:04}", i).into_bytes(), vbytes("v"))
                .unwrap();
        }
        let keys: Vec<Vec<u8>> = OrderedBackend::entries_rev(&t).map(|(k, _)| k).collect();
        let expected: Vec<Vec<u8>> = (0..10u32)
            .rev()
            .map(|i| format!("k{:04}", i).into_bytes())
            .collect();
        assert_eq!(keys, expected);
    }

    #[test]
    fn entries_rev_on_empty_tree() {
        use crate::core::backend::OrderedBackend;
        let p = tmp("entries_rev_empty");
        let t = create(&p);
        assert_eq!(OrderedBackend::entries_rev(&t).count(), 0);
    }

    // -- sync --

    #[test]
    fn sync_persists_writes() {
        let p = tmp("sync_persist");
        {
            let mut t = create(&p);
            t.put(k("a"), vbytes("v")).unwrap();
            t.sync().unwrap();
        }
        let t: BPlusTree<TestKey, TestVal> = open(&p);
        assert_eq!(vget(&t, &k("a")), Some(vbytes("v")));
    }

    #[test]
    fn flush_returns_ok_for_empty_tree() {
        let p = tmp("flush_empty");
        let t = create(&p);
        t.flush().unwrap();
        t.sync().unwrap();
    }

    // -- update --

    #[test]
    fn update_modifies_existing() {
        let p = tmp("update_exists");
        let mut t = create(&p);
        t.put(k("a"), vbytes("hello")).unwrap();
        t.update(&k("a"), |opt| {
            let mut v = opt.unwrap();
            v.extend_from_slice(b" world");
            Some(v)
        })
        .unwrap();
        assert_eq!(vget(&t, &k("a")), Some(vbytes("hello world")));
    }

    #[test]
    fn update_inserts_when_absent_and_returns_some() {
        let p = tmp("update_insert");
        let mut t = create(&p);
        t.update(&k("fresh"), |opt| {
            assert!(opt.is_none());
            Some(vbytes("hi"))
        })
        .unwrap();
        assert_eq!(vget(&t, &k("fresh")), Some(vbytes("hi")));
    }

    #[test]
    fn update_absent_returning_none_is_noop() {
        let p = tmp("update_missing");
        let mut t = create(&p);
        t.update(&k("ghost"), |opt| {
            assert!(opt.is_none());
            None
        })
        .unwrap();
        assert!(vget(&t, &k("ghost")).is_none());
    }

    #[test]
    fn update_returning_none_deletes_existing() {
        let p = tmp("update_delete");
        let mut t = create(&p);
        t.put(k("a"), vbytes("hello")).unwrap();
        t.update(&k("a"), |opt| {
            assert!(opt.is_some());
            None
        })
        .unwrap();
        assert!(vget(&t, &k("a")).is_none());
    }

    #[test]
    fn stats_track_entries_pages_and_free_pages() {
        let p = tmp("stats");
        let mut t = BPlusTree::create(
            &p,
            BPlusTreeConfig {
                compaction_ratio: 1.0,
            },
        )
        .unwrap();
        assert_eq!(
            *t.stats(),
            BPlusTreeStats {
                entries: 0,
                pages: 2,
                free_pages: 0,
                leaf_pages: 1,
                leaf_entry_bytes: 0,
            }
        );

        t.put(k("a"), vec![0xAA; PAGE_SIZE * 2]).unwrap();
        assert_eq!(t.stats().entries, 1);
        assert!(t.stats().pages > 2);

        t.put(k("a"), vbytes("inline")).unwrap();
        assert_eq!(t.stats().entries, 1);
        assert!(t.stats().free_pages > 0);
    }

    #[test]
    fn compact_rebuilds_sparse_tree() {
        let p = tmp("compact_sparse");
        let mut t = BPlusTree::create(
            &p,
            BPlusTreeConfig {
                compaction_ratio: 1.0,
            },
        )
        .unwrap();

        for i in 0u32..600 {
            t.put(format!("k{:04}", i).into_bytes(), vec![0xAB; 32])
                .unwrap();
        }
        for i in 100u32..600 {
            t.delete(&format!("k{:04}", i).into_bytes()).unwrap();
        }

        let before_pages = t.pages();
        assert!(t.fragmentation_ratio() > 0.0);
        t.compact().unwrap();

        assert!(t.pages() < before_pages);
        assert_eq!(t.size(), 100);
        for i in 0u32..100 {
            assert_eq!(
                vget(&t, &format!("k{:04}", i).into_bytes()),
                Some(vec![0xAB; 32])
            );
        }
    }

    #[test]
    fn zero_ratio_compacts_on_first_insert_after_reopen() {
        let p = tmp("compact_zero_after_reopen");
        {
            let mut t = BPlusTree::create(
                &p,
                BPlusTreeConfig {
                    compaction_ratio: 1.0,
                },
            )
            .unwrap();
            for i in 0u32..600 {
                t.put(format!("k{:04}", i).into_bytes(), vec![0xCD; 32])
                    .unwrap();
            }
            for i in 100u32..600 {
                t.delete(&format!("k{:04}", i).into_bytes()).unwrap();
            }
            t.sync().unwrap();
        }

        let mut t = BPlusTree::open(
            &p,
            BPlusTreeConfig {
                compaction_ratio: 0.0,
            },
        )
        .unwrap();
        let before_pages = t.pages();
        t.put(k("k9999"), vbytes("new")).unwrap();

        assert!(t.pages() < before_pages);
        assert_eq!(t.size(), 101);
        assert_eq!(vget(&t, &k("k9999")), Some(vbytes("new")));
    }
}
