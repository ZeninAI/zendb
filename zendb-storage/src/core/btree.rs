//! BPlusTree — persistent, ordered key-value store backed by a mmap'd B+ tree
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
//! the first page index of the extent and the true value length; the value
//! bytes occupy bytes `[0..value_len]` of the extent mmap slice.
//!
//! Extent allocation first looks for a contiguous run on the freelist, then
//! grows the file if no reusable run is available. Freed extent pages go onto
//! the same freelist used by tree pages.
//!
//! # Meta page
//!
//! Page 0 is the meta page:
//! `[magic: u32][root: u64][free_head: u64][pages: u64][entries: u64][rightmost_leaf: u64]
//! [leaf_pages: u64][leaf_entry_bytes: u64][free_pages: u64]`.
//!
//! # Suffix truncation
//!
//! When a leaf or internal page splits, the separator key pushed to the parent
//! is the shortest prefix of the new page's first key that is strictly greater
//! than the old page's last key.
//!
//! # Writes
//!
//! Writes mutate the mmap in place. This engine is single-threaded, so there
//! is no isolation, no rollback, and no dirty-page cache. Bulk paths
//! (`bulk_put`, `bulk_put_sorted`, `bulk_delete`) use backend-specific
//! implementations only where they provide real tree-level work.

use memmap2::MmapMut;
use std::{
    borrow::Cow,
    env, fmt,
    fs::{self, OpenOptions},
    hash::Hash,
    io,
    marker::PhantomData,
    path::{Path, PathBuf},
    process,
    time::{SystemTime, UNIX_EPOCH},
};

use bincode::{Decode, Encode};
use hashbrown::HashMap;

use crate::core::backend::{Backend, OrderedBackend};
use crate::utils::reusables::PooledBuf;
use crate::utils::serdes::{
    deserialize_from, rd_u16, rd_u32, rd_u64, serialize_to_vec, with_scratch, with_two_scratches,
    wr_u16, wr_u32, wr_u64,
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
// Layout:
//   magic(4) root(8) free_head(8) pages(8) entries(8) rightmost_leaf(8)
//   leaf_pages(8) leaf_entry_bytes(8) free_pages(8)
// The last three counters are maintained incrementally so `open()` can
// reconstitute `BPlusTreeStats` without a full leaf-chain scan or a
// freelist walk.
const META_ROOT: usize = 4;
const META_FREE_HEAD: usize = 12;
const META_PAGES: usize = 20;
const META_ENTRIES: usize = 28;
const META_RIGHTMOST_LEAF: usize = 36;
const META_LEAF_PAGES: usize = 44;
const META_LEAF_ENTRY_BYTES: usize = 52;
const META_FREE_PAGES: usize = 60;

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
struct PageSplit {
    left_page: u64,
    right_page: u64,
    separator_key: Vec<u8>,
}

/// Pre-computed leaf navigation. Returned by [`BPlusTree::descend_to_leaf`]
/// so single-descent overrides (`put_if_absent`, `replace`, `update`) can
/// reuse the same descent for both the read-side check and the write-side
/// insert/delete.
struct DescentHint {
    /// Leaf page number the key would land in.
    leaf_page: u64,
    /// Parent page numbers collected during the descent (from root, excluding leaf).
    path: DescentPath,
    /// Slot lookup result within the leaf: `Ok(slot)` on exact match,
    /// `Err(insertion_point)` otherwise.
    slot: Result<usize, usize>,
}

/// Maximum tree depth the inline descent path can handle without spilling
/// to the heap. A B+ tree with 4 KB pages and 8-byte keys fans out ~200×
/// per internal page, so depth `D` holds ~200^D entries: D=8 covers
/// ~2.5×10^18 keys. In practice trees stay much shallower; this is the
/// "never spills" bound.
const DESCENT_PATH_INLINE: usize = 8;

/// Stack-allocated LIFO of internal-page numbers collected on the way down
/// to a leaf. `descend_to_leaf` is on the hot put/replace/update path; the
/// previous `Vec<u64>` allocated on every call to hold 1-2 entries.
struct DescentPath {
    /// `len` ≤ `DESCENT_PATH_INLINE` for the inline path; if a tree ever
    /// exceeds this depth (it shouldn't), the entries beyond the inline
    /// capacity spill to `overflow`.
    inline: [u64; DESCENT_PATH_INLINE],
    len: usize,
    overflow: Vec<u64>,
}

impl DescentPath {
    #[inline]
    fn new() -> Self {
        Self {
            inline: [0; DESCENT_PATH_INLINE],
            len: 0,
            overflow: Vec::new(),
        }
    }

    #[inline]
    fn push(&mut self, page: u64) {
        if self.len < DESCENT_PATH_INLINE {
            self.inline[self.len] = page;
            self.len += 1;
        } else {
            self.overflow.push(page);
        }
    }

    /// Drain in reverse order — `cascade_split` walks parents from leaf-up.
    fn drain_rev(self) -> DescentPathRev {
        DescentPathRev { path: self }
    }
}

struct DescentPathRev {
    path: DescentPath,
}

impl Iterator for DescentPathRev {
    type Item = u64;
    fn next(&mut self) -> Option<u64> {
        if let Some(p) = self.path.overflow.pop() {
            return Some(p);
        }
        if self.path.len == 0 {
            return None;
        }
        self.path.len -= 1;
        Some(self.path.inline[self.path.len])
    }
}

/// Compute the **shortest prefix** of `right_first` that is strictly greater
/// than `left_last` and ≤ `right_first`. Used as the separator key when
/// splitting a page — stores only the bytes needed to distinguish the two
/// key ranges, maximizing internal-page fanout.
fn truncated_separator(left_last: &[u8], right_first: &[u8]) -> Vec<u8> {
    let n = left_last.len().min(right_first.len());
    for i in 0..n {
        if right_first[i] > left_last[i] {
            return right_first[..=i].to_vec();
        }
    }
    let mut s = left_last.to_vec();
    s.push(0);
    s
}

/// Byte cost of a single leaf entry: header + key + payload + slot.
#[inline]
fn leaf_entry_bytes(key_len: usize, payload_len: usize) -> u64 {
    (6 + key_len + payload_len + SLOT_SIZE) as u64
}

fn compact_temp_path() -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let mut p = env::temp_dir();
    p.push(format!(
        "zendb-btree-compact-{}-{}.tmp",
        process::id(),
        nanos
    ));
    p
}

// ---------------------------------------------------------------------------
// Public configuration / stats / state types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Encode, Decode)]
pub struct BPlusTreeConfig {
    /// Pre-allocated page count. File never shrinks below this after
    /// creation or compaction (minimum 2: meta page + root leaf).
    pub initial_capacity_pages: u64,
    /// Auto-compaction threshold. Values in `[0.0, 1.0]`:
    /// `0.0` compacts after every write, `1.0` disables automatic compaction.
    pub compaction_ratio: f64,
}

const DEFAULT_INITIAL_CAPACITY_PAGES: u64 = 64;

impl Default for BPlusTreeConfig {
    fn default() -> Self {
        BPlusTreeConfig {
            initial_capacity_pages: DEFAULT_INITIAL_CAPACITY_PAGES,
            compaction_ratio: DEFAULT_COMPACTION_RATIO,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Encode, Decode)]
pub struct BPlusTreeStats {
    pub entries: usize,
    pub pages: u64,
    pub free_pages: u64,
    /// Number of pages currently used as leaves. Maintained incrementally.
    pub leaf_pages: u64,
    /// Total live entry bytes across all leaves. Used together with
    /// `leaf_pages` to estimate the packed leaf count.
    pub leaf_entry_bytes: u64,
}

// ---------------------------------------------------------------------------
// Public BPlusTree
// ---------------------------------------------------------------------------

pub struct BPlusTree<K, V> {
    mmap: MmapMut,
    file: fs::File,
    config: BPlusTreeConfig,
    stats: BPlusTreeStats,
    _phantom: PhantomData<(K, V)>,
}

struct RawBTreeEntries<'a, K, V> {
    tree: &'a BPlusTree<K, V>,
    page: u64,
    slot: usize,
    count: usize,
}

impl<'a, K, V> RawBTreeEntries<'a, K, V> {
    fn new(tree: &'a BPlusTree<K, V>, first_leaf: u64) -> Self {
        let count = if first_leaf == 0 {
            0
        } else {
            rd_u16(&tree.page_bytes(first_leaf)[2..4]) as usize
        };
        Self {
            tree,
            page: first_leaf,
            slot: 0,
            count,
        }
    }
}

impl<'a, K, V> Iterator for RawBTreeEntries<'a, K, V> {
    type Item = (&'a [u8], &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        while self.page != 0 {
            if self.slot < self.count {
                // Borrow straight from the source mmap — the iterator's
                // `'a` lifetime ties each yielded slice to `&'a BPlusTree`,
                // so reading the slot fields and advancing `self.slot` does
                // not invalidate previously-yielded borrows.
                let leaf_off = page_offset(self.page);
                let leaf = &self.tree.mmap[leaf_off..leaf_off + PAGE_SIZE];
                let sp = LEAF_HEADER_SIZE + self.slot * SLOT_SIZE;
                let eo = rd_u16(&leaf[sp..sp + 2]) as usize;
                let kl = rd_u16(&leaf[eo..eo + 2]) as usize;
                let raw_vl = rd_u32(&leaf[eo + 2..eo + 6]);
                let key_bytes = &leaf[eo + 6..eo + 6 + kl];
                let value_bytes: &[u8] = if raw_vl & OVFL_FLAG != 0 {
                    let real_len = (raw_vl & !OVFL_FLAG) as usize;
                    let start = rd_u64(&leaf[eo + 6 + kl..eo + 6 + kl + 8]);
                    let xo = page_offset(start);
                    &self.tree.mmap[xo..xo + real_len]
                } else {
                    let vl = raw_vl as usize;
                    &leaf[eo + 6 + kl..eo + 6 + kl + vl]
                };
                self.slot += 1;
                return Some((key_bytes, value_bytes));
            }
            let cur_off = page_offset(self.page);
            self.page = rd_u64(&self.tree.mmap[cur_off + 8..cur_off + 16]);
            self.slot = 0;
            self.count = if self.page == 0 {
                0
            } else {
                let next_off = page_offset(self.page);
                rd_u16(&self.tree.mmap[next_off + 2..next_off + 4]) as usize
            };
        }
        None
    }
}

// ===========================================================================
// Inherent impl — only constructors, page dispatch, and shared low-level
// helpers. Every public method body lives in the `Backend` / `OrderedBackend`
// trait impls further down.
// ===========================================================================

impl<K, V> BPlusTree<K, V> {
    // ---- page-byte dispatch -----------------------------------------------
    //
    // Every page-byte access in this file routes through these helpers so
    // the dirty page cache stays correct. Outside a tx the dispatch is a
    // Direct mmap dispatch — writes mutate in place.

    /// Borrow the raw bytes of page `p`.
    #[inline]
    fn page_bytes(&self, p: u64) -> &[u8] {
        let off = page_offset(p);
        &self.mmap[off..off + PAGE_SIZE]
    }

    /// Mutable access to page `p`.
    #[inline]
    fn page_bytes_mut(&mut self, p: u64) -> &mut [u8] {
        let off = page_offset(p);
        &mut self.mmap[off..off + PAGE_SIZE]
    }

    /// Read the value bytes for slot `i` of leaf page `leaf_page`. Inline
    /// values borrow from the leaf page directly; extent values borrow from
    /// the extent run.
    fn value_bytes_at(&self, leaf_page: u64, i: usize) -> &[u8] {
        let leaf = self.page_bytes(leaf_page);
        let sp = LEAF_HEADER_SIZE + i * SLOT_SIZE;
        let eo = rd_u16(&leaf[sp..sp + 2]) as usize;
        let kl = rd_u16(&leaf[eo..eo + 2]) as usize;
        let raw_vl = rd_u32(&leaf[eo + 2..eo + 6]);
        if raw_vl & OVFL_FLAG != 0 {
            let real_len = (raw_vl & !OVFL_FLAG) as usize;
            let extent_start = rd_u64(&leaf[eo + 6 + kl..eo + 6 + kl + 8]);
            let off = page_offset(extent_start);
            &self.mmap[off..off + real_len]
        } else {
            let vl = raw_vl as usize;
            &leaf[eo + 6 + kl..eo + 6 + kl + vl]
        }
    }

    /// Key bytes for slot `i` of leaf `leaf_page`. Always borrowed.
    fn key_bytes_at(&self, leaf_page: u64, i: usize) -> &[u8] {
        let leaf = self.page_bytes(leaf_page);
        let sp = LEAF_HEADER_SIZE + i * SLOT_SIZE;
        let eo = rd_u16(&leaf[sp..sp + 2]) as usize;
        let kl = rd_u16(&leaf[eo..eo + 2]) as usize;
        &leaf[eo + 6..eo + 6 + kl]
    }
}

impl<K, V> BPlusTree<K, V>
where
    K: Encode + Decode<()> + Hash + Eq + Clone,
    V: Encode + Decode<()> + Clone,
{
    // ---- constructors -----------------------------------------------------

    /// Create a fresh BPlusTree at `path`, **truncating** any existing
    /// file. Pre-allocates `config.initial_capacity_pages` pages.
    pub fn create(path: &Path, config: BPlusTreeConfig) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(path)?;
        let np = 2u64;
        let initial_pages = config.initial_capacity_pages.max(2);
        file.set_len(initial_pages * PAGE_SIZE as u64)?;
        let mut mmap = unsafe { MmapMut::map_mut(&file)? };
        let m = page_offset(META_PAGE);
        wr_u32(&mut mmap[m..m + 4], MAGIC);
        wr_u64(&mut mmap[m + META_ROOT..m + META_ROOT + 8], 1);
        wr_u64(&mut mmap[m + META_FREE_HEAD..m + META_FREE_HEAD + 8], 0);
        wr_u64(&mut mmap[m + META_PAGES..m + META_PAGES + 8], np);
        wr_u64(&mut mmap[m + META_ENTRIES..m + META_ENTRIES + 8], 0);
        wr_u64(
            &mut mmap[m + META_RIGHTMOST_LEAF..m + META_RIGHTMOST_LEAF + 8],
            1,
        );
        // Persisted stats counters: one leaf page (the root), no entry
        // bytes, no free pages.
        wr_u64(&mut mmap[m + META_LEAF_PAGES..m + META_LEAF_PAGES + 8], 1);
        wr_u64(
            &mut mmap[m + META_LEAF_ENTRY_BYTES..m + META_LEAF_ENTRY_BYTES + 8],
            0,
        );
        wr_u64(&mut mmap[m + META_FREE_PAGES..m + META_FREE_PAGES + 8], 0);
        let r = page_offset(1);
        mmap[r] = PAGE_LEAF;
        mmap[r + 1] = FLAG_ROOT;
        wr_u16(&mut mmap[r + 2..r + 4], 0);
        wr_u32(&mut mmap[r + 4..r + 8], PAGE_SIZE as u32);
        wr_u64(&mut mmap[r + 8..r + 16], 0);
        wr_u64(&mut mmap[r + 16..r + 24], 0);
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

    /// Open an existing BPlusTree at `path`. Validates the MAGIC header at
    /// offset 0 (returns `InvalidData` if missing/mismatched).
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

    /// Current estimated reclaimable-page ratio. Public so callers can
    /// observe fragmentation without grabbing internal counters.
    pub fn fragmentation_ratio(&self) -> f64 {
        let allocated = self.pages().saturating_sub(1);
        if allocated == 0 {
            return 0.0;
        }
        let capacity = (PAGE_SIZE - LEAF_HEADER_SIZE) as u64;
        let packed_leaf_pages = if self.stats.leaf_entry_bytes == 0 {
            1
        } else {
            self.stats.leaf_entry_bytes.div_ceil(capacity)
        };
        let reclaimable_leaf_pages = self.stats.leaf_pages.saturating_sub(packed_leaf_pages);
        let free_pages = self.stats.free_pages;
        (free_pages + reclaimable_leaf_pages) as f64 / allocated as f64
    }

    /// Public accessor for the current `pages` counter — used by tests to
    /// observe file growth and freelist behavior. The real counter lives
    /// in the meta page; `stats.pages` mirrors it in lockstep, so we
    /// return the in-memory mirror to avoid a page_bytes dispatch.
    pub fn pages(&self) -> u64 {
        self.stats.pages
    }

    // ---- byte-level lookups -----------------------------------------------

    /// Navigate the tree to the leaf that would contain `key_bytes`, then
    /// return the contiguous encoded value bytes (inline or extent).
    /// Returns `None` if the key is not present.
    fn value_bytes_for(&self, key_bytes: &[u8]) -> Option<&[u8]> {
        let leaf = self.find_leaf(self.root(), key_bytes);
        let leaf_buf = self.page_bytes(leaf);
        let count = rd_u16(&leaf_buf[2..4]) as usize;
        let i = match self.leaf_find_slot(leaf, count, key_bytes) {
            Ok(i) => i,
            Err(_) => return None,
        };
        Some(self.value_bytes_at(leaf, i))
    }

    // ---- meta accessors — all reads/writes go through page dispatch -------

    fn root(&self) -> u64 {
        rd_u64(&self.page_bytes(META_PAGE)[META_ROOT..META_ROOT + 8])
    }
    fn set_root(&mut self, p: u64) {
        let meta = self.page_bytes_mut(META_PAGE);
        wr_u64(&mut meta[META_ROOT..META_ROOT + 8], p);
    }
    fn rightmost_leaf(&self) -> u64 {
        rd_u64(&self.page_bytes(META_PAGE)[META_RIGHTMOST_LEAF..META_RIGHTMOST_LEAF + 8])
    }
    fn set_rightmost_leaf(&mut self, p: u64) {
        let meta = self.page_bytes_mut(META_PAGE);
        wr_u64(&mut meta[META_RIGHTMOST_LEAF..META_RIGHTMOST_LEAF + 8], p);
    }
    fn free_head(&self) -> u64 {
        rd_u64(&self.page_bytes(META_PAGE)[META_FREE_HEAD..META_FREE_HEAD + 8])
    }
    fn set_free_head(&mut self, p: u64) {
        let meta = self.page_bytes_mut(META_PAGE);
        wr_u64(&mut meta[META_FREE_HEAD..META_FREE_HEAD + 8], p);
    }
    fn pages_counter(&self) -> u64 {
        rd_u64(&self.page_bytes(META_PAGE)[META_PAGES..META_PAGES + 8])
    }
    fn set_pages_counter(&mut self, n: u64) {
        let meta = self.page_bytes_mut(META_PAGE);
        wr_u64(&mut meta[META_PAGES..META_PAGES + 8], n);
        self.stats.pages = n;
    }
    fn inc_entries(&mut self, d: u64) {
        let cur = rd_u64(&self.page_bytes(META_PAGE)[META_ENTRIES..META_ENTRIES + 8]);
        let next = cur.wrapping_add(d);
        let meta = self.page_bytes_mut(META_PAGE);
        wr_u64(&mut meta[META_ENTRIES..META_ENTRIES + 8], next);
        self.stats.entries = next as usize;
    }
    fn dec_entries(&mut self) {
        let cur = rd_u64(&self.page_bytes(META_PAGE)[META_ENTRIES..META_ENTRIES + 8]);
        let next = cur.wrapping_sub(1);
        let meta = self.page_bytes_mut(META_PAGE);
        wr_u64(&mut meta[META_ENTRIES..META_ENTRIES + 8], next);
        self.stats.entries = next as usize;
    }

    // ---- incremental counter setters --------------------------------------
    //
    // Each setter mirrors `inc_entries`'s pattern: update the meta page
    // and the in-memory `stats` field together. Keeping them in lockstep
    // lets `open()` reconstitute stats from a single meta read instead of
    // walking the entire leaf chain + freelist.

    fn write_leaf_pages(&mut self, n: u64) {
        let meta = self.page_bytes_mut(META_PAGE);
        wr_u64(&mut meta[META_LEAF_PAGES..META_LEAF_PAGES + 8], n);
        self.stats.leaf_pages = n;
    }
    fn inc_leaf_pages(&mut self) {
        self.write_leaf_pages(self.stats.leaf_pages + 1);
    }
    fn dec_leaf_pages(&mut self) {
        self.write_leaf_pages(self.stats.leaf_pages.saturating_sub(1));
    }

    fn write_leaf_entry_bytes(&mut self, n: u64) {
        let meta = self.page_bytes_mut(META_PAGE);
        wr_u64(
            &mut meta[META_LEAF_ENTRY_BYTES..META_LEAF_ENTRY_BYTES + 8],
            n,
        );
        self.stats.leaf_entry_bytes = n;
    }
    fn add_leaf_entry_bytes(&mut self, d: u64) {
        self.write_leaf_entry_bytes(self.stats.leaf_entry_bytes.saturating_add(d));
    }
    fn sub_leaf_entry_bytes(&mut self, d: u64) {
        self.write_leaf_entry_bytes(self.stats.leaf_entry_bytes.saturating_sub(d));
    }

    fn write_free_pages(&mut self, n: u64) {
        let meta = self.page_bytes_mut(META_PAGE);
        wr_u64(&mut meta[META_FREE_PAGES..META_FREE_PAGES + 8], n);
        self.stats.free_pages = n;
    }
    fn inc_free_pages(&mut self) {
        self.write_free_pages(self.stats.free_pages + 1);
    }
    fn dec_free_pages(&mut self) {
        self.write_free_pages(self.stats.free_pages.saturating_sub(1));
    }

    // ---- tree traversal ---------------------------------------------------

    /// Leftmost leaf — walks down from the root following `leftmost_child`.
    /// May be empty: per-entry `delete` does not unlink emptied leaves
    /// (only `bulk_delete_sorted` does, via `rebuild_internal_from_leaves`),
    /// so callers that need the leaf holding the smallest live entry
    /// should use [`leftmost_nonempty_leaf`] instead.
    fn first_leaf(&self) -> u64 {
        let mut p = self.root();
        loop {
            let buf = self.page_bytes(p);
            if buf[0] == PAGE_LEAF {
                return p;
            }
            p = rd_u64(&buf[8..16]);
        }
    }

    /// Walk right from `first_leaf` skipping empty leaves. Returns the leaf
    /// containing the smallest live key, or `None` if the tree is empty.
    ///
    /// **Why:** per-entry `delete` decrements `count` but does not free the
    /// leaf or unlink it from the chain. If every key in the leftmost leaf
    /// has been deleted, descending via `leftmost_child` lands on an empty
    /// leaf even though `size() > 0`. Iterators handle this naturally by
    /// walking `next_leaf` in their loop; point queries like `first()` need
    /// to do the same.
    fn leftmost_nonempty_leaf(&self) -> Option<u64> {
        let mut page = self.first_leaf();
        while page != 0 {
            let count = rd_u16(&self.page_bytes(page)[2..4]) as usize;
            if count != 0 {
                return Some(page);
            }
            page = rd_u64(&self.page_bytes(page)[8..16]);
        }
        None
    }

    /// Walk left from `rightmost_leaf` skipping empty leaves. Symmetric to
    /// [`leftmost_nonempty_leaf`] — see that helper for why an empty leaf
    /// can appear at either end of the chain.
    fn rightmost_nonempty_leaf(&self) -> Option<u64> {
        let mut page = self.rightmost_leaf();
        while page != 0 {
            let count = rd_u16(&self.page_bytes(page)[2..4]) as usize;
            if count != 0 {
                return Some(page);
            }
            page = rd_u64(&self.page_bytes(page)[16..24]);
        }
        None
    }

    /// Walk from `root` down to the leaf page that would contain `key`.
    fn find_leaf(&self, root: u64, key: &[u8]) -> u64 {
        let mut p = root;
        loop {
            let buf = self.page_bytes(p);
            if buf[0] == PAGE_LEAF {
                return p;
            }
            p = self.internal_search(p, key);
        }
    }

    /// Binary search through an internal page's sorted separator keys.
    /// Returns the child page number that should be descended to.
    fn internal_search(&self, page: u64, key: &[u8]) -> u64 {
        let buf = self.page_bytes(page);
        let count = rd_u16(&buf[2..4]) as usize;
        let leftmost = rd_u64(&buf[8..16]);
        let mut lo = 0usize;
        let mut hi = count;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let sp = HEADER_SIZE + mid * SLOT_SIZE;
            let eo = rd_u16(&buf[sp..sp + 2]) as usize;
            let kl = rd_u16(&buf[eo..eo + 2]) as usize;
            if key < &buf[eo + 10..eo + 10 + kl] {
                hi = mid;
            } else {
                lo = mid + 1;
            }
        }
        if lo == 0 {
            leftmost
        } else {
            let sp = HEADER_SIZE + (lo - 1) * SLOT_SIZE;
            let eo = rd_u16(&buf[sp..sp + 2]) as usize;
            rd_u64(&buf[eo + 2..eo + 10])
        }
    }

    /// Binary search a leaf page's slots for `key`. `Ok(i)` on exact match,
    /// `Err(insertion_point)` otherwise.
    fn leaf_find_slot(&self, page: u64, count: usize, key: &[u8]) -> Result<usize, usize> {
        let buf = self.page_bytes(page);
        let mut lo = 0usize;
        let mut hi = count;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let sp = LEAF_HEADER_SIZE + mid * SLOT_SIZE;
            let eo = rd_u16(&buf[sp..sp + 2]) as usize;
            let kl = rd_u16(&buf[eo..eo + 2]) as usize;
            match buf[eo + 6..eo + 6 + kl].cmp(key) {
                std::cmp::Ordering::Equal => return Ok(mid),
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
            }
        }
        Err(lo)
    }

    /// Walk down to the leaf for `key_bytes`, collecting the parent path
    /// and the slot lookup result. Used by single-descent overrides.
    fn descend_to_leaf(&self, key_bytes: &[u8]) -> DescentHint {
        let mut path = DescentPath::new();
        let mut page = self.root();
        loop {
            let buf = self.page_bytes(page);
            if buf[0] == PAGE_LEAF {
                break;
            }
            path.push(page);
            page = self.internal_search(page, key_bytes);
        }
        let count = rd_u16(&self.page_bytes(page)[2..4]) as usize;
        let slot = self.leaf_find_slot(page, count, key_bytes);
        DescentHint {
            leaf_page: page,
            path,
            slot,
        }
    }

    // ---- page allocation / freelist ---------------------------------------

    fn alloc_page(&mut self) -> io::Result<u64> {
        let free = self.free_head();
        if free != 0 {
            let next = rd_u64(&self.page_bytes(free)[..8]);
            self.set_free_head(next);
            self.dec_free_pages();
            return Ok(free);
        }
        let p = self.pages_counter();
        self.ensure_capacity((p + 1) * PAGE_SIZE as u64)?;
        self.set_pages_counter(p + 1);
        Ok(p)
    }

    fn free_page(&mut self, page: u64) {
        let head = self.free_head();
        let buf = self.page_bytes_mut(page);
        wr_u64(&mut buf[..8], head);
        self.set_free_head(page);
        self.inc_free_pages();
    }

    /// Try to unlink a descending contiguous run from the freelist.
    ///
    /// `free_extent(start, n)` pushes pages as `start+n-1 -> ... -> start`,
    /// so this cheap chain scan catches the common large-value churn case
    /// without adding a separate extent freelist.
    fn alloc_extent_from_freelist(&mut self, n_pages: u64) -> Option<u64> {
        if n_pages == 0 {
            return None;
        }
        if n_pages == 1 {
            let free = self.free_head();
            if free == 0 {
                return None;
            }
            let next = rd_u64(&self.page_bytes(free)[..8]);
            self.set_free_head(next);
            self.dec_free_pages();
            return Some(free);
        }

        let mut prev: Option<u64> = None;
        let mut cur = self.free_head();
        let max_pages = self.pages_counter();
        let mut scanned = 0u64;
        while cur != 0 && scanned < max_pages {
            scanned += 1;
            let mut run_len = 1u64;
            let mut last = cur;
            while run_len < n_pages {
                let next = rd_u64(&self.page_bytes(last)[..8]);
                if next == 0 || next + 1 != last {
                    break;
                }
                last = next;
                run_len += 1;
            }
            if run_len == n_pages {
                let after = rd_u64(&self.page_bytes(last)[..8]);
                if let Some(p) = prev {
                    let buf = self.page_bytes_mut(p);
                    wr_u64(&mut buf[..8], after);
                } else {
                    self.set_free_head(after);
                }
                self.write_free_pages(self.stats.free_pages.saturating_sub(n_pages));
                return Some(last);
            }
            prev = Some(cur);
            cur = rd_u64(&self.page_bytes(cur)[..8]);
        }
        None
    }

    /// Allocate `n_pages` contiguous pages. Reuses a contiguous freelist
    /// run when available; otherwise bumps the `pages` counter and grows
    /// the file.
    fn alloc_extent(&mut self, n_pages: u64) -> io::Result<u64> {
        if let Some(start) = self.alloc_extent_from_freelist(n_pages) {
            return Ok(start);
        }
        let start = self.pages_counter();
        self.ensure_capacity((start + n_pages) * PAGE_SIZE as u64)?;
        self.set_pages_counter(start + n_pages);
        Ok(start)
    }

    fn write_extent(&mut self, value_bytes: &[u8]) -> io::Result<u64> {
        let n_pages = value_bytes.len().div_ceil(PAGE_SIZE) as u64;
        let start = self.alloc_extent(n_pages)?;
        let off = page_offset(start);
        self.mmap[off..off + value_bytes.len()].copy_from_slice(value_bytes);
        Ok(start)
    }

    fn free_extent(&mut self, start: u64, value_len: u32) {
        let n_pages = (value_len as usize).div_ceil(PAGE_SIZE) as u64;
        for i in 0..n_pages {
            self.free_page(start + i);
        }
    }

    fn refresh_stats_from_meta(&mut self) {
        let meta = self.page_bytes(META_PAGE);
        let entries = rd_u64(&meta[META_ENTRIES..META_ENTRIES + 8]) as usize;
        let pages = rd_u64(&meta[META_PAGES..META_PAGES + 8]);
        let leaf_pages = rd_u64(&meta[META_LEAF_PAGES..META_LEAF_PAGES + 8]);
        let leaf_entry_bytes = rd_u64(&meta[META_LEAF_ENTRY_BYTES..META_LEAF_ENTRY_BYTES + 8]);
        let free_pages = rd_u64(&meta[META_FREE_PAGES..META_FREE_PAGES + 8]);
        self.stats = BPlusTreeStats {
            entries,
            pages,
            free_pages,
            leaf_pages,
            leaf_entry_bytes,
        };
    }

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

    fn init_leaf(&mut self, page: u64, root: bool) {
        let buf = self.page_bytes_mut(page);
        buf[0] = PAGE_LEAF;
        buf[1] = if root { FLAG_ROOT } else { 0 };
        wr_u16(&mut buf[2..4], 0);
        wr_u32(&mut buf[4..8], PAGE_SIZE as u32);
        wr_u64(&mut buf[8..16], 0);
        wr_u64(&mut buf[16..24], 0);
        self.inc_leaf_pages();
    }

    fn init_internal(&mut self, page: u64) {
        let buf = self.page_bytes_mut(page);
        buf[0] = PAGE_INTERNAL;
        buf[1] = 0;
        wr_u16(&mut buf[2..4], 0);
        wr_u32(&mut buf[4..8], PAGE_SIZE as u32);
        wr_u64(&mut buf[8..16], 0);
    }

    fn maybe_compact(&mut self) -> io::Result<()> {
        let threshold = self.config.compaction_ratio;
        if threshold >= 1.0 {
            return Ok(());
        }
        if self.fragmentation_ratio() >= threshold {
            self.do_compact()?;
        }
        Ok(())
    }

    // ---- byte-level write path --------------------------------------------

    /// Insert or overwrite `(key_bytes, value_bytes)`. If `hint` is provided,
    /// skips the descent — the caller already navigated. Callers encode the
    /// value into a scratch buffer once and hand the resulting slice here;
    /// this routine never re-encodes.
    fn insert_bytes(
        &mut self,
        key: &[u8],
        value_bytes: &[u8],
        hint: Option<DescentHint>,
    ) -> io::Result<bool> {
        let DescentHint {
            leaf_page,
            path,
            slot,
        } = hint.unwrap_or_else(|| self.descend_to_leaf(key));

        let existed = slot.is_ok();
        let split = self.leaf_insert_bytes(leaf_page, key, value_bytes, Some(slot))?;
        if !existed {
            self.inc_entries(1);
        }
        if let Some(si) = split {
            self.cascade_split(path, si)?;
        }
        Ok(existed)
    }

    fn write_leaf_entry_raw(
        &mut self,
        page: u64,
        nd: usize,
        key: &[u8],
        value_bytes: &[u8],
        is_ovfl: bool,
    ) -> io::Result<()> {
        if is_ovfl {
            // alloc + write extent first (mutates other pages), then back
            // to writing this leaf entry.
            let extent_start = self.write_extent(value_bytes)?;
            let leaf = self.page_bytes_mut(page);
            wr_u16(&mut leaf[nd..nd + 2], key.len() as u16);
            wr_u32(
                &mut leaf[nd + 2..nd + 6],
                value_bytes.len() as u32 | OVFL_FLAG,
            );
            leaf[nd + 6..nd + 6 + key.len()].copy_from_slice(key);
            wr_u64(
                &mut leaf[nd + 6 + key.len()..nd + 6 + key.len() + 8],
                extent_start,
            );
        } else {
            let leaf = self.page_bytes_mut(page);
            wr_u16(&mut leaf[nd..nd + 2], key.len() as u16);
            wr_u32(&mut leaf[nd + 2..nd + 6], value_bytes.len() as u32);
            leaf[nd + 6..nd + 6 + key.len()].copy_from_slice(key);
            leaf[nd + 6 + key.len()..nd + 6 + key.len() + value_bytes.len()]
                .copy_from_slice(value_bytes);
        }
        Ok(())
    }

    fn delete_bytes(&mut self, key: &[u8]) -> io::Result<bool> {
        let leaf_page = self.find_leaf(self.root(), key);
        let count = rd_u16(&self.page_bytes(leaf_page)[2..4]) as usize;
        match self.leaf_find_slot(leaf_page, count, key) {
            Ok(i) => {
                self.delete_at(leaf_page, i);
                Ok(true)
            }
            Err(_) => Ok(false),
        }
    }

    /// Remove the entry at `slot` of `leaf_page`. Caller has already
    /// resolved the slot, so unlike [`delete_bytes`] this skips the
    /// descent + `leaf_find_slot`. Used by descent-aware overrides that
    /// already hold a `DescentHint` (`update`'s delete branch).
    ///
    /// Does not unlink an emptied leaf — see [`first_leaf`] for the
    /// consequences and [`leftmost_nonempty_leaf`] for how callers cope.
    fn delete_at(&mut self, leaf_page: u64, slot: usize) {
        let (count, data_off) = {
            let leaf = self.page_bytes(leaf_page);
            (rd_u16(&leaf[2..4]) as usize, rd_u32(&leaf[4..8]) as usize)
        };
        let (raw_vl, extent_start) = {
            let leaf = self.page_bytes(leaf_page);
            let sp = LEAF_HEADER_SIZE + slot * SLOT_SIZE;
            let eo = rd_u16(&leaf[sp..sp + 2]) as usize;
            let raw_vl = rd_u32(&leaf[eo + 2..eo + 6]);
            let ext = if raw_vl & OVFL_FLAG != 0 {
                let kl = rd_u16(&leaf[eo..eo + 2]) as usize;
                rd_u64(&leaf[eo + 6 + kl..eo + 6 + kl + 8])
            } else {
                0
            };
            (raw_vl, ext)
        };
        if raw_vl & OVFL_FLAG != 0 {
            self.free_extent(extent_start, raw_vl & !OVFL_FLAG);
        }
        self.leaf_remove_entry(leaf_page, slot, count, data_off);
        self.dec_entries();
    }

    // ---- leaf-level mutations ---------------------------------------------

    /// Place `(key, value)` into the leaf at `page`. `slot_hint`, when
    /// set, short-circuits the per-call `leaf_find_slot` — used by the
    /// descent-aware overrides (`put_if_absent`, `replace`, `update`,
    /// and the `put` path via `insert_typed`) to avoid re-running the
    /// binary search the descent already produced.
    fn leaf_insert_bytes(
        &mut self,
        page: u64,
        key: &[u8],
        value_bytes: &[u8],
        slot_hint: Option<Result<usize, usize>>,
    ) -> io::Result<Option<PageSplit>> {
        let (count, data_off) = {
            let leaf = self.page_bytes(page);
            (rd_u16(&leaf[2..4]) as usize, rd_u32(&leaf[4..8]) as usize)
        };

        let v_size = value_bytes.len();
        let ovfl = key.len() + v_size + 6 > MAX_INLINE;
        let leaf_es = if ovfl {
            6 + key.len() + 8
        } else {
            6 + key.len() + v_size
        };
        let needed = leaf_es + SLOT_SIZE;
        let free_start = LEAF_HEADER_SIZE + count * SLOT_SIZE;

        let slot = slot_hint.unwrap_or_else(|| self.leaf_find_slot(page, count, key));
        match slot {
            Ok(i) => {
                let (raw_vl, kl, eo, old_extent) = {
                    let leaf = self.page_bytes(page);
                    let sp = LEAF_HEADER_SIZE + i * SLOT_SIZE;
                    let eo = rd_u16(&leaf[sp..sp + 2]) as usize;
                    let kl = rd_u16(&leaf[eo..eo + 2]) as usize;
                    let raw_vl = rd_u32(&leaf[eo + 2..eo + 6]);
                    let old_extent = if raw_vl & OVFL_FLAG != 0 {
                        Some(rd_u64(&leaf[eo + 6 + kl..eo + 6 + kl + 8]))
                    } else {
                        None
                    };
                    (raw_vl, kl, eo, old_extent)
                };
                if let Some(old_start) = old_extent {
                    let old_len = raw_vl & !OVFL_FLAG;
                    self.free_extent(old_start, old_len);
                }
                // Fast path: same-size inline overwrite.
                if raw_vl & OVFL_FLAG == 0 && !ovfl && raw_vl as usize == v_size {
                    let leaf = self.page_bytes_mut(page);
                    let v_off = eo + 6 + kl;
                    leaf[v_off..v_off + v_size].copy_from_slice(value_bytes);
                    return Ok(None);
                }
                self.leaf_remove_entry(page, i, count, data_off);
                // After removal, the new entry's insertion point is
                // exactly the freed slot `i` — pass that to the
                // recursive call so it skips a redundant
                // `leaf_find_slot`.
                self.leaf_insert_bytes(page, key, value_bytes, Some(Err(i)))
            }
            Err(pos) => {
                if free_start + needed > data_off {
                    return self.leaf_split(page, key, value_bytes);
                }

                // Shift slots right.
                {
                    let leaf = self.page_bytes_mut(page);
                    let ss = LEAF_HEADER_SIZE;
                    for j in (pos..count).rev() {
                        let v = rd_u16(&leaf[ss + j * SLOT_SIZE..ss + j * SLOT_SIZE + 2]);
                        wr_u16(
                            &mut leaf[ss + (j + 1) * SLOT_SIZE..ss + (j + 1) * SLOT_SIZE + 2],
                            v,
                        );
                    }
                }
                let nd = data_off - leaf_es;
                self.write_leaf_entry_raw(page, nd, key, value_bytes, ovfl)?;
                {
                    let leaf = self.page_bytes_mut(page);
                    let ss = LEAF_HEADER_SIZE;
                    wr_u16(
                        &mut leaf[ss + pos * SLOT_SIZE..ss + pos * SLOT_SIZE + 2],
                        nd as u16,
                    );
                    wr_u16(&mut leaf[2..4], (count + 1) as u16);
                    wr_u32(&mut leaf[4..8], nd as u32);
                }
                let payload = if ovfl { 8 } else { v_size };
                self.add_leaf_entry_bytes(leaf_entry_bytes(key.len(), payload));
                Ok(None)
            }
        }
    }

    /// Append an entry to `page` by memcpy'ing one already-encoded slot
    /// out of a scratch buffer. The slot layout
    /// `[key_len: u16][value_len: u32][key][payload]` is identical in the
    /// source and destination pages — for extent entries `payload` is
    /// the 8-byte start pointer, for inline it's the value bytes. Either
    /// way the extent allocation in the file is shared, never duplicated.
    fn leaf_append_from_scratch_slot(&mut self, page: u64, scratch: &[u8], slot_eo: usize) {
        let kl = rd_u16(&scratch[slot_eo..slot_eo + 2]) as usize;
        let raw_vl = rd_u32(&scratch[slot_eo + 2..slot_eo + 6]);
        let payload_bytes = if raw_vl & OVFL_FLAG != 0 {
            8
        } else {
            raw_vl as usize
        };
        let (count, data_off) = {
            let leaf = self.page_bytes(page);
            (rd_u16(&leaf[2..4]) as usize, rd_u32(&leaf[4..8]) as usize)
        };
        let leaf_es = 6 + kl + payload_bytes;
        let nd = data_off - leaf_es;
        {
            let leaf = self.page_bytes_mut(page);
            leaf[nd..nd + leaf_es].copy_from_slice(&scratch[slot_eo..slot_eo + leaf_es]);
            let ss = LEAF_HEADER_SIZE;
            wr_u16(
                &mut leaf[ss + count * SLOT_SIZE..ss + count * SLOT_SIZE + 2],
                nd as u16,
            );
            wr_u16(&mut leaf[2..4], (count + 1) as u16);
            wr_u32(&mut leaf[4..8], nd as u32);
        }
    }

    /// Append a fresh inline `(key, value)` to `page`. Escalates to an
    /// extent if the entry would exceed `MAX_INLINE`. Used by `leaf_split`
    /// to place the new entry that triggered the split.
    fn leaf_append_inline_entry(&mut self, page: u64, key: &[u8], value: &[u8]) -> io::Result<()> {
        let is_ovfl = key.len() + value.len() + 6 > MAX_INLINE;
        let (count, data_off) = {
            let leaf = self.page_bytes(page);
            (rd_u16(&leaf[2..4]) as usize, rd_u32(&leaf[4..8]) as usize)
        };
        let leaf_es = if is_ovfl {
            6 + key.len() + 8
        } else {
            6 + key.len() + value.len()
        };
        let nd = data_off - leaf_es;
        // Entry write (handles inline + extent) is shared with the typed
        // insert path via `write_leaf_entry_raw`. This wrapper only adds the
        // slot-array bookkeeping that the appending caller needs.
        self.write_leaf_entry_raw(page, nd, key, value, is_ovfl)?;
        let leaf = self.page_bytes_mut(page);
        let ss = LEAF_HEADER_SIZE;
        wr_u16(
            &mut leaf[ss + count * SLOT_SIZE..ss + count * SLOT_SIZE + 2],
            nd as u16,
        );
        wr_u16(&mut leaf[2..4], (count + 1) as u16);
        wr_u32(&mut leaf[4..8], nd as u32);
        Ok(())
    }

    fn leaf_split(&mut self, page: u64, key: &[u8], value: &[u8]) -> io::Result<Option<PageSplit>> {
        // Snapshot the original page into a single page-sized pooled
        // scratch buffer. Old-entry reads during the repack borrow from
        // `scratch`, leaving us free to reset & rewrite the live page
        // without aliasing. Replaces the previous per-entry
        // `Vec<(Vec<u8>, SplitVal)>` snapshot — one heap acquire instead
        // of N.
        let mut scratch = PooledBuf::acquire();
        scratch.resize(PAGE_SIZE, 0);
        scratch[..PAGE_SIZE].copy_from_slice(self.page_bytes(page));

        let count = rd_u16(&scratch[2..4]) as usize;
        // Slot-offset sentinel that doesn't collide with any real slot
        // entry offset (which fit in u16). Used in the order list to
        // mark where the new entry should be placed.
        const NEW_SLOT: usize = usize::MAX;

        // Binary search the new entry's insertion position in scratch.
        let ip = {
            let mut lo = 0usize;
            let mut hi = count;
            while lo < hi {
                let m = lo + (hi - lo) / 2;
                let sp = LEAF_HEADER_SIZE + m * SLOT_SIZE;
                let eo = rd_u16(&scratch[sp..sp + 2]) as usize;
                let kl = rd_u16(&scratch[eo..eo + 2]) as usize;
                match scratch[eo + 6..eo + 6 + kl].cmp(key) {
                    std::cmp::Ordering::Less => lo = m + 1,
                    _ => hi = m,
                }
            }
            lo
        };

        // Order of slot offsets in ascending key order. NEW_SLOT marks
        // the new entry's logical position.
        let mut order: Vec<usize> = Vec::with_capacity(count + 1);
        for i in 0..ip {
            let sp = LEAF_HEADER_SIZE + i * SLOT_SIZE;
            order.push(rd_u16(&scratch[sp..sp + 2]) as usize);
        }
        order.push(NEW_SLOT);
        for i in ip..count {
            let sp = LEAF_HEADER_SIZE + i * SLOT_SIZE;
            order.push(rd_u16(&scratch[sp..sp + 2]) as usize);
        }

        let new_ovfl = key.len() + value.len() + 6 > MAX_INLINE;
        let new_payload = if new_ovfl { 8 } else { value.len() };
        let new_entry_bytes = leaf_entry_bytes(key.len(), new_payload);
        let new_size_in_page = 6 + key.len() + new_payload + SLOT_SIZE;

        // Determine split mid by cumulative byte cost.
        let entry_size = |slot_eo: usize| -> usize {
            if slot_eo == NEW_SLOT {
                new_size_in_page
            } else {
                let kl = rd_u16(&scratch[slot_eo..slot_eo + 2]) as usize;
                let raw_vl = rd_u32(&scratch[slot_eo + 2..slot_eo + 6]);
                let payload = if raw_vl & OVFL_FLAG != 0 {
                    8
                } else {
                    raw_vl as usize
                };
                6 + kl + payload + SLOT_SIZE
            }
        };
        let key_bytes_of = |slot_eo: usize| -> &[u8] {
            if slot_eo == NEW_SLOT {
                key
            } else {
                let kl = rd_u16(&scratch[slot_eo..slot_eo + 2]) as usize;
                &scratch[slot_eo + 6..slot_eo + 6 + kl]
            }
        };

        // First, check whether the merged set fits in a single page — if so,
        // re-pack only and skip the split. Otherwise pick `mid` at the byte
        // midpoint so both halves get ~50% load. The previous "fill left
        // until overflow" policy collapsed for prepend-heavy workloads:
        // the new entry plus the smallest old entries fully repacked the
        // original page, the next prepend hit the same full page, and the
        // tree degenerated to ~1 entry per leaf.
        let total: usize = order.iter().map(|&s| entry_size(s)).sum();

        // Reset the page header for fresh packing.
        {
            let leaf = self.page_bytes_mut(page);
            wr_u16(&mut leaf[2..4], 0);
            wr_u32(&mut leaf[4..8], PAGE_SIZE as u32);
        }

        if total <= PAGE_SIZE - LEAF_HEADER_SIZE {
            // Everything fits in the original page; just re-pack.
            for &slot_eo in &order {
                if slot_eo == NEW_SLOT {
                    self.leaf_append_inline_entry(page, key, value)?;
                } else {
                    self.leaf_append_from_scratch_slot(page, &scratch, slot_eo);
                }
            }
            self.add_leaf_entry_bytes(new_entry_bytes);
            return Ok(None);
        }

        let half = total / 2;
        let mid = {
            let mut used = 0usize;
            let mut m = 1usize;
            for (j, &slot_eo) in order.iter().enumerate() {
                used += entry_size(slot_eo);
                if used >= half {
                    m = j + 1;
                    break;
                }
            }
            m.clamp(1, order.len() - 1)
        };

        // Compute the separator before allocating the right page —
        // `truncated_separator` is a free function so the `scratch`
        // borrows end at the semicolon and don't conflict with the
        // `&mut self` calls that follow.
        let sep = truncated_separator(key_bytes_of(order[mid - 1]), key_bytes_of(order[mid]));

        let right = self.alloc_page()?;
        self.init_leaf(right, false);

        for &slot_eo in &order[..mid] {
            if slot_eo == NEW_SLOT {
                self.leaf_append_inline_entry(page, key, value)?;
            } else {
                self.leaf_append_from_scratch_slot(page, &scratch, slot_eo);
            }
        }
        for &slot_eo in &order[mid..] {
            if slot_eo == NEW_SLOT {
                self.leaf_append_inline_entry(right, key, value)?;
            } else {
                self.leaf_append_from_scratch_slot(right, &scratch, slot_eo);
            }
        }
        self.add_leaf_entry_bytes(new_entry_bytes);

        // Link prev/next pointers (read old_next from scratch — the live
        // page's header has been reset).
        let old_next = rd_u64(&scratch[8..16]);
        {
            let r = self.page_bytes_mut(right);
            wr_u64(&mut r[8..16], old_next);
            wr_u64(&mut r[16..24], page);
        }
        {
            let l = self.page_bytes_mut(page);
            wr_u64(&mut l[8..16], right);
        }
        if old_next != 0 {
            let r2 = self.page_bytes_mut(old_next);
            wr_u64(&mut r2[16..24], right);
        } else {
            self.set_rightmost_leaf(right);
        }

        Ok(Some(PageSplit {
            left_page: page,
            right_page: right,
            separator_key: sep,
        }))
    }

    fn leaf_remove_entry(&mut self, page: u64, pos: usize, count: usize, data_off: usize) {
        let (eo_rel, kl, raw_vl) = {
            let leaf = self.page_bytes(page);
            let sp = LEAF_HEADER_SIZE + pos * SLOT_SIZE;
            let eo_rel = rd_u16(&leaf[sp..sp + 2]) as usize;
            let kl = rd_u16(&leaf[eo_rel..eo_rel + 2]) as usize;
            let raw_vl = rd_u32(&leaf[eo_rel + 2..eo_rel + 6]) as usize;
            (eo_rel, kl, raw_vl)
        };
        let payload = if raw_vl & OVFL_FLAG as usize != 0 {
            8
        } else {
            raw_vl
        };
        let es = 6 + kl + payload;
        let removed = leaf_entry_bytes(kl, payload);
        self.sub_leaf_entry_bytes(removed);

        let leaf = self.page_bytes_mut(page);
        let ss = LEAF_HEADER_SIZE;
        // Shift slots [pos+1..count) left by one.
        for j in pos + 1..count {
            let v = rd_u16(&leaf[ss + j * SLOT_SIZE..ss + j * SLOT_SIZE + 2]);
            wr_u16(
                &mut leaf[ss + (j - 1) * SLOT_SIZE..ss + (j - 1) * SLOT_SIZE + 2],
                v,
            );
        }
        // Slide data gap right over the removed bytes.
        let dlen = eo_rel - data_off;
        leaf.copy_within(data_off..data_off + dlen, data_off + es);
        for j in 0..count - 1 {
            let mut e = rd_u16(&leaf[ss + j * SLOT_SIZE..ss + j * SLOT_SIZE + 2]) as usize;
            if e <= eo_rel {
                e += es;
            }
            wr_u16(
                &mut leaf[ss + j * SLOT_SIZE..ss + j * SLOT_SIZE + 2],
                e as u16,
            );
        }
        wr_u16(&mut leaf[2..4], (count - 1) as u16);
        wr_u32(&mut leaf[4..8], (data_off + es) as u32);
    }

    // ---- internal page mutations + cascade --------------------------------

    fn cascade_split(&mut self, path: DescentPath, split: PageSplit) -> io::Result<()> {
        let (mut sep, mut left, mut right) =
            (split.separator_key, split.left_page, split.right_page);
        for pg in path.drain_rev() {
            match self.internal_insert(pg, &sep, right)? {
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
        {
            let buf = self.page_bytes_mut(nr);
            wr_u64(&mut buf[8..16], left);
        }
        self.internal_insert(nr, &sep, right)?;
        {
            let buf = self.page_bytes_mut(nr);
            buf[1] |= FLAG_ROOT;
        }
        {
            let l = self.page_bytes_mut(left);
            l[1] &= !FLAG_ROOT;
        }
        self.set_root(nr);
        Ok(())
    }

    fn internal_insert(
        &mut self,
        page: u64,
        key: &[u8],
        child: u64,
    ) -> io::Result<Option<PageSplit>> {
        let (count, data_off) = {
            let buf = self.page_bytes(page);
            (rd_u16(&buf[2..4]) as usize, rd_u32(&buf[4..8]) as usize)
        };
        let es = 10 + key.len();
        let free_start = HEADER_SIZE + count * SLOT_SIZE;
        if free_start + es + SLOT_SIZE > data_off {
            return self.internal_split(page, key, child);
        }

        // Find insertion point via binary search on separator keys.
        let pos = {
            let buf = self.page_bytes(page);
            let mut lo = 0usize;
            let mut hi = count;
            while lo < hi {
                let mid = lo + (hi - lo) / 2;
                let sp = HEADER_SIZE + mid * SLOT_SIZE;
                let eo = rd_u16(&buf[sp..sp + 2]) as usize;
                let kl = rd_u16(&buf[eo..eo + 2]) as usize;
                if key < &buf[eo + 10..eo + 10 + kl] {
                    hi = mid;
                } else {
                    lo = mid + 1;
                }
            }
            lo
        };
        // Shift higher slots and write the entry.
        let buf = self.page_bytes_mut(page);
        let ss = HEADER_SIZE;
        for j in (pos..count).rev() {
            let v = rd_u16(&buf[ss + j * SLOT_SIZE..ss + j * SLOT_SIZE + 2]);
            wr_u16(
                &mut buf[ss + (j + 1) * SLOT_SIZE..ss + (j + 1) * SLOT_SIZE + 2],
                v,
            );
        }
        let nd = data_off - es;
        wr_u16(&mut buf[nd..nd + 2], key.len() as u16);
        wr_u64(&mut buf[nd + 2..nd + 10], child);
        buf[nd + 10..nd + 10 + key.len()].copy_from_slice(key);
        wr_u16(
            &mut buf[ss + pos * SLOT_SIZE..ss + pos * SLOT_SIZE + 2],
            nd as u16,
        );
        wr_u16(&mut buf[2..4], (count + 1) as u16);
        wr_u32(&mut buf[4..8], nd as u32);
        Ok(None)
    }

    /// Append an `(separator_key, child)` pair at the **tail** of an
    /// internal page whose slots are already in ascending key order.
    ///
    /// Unlike [`internal_insert`], this skips the binary-search for the
    /// insertion position and the slot-shift that follows — the caller
    /// is responsible for the sort invariant. Used by `internal_split`
    /// to repack from a pre-sorted vector in O(n) instead of O(n²).
    fn internal_append_entry(&mut self, page: u64, key: &[u8], child: u64) {
        let (count, data_off) = {
            let buf = self.page_bytes(page);
            (rd_u16(&buf[2..4]) as usize, rd_u32(&buf[4..8]) as usize)
        };
        let es = 10 + key.len();
        let nd = data_off - es;
        let buf = self.page_bytes_mut(page);
        wr_u16(&mut buf[nd..nd + 2], key.len() as u16);
        wr_u64(&mut buf[nd + 2..nd + 10], child);
        buf[nd + 10..nd + 10 + key.len()].copy_from_slice(key);
        let ss = HEADER_SIZE;
        wr_u16(
            &mut buf[ss + count * SLOT_SIZE..ss + count * SLOT_SIZE + 2],
            nd as u16,
        );
        wr_u16(&mut buf[2..4], (count + 1) as u16);
        wr_u32(&mut buf[4..8], nd as u32);
    }

    /// Append an internal entry from a scratch buffer slot via one memcpy.
    /// Entry layout `[key_len: u16][child: u64][separator_key]` is
    /// position-stable, so no per-field rewrite is needed.
    fn internal_append_from_scratch_slot(&mut self, page: u64, scratch: &[u8], slot_eo: usize) {
        let kl = rd_u16(&scratch[slot_eo..slot_eo + 2]) as usize;
        let (count, data_off) = {
            let buf = self.page_bytes(page);
            (rd_u16(&buf[2..4]) as usize, rd_u32(&buf[4..8]) as usize)
        };
        let es = 10 + kl;
        let nd = data_off - es;
        let buf = self.page_bytes_mut(page);
        buf[nd..nd + es].copy_from_slice(&scratch[slot_eo..slot_eo + es]);
        let ss = HEADER_SIZE;
        wr_u16(
            &mut buf[ss + count * SLOT_SIZE..ss + count * SLOT_SIZE + 2],
            nd as u16,
        );
        wr_u16(&mut buf[2..4], (count + 1) as u16);
        wr_u32(&mut buf[4..8], nd as u32);
    }

    fn internal_split(
        &mut self,
        page: u64,
        key: &[u8],
        child: u64,
    ) -> io::Result<Option<PageSplit>> {
        // Snapshot the original page into a single pooled scratch
        // buffer — one allocation instead of N per-key `Vec<u8>`s.
        let mut scratch = PooledBuf::acquire();
        scratch.resize(PAGE_SIZE, 0);
        scratch[..PAGE_SIZE].copy_from_slice(self.page_bytes(page));

        let count = rd_u16(&scratch[2..4]) as usize;
        const NEW_SLOT: usize = usize::MAX;

        let ip = {
            let mut lo = 0usize;
            let mut hi = count;
            while lo < hi {
                let m = lo + (hi - lo) / 2;
                let sp = HEADER_SIZE + m * SLOT_SIZE;
                let eo = rd_u16(&scratch[sp..sp + 2]) as usize;
                let kl = rd_u16(&scratch[eo..eo + 2]) as usize;
                if key < &scratch[eo + 10..eo + 10 + kl] {
                    hi = m;
                } else {
                    lo = m + 1;
                }
            }
            lo
        };

        let mut order: Vec<usize> = Vec::with_capacity(count + 1);
        for i in 0..ip {
            let sp = HEADER_SIZE + i * SLOT_SIZE;
            order.push(rd_u16(&scratch[sp..sp + 2]) as usize);
        }
        order.push(NEW_SLOT);
        for i in ip..count {
            let sp = HEADER_SIZE + i * SLOT_SIZE;
            order.push(rd_u16(&scratch[sp..sp + 2]) as usize);
        }

        let key_bytes_of = |slot_eo: usize| -> &[u8] {
            if slot_eo == NEW_SLOT {
                key
            } else {
                let kl = rd_u16(&scratch[slot_eo..slot_eo + 2]) as usize;
                &scratch[slot_eo + 10..slot_eo + 10 + kl]
            }
        };
        let child_of = |slot_eo: usize| -> u64 {
            if slot_eo == NEW_SLOT {
                child
            } else {
                rd_u64(&scratch[slot_eo + 2..slot_eo + 10])
            }
        };
        let entry_size = |slot_eo: usize| -> usize { 10 + key_bytes_of(slot_eo).len() + SLOT_SIZE };

        // See `leaf_split` for why splitting at the byte midpoint matters —
        // the previous "fill left until overflow" policy degenerated for
        // prepend-heavy separator inserts.
        let total: usize = order.iter().map(|&s| entry_size(s)).sum();

        // Reset header. The leftmost_child pointer at [8..16] is
        // preserved (we only zero count + data_off).
        {
            let buf = self.page_bytes_mut(page);
            wr_u16(&mut buf[2..4], 0);
            wr_u32(&mut buf[4..8], PAGE_SIZE as u32);
        }

        // Append-only repack — `order` is already in ascending key order.
        if total <= PAGE_SIZE - HEADER_SIZE {
            for &slot_eo in &order {
                if slot_eo == NEW_SLOT {
                    self.internal_append_entry(page, key, child);
                } else {
                    self.internal_append_from_scratch_slot(page, &scratch, slot_eo);
                }
            }
            return Ok(None);
        }

        let half = total / 2;
        let mid = {
            let mut used = 0usize;
            let mut m = 1usize;
            for (j, &slot_eo) in order.iter().enumerate() {
                used += entry_size(slot_eo);
                if used >= half {
                    m = j + 1;
                    break;
                }
            }
            m.clamp(1, order.len() - 1)
        };

        let sep = truncated_separator(key_bytes_of(order[mid - 1]), key_bytes_of(order[mid]));
        // The entry at `mid` is the "lifted" entry — its child becomes
        // the right page's leftmost child, and only its separator key
        // is stored in the parent.
        let right_leftmost = child_of(order[mid]);

        let right = self.alloc_page()?;
        self.init_internal(right);
        {
            let r = self.page_bytes_mut(right);
            wr_u64(&mut r[8..16], right_leftmost);
        }

        for &slot_eo in &order[..mid] {
            if slot_eo == NEW_SLOT {
                self.internal_append_entry(page, key, child);
            } else {
                self.internal_append_from_scratch_slot(page, &scratch, slot_eo);
            }
        }
        for &slot_eo in &order[mid + 1..] {
            if slot_eo == NEW_SLOT {
                self.internal_append_entry(right, key, child);
            } else {
                self.internal_append_from_scratch_slot(right, &scratch, slot_eo);
            }
        }
        Ok(Some(PageSplit {
            left_page: page,
            right_page: right,
            separator_key: sep,
        }))
    }

    // ---- compact (streaming shadow rebuild) -------------------------------

    fn do_compact(&mut self) -> io::Result<()> {
        let tmp_path = compact_temp_path();
        let result = (|| -> io::Result<()> {
            let mut shadow = BPlusTree::<K, V>::create(
                &tmp_path,
                BPlusTreeConfig {
                    initial_capacity_pages: 2,
                    compaction_ratio: 1.0,
                },
            )?;
            shadow.bottom_up_build_raw(RawBTreeEntries::new(self, self.first_leaf()).map(Ok))?;
            let used = (shadow.pages_counter() as usize) * PAGE_SIZE;
            self.mmap[..used].copy_from_slice(&shadow.mmap[..used]);
            self.stats = shadow.stats.clone();

            // Shrink the backing file to the compacted size, but never below
            // the initial-capacity floor so the next writes don't immediately
            // re-grow the file.
            let new_pages = shadow
                .pages_counter()
                .max(self.config.initial_capacity_pages);
            let new_size = (new_pages as usize) * PAGE_SIZE;
            self.mmap = MmapMut::map_anon(1)?;
            self.file.set_len(new_size as u64)?;
            self.mmap = unsafe { MmapMut::map_mut(&self.file)? };

            Ok(())
        })();
        let _ = fs::remove_file(&tmp_path);
        result?;
        Ok(())
    }

    /// Reset tree to post-`create` state without going through the public
    /// `clear` (which has a tx debug_assert).
    fn do_clear(&mut self) -> io::Result<()> {
        {
            let meta = self.page_bytes_mut(META_PAGE);
            wr_u64(&mut meta[META_ROOT..META_ROOT + 8], 1);
            wr_u64(&mut meta[META_FREE_HEAD..META_FREE_HEAD + 8], 0);
            wr_u64(&mut meta[META_PAGES..META_PAGES + 8], 2);
            wr_u64(&mut meta[META_ENTRIES..META_ENTRIES + 8], 0);
            wr_u64(&mut meta[META_RIGHTMOST_LEAF..META_RIGHTMOST_LEAF + 8], 1);
            wr_u64(&mut meta[META_LEAF_PAGES..META_LEAF_PAGES + 8], 1);
            wr_u64(
                &mut meta[META_LEAF_ENTRY_BYTES..META_LEAF_ENTRY_BYTES + 8],
                0,
            );
            wr_u64(&mut meta[META_FREE_PAGES..META_FREE_PAGES + 8], 0);
        }
        self.stats = BPlusTreeStats {
            entries: 0,
            pages: 2,
            free_pages: 0,
            leaf_pages: 1,
            leaf_entry_bytes: 0,
        };
        // Re-initialize page 1 as the root leaf.
        let r = self.page_bytes_mut(1);
        r[0] = PAGE_LEAF;
        r[1] = FLAG_ROOT;
        wr_u16(&mut r[2..4], 0);
        wr_u32(&mut r[4..8], PAGE_SIZE as u32);
        wr_u64(&mut r[8..16], 0);
        wr_u64(&mut r[16..24], 0);
        Ok(())
    }

    // ---- bottom-up bulk build --------------------------------------------

    /// Build the tree bottom-up from raw `(key_bytes, value_bytes)` pairs
    /// in ascending key order. Tree must be empty on entry.
    ///
    /// Items are wrapped in `io::Result` so a streaming encoder (used by
    /// `bulk_put_sorted`) can surface bincode errors without materializing
    /// the whole encoded dataset upfront. Infallible sources (the
    /// `do_compact` shadow-rebuild walks `RawBTreeEntries`, which yields
    /// borrowed page slices) wrap each item with `Ok`.
    fn bottom_up_build_raw<I, K2, V2>(&mut self, items: I) -> io::Result<()>
    where
        I: IntoIterator<Item = io::Result<(K2, V2)>>,
        K2: AsRef<[u8]>,
        V2: AsRef<[u8]>,
    {
        // Tree must be empty: `do_clear()` produces this state.
        debug_assert!(self.size() == 0, "bottom_up_build requires empty tree");

        let mut current_leaf: u64 = 1; // root after do_clear
        let mut leaves: Vec<u64> = vec![current_leaf];
        // First key of each leaf: used as the smallest key reachable
        // through each internal-level child pointer. Pushed when a leaf
        // is finalized.
        let mut leaf_first_keys: Vec<Vec<u8>> = Vec::new();
        // Last key of each leaf: used as the left side of
        // `truncated_separator` when computing internal separators.
        // Using the FIRST key would yield separators too short to
        // correctly route lookups whose keys are between the leaf's
        // first and last key but share a prefix with the next leaf's
        // first key.
        let mut leaf_last_keys: Vec<Vec<u8>> = Vec::new();
        let mut current_first_key: Option<Vec<u8>> = None;
        let mut current_last_key: Option<Vec<u8>> = None;
        let mut entry_count: u64 = 0;

        for item in items {
            let (k, v) = item?;
            entry_count += 1;
            let kb = k.as_ref();
            let vb = v.as_ref();
            let v_len = vb.len();
            let ovfl = kb.len() + v_len + 6 > MAX_INLINE;
            let leaf_es = if ovfl {
                6 + kb.len() + 8
            } else {
                6 + kb.len() + v_len
            };
            let needed = leaf_es + SLOT_SIZE;

            let (count, data_off) = {
                let leaf = self.page_bytes(current_leaf);
                (rd_u16(&leaf[2..4]) as usize, rd_u32(&leaf[4..8]) as usize)
            };
            let free_start = LEAF_HEADER_SIZE + count * SLOT_SIZE;
            if free_start + needed > data_off {
                // Finalize current leaf, allocate a new one.
                if let Some(fk) = current_first_key.take() {
                    leaf_first_keys.push(fk);
                }
                if let Some(lk) = current_last_key.take() {
                    leaf_last_keys.push(lk);
                }
                let new_leaf = self.alloc_page()?;
                self.init_leaf(new_leaf, false);
                // Link prev_leaf / next_leaf.
                {
                    let cur = self.page_bytes_mut(current_leaf);
                    wr_u64(&mut cur[8..16], new_leaf);
                }
                {
                    let nl = self.page_bytes_mut(new_leaf);
                    wr_u64(&mut nl[16..24], current_leaf);
                }
                leaves.push(new_leaf);
                current_leaf = new_leaf;
            }
            if current_first_key.is_none() {
                current_first_key = Some(kb.to_vec());
            }
            // Insert (always at the tail of the current leaf).
            let (count, data_off) = {
                let leaf = self.page_bytes(current_leaf);
                (rd_u16(&leaf[2..4]) as usize, rd_u32(&leaf[4..8]) as usize)
            };
            let nd = data_off - leaf_es;
            // Write entry payload via the same helper that handles extents.
            self.write_leaf_entry_raw(current_leaf, nd, kb, vb, ovfl)?;
            {
                let leaf = self.page_bytes_mut(current_leaf);
                let ss = LEAF_HEADER_SIZE;
                wr_u16(
                    &mut leaf[ss + count * SLOT_SIZE..ss + count * SLOT_SIZE + 2],
                    nd as u16,
                );
                wr_u16(&mut leaf[2..4], (count + 1) as u16);
                wr_u32(&mut leaf[4..8], nd as u32);
            }
            let payload = if ovfl { 8 } else { v_len };
            self.add_leaf_entry_bytes(leaf_entry_bytes(kb.len(), payload));
            // Track this entry's key as the (provisional) last key of
            // the current leaf — refined on every subsequent insert.
            current_last_key = Some(kb.to_vec());
        }
        // Finalize the last leaf's first / last keys.
        if let Some(fk) = current_first_key.take() {
            leaf_first_keys.push(fk);
        }
        if let Some(lk) = current_last_key.take() {
            leaf_last_keys.push(lk);
        }
        // Set rightmost_leaf and inc_entries.
        self.set_rightmost_leaf(current_leaf);
        self.inc_entries(entry_count);

        // Build internal levels bottom-up.
        if leaves.len() > 1 {
            self.build_internal_levels(leaves, leaf_first_keys, leaf_last_keys)?;
        } else {
            // Single leaf is the root — root remains page 1 (set by do_clear).
            // Ensure FLAG_ROOT is set.
            let r = self.page_bytes_mut(1);
            r[1] |= FLAG_ROOT;
        }
        Ok(())
    }

    /// Construct internal levels from a sorted list of leaf (or internal)
    /// page numbers and the first/last keys reachable through each.
    ///
    /// `first_keys[i]` and `last_keys[i]` describe the range of keys
    /// reachable through `children[i]`. Separator keys between adjacent
    /// children are computed as `truncated_separator(last_keys[i-1],
    /// first_keys[i])` so lookups correctly route any key in the left
    /// child's range — including keys that share a prefix with
    /// `first_keys[i]` but are actually less than it.
    fn build_internal_levels(
        &mut self,
        mut children: Vec<u64>,
        mut first_keys: Vec<Vec<u8>>,
        mut last_keys: Vec<Vec<u8>>,
    ) -> io::Result<()> {
        debug_assert_eq!(children.len(), first_keys.len());
        debug_assert_eq!(children.len(), last_keys.len());
        debug_assert!(children.len() > 1);

        while children.len() > 1 {
            let mut next_pages: Vec<u64> = Vec::new();
            let mut next_first_keys: Vec<Vec<u8>> = Vec::new();
            let mut next_last_keys: Vec<Vec<u8>> = Vec::new();
            let mut i = 0usize;
            while i < children.len() {
                let current = self.alloc_page()?;
                self.init_internal(current);
                // leftmost_child is children[i].
                {
                    let buf = self.page_bytes_mut(current);
                    wr_u64(&mut buf[8..16], children[i]);
                }
                let group_start = i;
                next_first_keys.push(first_keys[group_start].clone());
                next_pages.push(current);
                i += 1;
                // Pack subsequent (separator, child) into the same page
                // while space allows.
                while i < children.len() {
                    // Use the LAST key of the previous child so the
                    // separator strictly exceeds every key in that
                    // child. Using `first_keys[i-1]` would produce a
                    // separator too short to distinguish keys in the
                    // left child whose bytes match the prefix at the
                    // divergence point with `first_keys[i]`.
                    let sep = truncated_separator(&last_keys[i - 1], &first_keys[i]);
                    let es = 10 + sep.len();
                    let (count, data_off) = {
                        let buf = self.page_bytes(current);
                        (rd_u16(&buf[2..4]) as usize, rd_u32(&buf[4..8]) as usize)
                    };
                    let free_start = HEADER_SIZE + count * SLOT_SIZE;
                    if free_start + es + SLOT_SIZE > data_off {
                        break;
                    }
                    let nd = data_off - es;
                    let child_page = children[i];
                    let buf = self.page_bytes_mut(current);
                    wr_u16(&mut buf[nd..nd + 2], sep.len() as u16);
                    wr_u64(&mut buf[nd + 2..nd + 10], child_page);
                    buf[nd + 10..nd + 10 + sep.len()].copy_from_slice(&sep);
                    let ss = HEADER_SIZE;
                    wr_u16(
                        &mut buf[ss + count * SLOT_SIZE..ss + count * SLOT_SIZE + 2],
                        nd as u16,
                    );
                    wr_u16(&mut buf[2..4], (count + 1) as u16);
                    wr_u32(&mut buf[4..8], nd as u32);
                    i += 1;
                }
                // The last child packed into this internal page is at
                // index `i - 1`; its last_key bounds the range of this
                // internal page.
                next_last_keys.push(last_keys[i - 1].clone());
            }
            children = next_pages;
            first_keys = next_first_keys;
            last_keys = next_last_keys;
        }
        // children.len() == 1; that's the new root.
        let root = children[0];
        // Mark FLAG_ROOT, clear it on the old root leaf (page 1) if root != 1.
        {
            let r = self.page_bytes_mut(root);
            r[1] |= FLAG_ROOT;
        }
        if root != 1 {
            let old = self.page_bytes_mut(1);
            old[1] &= !FLAG_ROOT;
        }
        self.set_root(root);
        Ok(())
    }
}

// ===========================================================================
// Backend impl — all public method bodies live here.
// ===========================================================================

impl<K, V> Backend<K, V> for BPlusTree<K, V>
where
    K: Encode + Decode<()> + Hash + Eq + Clone,
    V: Encode + Decode<()> + Clone,
{
    type Stats<'a>
        = &'a BPlusTreeStats
    where
        Self: 'a;
    type Config = BPlusTreeConfig;

    fn get(&self, key: &K) -> Option<Cow<'_, V>> {
        with_scratch(key, |kb| {
            Ok(self
                .value_bytes_for(kb)
                .map(|slice| deserialize_from::<V>(slice).expect("valid encoded value")))
        })
        .ok()
        .flatten()
        .map(Cow::Owned)
    }

    fn contains(&self, key: &K) -> bool {
        with_scratch(key, |kb| Ok(self.value_bytes_for(kb).is_some())).unwrap_or(false)
    }

    fn put(&mut self, key: K, value: V) -> io::Result<()> {
        with_two_scratches(&key, &value, |kb, vb| {
            self.insert_bytes(kb, vb, None).map(|_| ())
        })?;
        self.maybe_compact()?;
        Ok(())
    }

    /// One descent: probe via `descend_to_leaf`; if found, return false
    /// without writing. Otherwise pass the descent to `insert_bytes`.
    fn put_if_absent(&mut self, key: K, value: V) -> io::Result<bool> {
        let inserted = with_two_scratches(&key, &value, |kb, vb| {
            let hint = self.descend_to_leaf(kb);
            if hint.slot.is_ok() {
                return Ok(false);
            }
            self.insert_bytes(kb, vb, Some(hint))?;
            Ok(true)
        })?;
        if inserted {
            self.maybe_compact()?;
        }
        Ok(inserted)
    }

    /// One descent: probe + read old value via the descent's slot; then
    /// pass the same descent to `insert_bytes`. Returned value is
    /// always `Cow::Owned` — decoded out of the page bytes before the
    /// slot gets overwritten.
    fn replace(&mut self, key: K, value: V) -> io::Result<Option<Cow<'_, V>>> {
        let prev: Option<V> = with_two_scratches(&key, &value, |kb, vb| {
            let hint = self.descend_to_leaf(kb);
            let prev = if let Ok(slot) = hint.slot {
                let bytes = self.value_bytes_at(hint.leaf_page, slot);
                Some(deserialize_from(bytes)?)
            } else {
                None
            };
            self.insert_bytes(kb, vb, Some(hint))?;
            Ok(prev)
        })?;
        self.maybe_compact()?;
        Ok(prev.map(Cow::Owned))
    }

    fn delete(&mut self, key: &K) -> io::Result<bool> {
        let deleted = with_scratch(key, |kb| self.delete_bytes(kb))?;
        if deleted {
            self.maybe_compact()?;
        }
        Ok(deleted)
    }

    /// One descent that's reused for the read AND the write. Reads the
    /// current value via the descent's slot, then routes to
    /// [`insert_bytes`] (overwrite/insert) or [`delete_at`] (delete) —
    /// both consume the resolved `(leaf_page, slot)` directly so the
    /// descent isn't repeated.
    fn update<F>(&mut self, key: &K, f: F) -> io::Result<()>
    where
        F: FnOnce(Option<V>) -> Option<V>,
    {
        with_scratch(key, |kb| {
            let hint = self.descend_to_leaf(kb);
            let current: Option<V> = if let Ok(slot) = hint.slot {
                let bytes = self.value_bytes_at(hint.leaf_page, slot);
                Some(deserialize_from(bytes)?)
            } else {
                None
            };
            let had_value = current.is_some();
            match (had_value, f(current)) {
                (_, Some(new_v)) => {
                    with_scratch(&new_v, |vb| self.insert_bytes(kb, vb, Some(hint)))?;
                    self.maybe_compact()?;
                }
                (true, None) => {
                    let slot = hint.slot.expect("had_value ⇒ slot is Ok");
                    self.delete_at(hint.leaf_page, slot);
                    self.maybe_compact()?;
                }
                (false, None) => {}
            }
            Ok(())
        })
    }

    /// Auto-tx wrapped loop of `put`. If the caller already has a tx
    /// open, joins it; otherwise opens one and closes on return.
    fn bulk_put<I>(&mut self, items: I) -> io::Result<()>
    where
        I: IntoIterator<Item = (K, V)>,
    {
        for (k, v) in items {
            self.put(k, v)?;
        }
        Ok(())
    }

    /// Bottom-up bulk-load when the tree is empty. Falls back to
    /// `bulk_put` when non-empty.
    ///
    /// Streams: keys/values are encoded on the fly by the iterator passed
    /// to `bottom_up_build_raw`, so peak memory is one `(Vec<u8>,
    /// Vec<u8>)` pair rather than the whole encoded dataset. Encoding
    /// errors short-circuit the build via the `Result`-bearing iterator.
    fn bulk_put_sorted<I>(&mut self, sorted: I) -> io::Result<()>
    where
        I: IntoIterator<Item = (K, V)>,
    {
        if self.size() != 0 {
            return self.bulk_put(sorted);
        }
        let encoded = sorted
            .into_iter()
            .map(|(k, v)| Ok((serialize_to_vec(&k)?, serialize_to_vec(&v)?)));
        self.bottom_up_build_raw(encoded)
    }

    fn bulk_delete<'a, I>(&mut self, keys: I) -> io::Result<usize>
    where
        I: IntoIterator<Item = &'a K>,
        K: 'a,
    {
        let mut n: usize = 0;
        for k in keys {
            if self.delete(k)? {
                n += 1;
            }
        }
        Ok(n)
    }

    /// Sorted bulk-delete: single leaf-chain pass + rewrite per leaf,
    /// free emptied leaves. After the walk, rebuild internal levels from
    /// the surviving leaves' first keys to eliminate stale references.
    fn bulk_delete_sorted<'a, I>(&mut self, sorted: I) -> io::Result<usize>
    where
        I: IntoIterator<Item = &'a K>,
        K: 'a,
    {
        // Pre-encode keys.
        let mut targets: Vec<Vec<u8>> = Vec::new();
        for k in sorted {
            targets.push(serialize_to_vec(k)?);
        }
        if targets.is_empty() {
            return Ok(0);
        }

        // Walk the leaf chain, removing matched entries in-place.
        let mut removed: usize = 0;
        let mut target_idx = 0usize;
        let mut leaf_page = self.first_leaf();
        while leaf_page != 0 && target_idx < targets.len() {
            let next_leaf = rd_u64(&self.page_bytes(leaf_page)[8..16]);
            let mut count = rd_u16(&self.page_bytes(leaf_page)[2..4]) as usize;
            let mut i = 0usize;
            while i < count && target_idx < targets.len() {
                let key_slice = self.key_bytes_at(leaf_page, i).to_vec();
                // Advance target_idx past keys < current entry key.
                while target_idx < targets.len()
                    && targets[target_idx].as_slice() < key_slice.as_slice()
                {
                    target_idx += 1;
                }
                if target_idx < targets.len()
                    && targets[target_idx].as_slice() == key_slice.as_slice()
                {
                    // Match: free extent if any, then remove.
                    let data_off = rd_u32(&self.page_bytes(leaf_page)[4..8]) as usize;
                    let (raw_vl, kl, ext) = {
                        let leaf = self.page_bytes(leaf_page);
                        let sp = LEAF_HEADER_SIZE + i * SLOT_SIZE;
                        let eo = rd_u16(&leaf[sp..sp + 2]) as usize;
                        let kl = rd_u16(&leaf[eo..eo + 2]) as usize;
                        let raw_vl = rd_u32(&leaf[eo + 2..eo + 6]);
                        let ext = if raw_vl & OVFL_FLAG != 0 {
                            rd_u64(&leaf[eo + 6 + kl..eo + 6 + kl + 8])
                        } else {
                            0
                        };
                        (raw_vl, kl, ext)
                    };
                    if raw_vl & OVFL_FLAG != 0 {
                        self.free_extent(ext, raw_vl & !OVFL_FLAG);
                        let _ = kl;
                    }
                    self.leaf_remove_entry(leaf_page, i, count, data_off);
                    self.dec_entries();
                    removed += 1;
                    count -= 1;
                    target_idx += 1;
                    // Do NOT increment i — the slots shifted left.
                } else {
                    i += 1;
                }
            }
            leaf_page = next_leaf;
        }

        // Rebuild internal levels from surviving leaves.
        if removed > 0 {
            self.rebuild_internal_from_leaves()?;
        }
        Ok(removed)
    }

    fn clear(&mut self) -> io::Result<()> {
        self.do_clear()
    }

    fn compact(&mut self) -> io::Result<()> {
        self.do_compact()
    }

    fn keys<'a>(&'a self) -> impl Iterator<Item = Cow<'a, K>> + 'a
    where
        K: 'a,
    {
        self.entries().map(|(k, _)| k)
    }

    fn values<'a>(&'a self) -> impl Iterator<Item = Cow<'a, V>> + 'a
    where
        V: 'a,
    {
        self.entries().map(|(_, v)| v)
    }

    fn entries<'a>(&'a self) -> impl Iterator<Item = (Cow<'a, K>, Cow<'a, V>)> + 'a
    where
        K: 'a,
        V: 'a,
    {
        let page = self.first_leaf();
        let count = if page == 0 {
            0
        } else {
            rd_u16(&self.page_bytes(page)[2..4]) as usize
        };
        BTreeIter {
            tree: self,
            page,
            slot: 0,
            count,
        }
    }

    fn size(&self) -> usize {
        self.stats.entries
    }

    fn stats(&self) -> Self::Stats<'_> {
        &self.stats
    }

    fn config(&self) -> &Self::Config {
        &self.config
    }

    /// Schedule mmap writeback asynchronously.
    fn flush(&self) -> io::Result<()> {
        self.mmap.flush_async()
    }

    /// Block until pending mmap writes have been flushed.
    fn sync(&self) -> io::Result<()> {
        self.mmap.flush()
    }
}

// ===========================================================================
// OrderedBackend impl
// ===========================================================================

impl<K, V> OrderedBackend<K, V> for BPlusTree<K, V>
where
    K: Encode + Decode<()> + Hash + Eq + Clone + Ord,
    V: Encode + Decode<()> + Clone,
{
    fn range<'a>(
        &'a self,
        start: &K,
        end: &K,
    ) -> impl Iterator<Item = (Cow<'a, K>, Cow<'a, V>)> + 'a
    where
        K: 'a,
        V: 'a,
    {
        let Ok(start_bytes) = serialize_to_vec(start) else {
            return BTreeRange {
                tree: self,
                page: 0,
                slot: 0,
                end: Vec::new(),
                count: 0,
            };
        };
        let Ok(end_bytes) = serialize_to_vec(end) else {
            return BTreeRange {
                tree: self,
                page: 0,
                slot: 0,
                end: Vec::new(),
                count: 0,
            };
        };
        let leaf = self.find_leaf(self.root(), &start_bytes);
        let count = rd_u16(&self.page_bytes(leaf)[2..4]) as usize;
        let slot = match self.leaf_find_slot(leaf, count, &start_bytes) {
            Ok(i) | Err(i) => i,
        };
        BTreeRange {
            tree: self,
            page: leaf,
            slot,
            end: end_bytes,
            count,
        }
    }

    /// Smallest live entry. Walks right from the leftmost leaf via
    /// [`leftmost_nonempty_leaf`] to skip leaves that `delete` may have
    /// drained but not unlinked.
    fn first<'a>(&'a self) -> Option<(Cow<'a, K>, Cow<'a, V>)>
    where
        K: 'a,
        V: 'a,
    {
        if self.size() == 0 {
            return None;
        }
        let page = self.leftmost_nonempty_leaf()?;
        let k: K = deserialize_from(self.key_bytes_at(page, 0)).expect("valid encoded key");
        let v: V = deserialize_from(self.value_bytes_at(page, 0)).expect("valid encoded value");
        Some((Cow::Owned(k), Cow::Owned(v)))
    }

    /// Largest live entry. Mirror of [`first`] — walks left via
    /// [`rightmost_nonempty_leaf`].
    fn last<'a>(&'a self) -> Option<(Cow<'a, K>, Cow<'a, V>)>
    where
        K: 'a,
        V: 'a,
    {
        if self.size() == 0 {
            return None;
        }
        let page = self.rightmost_nonempty_leaf()?;
        let last_slot = rd_u16(&self.page_bytes(page)[2..4]) as usize - 1;
        let k: K = deserialize_from(self.key_bytes_at(page, last_slot)).expect("valid encoded key");
        let v: V =
            deserialize_from(self.value_bytes_at(page, last_slot)).expect("valid encoded value");
        Some((Cow::Owned(k), Cow::Owned(v)))
    }

    /// Streaming reverse iteration via `prev_leaf`.
    fn entries_rev<'a>(&'a self) -> impl Iterator<Item = (Cow<'a, K>, Cow<'a, V>)> + 'a
    where
        K: 'a,
        V: 'a,
    {
        let page = if self.size() == 0 {
            0
        } else {
            self.rightmost_leaf()
        };
        let count = if page == 0 {
            0
        } else {
            rd_u16(&self.page_bytes(page)[2..4]) as usize
        };
        BTreeIterRev {
            tree: self,
            page,
            slot: 0,
            count,
        }
    }

    /// Streaming `[start, end)` in descending order.
    fn range_rev<'a>(
        &'a self,
        start: &K,
        end: &K,
    ) -> impl Iterator<Item = (Cow<'a, K>, Cow<'a, V>)> + 'a
    where
        K: 'a,
        V: 'a,
    {
        let Ok(start_bytes) = serialize_to_vec(start) else {
            return BTreeRangeRev {
                tree: self,
                page: 0,
                slot_from_end: 0,
                start: Vec::new(),
                count: 0,
            };
        };
        let Ok(end_bytes) = serialize_to_vec(end) else {
            return BTreeRangeRev {
                tree: self,
                page: 0,
                slot_from_end: 0,
                start: Vec::new(),
                count: 0,
            };
        };
        // Find the leaf containing the predecessor of `end`.
        let leaf = self.find_leaf(self.root(), &end_bytes);
        let count = rd_u16(&self.page_bytes(leaf)[2..4]) as usize;
        // Insertion point for `end` in this leaf — the first slot whose
        // key is >= end. Reverse iter starts from slot (i - 1) of this
        // leaf, or descends to prev_leaf's last slot if i == 0.
        let i = match self.leaf_find_slot(leaf, count, &end_bytes) {
            Ok(i) | Err(i) => i,
        };
        if i == 0 {
            let prev = rd_u64(&self.page_bytes(leaf)[16..24]);
            if prev == 0 {
                BTreeRangeRev {
                    tree: self,
                    page: 0,
                    slot_from_end: 0,
                    start: start_bytes,
                    count: 0,
                }
            } else {
                let prev_count = rd_u16(&self.page_bytes(prev)[2..4]) as usize;
                BTreeRangeRev {
                    tree: self,
                    page: prev,
                    slot_from_end: 0,
                    start: start_bytes,
                    count: prev_count,
                }
            }
        } else {
            // Start at slot i-1 of `leaf`, which means skipping
            // (count - i) entries from the end.
            BTreeRangeRev {
                tree: self,
                page: leaf,
                slot_from_end: count - i,
                start: start_bytes,
                count,
            }
        }
    }
}

// ---------------------------------------------------------------------------
// internal-level rebuild (used by bulk_delete_sorted)
// ---------------------------------------------------------------------------

impl<K, V> BPlusTree<K, V>
where
    K: Encode + Decode<()> + Hash + Eq + Clone,
    V: Encode + Decode<()> + Clone,
{
    /// Walk the leaf chain, free every internal page, then rebuild the
    /// internal levels from the surviving leaves. Used by
    /// `bulk_delete_sorted` to eliminate stale internal-entry references
    /// to freed leaves.
    fn rebuild_internal_from_leaves(&mut self) -> io::Result<()> {
        // Collect surviving leaves + first/last keys per leaf (skip
        // empty leaves and chain-unlink them).
        let mut leaves: Vec<u64> = Vec::new();
        let mut first_keys: Vec<Vec<u8>> = Vec::new();
        let mut last_keys: Vec<Vec<u8>> = Vec::new();
        let mut leaf_page = self.first_leaf();
        let mut prev_nonempty: u64 = 0;
        while leaf_page != 0 {
            let count = rd_u16(&self.page_bytes(leaf_page)[2..4]) as usize;
            let next = rd_u64(&self.page_bytes(leaf_page)[8..16]);
            if count == 0 {
                // Free the empty leaf and unlink from chain.
                if prev_nonempty != 0 {
                    let prev_buf = self.page_bytes_mut(prev_nonempty);
                    wr_u64(&mut prev_buf[8..16], next);
                }
                if next != 0 {
                    let next_buf = self.page_bytes_mut(next);
                    wr_u64(&mut next_buf[16..24], prev_nonempty);
                }
                self.free_page(leaf_page);
                self.dec_leaf_pages();
            } else {
                let fk = self.key_bytes_at(leaf_page, 0).to_vec();
                let lk = self.key_bytes_at(leaf_page, count - 1).to_vec();
                leaves.push(leaf_page);
                first_keys.push(fk);
                last_keys.push(lk);
                prev_nonempty = leaf_page;
            }
            leaf_page = next;
        }
        // If no leaves survived, leave one empty root leaf in place
        // (matches do_clear's state).
        if leaves.is_empty() {
            // Allocate a fresh empty leaf as the root.
            let p = self.alloc_page()?;
            self.init_leaf(p, true);
            self.set_root(p);
            self.set_rightmost_leaf(p);
            return Ok(());
        }
        // Update rightmost_leaf.
        self.set_rightmost_leaf(*leaves.last().unwrap());
        // Free all old internal pages reachable from the root. Easier:
        // we'll allocate fresh internals for the rebuild. Old internals
        // will become orphaned but the freelist will collect them as
        // they're freed.
        //
        // Walk root → internals (NOT into leaves) and free each. The
        // current `root` may be a leaf (single-leaf tree); skip the walk
        // in that case.
        let old_root = self.root();
        if old_root != leaves[0] && old_root != self.rightmost_leaf() {
            // Free internal pages by DFS from old_root. The starting
            // page must be internal here (we'd have returned early on a
            // single-leaf tree above), and children are filtered against
            // their page type before being pushed — leaves never enter
            // the stack, so we don't waste a pop just to discard them.
            let mut stack: Vec<u64> = vec![old_root];
            let mut seen: HashMap<u64, ()> = HashMap::new();
            while let Some(p) = stack.pop() {
                if seen.contains_key(&p) {
                    continue;
                }
                seen.insert(p, ());
                // Collect this internal page's internal children only.
                let internal_children: Vec<u64> = {
                    let buf = self.page_bytes(p);
                    debug_assert_eq!(buf[0], PAGE_INTERNAL);
                    let count = rd_u16(&buf[2..4]) as usize;
                    let leftmost = rd_u64(&buf[8..16]);
                    let mut all: Vec<u64> = Vec::with_capacity(count + 1);
                    all.push(leftmost);
                    for i in 0..count {
                        let sp = HEADER_SIZE + i * SLOT_SIZE;
                        let eo = rd_u16(&buf[sp..sp + 2]) as usize;
                        all.push(rd_u64(&buf[eo + 2..eo + 10]));
                    }
                    // Keep only children that are themselves internal.
                    all.into_iter()
                        .filter(|&c| self.page_bytes(c)[0] == PAGE_INTERNAL)
                        .collect()
                };
                for c in internal_children {
                    stack.push(c);
                }
                self.free_page(p);
            }
        }
        // Build internal levels from surviving leaves.
        if leaves.len() == 1 {
            // Single leaf becomes the root.
            let only = leaves[0];
            {
                let buf = self.page_bytes_mut(only);
                buf[1] |= FLAG_ROOT;
            }
            self.set_root(only);
        } else {
            self.build_internal_levels(leaves, first_keys, last_keys)?;
        }
        Ok(())
    }
}

// ===========================================================================
// Iterators
// ===========================================================================

pub struct BTreeIter<'a, K, V> {
    tree: &'a BPlusTree<K, V>,
    page: u64,
    slot: usize,
    /// Cached entry count for `page`. Refreshed on every page transition;
    /// avoids the per-`next()` header read.
    count: usize,
}

impl<'a, K, V> Iterator for BTreeIter<'a, K, V>
where
    K: Decode<()> + Clone,
    V: Decode<()> + Clone,
{
    type Item = (Cow<'a, K>, Cow<'a, V>);
    fn next(&mut self) -> Option<Self::Item> {
        while self.page != 0 {
            if self.slot < self.count {
                let k: K = deserialize_from(self.tree.key_bytes_at(self.page, self.slot))
                    .expect("valid encoded key");
                let v: V = deserialize_from(self.tree.value_bytes_at(self.page, self.slot))
                    .expect("valid encoded value");
                self.slot += 1;
                return Some((Cow::Owned(k), Cow::Owned(v)));
            }
            self.page = rd_u64(&self.tree.page_bytes(self.page)[8..16]);
            self.slot = 0;
            self.count = if self.page == 0 {
                0
            } else {
                rd_u16(&self.tree.page_bytes(self.page)[2..4]) as usize
            };
        }
        None
    }
}

pub struct BTreeIterRev<'a, K, V> {
    tree: &'a BPlusTree<K, V>,
    page: u64,
    slot: usize, // walks backward: 0 = last slot of page
    /// Cached entry count for `page`. Refreshed on every page transition.
    count: usize,
}

impl<'a, K, V> Iterator for BTreeIterRev<'a, K, V>
where
    K: Decode<()> + Clone,
    V: Decode<()> + Clone,
{
    type Item = (Cow<'a, K>, Cow<'a, V>);
    fn next(&mut self) -> Option<Self::Item> {
        while self.page != 0 {
            if self.slot < self.count {
                let idx = self.count - 1 - self.slot;
                let k: K = deserialize_from(self.tree.key_bytes_at(self.page, idx))
                    .expect("valid encoded key");
                let v: V = deserialize_from(self.tree.value_bytes_at(self.page, idx))
                    .expect("valid encoded value");
                self.slot += 1;
                return Some((Cow::Owned(k), Cow::Owned(v)));
            }
            self.page = rd_u64(&self.tree.page_bytes(self.page)[16..24]);
            self.slot = 0;
            self.count = if self.page == 0 {
                0
            } else {
                rd_u16(&self.tree.page_bytes(self.page)[2..4]) as usize
            };
        }
        None
    }
}

pub struct BTreeRange<'a, K, V> {
    tree: &'a BPlusTree<K, V>,
    page: u64,
    slot: usize,
    end: Vec<u8>,
    /// Cached entry count for `page`. Refreshed on every page transition.
    count: usize,
}

impl<'a, K, V> Iterator for BTreeRange<'a, K, V>
where
    K: Decode<()> + Clone,
    V: Decode<()> + Clone,
{
    type Item = (Cow<'a, K>, Cow<'a, V>);
    fn next(&mut self) -> Option<Self::Item> {
        while self.page != 0 {
            if self.slot < self.count {
                let ks = self.tree.key_bytes_at(self.page, self.slot);
                if ks >= self.end.as_slice() {
                    return None;
                }
                let k: K = deserialize_from(ks).expect("valid encoded key");
                let v: V = deserialize_from(self.tree.value_bytes_at(self.page, self.slot))
                    .expect("valid encoded value");
                self.slot += 1;
                return Some((Cow::Owned(k), Cow::Owned(v)));
            }
            self.page = rd_u64(&self.tree.page_bytes(self.page)[8..16]);
            self.slot = 0;
            self.count = if self.page == 0 {
                0
            } else {
                rd_u16(&self.tree.page_bytes(self.page)[2..4]) as usize
            };
        }
        None
    }
}

pub struct BTreeRangeRev<'a, K, V> {
    tree: &'a BPlusTree<K, V>,
    page: u64,
    /// Number of slots already yielded from the current page, counted
    /// from the end. `count - 1 - slot_from_end` is the next slot to
    /// yield.
    slot_from_end: usize,
    start: Vec<u8>,
    /// Cached entry count for `page`. Refreshed on every page transition.
    count: usize,
}

impl<'a, K, V> Iterator for BTreeRangeRev<'a, K, V>
where
    K: Decode<()> + Clone,
    V: Decode<()> + Clone,
{
    type Item = (Cow<'a, K>, Cow<'a, V>);
    fn next(&mut self) -> Option<Self::Item> {
        while self.page != 0 {
            if self.slot_from_end < self.count {
                let idx = self.count - 1 - self.slot_from_end;
                let ks = self.tree.key_bytes_at(self.page, idx);
                if ks < self.start.as_slice() {
                    return None;
                }
                let k: K = deserialize_from(ks).expect("valid encoded key");
                let v: V = deserialize_from(self.tree.value_bytes_at(self.page, idx))
                    .expect("valid encoded value");
                self.slot_from_end += 1;
                return Some((Cow::Owned(k), Cow::Owned(v)));
            }
            self.page = rd_u64(&self.tree.page_bytes(self.page)[16..24]);
            self.slot_from_end = 0;
            self.count = if self.page == 0 {
                0
            } else {
                rd_u16(&self.tree.page_bytes(self.page)[2..4]) as usize
            };
        }
        None
    }
}

// ---------------------------------------------------------------------------
// Drop / Debug
// ---------------------------------------------------------------------------

impl<K, V> Drop for BPlusTree<K, V> {
    /// Schedule a final writeback without blocking. We don't promise
    /// crash recovery; callers that need durability should call
    /// `Backend::sync` explicitly before dropping.
    fn drop(&mut self) {
        let _ = self.mmap.flush_async();
    }
}

impl<K, V> fmt::Debug for BPlusTree<K, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.stats, f)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
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
    fn vget(t: &BPlusTree<TestKey, TestVal>, key: &TestKey) -> Option<Vec<u8>> {
        t.get(key).map(Cow::into_owned)
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
        let keys: Vec<Vec<u8>> = t.entries().map(|(k, _)| k.into_owned()).collect();
        assert_eq!(keys, vec![k("a"), k("b"), k("c")]);
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
            .map(|(k, _)| String::from_utf8(k.into_owned()).unwrap())
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
        let keys: Vec<Vec<u8>> = t
            .range(&k("a"), &k("c"))
            .map(|(k, _)| k.into_owned())
            .collect();
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

    // -- extents --

    #[test]
    fn extent_value_round_trip() {
        let p = tmp("ext");
        let mut t = create(&p);
        let big = vec![0xABu8; 10_000];
        t.put(k("big"), big.clone()).unwrap();
        t.flush().unwrap();
        assert_eq!(vget(&t, &k("big")), Some(big.clone()));
        let bigger = vec![0xCDu8; 15_000];
        t.put(k("big"), bigger.clone()).unwrap();
        assert_eq!(vget(&t, &k("big")), Some(bigger.clone()));
        assert!(t.delete(&k("big")).unwrap());
        assert_eq!(vget(&t, &k("big")), None);
        assert_eq!(t.size(), 0);
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

    #[test]
    fn extent_pages_reuse_freed_contiguous_run() {
        let p = tmp("extent_reuse");
        let mut t = BPlusTree::create(
            &p,
            BPlusTreeConfig {
                initial_capacity_pages: 64,
                compaction_ratio: 1.0,
            },
        )
        .unwrap();
        let big = vec![0xAA; PAGE_SIZE * 3 + 17];
        t.put(k("big"), big).unwrap();
        let pages_after_first = t.pages();
        assert!(t.delete(&k("big")).unwrap());
        assert!(t.stats().free_pages >= 4);
        t.put(k("big2"), vec![0xBB; PAGE_SIZE * 3 + 11]).unwrap();
        assert_eq!(
            t.pages(),
            pages_after_first,
            "extent allocation should reuse the freed contiguous run"
        );
    }

    #[test]
    fn reopen_uses_persisted_stats_counters() {
        let p = tmp("persisted_stats");
        let stats_before = {
            let mut t = create(&p);
            for i in 0u32..200 {
                t.put(format!("k{:04}", i).into_bytes(), vec![0xCC; 64])
                    .unwrap();
            }
            for i in 0u32..50 {
                assert!(t.delete(&format!("k{:04}", i).into_bytes()).unwrap());
            }
            t.flush().unwrap();
            t.stats().clone()
        };
        let t: BPlusTree<TestKey, TestVal> = open(&p);
        assert_eq!(t.stats(), &stats_before);
    }

    #[test]
    fn truncated_separator_basic() {
        assert_eq!(truncated_separator(b"abc", b"abd"), b"abd".to_vec());
        assert_eq!(
            truncated_separator(b"abc", b"abcd"),
            vec![b'a', b'b', b'c', 0]
        );
        assert_eq!(truncated_separator(b"hello", b"world"), b"w".to_vec());
    }

    // -- clear / compact --

    #[test]
    fn clear_removes_all_entries() {
        let p = tmp("clear");
        let mut t = create(&p);
        t.put(k("a"), vbytes("av")).unwrap();
        t.put(k("b"), vbytes("bv")).unwrap();
        assert_eq!(t.size(), 2);
        t.clear().unwrap();
        assert_eq!(t.size(), 0);
        assert!(t.is_empty());
        assert_eq!(vget(&t, &k("a")), None);
    }

    #[test]
    fn compact_rebuilds_tree() {
        let p = tmp("compact");
        let mut t = BPlusTree::create(
            &p,
            BPlusTreeConfig {
                initial_capacity_pages: 64,
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
        let before = t.pages();
        t.compact().unwrap();
        assert!(t.pages() <= before);
        assert_eq!(t.size(), 100);
        for i in 0u32..100 {
            assert_eq!(
                vget(&t, &format!("k{:04}", i).into_bytes()),
                Some(vec![0xAB; 32])
            );
        }
    }

    // -- put_if_absent / replace --

    #[test]
    fn put_if_absent_inserts_when_missing() {
        let p = tmp("pia_insert");
        let mut t = create(&p);
        assert!(Backend::put_if_absent(&mut t, k("a"), vbytes("first")).unwrap());
        assert_eq!(vget(&t, &k("a")), Some(vbytes("first")));
    }

    #[test]
    fn put_if_absent_noop_when_present() {
        let p = tmp("pia_noop");
        let mut t = create(&p);
        t.put(k("a"), vbytes("first")).unwrap();
        assert!(!Backend::put_if_absent(&mut t, k("a"), vbytes("second")).unwrap());
        assert_eq!(vget(&t, &k("a")), Some(vbytes("first")));
    }

    #[test]
    fn replace_returns_previous_value() {
        let p = tmp("replace");
        let mut t = create(&p);
        t.put(k("a"), vbytes("first")).unwrap();
        let prev = Backend::replace(&mut t, k("a"), vbytes("second"))
            .unwrap()
            .map(Cow::into_owned);
        assert_eq!(prev, Some(vbytes("first")));
        assert_eq!(vget(&t, &k("a")), Some(vbytes("second")));
    }

    #[test]
    fn replace_returns_none_when_absent() {
        let p = tmp("replace_absent");
        let mut t = create(&p);
        let prev = Backend::replace(&mut t, k("a"), vbytes("new")).unwrap();
        assert!(prev.is_none());
        assert_eq!(vget(&t, &k("a")), Some(vbytes("new")));
    }

    // -- bulk --

    #[test]
    fn bulk_put_inserts_all_items() {
        let p = tmp("bulk_put");
        let mut t = create(&p);
        let items: Vec<(TestKey, TestVal)> = (0u32..50)
            .map(|i| (format!("k{:04}", i).into_bytes(), vbytes("v")))
            .collect();
        Backend::bulk_put(&mut t, items).unwrap();
        assert_eq!(t.size(), 50);
    }

    #[test]
    fn bulk_put_sorted_empty_tree_bottom_up() {
        let p = tmp("bulk_put_sorted_empty");
        let mut t = create(&p);
        let items: Vec<(TestKey, TestVal)> = (0u32..500)
            .map(|i| (format!("k{:04}", i).into_bytes(), vbytes("v")))
            .collect();
        Backend::bulk_put_sorted(&mut t, items).unwrap();
        assert_eq!(t.size(), 500);
        // Verify lookups via tree navigation (not just leaf chain).
        for i in 0u32..500 {
            assert_eq!(
                vget(&t, &format!("k{:04}", i).into_bytes()),
                Some(vbytes("v"))
            );
        }
        // Iteration order is correct.
        let collected: Vec<Vec<u8>> = t.entries().map(|(k, _)| k.into_owned()).collect();
        assert_eq!(collected.len(), 500);
        for i in 0..500 {
            assert_eq!(collected[i], format!("k{:04}", i).into_bytes());
        }
    }

    #[test]
    fn bulk_put_sorted_single_entry() {
        let p = tmp("bulk_put_sorted_one");
        let mut t = create(&p);
        Backend::bulk_put_sorted(&mut t, vec![(k("only"), vbytes("v"))]).unwrap();
        assert_eq!(t.size(), 1);
        assert_eq!(vget(&t, &k("only")), Some(vbytes("v")));
    }

    #[test]
    fn bulk_put_sorted_falls_back_on_nonempty() {
        let p = tmp("bulk_put_sorted_nonempty");
        let mut t = create(&p);
        t.put(k("seed"), vbytes("s")).unwrap();
        let items: Vec<(TestKey, TestVal)> = (0u32..20)
            .map(|i| (format!("k{:04}", i).into_bytes(), vbytes("v")))
            .collect();
        Backend::bulk_put_sorted(&mut t, items).unwrap();
        assert_eq!(t.size(), 21);
        assert_eq!(vget(&t, &k("seed")), Some(vbytes("s")));
        assert_eq!(vget(&t, &k("k0000")), Some(vbytes("v")));
    }

    #[test]
    fn bulk_put_sorted_with_extents() {
        let p = tmp("bulk_put_sorted_ext");
        let mut t = create(&p);
        let big = vec![0xAA; 8000];
        let items: Vec<(TestKey, TestVal)> = (0u32..10)
            .map(|i| (format!("k{:04}", i).into_bytes(), big.clone()))
            .collect();
        Backend::bulk_put_sorted(&mut t, items).unwrap();
        assert_eq!(t.size(), 10);
        for i in 0u32..10 {
            assert_eq!(
                vget(&t, &format!("k{:04}", i).into_bytes()),
                Some(big.clone())
            );
        }
    }

    #[test]
    fn bulk_put_sorted_followed_by_reopen() {
        let p = tmp("bulk_put_sorted_reopen");
        {
            let mut t = create(&p);
            let items: Vec<(TestKey, TestVal)> = (0u32..200)
                .map(|i| (format!("k{:04}", i).into_bytes(), vbytes("v")))
                .collect();
            Backend::bulk_put_sorted(&mut t, items).unwrap();
            t.flush().unwrap();
        }
        let t: BPlusTree<TestKey, TestVal> = open(&p);
        assert_eq!(t.size(), 200);
        for i in 0u32..200 {
            assert_eq!(
                vget(&t, &format!("k{:04}", i).into_bytes()),
                Some(vbytes("v"))
            );
        }
    }

    #[test]
    fn bulk_delete_returns_removed_count() {
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

    #[test]
    fn bulk_delete_sorted_removes_hits_skips_misses() {
        let p = tmp("bulk_delete_sorted");
        let mut t = create(&p);
        for i in 0u32..50 {
            t.put(format!("k{:04}", i).into_bytes(), vbytes("v"))
                .unwrap();
        }
        // Delete every other key.
        let to_delete: Vec<TestKey> = (0u32..50)
            .filter(|i| i % 2 == 0)
            .map(|i| format!("k{:04}", i).into_bytes())
            .collect();
        // Plus some misses.
        let mut all: Vec<TestKey> = to_delete.clone();
        all.push(k("zzzz"));
        let n = Backend::bulk_delete_sorted(&mut t, all.iter()).unwrap();
        assert_eq!(n, 25);
        assert_eq!(t.size(), 25);
        // Verify survivors via tree navigation.
        for i in 0u32..50 {
            let key = format!("k{:04}", i).into_bytes();
            if i % 2 == 0 {
                assert_eq!(vget(&t, &key), None);
            } else {
                assert_eq!(vget(&t, &key), Some(vbytes("v")));
            }
        }
    }

    #[test]
    fn bulk_delete_sorted_followed_by_put_does_not_corrupt() {
        let p = tmp("bulk_delete_sorted_then_put");
        let mut t = create(&p);
        for i in 0u32..200 {
            t.put(format!("k{:04}", i).into_bytes(), vbytes("v"))
                .unwrap();
        }
        let to_delete: Vec<TestKey> = (0u32..150)
            .map(|i| format!("k{:04}", i).into_bytes())
            .collect();
        Backend::bulk_delete_sorted(&mut t, to_delete.iter()).unwrap();
        // Now insert new keys; should not conflict with stale internals.
        for i in 0u32..50 {
            t.put(format!("new{:04}", i).into_bytes(), vbytes("nv"))
                .unwrap();
        }
        // Verify both old survivors and new keys.
        for i in 150u32..200 {
            assert_eq!(
                vget(&t, &format!("k{:04}", i).into_bytes()),
                Some(vbytes("v"))
            );
        }
        for i in 0u32..50 {
            assert_eq!(
                vget(&t, &format!("new{:04}", i).into_bytes()),
                Some(vbytes("nv"))
            );
        }
    }

    // -- ordering --

    #[test]
    fn first_and_last_endpoints() {
        let p = tmp("first_last");
        let mut t = create(&p);
        for i in 0u32..30 {
            t.put(format!("k{:04}", i).into_bytes(), vbytes("v"))
                .unwrap();
        }
        let (fk, _) = OrderedBackend::first(&t).unwrap();
        let (lk, _) = OrderedBackend::last(&t).unwrap();
        assert_eq!(fk.into_owned(), k("k0000"));
        assert_eq!(lk.into_owned(), k("k0029"));
    }

    #[test]
    fn first_and_last_on_empty_tree() {
        let p = tmp("first_last_empty");
        let t = create(&p);
        assert!(OrderedBackend::first(&t).is_none());
        assert!(OrderedBackend::last(&t).is_none());
    }

    /// Regression: per-entry `delete` does not unlink emptied leaves
    /// (only `bulk_delete_sorted` does, via `rebuild_internal_from_leaves`),
    /// so the leaf at the end of the leftmost-child descent can be empty
    /// while `size() > 0`. `first()` must walk `next_leaf` past it; the
    /// previous implementation returned `None` and falsely reported an
    /// empty tree. `last()` is the symmetric case via `prev_leaf`.
    #[test]
    fn first_and_last_skip_empty_endpoint_leaves() {
        let p = tmp("first_last_skip_empty");
        let mut t = create(&p);
        // Span several leaves so deleting a prefix/suffix of keys can
        // fully drain the leaf at either end of the chain.
        for i in 0u32..500 {
            t.put(format!("k{:04}", i).into_bytes(), vbytes("v"))
                .unwrap();
        }
        // Drain the smallest 200 keys — definitely empties the leftmost
        // leaf (and likely more). first() must walk to k0200.
        for i in 0u32..200 {
            assert!(t.delete(&format!("k{:04}", i).into_bytes()).unwrap());
        }
        // Same on the upper end for last().
        for i in 300u32..500 {
            assert!(t.delete(&format!("k{:04}", i).into_bytes()).unwrap());
        }
        assert_eq!(t.size(), 100);
        let (fk, _) = OrderedBackend::first(&t).expect("first should find k0200");
        let (lk, _) = OrderedBackend::last(&t).expect("last should find k0299");
        assert_eq!(fk.into_owned(), k("k0200"));
        assert_eq!(lk.into_owned(), k("k0299"));
    }

    #[test]
    fn entries_rev_yields_descending_order() {
        let p = tmp("entries_rev");
        let mut t = create(&p);
        for i in 0u32..10 {
            t.put(format!("k{:04}", i).into_bytes(), vbytes("v"))
                .unwrap();
        }
        let keys: Vec<Vec<u8>> = OrderedBackend::entries_rev(&t)
            .map(|(k, _)| k.into_owned())
            .collect();
        let expected: Vec<Vec<u8>> = (0..10u32)
            .rev()
            .map(|i| format!("k{:04}", i).into_bytes())
            .collect();
        assert_eq!(keys, expected);
    }

    #[test]
    fn range_rev_descending() {
        let p = tmp("range_rev");
        let mut t = create(&p);
        for i in 0u32..20 {
            t.put(format!("k{:04}", i).into_bytes(), vbytes("v"))
                .unwrap();
        }
        let got: Vec<Vec<u8>> = OrderedBackend::range_rev(&t, &k("k0005"), &k("k0010"))
            .map(|(k, _)| k.into_owned())
            .collect();
        let expected: Vec<Vec<u8>> = (5..10u32)
            .rev()
            .map(|i| format!("k{:04}", i).into_bytes())
            .collect();
        assert_eq!(got, expected);
    }

    #[test]
    fn range_rev_empty_when_start_eq_end() {
        let p = tmp("range_rev_empty");
        let mut t = create(&p);
        for i in 0u32..5 {
            t.put(format!("k{:04}", i).into_bytes(), vbytes("v"))
                .unwrap();
        }
        let got: Vec<Vec<u8>> = OrderedBackend::range_rev(&t, &k("k0002"), &k("k0002"))
            .map(|(k, _)| k.into_owned())
            .collect();
        assert!(got.is_empty());
    }

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
    fn update_inserts_when_absent() {
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

    // -- additional coverage --

    #[test]
    fn bulk_put_sorted_routes_through_internal_pages() {
        // Verify lookups via internal-page navigation correctness for
        // every key in a multi-leaf tree, including keys whose bytes
        // share a prefix with adjacent leaves' boundaries.
        let p = tmp("bulk_put_sorted_internal_nav");
        let mut t = create(&p);
        let items: Vec<(TestKey, TestVal)> = (0u32..1000)
            .map(|i| (format!("k{:06}", i).into_bytes(), vbytes("v")))
            .collect();
        Backend::bulk_put_sorted(&mut t, items).unwrap();
        assert_eq!(t.size(), 1000);
        // Every key — including ones that fall on potential separator
        // boundaries (...0099, ...0100, etc.) — must round-trip via
        // tree navigation.
        for i in 0u32..1000 {
            let key = format!("k{:06}", i).into_bytes();
            assert_eq!(vget(&t, &key), Some(vbytes("v")), "missing i={}", i);
        }
    }

    #[test]
    fn bulk_delete_sorted_frees_emptied_leaves() {
        let p = tmp("bulk_delete_sorted_frees");
        let mut t = BPlusTree::create(
            &p,
            BPlusTreeConfig {
                initial_capacity_pages: 64,
                compaction_ratio: 1.0,
            },
        )
        .unwrap();
        for i in 0u32..400 {
            t.put(format!("k{:04}", i).into_bytes(), vbytes("v"))
                .unwrap();
        }
        let pages_before = t.pages();
        let free_before = t.stats().free_pages;
        // Delete a contiguous block large enough to empty entire leaves.
        let to_delete: Vec<TestKey> = (0u32..300)
            .map(|i| format!("k{:04}", i).into_bytes())
            .collect();
        let n = Backend::bulk_delete_sorted(&mut t, to_delete.iter()).unwrap();
        assert_eq!(n, 300);
        assert_eq!(t.size(), 100);
        // At least some pages must have ended up on the freelist.
        assert!(
            t.stats().free_pages > free_before,
            "free_pages did not grow: before={} after={}",
            free_before,
            t.stats().free_pages
        );
        // Total pages should be the same (no new allocs needed).
        assert!(t.pages() >= pages_before);
        // Survivors still reachable.
        for i in 300u32..400 {
            assert_eq!(
                vget(&t, &format!("k{:04}", i).into_bytes()),
                Some(vbytes("v"))
            );
        }
    }

    #[test]
    fn bulk_delete_sorted_deletes_all_entries() {
        let p = tmp("bulk_delete_sorted_all");
        let mut t = create(&p);
        for i in 0u32..50 {
            t.put(format!("k{:04}", i).into_bytes(), vbytes("v"))
                .unwrap();
        }
        let keys: Vec<TestKey> = (0u32..50)
            .map(|i| format!("k{:04}", i).into_bytes())
            .collect();
        let n = Backend::bulk_delete_sorted(&mut t, keys.iter()).unwrap();
        assert_eq!(n, 50);
        assert_eq!(t.size(), 0);
        assert!(t.is_empty());
        // Inserts after a full delete still work.
        t.put(k("after"), vbytes("v")).unwrap();
        assert_eq!(vget(&t, &k("after")), Some(vbytes("v")));
    }

    #[test]
    fn range_rev_across_leaf_boundary() {
        let p = tmp("range_rev_cross_leaf");
        let mut t = create(&p);
        for i in 0u32..400 {
            t.put(format!("k{:04}", i).into_bytes(), vbytes("v"))
                .unwrap();
        }
        // Pick a range that spans multiple leaves.
        let got: Vec<Vec<u8>> = OrderedBackend::range_rev(&t, &k("k0050"), &k("k0250"))
            .map(|(k, _)| k.into_owned())
            .collect();
        let expected: Vec<Vec<u8>> = (50..250u32)
            .rev()
            .map(|i| format!("k{:04}", i).into_bytes())
            .collect();
        assert_eq!(got, expected);
    }

    #[test]
    fn bulk_put_sorted_exactly_fills_one_leaf() {
        // Edge: input small enough to fit in a single leaf — no
        // internal levels built; the root stays as page 1.
        let p = tmp("bulk_put_sorted_one_leaf");
        let mut t = create(&p);
        let items: Vec<(TestKey, TestVal)> = (0u32..50)
            .map(|i| (format!("k{:04}", i).into_bytes(), vbytes("v")))
            .collect();
        Backend::bulk_put_sorted(&mut t, items).unwrap();
        assert_eq!(t.size(), 50);
        for i in 0u32..50 {
            assert_eq!(
                vget(&t, &format!("k{:04}", i).into_bytes()),
                Some(vbytes("v"))
            );
        }
    }

    #[test]
    fn compact_after_bulk_operations_yields_dense_tree() {
        let p = tmp("compact_dense");
        let mut t = BPlusTree::create(
            &p,
            BPlusTreeConfig {
                initial_capacity_pages: 64,
                compaction_ratio: 1.0,
            },
        )
        .unwrap();
        // Seed, then delete most, then compact — final pages count
        // should be much smaller than the pre-compact one.
        for i in 0u32..600 {
            t.put(format!("k{:04}", i).into_bytes(), vec![0xAB; 32])
                .unwrap();
        }
        for i in 50u32..600 {
            t.delete(&format!("k{:04}", i).into_bytes()).unwrap();
        }
        let before = t.pages();
        t.compact().unwrap();
        let after = t.pages();
        assert!(after <= before);
        assert_eq!(t.size(), 50);
        for i in 0u32..50 {
            assert_eq!(
                vget(&t, &format!("k{:04}", i).into_bytes()),
                Some(vec![0xAB; 32])
            );
        }
    }

    /// BPlusTree decodes both keys and values from page bytes, so all
    /// retrieval methods must return `Cow::Owned`.
    #[test]
    fn retrieval_methods_return_owned() {
        let p = tmp("cow_owned");
        let mut t = create(&p);
        for i in 0u32..5 {
            t.put(format!("k{:02}", i).into_bytes(), vbytes("v"))
                .unwrap();
        }

        assert!(matches!(t.get(&k("k00")), Some(Cow::Owned(_))));
        assert!(t.keys().all(|c| matches!(c, Cow::Owned(_))));
        assert!(t.values().all(|c| matches!(c, Cow::Owned(_))));
        assert!(t
            .entries()
            .all(|(k, v)| matches!(k, Cow::Owned(_)) && matches!(v, Cow::Owned(_))));

        assert!(t
            .range(&k("k00"), &k("k05"))
            .all(|(k, v)| matches!(k, Cow::Owned(_)) && matches!(v, Cow::Owned(_))));
        assert!(matches!(
            OrderedBackend::first(&t),
            Some((Cow::Owned(_), Cow::Owned(_)))
        ));
        assert!(matches!(
            OrderedBackend::last(&t),
            Some((Cow::Owned(_), Cow::Owned(_)))
        ));
        assert!(OrderedBackend::entries_rev(&t)
            .all(|(k, v)| matches!(k, Cow::Owned(_)) && matches!(v, Cow::Owned(_))));
    }

    /// Round-trip retrieval through `Cow::into_owned()` returns the
    /// same decoded bytes.
    #[test]
    fn into_owned_yields_decoded_values() {
        let p = tmp("cow_into_owned");
        let mut t = create(&p);
        t.put(k("alpha"), vbytes("1")).unwrap();
        t.put(k("bravo"), vbytes("2")).unwrap();

        let owned: TestVal = t.get(&k("alpha")).unwrap().into_owned();
        assert_eq!(owned, vbytes("1"));

        let owned_entries: Vec<(TestKey, TestVal)> = t
            .entries()
            .map(|(k, v)| (k.into_owned(), v.into_owned()))
            .collect();
        assert_eq!(
            owned_entries,
            vec![(k("alpha"), vbytes("1")), (k("bravo"), vbytes("2"))]
        );
    }

    #[test]
    fn delete_then_reuse_via_freelist() {
        // Free pages should be picked up by subsequent allocs.
        let p = tmp("freelist_reuse");
        let mut t = create(&p);
        for i in 0u32..200 {
            t.put(format!("k{:04}", i).into_bytes(), vec![0xAA; 32])
                .unwrap();
        }
        // Delete a contiguous block.
        for i in 50u32..150 {
            t.delete(&format!("k{:04}", i).into_bytes()).unwrap();
        }
        let pages_before = t.pages();
        // Re-insert similar entries — should consume freelist slots.
        for i in 50u32..150 {
            t.put(format!("k{:04}", i).into_bytes(), vec![0xBB; 32])
                .unwrap();
        }
        let pages_after = t.pages();
        // Allowed: pages_after equals or is close to pages_before
        // since we mostly reuse freed pages.
        assert!(pages_after <= pages_before + 5);
        for i in 50u32..150 {
            assert_eq!(
                vget(&t, &format!("k{:04}", i).into_bytes()),
                Some(vec![0xBB; 32])
            );
        }
    }
}
