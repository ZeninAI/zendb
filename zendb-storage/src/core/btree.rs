//! BPlusTree — persistent, ordered key-value store backed by a mmap'd B+ tree
//! with suffix truncation and **contiguous extents** for large values.
//!
//! # Generic over K, V (rkyv-archived)
//!
//! Keys and values are serialized through rkyv. Reads are zero-copy:
//! `get` returns `&Archived<V>` borrowed directly from the mmap.
//! Iteration yields `(&K, &Archived<V>)`. Keys are kept in an in-memory
//! `HashMap<Vec<u8>, K>` (mirroring `KeyDir`'s model) so the trait can
//! hand back `&K` cheaply.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────┐
//! │                    BPlusTree (mmap'd)                    │
//! │  Internal pages:              Leaf pages:               │
//! │  [key₁ child₁] [key₂ child₂]  [K:V] [K:V] [K:V] ...    │
//! │  [leftmost_child──────▶leaf]  sorted → next_leaf→       │
//! └─────────────────────────────────────────────────────────┘
//! ```
//!
//! Every internal page holds a `leftmost_child` pointer plus an array of
//! `(child_page, separator_key)` pairs. Binary search on the separator keys
//! routes point-lookups to the correct subtree. All values live in leaves;
//! internal pages carry only keys for navigation.
//!
//! # Comparison
//!
//! Tree navigation compares **serialized key bytes** lexicographically.
//! Callers requiring meaningful ordering should pick a `K` whose rkyv
//! encoding sorts in the desired order (byte strings, fixed-width
//! big-endian integers, etc.).
//!
//! # Page layout (4096 bytes, slotted)
//!
//! ```text
//! ┌──────────────┬───────┬───────────┐
//! │ header (16B) │ slots │ free      │ entries  ────│
//! └──────────────┴───────┴───────────┴──────────────┘
//! 0              16      slot_end    data_off        4096
//! ```
//!
//! **Header**: `type(u8) flags(u8) count(u16 LE) data_off(u32 LE) ptr(u64 LE)`.
//! `ptr` = `next_leaf` (leaves) or `leftmost_child` (internal).
//!
//! Slots are a grow-down array of `u16` LE offsets from the slot array
//! itself — each points to an entry's starting byte within the page.
//! Entries grow up from the bottom. When the two regions collide,
//! the page splits.
//!
//! # Entry formats
//!
//! Leaf, inline:   `[key_len: u16][value_len: u32][key_bytes][value_bytes]`
//! Leaf, extent:   `[key_len: u16][value_len: u32 | OVFL_FLAG][key_bytes][extent_start: u64]`
//! Internal:       `[key_len: u16][child_page: u64][separator_key]`
//!
//! # Extents (contiguous overflow)
//!
//! Values exceeding the per-page inline limit (`MAX_INLINE`) are stored in a
//! **contiguous run of pages** allocated at the file's tail. The leaf
//! entry stores the first page index of the extent and the true value
//! length; the value bytes occupy bytes `[0..value_len]` of the extent
//! mmap slice. This keeps the value contiguous in memory so rkyv's
//! relative-pointer-based zero-copy read works for any size.
//!
//! Extent allocation always grows the file (no extent freelist reuse
//! in v1); freed extent pages go onto the single-page freelist so they
//! can be reused as tree (internal/leaf) pages.
//!
//! # Meta page (page 0)
//!
//! The first page of the file is the meta page, not a tree node:
//! ```text
//! [magic: u32][version: u32][root: u64][free_head: u64][pages: u64][entries: u64] …
//! ```
//! `root` is the page number of the root node (0 = empty tree).
//! `free_head` is the head of the single-page freelist.
//! `pages` is the total number of pages in the file.
//! `entries` is the count of live key-value pairs.
//!
//! # Suffix truncation
//!
//! When a leaf or internal page splits, the separator key pushed to the
//! parent is the **shortest prefix** of the new page's first key that is
//! strictly greater than the old page's last key (see [`truncated_separator`]).
//! This maximizes fanout in internal pages by storing the minimum
//! distinguishing prefix.

use memmap2::MmapMut;
use std::{
    fmt,
    fs::{self, OpenOptions},
    hash::Hash,
    io,
    marker::PhantomData,
    path::Path,
};

use rkyv::{
    api::high::{HighDeserializer, HighSerializer},
    rancor::Error as RkyvError,
    ser::{allocator::ArenaHandle, writer::Buffer},
    Archive, Archived, Deserialize, Portable, Serialize,
};

use crate::utils::serdes::{
    rd_u16, rd_u32, rd_u64, serialize_into, serialize_to_vec, serialized_size, wr_u16, wr_u32,
    wr_u64, CountingWriter, ValueRef,
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const PAGE_SIZE: usize = 4096;
const HEADER_SIZE: usize = 16;
const SLOT_SIZE: usize = 2;
const MAGIC: u32 = 0x5450425A;
const META_PAGE: u64 = 0;
const PAGE_LEAF: u8 = 1;
const PAGE_INTERNAL: u8 = 2;
const FLAG_ROOT: u8 = 0x01;

/// High bit of `value_len` marks an extent value (stored in dedicated pages).
const OVFL_FLAG: u32 = 0x8000_0000;

/// Maximum inline entry size: a single entry must fit in a fresh page.
/// = PAGE_SIZE - HEADER_SIZE - SLOT_SIZE - 6 (key_len u16 + value_len u32)
const MAX_INLINE: usize = PAGE_SIZE - HEADER_SIZE - SLOT_SIZE - 6;

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
    _phantom: PhantomData<(K, V)>,
}

/// Byte-level helpers — no rkyv bounds, just raw mmap access. Lives in
/// its own impl block so the iterator types (which only carry `Archive`
/// bounds, not the full `Serialize` set) can call them.
impl<K, V> BPlusTree<K, V> {
    /// Read the value slice at leaf page `off`, slot `i`. Handles both
    /// inline and extent layouts and returns a single contiguous slice.
    fn value_slice_at(&self, off: usize, i: usize) -> &[u8] {
        let sp = off + HEADER_SIZE + i * SLOT_SIZE;
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
        let sp = off + HEADER_SIZE + i * SLOT_SIZE;
        let eo = off + rd_u16(&self.mmap[sp..sp + 2]) as usize;
        let kl = rd_u16(&self.mmap[eo..eo + 2]) as usize;
        &self.mmap[eo + 6..eo + 6 + kl]
    }
}

impl<K, V> BPlusTree<K, V>
where
    K: Archive + Hash + Eq + Clone,
    for<'buf, 'a> K: Serialize<HighSerializer<Buffer<'buf>, ArenaHandle<'a>, RkyvError>>,
    for<'a> K: Serialize<HighSerializer<CountingWriter, ArenaHandle<'a>, RkyvError>>,
    <K as Archive>::Archived: Portable + Deserialize<K, HighDeserializer<RkyvError>> + 'static,
    V: Archive,
    for<'buf, 'a> V: Serialize<HighSerializer<Buffer<'buf>, ArenaHandle<'a>, RkyvError>>,
    for<'a> V: Serialize<HighSerializer<CountingWriter, ArenaHandle<'a>, RkyvError>>,
    <V as Archive>::Archived: Portable + Deserialize<V, HighDeserializer<RkyvError>> + 'static,
{
    // -- constructors --

    pub fn create(path: &Path) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(path)?;
        let np = 2u64;
        file.set_len(np * PAGE_SIZE as u64)?;
        let mut mmap = unsafe { MmapMut::map_mut(&file)? };
        let m = page_offset(META_PAGE);
        wr_u32(&mut mmap[m..m + 4], MAGIC);
        wr_u32(&mut mmap[m + 4..m + 8], 1);
        wr_u64(&mut mmap[m + 8..m + 16], 1); // root page
        wr_u64(&mut mmap[m + 16..m + 24], 0); // free list head
        wr_u64(&mut mmap[m + 24..m + 32], np); // total pages
        wr_u64(&mut mmap[m + 32..m + 40], 0); // entry count
        let r = page_offset(1);
        mmap[r] = PAGE_LEAF;
        mmap[r + 1] = FLAG_ROOT;
        wr_u16(&mut mmap[r + 2..r + 4], 0);
        wr_u32(&mut mmap[r + 4..r + 8], PAGE_SIZE as u32);
        wr_u64(&mut mmap[r + 8..r + 16], 0);
        Ok(BPlusTree {
            mmap,
            file,
            _phantom: PhantomData,
        })
    }

    pub fn open(path: &Path) -> io::Result<Self> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        let mmap = unsafe { MmapMut::map_mut(&file)? };
        if rd_u32(&mmap[0..4]) != MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "not a BPlusTree file",
            ));
        }
        let this = BPlusTree {
            mmap,
            file,
            _phantom: PhantomData,
        };
        Ok(this)
    }

    // -- public generic API --

    /// Look up `key`, returning a zero-copy [`ValueRef`] into the mmap.
    pub fn get(&self, key: &K) -> Option<ValueRef<'_, V>> {
        let key_bytes = serialize_to_vec(key).ok()?;
        let slice = self.value_slice_for(&key_bytes)?;
        Some(ValueRef::from_bytes(slice))
    }

    /// Convenience: existence check without materializing a [`ValueRef`].
    pub fn contains(&self, key: &K) -> bool {
        let Ok(kb) = serialize_to_vec(key) else {
            return false;
        };
        self.value_slice_for(&kb).is_some()
    }

    /// Insert or overwrite. Both `key` and `value` are consumed.
    ///
    /// The value's archive is written **directly into the mmap** — no
    /// intermediate buffer. The key is serialized into a single tight
    /// `Vec<u8>` used for tree navigation and then copied into the leaf
    /// slot in one pass.
    pub fn put(&mut self, key: K, value: V) -> io::Result<()> {
        let key_bytes = serialize_to_vec(&key)?;
        let v_size = serialized_size(&value)?;
        self.insert_typed(&key_bytes, &value, v_size)
    }

    /// Read-modify-write. Deserializes the value, passes it to `f`,
    /// and serializes the result back. Returns `true` if the key existed,
    /// `false` otherwise.
    pub fn update<F>(&mut self, key: &K, f: F) -> io::Result<bool>
    where
        F: FnOnce(V) -> V,
    {
        let key_bytes = serialize_to_vec(key)?;
        let Some(slice) = self.value_slice_for(&key_bytes) else {
            return Ok(false);
        };
        let current = ValueRef::<V>::from_bytes(slice).to_owned();
        let new_v = f(current);
        let v_size = serialized_size(&new_v)?;
        self.insert_typed(&key_bytes, &new_v, v_size)?;
        Ok(true)
    }

    /// In-place archived update. The closure receives `&mut Archived<V>` —
    /// direct mutation of the value's bytes in storage, whether the value
    /// lives inline in the leaf page or in a dedicated extent.
    /// Returns whether the key existed.
    ///
    /// # Safety contract
    /// The closure must not change the byte length of the archive.
    /// Only size-stable mutations are sound (integer fields, flags,
    /// fixed-width numeric updates). The trait can't enforce this — the
    /// caller is responsible. Growing an `ArchivedVec` or changing an
    /// `ArchivedString` length will corrupt the file.
    pub fn update_in_place<F>(&mut self, key: &K, f: F) -> io::Result<bool>
    where
        F: FnOnce(&mut Archived<V>),
    {
        let key_bytes = serialize_to_vec(key)?;
        let root = self.root();
        if root == 0 {
            return Ok(false);
        }
        let leaf = self.find_leaf(root, &key_bytes);
        let off = page_offset(leaf);
        let count = rd_u16(&self.mmap[off + 2..off + 4]) as usize;
        let i = match self.leaf_find_slot(off, count, &key_bytes) {
            Ok(i) => i,
            Err(_) => return Ok(false),
        };
        let sp = off + HEADER_SIZE + i * SLOT_SIZE;
        let eo = off + rd_u16(&self.mmap[sp..sp + 2]) as usize;
        let kl = rd_u16(&self.mmap[eo..eo + 2]) as usize;
        let raw_vl = rd_u32(&self.mmap[eo + 2..eo + 6]);
        // Locate the archived value bytes — inline in the leaf, or in an
        // extent. Either way, mutate them directly under the size-stable
        // safety contract above.
        let (vstart, vlen) = if raw_vl & OVFL_FLAG != 0 {
            let real_len = (raw_vl & !OVFL_FLAG) as usize;
            let extent_start = rd_u64(&self.mmap[eo + 6 + kl..eo + 6 + kl + 8]);
            (page_offset(extent_start), real_len)
        } else {
            (eo + 6 + kl, raw_vl as usize)
        };
        // SAFETY: `vstart..vstart+vlen` is a valid archive of V (maintained
        // by `put`/`insert_typed`). The caller's closure must not change
        // the byte length — documented contract above.
        let sealed = unsafe {
            rkyv::access_unchecked_mut::<Archived<V>>(&mut self.mmap[vstart..vstart + vlen])
        };
        let archived: &mut Archived<V> = unsafe { sealed.unseal_unchecked() };
        f(archived);
        Ok(true)
    }

    /// Remove `key`. Returns true if the key existed.
    pub fn delete(&mut self, key: &K) -> io::Result<bool> {
        let key_bytes = serialize_to_vec(key)?;
        self.delete_bytes(&key_bytes)
    }

    pub fn len(&self) -> usize {
        rd_u64(&self.mmap[32..40]) as usize
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn flush(&self) -> io::Result<()> {
        self.mmap.flush()
    }

    /// Iterate all entries in serialized-key order. Keys are deserialized;
    /// values are zero-copy [`ValueRef`]s into the mmap.
    pub fn entries(&self) -> BTreeIter<'_, K, V> {
        BTreeIter {
            tree: self,
            page: self.first_leaf(),
            slot: 0,
        }
    }

    /// Iterate all keys in serialized-key order (deserialized, owned).
    pub fn keys(&self) -> impl Iterator<Item = K> + '_ {
        self.entries().map(|(k, _)| k)
    }

    /// Iterate all values in serialized-key order (zero-copy into mmap).
    pub fn values(&self) -> impl Iterator<Item = ValueRef<'_, V>> + '_ {
        self.entries().map(|(_, v)| v)
    }

    /// Iterate entries with serialized keys in `[start, end)`.
    pub fn range(&self, start: &K, end: &K) -> BTreeRange<'_, K, V> {
        // Best-effort: if serialization fails, return an empty iterator.
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
        let root = self.root();
        if root == 0 {
            return BTreeRange {
                tree: self,
                page: 0,
                slot: 0,
                end: end_bytes,
            };
        }
        let leaf = self.find_leaf(root, &start_bytes);
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
    // code while the byte-level machinery is agnostic to rkyv types.
    // -----------------------------------------------------------------------

    /// Navigate the tree to the leaf that would contain `key_bytes`, then
    /// return the contiguous archived value bytes (inline or extent).
    /// Returns `None` if the tree is empty or the key is not present.
    /// The returned slice borrows from `self.mmap`.
    fn value_slice_for(&self, key_bytes: &[u8]) -> Option<&[u8]> {
        let root = self.root();
        if root == 0 {
            return None;
        }
        let leaf = self.find_leaf(root, key_bytes);
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
    fn insert_typed(&mut self, key: &[u8], value: &V, v_size: usize) -> io::Result<()> {
        let root = self.root();
        if root == 0 {
            let p = self.alloc_page()?;
            self.set_root(p);
            self.init_leaf(p, true);
            self.leaf_insert_typed(page_offset(p), p, key, value, v_size)?;
            self.inc_entries(1);
            return Ok(());
        }
        let mut path: Vec<(u64, usize)> = Vec::new();
        let mut page = root;
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
        Ok(())
    }

    /// Remove the entry identified by serialized key bytes. Frees any
    /// extent pages backing the value and calls [`leaf_remove_entry`] to
    /// slide the leaf's slot array and data gap. Returns `true` if the
    /// key existed (and was removed).
    fn delete_bytes(&mut self, key: &[u8]) -> io::Result<bool> {
        let root = self.root();
        if root == 0 {
            return Ok(false);
        }
        let leaf = self.find_leaf(root, key);
        let off = page_offset(leaf);
        if self.mmap[off] != PAGE_LEAF {
            return Ok(false);
        }
        let count = rd_u16(&self.mmap[off + 2..off + 4]) as usize;
        let data_off = rd_u32(&self.mmap[off + 4..off + 8]) as usize;
        match self.leaf_find_slot(off, count, key) {
            Ok(i) => {
                let sp = off + HEADER_SIZE + i * SLOT_SIZE;
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

    /// Page number of the root node. `0` means the tree is empty.
    fn root(&self) -> u64 {
        rd_u64(&self.mmap[8..16])
    }
    /// Update the root page pointer in the meta page.
    fn set_root(&mut self, p: u64) {
        wr_u64(&mut self.mmap[8..16], p);
    }
    /// Atomically increment the global entry counter by `d` (wrapping).
    fn inc_entries(&mut self, d: u64) {
        let c = rd_u64(&self.mmap[32..40]);
        wr_u64(&mut self.mmap[32..40], c.wrapping_add(d));
    }
    /// Decrement the global entry counter by 1 (wrapping). Caller must
    /// ensure the counter is ≥ 1 before calling.
    fn dec_entries(&mut self) {
        let c = rd_u64(&self.mmap[32..40]);
        wr_u64(&mut self.mmap[32..40], c.wrapping_sub(1));
    }

    // -- traversal — navigate the tree using serialized key bytes --

    /// Walk from the root down the leftmost-child path to the first leaf
    /// page (the one with the smallest key range). Used to seed iteration.
    fn first_leaf(&self) -> u64 {
        let mut p = self.root();
        if p == 0 {
            return 0;
        }
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
        if count == 0 {
            return leftmost;
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
            let sp = off + HEADER_SIZE + mid * SLOT_SIZE;
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
        let free_start = HEADER_SIZE + count * SLOT_SIZE;

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

        match self.leaf_find_slot(off, count, key) {
            Ok(i) => {
                let sp = off + HEADER_SIZE + i * SLOT_SIZE;
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
                if !ovfl && raw_vl & OVFL_FLAG == 0 && raw_vl as usize == v_size {
                    let v_off = eo + 6 + kl;
                    let written = serialize_into(value, &mut self.mmap[v_off..v_off + v_size])?;
                    debug_assert_eq!(written, v_size);
                    return Ok(None);
                }
                // Size changed (or inline/extent flipped): drop the old
                // entry then re-insert via the Err(pos) arm below.
                self.leaf_remove_entry(off, i, count, data_off);
                return self.leaf_insert_typed(off, page, key, value, v_size);
            }
            Err(pos) => {
                let ss = off + HEADER_SIZE;
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
                Ok(None)
            }
        }
    }

    /// Write a complete leaf entry (header + key + value) at byte `nd`
    /// within the page. For inline values, the archive is serialized directly
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
            let sp = off + HEADER_SIZE + i * SLOT_SIZE;
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
                if used + sz > PAGE_SIZE - HEADER_SIZE {
                    return true;
                }
                used += sz;
                false
            })
            .unwrap_or(entries.len());

        if mid >= entries.len() {
            wr_u16(&mut self.mmap[off + 2..off + 4], 0);
            wr_u32(&mut self.mmap[off + 4..off + 8], PAGE_SIZE as u32);
            for (k, v) in entries {
                self.leaf_insert_split_entry(off, &k, v)?;
            }
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

        let old_next = rd_u64(&self.mmap[off + 8..off + 16]);
        wr_u64(&mut self.mmap[ro + 8..ro + 16], old_next);
        wr_u64(&mut self.mmap[off + 8..off + 16], right);

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

        let ss = off + HEADER_SIZE;
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
        let sp = off + HEADER_SIZE + pos * SLOT_SIZE;
        let eo_rel = rd_u16(&self.mmap[sp..sp + 2]) as usize;
        let eo = off + eo_rel;
        let kl = rd_u16(&self.mmap[eo..eo + 2]) as usize;
        let raw_vl = rd_u32(&self.mmap[eo + 2..eo + 6]) as usize;
        let es = if raw_vl & OVFL_FLAG as usize != 0 {
            6 + kl + 8
        } else {
            6 + kl + raw_vl
        };
        let ss = off + HEADER_SIZE;
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
        self.grow(n_pages)?;
        self.set_pages(start + n_pages);
        Ok(start)
    }

    // -- page allocation --

    /// Head of the single-page freelist. `0` means the list is empty.
    fn free_head(&self) -> u64 {
        rd_u64(&self.mmap[16..24])
    }
    fn set_free_head(&mut self, p: u64) {
        wr_u64(&mut self.mmap[16..24], p);
    }
    /// Total number of pages in the file (including meta page and extents).
    fn pages(&self) -> u64 {
        rd_u64(&self.mmap[24..32])
    }
    fn set_pages(&mut self, n: u64) {
        wr_u64(&mut self.mmap[24..32], n);
    }

    /// Allocate a single free page. Pulls from the freelist (if non-empty)
    /// or grows the file by one page. The returned page is **not** zeroed —
    /// the caller is responsible for initializing it (e.g. via
    /// `init_leaf` / `init_internal`).
    fn alloc_page(&mut self) -> io::Result<u64> {
        let free = self.free_head();
        if free != 0 {
            let next = rd_u64(&self.mmap[page_offset(free)..page_offset(free) + 8]);
            self.set_free_head(next);
            return Ok(free);
        }
        let tp = self.pages();
        self.grow(1)?;
        self.set_pages(tp + 1);
        Ok(tp)
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
    }

    /// Grow the file by `extra` pages and re-mmap. Invalidates existing
    /// mmap references — callers must re-derive offsets from `self.mmap`
    /// after `grow` returns.
    fn grow(&mut self, extra: u64) -> io::Result<()> {
        let np = (self.pages() + extra) * PAGE_SIZE as u64;
        self.file.set_len(np)?;
        self.mmap = unsafe { MmapMut::map_mut(&self.file)? };
        Ok(())
    }

    /// Initialize a page as a leaf. Zeros out the header and slot array.
    /// If `root` is true, the `FLAG_ROOT` bit is set.
    fn init_leaf(&mut self, page: u64, root: bool) {
        let off = page_offset(page);
        self.mmap[off] = PAGE_LEAF;
        self.mmap[off + 1] = if root { FLAG_ROOT } else { 0 };
        wr_u16(&mut self.mmap[off + 2..off + 4], 0);
        wr_u32(&mut self.mmap[off + 4..off + 8], PAGE_SIZE as u32);
        wr_u64(&mut self.mmap[off + 8..off + 16], 0);
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
/// Yields deserialized keys (owned) and zero-copy [`ValueRef`] values.
pub struct BTreeIter<'a, K, V> {
    tree: &'a BPlusTree<K, V>,
    page: u64,
    slot: usize,
}

impl<'a, K, V> Iterator for BTreeIter<'a, K, V>
where
    K: Archive,
    <K as Archive>::Archived: Portable + Deserialize<K, HighDeserializer<RkyvError>> + 'static,
    V: Archive,
    <V as Archive>::Archived: Portable + 'static,
{
    type Item = (K, ValueRef<'a, V>);
    fn next(&mut self) -> Option<Self::Item> {
        while self.page != 0 {
            let off = page_offset(self.page);
            let count = rd_u16(&self.tree.mmap[off + 2..off + 4]) as usize;
            if self.slot < count {
                let key_slice = self.tree.key_slice_at(off, self.slot);
                let value_slice = self.tree.value_slice_at(off, self.slot);
                self.slot += 1;
                let k = rkyv::deserialize::<K, RkyvError>(unsafe {
                    rkyv::access_unchecked::<Archived<K>>(key_slice)
                })
                .expect("valid archive");
                let v = ValueRef::from_bytes(value_slice);
                return Some((k, v));
            }
            self.page = rd_u64(&self.tree.mmap[off + 8..off + 16]);
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
    K: Archive,
    <K as Archive>::Archived: Portable + Deserialize<K, HighDeserializer<RkyvError>> + 'static,
    V: Archive,
    <V as Archive>::Archived: Portable + 'static,
{
    type Item = (K, ValueRef<'a, V>);
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
                let k = rkyv::deserialize::<K, RkyvError>(unsafe {
                    rkyv::access_unchecked::<Archived<K>>(key_slice)
                })
                .expect("valid archive");
                let v = ValueRef::from_bytes(value_slice);
                return Some((k, v));
            }
            self.page = rd_u64(&self.tree.mmap[off + 8..off + 16]);
            self.slot = 0;
        }
        None
    }
}

impl<K, V> Drop for BPlusTree<K, V> {
    /// Flush the mmap to disk on drop as a best-effort durability measure.
    /// Errors are silently ignored — call [`flush`](BPlusTree::flush)
    /// explicitly if you need to guarantee data is on disk.
    fn drop(&mut self) {
        let _ = self.mmap.flush();
    }
}

impl<K, V> fmt::Debug for BPlusTree<K, V> {
    /// Read counters directly from the mmap so this impl has no trait bounds
    /// on `K` or `V`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BPlusTree")
            .field("entries", &rd_u64(&self.mmap[32..40]))
            .field("pages", &rd_u64(&self.mmap[24..32]))
            .finish()
    }
}

// ---------------------------------------------------------------------------
// Backend / OrderedBackend impls
//
// The BPlusTree satisfies both the common `Backend` contract and the
// `OrderedBackend` extension (range scans). Delegation is straightforward
// — every trait method forwards to the identically-named inherent method.
// ---------------------------------------------------------------------------

use crate::core::backend::{Backend, OrderedBackend};

impl<K, V> Backend<K, V> for BPlusTree<K, V>
where
    K: Archive + Hash + Eq + Clone,
    for<'buf, 'a> K: Serialize<HighSerializer<Buffer<'buf>, ArenaHandle<'a>, RkyvError>>,
    for<'a> K: Serialize<HighSerializer<CountingWriter, ArenaHandle<'a>, RkyvError>>,
    <K as Archive>::Archived: Portable + Deserialize<K, HighDeserializer<RkyvError>> + 'static,
    V: Archive,
    for<'buf, 'a> V: Serialize<HighSerializer<Buffer<'buf>, ArenaHandle<'a>, RkyvError>>,
    for<'a> V: Serialize<HighSerializer<CountingWriter, ArenaHandle<'a>, RkyvError>>,
    <V as Archive>::Archived: Portable + Deserialize<V, HighDeserializer<RkyvError>> + 'static,
{
    fn get(&self, key: &K) -> Option<ValueRef<'_, V>> {
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

    fn update<F>(&mut self, key: &K, f: F) -> io::Result<bool>
    where
        F: FnOnce(V) -> V,
    {
        BPlusTree::update(self, key, f)
    }

    fn update_in_place<F>(&mut self, key: &K, f: F) -> io::Result<bool>
    where
        F: FnOnce(&mut Archived<V>),
    {
        BPlusTree::update_in_place(self, key, f)
    }

    fn keys(&self) -> impl Iterator<Item = K> + '_ {
        BPlusTree::keys(self)
    }

    fn values(&self) -> impl Iterator<Item = ValueRef<'_, V>> + '_ {
        BPlusTree::values(self)
    }

    fn entries(&self) -> impl Iterator<Item = (K, ValueRef<'_, V>)> + '_ {
        BPlusTree::entries(self)
    }

    fn len(&self) -> usize {
        BPlusTree::len(self)
    }

    fn is_empty(&self) -> bool {
        BPlusTree::is_empty(self)
    }

    fn flush(&self) -> io::Result<()> {
        BPlusTree::flush(self)
    }
}

impl<K, V> OrderedBackend<K, V> for BPlusTree<K, V>
where
    K: Archive + Hash + Eq + Clone,
    for<'buf, 'a> K: Serialize<HighSerializer<Buffer<'buf>, ArenaHandle<'a>, RkyvError>>,
    for<'a> K: Serialize<HighSerializer<CountingWriter, ArenaHandle<'a>, RkyvError>>,
    <K as Archive>::Archived: Portable + Deserialize<K, HighDeserializer<RkyvError>> + 'static,
    V: Archive,
    for<'buf, 'a> V: Serialize<HighSerializer<Buffer<'buf>, ArenaHandle<'a>, RkyvError>>,
    for<'a> V: Serialize<HighSerializer<CountingWriter, ArenaHandle<'a>, RkyvError>>,
    <V as Archive>::Archived: Portable + Deserialize<V, HighDeserializer<RkyvError>> + 'static,
{
    fn range(&self, start: &K, end: &K) -> impl Iterator<Item = (K, ValueRef<'_, V>)> + '_ {
        BPlusTree::range(self, start, end)
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
        BPlusTree::create(p).unwrap()
    }

    fn open(p: &Path) -> BPlusTree<TestKey, TestVal> {
        BPlusTree::open(p).unwrap()
    }

    fn k(s: &str) -> TestKey {
        s.as_bytes().to_vec()
    }
    fn vbytes(s: &str) -> TestVal {
        s.as_bytes().to_vec()
    }

    /// Helper: read the archived value as owned bytes (the only sensible
    /// equality check for `&ArchivedVec<u8>`).
    fn vget(t: &BPlusTree<TestKey, TestVal>, key: &TestKey) -> Option<Vec<u8>> {
        t.get(key).map(|vr| vr.archived().as_slice().to_vec())
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
        assert_eq!(t.len(), 1);
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
        assert_eq!(t.len(), 1);
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
        assert_eq!(t.len(), 500);
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
        assert_eq!(t.len(), 0);
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
        assert_eq!(t.len(), 30);
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
        assert_eq!(t.len(), 1);
    }

    #[test]
    fn overwrite_extent_with_inline() {
        let p = tmp("ow_ext2inl");
        let mut t = create(&p);
        let big = vec![0x77u8; 9_000];
        t.put(k("k"), big).unwrap();
        t.put(k("k"), vbytes("tiny")).unwrap();
        assert_eq!(vget(&t, &k("k")), Some(vbytes("tiny")));
        assert_eq!(t.len(), 1);
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
        assert_eq!(t.len(), 1);
    }

    // -- update_in_place on extent values --

    #[test]
    fn update_in_place_on_extent_value() {
        use rkyv::{Archive, Deserialize, Serialize};

        #[derive(Archive, Serialize, Deserialize, Debug, Clone)]
        struct Big {
            counter: u32,
            payload: Vec<u8>,
        }

        let p = tmp("uip_ext");
        let mut t: BPlusTree<Vec<u8>, Big> = BPlusTree::create(&p).unwrap();
        let big = Big {
            counter: 1,
            // Push it well past MAX_INLINE so it goes to an extent.
            payload: vec![0xEEu8; 8_000],
        };
        t.put(k("big"), big).unwrap();

        let existed = t
            .update_in_place(&k("big"), |archived| {
                let new = archived.counter.to_native() + 41;
                archived.counter = new.into();
            })
            .unwrap();
        assert!(existed, "extent update_in_place must report key existed");

        let vr = t.get(&k("big")).unwrap();
        assert_eq!(vr.counter.to_native(), 42);
        assert_eq!(vr.payload.len(), 8_000);
    }

    #[test]
    fn update_in_place_on_extent_persists_after_reopen() {
        use rkyv::{Archive, Deserialize, Serialize};

        #[derive(Archive, Serialize, Deserialize, Debug, Clone)]
        struct Big {
            counter: u32,
            payload: Vec<u8>,
        }

        let p = tmp("uip_ext_reopen");
        {
            let mut t: BPlusTree<Vec<u8>, Big> = BPlusTree::create(&p).unwrap();
            t.put(
                k("big"),
                Big {
                    counter: 100,
                    payload: vec![0xCCu8; 9_500],
                },
            )
            .unwrap();
            t.update_in_place(&k("big"), |archived| {
                archived.counter = 999u32.into();
            })
            .unwrap();
            t.flush().unwrap();
        }
        let t: BPlusTree<Vec<u8>, Big> = BPlusTree::open(&p).unwrap();
        let vr = t.get(&k("big")).unwrap();
        assert_eq!(vr.counter.to_native(), 999);
        assert_eq!(vr.payload.len(), 9_500);
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
        assert_eq!(t.len(), 100);
        for i in 0u32..100 {
            assert!(t.delete(&format!("k{:04}", i).into_bytes()).unwrap());
        }
        assert_eq!(t.len(), 0);
    }
}
