//! BPlusTree — persistent, ordered key-value store backed by a mmap'd B+ tree
//! with suffix truncation, overflow pages for large values, and a bulk-merge
//! fast path.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────┐
//! │                    BPlusTree (mmap'd)                    │
//! │  Internal pages:              Leaf pages:               │
//! │  [key₁ child₁] [key₂ child₂]  [K:V] [K:V] [K:V] ...   │
//! │  [leftmost_child──────▶leaf]  sorted → next_leaf→      │
//! └─────────────────────────────────────────────────────────┘
//! ```
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
//! Slots: array of `count` 2-byte page-relative offsets to entry data.
//! Data grows downward from `data_off`; slots grow upward from byte 16.
//!
//! # Entry formats
//!
//! Leaf, normal:   `[key_len: u16 LE][value_len: u32 LE][key][value]`
//! Leaf, overflow: `[key_len: u16 LE][value_len: u32 LE | 0x8000_0000][key][ovfl_page: u64 LE]`
//! Internal:       `[key_len: u16 LE][child_page: u64 LE][separator_key]`
//!
//! Internal separators are **suffix-truncated**: only enough bytes to
//! distinguish the left subtree's maximum key from the right subtree's
//! minimum key.  This keeps fanout high even with large user keys.
//!
//! # Overflow pages
//!
//! Values exceeding ~4 KiB are stored across a linked chain of pages:
//! each page holds 4088 bytes of data followed by an 8-byte `next_page`
//! pointer (0 = end).  The leaf entry stores the first page.

use memmap2::MmapMut;
use std::{fmt, fs, io, path::Path};

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

/// High bit of `value_len` marks an overflow value (stored in separate pages).
const OVFL_FLAG: u32 = 0x8000_0000;
/// Bytes of payload per overflow page (remainder is next-pointer).
const OVFL_DATA: usize = PAGE_SIZE - 8;

/// Maximum inline entry size: a single entry must fit in a fresh page.
/// = PAGE_SIZE - HEADER_SIZE - SLOT_SIZE - 6 (key_len u16 + value_len u32)
const MAX_INLINE: usize = PAGE_SIZE - HEADER_SIZE - SLOT_SIZE - 6;

// ---- raw r/w helpers ----

fn page_offset(page: u64) -> usize {
    page as usize * PAGE_SIZE
}
fn rd_u16(b: &[u8]) -> u16 {
    u16::from_le_bytes([b[0], b[1]])
}
fn rd_u32(b: &[u8]) -> u32 {
    u32::from_le_bytes([b[0], b[1], b[2], b[3]])
}
fn rd_u64(b: &[u8]) -> u64 {
    u64::from_le_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]])
}
fn wr_u16(b: &mut [u8], v: u16) {
    b[..2].copy_from_slice(&v.to_le_bytes());
}
fn wr_u32(b: &mut [u8], v: u32) {
    b[..4].copy_from_slice(&v.to_le_bytes());
}
fn wr_u64(b: &mut [u8], v: u64) {
    b[..8].copy_from_slice(&v.to_le_bytes());
}

struct PageSplit {
    left_page: u64,
    right_page: u64,
    separator_key: Vec<u8>,
}

/// Value carried through a leaf split without materializing overflow data.
enum SplitVal {
    /// Small value stored inline.
    Inline(Vec<u8>),
    /// Large value already in an overflow chain — carry the pointer as-is.
    Overflow { page: u64, real_len: u32 },
}

/// Compute the minimal prefix of `right_first` that is strictly greater
/// than `left_last` and ≤ `right_first`.  This keeps internal-page fanout
/// high when user keys are large.
fn truncated_separator(left_last: &[u8], right_first: &[u8]) -> Vec<u8> {
    let n = left_last.len().min(right_first.len());
    for i in 0..n {
        if right_first[i] > left_last[i] {
            return right_first[..=i].to_vec();
        }
    }
    // left_last is a prefix of right_first (e.g. "abc" / "abcd").
    // Append 0x00 so: left_last < left_last+[0] <= right_first.
    let mut s = left_last.to_vec();
    s.push(0);
    s
}

// ---------------------------------------------------------------------------
// BPlusTree
// ---------------------------------------------------------------------------

pub struct BPlusTree {
    mmap: MmapMut,
    file: fs::File,
}

impl BPlusTree {
    // -- constructors --

    pub fn create(path: &Path) -> io::Result<Self> {
        let file = fs::OpenOptions::new()
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
        // [4..8] reserved (version = 1)
        wr_u32(&mut mmap[m + 4..m + 8], 1);
        wr_u64(&mut mmap[m + 8..m + 16], 1);   // root page
        wr_u64(&mut mmap[m + 16..m + 24], 0);  // free list head
        wr_u64(&mut mmap[m + 24..m + 32], np); // total pages
        wr_u64(&mut mmap[m + 32..m + 40], 0);  // entry count
        let r = page_offset(1);
        mmap[r] = PAGE_LEAF;
        mmap[r + 1] = FLAG_ROOT;
        wr_u16(&mut mmap[r + 2..r + 4], 0);
        wr_u32(&mut mmap[r + 4..r + 8], PAGE_SIZE as u32);
        wr_u64(&mut mmap[r + 8..r + 16], 0);
        Ok(BPlusTree { mmap, file })
    }

    pub fn open(path: &Path) -> io::Result<Self> {
        let file = fs::OpenOptions::new().read(true).write(true).open(path)?;
        let mmap = unsafe { MmapMut::map_mut(&file)? };
        if rd_u32(&mmap[0..4]) != MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "not a BPlusTree file",
            ));
        }
        Ok(BPlusTree { mmap, file })
    }

    // -- read --

    /// Get an owned copy of the value for `key`.  Returns `None` if absent.
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let root = self.root();
        if root == 0 {
            return None;
        }
        let leaf = self.find_leaf(root, key);
        let off = page_offset(leaf);
        let count = rd_u16(&self.mmap[off + 2..off + 4]) as usize;
        match self.leaf_find_slot(off, count, key) {
            Ok(i) => {
                let sp = off + HEADER_SIZE + i * SLOT_SIZE;
                let eo = off + rd_u16(&self.mmap[sp..sp + 2]) as usize;
                let kl = rd_u16(&self.mmap[eo..eo + 2]) as usize;
                let raw_vl = rd_u32(&self.mmap[eo + 2..eo + 6]);
                if raw_vl & OVFL_FLAG != 0 {
                    let real_len = (raw_vl & !OVFL_FLAG) as usize;
                    let ovfl = rd_u64(&self.mmap[eo + 6 + kl..eo + 6 + kl + 8]);
                    Some(self.read_overflow(ovfl, real_len))
                } else {
                    let vl = raw_vl as usize;
                    Some(self.mmap[eo + 6 + kl..eo + 6 + kl + vl].to_vec())
                }
            }
            Err(_) => None,
        }
    }

    pub fn len(&self) -> u64 {
        rd_u64(&self.mmap[32..40])
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    // -- write --

    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> io::Result<()> {
        let root = self.root();
        if root == 0 {
            let p = self.alloc_page()?;
            self.set_root(p);
            self.init_leaf(p, true);
            self.leaf_insert(page_offset(p), p, key, value)?;
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
        let existed = self.leaf_find_slot(loff, rd_u16(&self.mmap[loff + 2..loff + 4]) as usize, key).is_ok();
        let split = self.leaf_insert(loff, page, key, value)?;
        if !existed {
            self.inc_entries(1);
        }
        if let Some(si) = split {
            self.cascade_split(path, si)?;
        }
        Ok(())
    }

    pub fn delete(&mut self, key: &[u8]) -> io::Result<bool> {
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
                    let ovfl = rd_u64(&self.mmap[eo + 6 + kl..eo + 6 + kl + 8]);
                    self.free_overflow(ovfl);
                }
                self.leaf_remove_entry(off, i, count, data_off);
                self.dec_entries();
                Ok(true)
            }
            Err(_) => Ok(false),
        }
    }

    pub fn flush(&self) -> io::Result<()> {
        self.mmap.flush()
    }

    /// Iterate all entries in key order.
    pub fn iter(&self) -> BTreeIter<'_> {
        let mut leaf = self.root();
        if leaf != 0 {
            loop {
                let off = page_offset(leaf);
                if self.mmap[off] == PAGE_LEAF {
                    break;
                }
                leaf = rd_u64(&self.mmap[off + 8..off + 16]);
            }
        }
        BTreeIter {
            tree: self,
            page: leaf,
            slot: 0,
        }
    }

    /// Iterate entries with keys in `[start, end)` in key order.
    pub fn range<'a>(&'a self, start: &[u8], end: &'a [u8]) -> BTreeRange<'a> {
        let root = self.root();
        if root == 0 {
            return BTreeRange { tree: self, page: 0, slot: 0, end };
        }
        let leaf = self.find_leaf(root, start);
        let off = page_offset(leaf);
        let count = rd_u16(&self.mmap[off + 2..off + 4]) as usize;
        // Ok(i) = exact match at i; Err(i) = first slot >= start at i.
        let slot = match self.leaf_find_slot(off, count, start) {
            Ok(i) | Err(i) => i,
        };
        BTreeRange { tree: self, page: leaf, slot, end }
    }

    // -- meta accessors --

    fn root(&self) -> u64 {
        rd_u64(&self.mmap[8..16])
    }
    fn set_root(&mut self, p: u64) {
        wr_u64(&mut self.mmap[8..16], p);
    }
    fn inc_entries(&mut self, d: u64) {
        let c = rd_u64(&self.mmap[32..40]);
        wr_u64(&mut self.mmap[32..40], c.wrapping_add(d));
    }
    fn dec_entries(&mut self) {
        let c = rd_u64(&self.mmap[32..40]);
        wr_u64(&mut self.mmap[32..40], c.wrapping_sub(1));
    }

    // -- traversal --

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

    /// Binary search through the internal page's sorted separator array.
    /// Returns the child page to follow for `key`.
    fn internal_search(&self, off: usize, key: &[u8]) -> u64 {
        let count = rd_u16(&self.mmap[off + 2..off + 4]) as usize;
        let leftmost = rd_u64(&self.mmap[off + 8..off + 16]);
        if count == 0 {
            return leftmost;
        }
        // Find first separator strictly greater than key.
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

    /// Binary search through a leaf page's sorted slot array.
    ///
    /// Returns `Ok(slot)` if `key` is found, `Err(slot)` with the insertion
    /// point (first slot whose key > search key) if not found.
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

    // -- leaf insert --

    fn leaf_insert(
        &mut self,
        off: usize,
        page: u64,
        key: &[u8],
        value: &[u8],
    ) -> io::Result<Option<PageSplit>> {
        let count = rd_u16(&self.mmap[off + 2..off + 4]) as usize;
        let data_off = rd_u32(&self.mmap[off + 4..off + 8]) as usize;

        let ovfl = key.len() + value.len() + 6 > MAX_INLINE;
        let leaf_es = if ovfl { 6 + key.len() + 8 } else { 6 + key.len() + value.len() };
        let needed = leaf_es + SLOT_SIZE;
        let free_start = HEADER_SIZE + count * SLOT_SIZE;

        if free_start + needed > data_off {
            return self.leaf_split(off, page, key, value);
        }

        match self.leaf_find_slot(off, count, key) {
            Ok(i) => {
                // Key exists — handle overwrite.
                let sp = off + HEADER_SIZE + i * SLOT_SIZE;
                let eo = off + rd_u16(&self.mmap[sp..sp + 2]) as usize;
                let kl = rd_u16(&self.mmap[eo..eo + 2]) as usize;
                let raw_vl = rd_u32(&self.mmap[eo + 2..eo + 6]);
                // Free old overflow chain if present.
                if raw_vl & OVFL_FLAG != 0 {
                    let old_ovfl = rd_u64(&self.mmap[eo + 6 + kl..eo + 6 + kl + 8]);
                    self.free_overflow(old_ovfl);
                }
                // Fast path: same size inline value → overwrite in place.
                if !ovfl && raw_vl & OVFL_FLAG == 0 && raw_vl as usize == value.len() {
                    self.mmap[eo + 6 + kl..eo + 6 + kl + value.len()].copy_from_slice(value);
                    return Ok(None);
                }
                // Otherwise: remove then re-insert (size changed).
                self.leaf_remove_entry(off, i, count, data_off);
                return self.leaf_insert(off, page, key, value);
            }
            Err(pos) => {
                // New key — shift slots right to make room at `pos`.
                let ss = off + HEADER_SIZE;
                for j in (pos..count).rev() {
                    let v = rd_u16(&self.mmap[ss + j * SLOT_SIZE..ss + j * SLOT_SIZE + 2]);
                    wr_u16(
                        &mut self.mmap[ss + (j + 1) * SLOT_SIZE..ss + (j + 1) * SLOT_SIZE + 2],
                        v,
                    );
                }
                let nd = data_off - leaf_es;
                self.write_leaf_entry(off, nd, key, &SplitVal::Inline(value.to_vec()), ovfl)?;
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

    /// Write a leaf entry at byte offset `nd` within the page.
    /// For `SplitVal::Overflow`, the existing overflow chain is reused — no
    /// new pages are allocated and no data is read from disk.
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
                let ovfl_page = self.write_overflow(v)?;
                wr_u32(
                    &mut self.mmap[off + nd + 2..off + nd + 6],
                    v.len() as u32 | OVFL_FLAG,
                );
                self.mmap[off + nd + 6..off + nd + 6 + key.len()].copy_from_slice(key);
                wr_u64(
                    &mut self.mmap[off + nd + 6 + key.len()..off + nd + 6 + key.len() + 8],
                    ovfl_page,
                );
            }
            SplitVal::Inline(v) => {
                wr_u32(&mut self.mmap[off + nd + 2..off + nd + 6], v.len() as u32);
                self.mmap[off + nd + 6..off + nd + 6 + key.len()].copy_from_slice(key);
                self.mmap[off + nd + 6 + key.len()..off + nd + 6 + key.len() + v.len()]
                    .copy_from_slice(v);
            }
            SplitVal::Overflow { page: ovfl_page, real_len } => {
                wr_u32(
                    &mut self.mmap[off + nd + 2..off + nd + 6],
                    real_len | OVFL_FLAG,
                );
                self.mmap[off + nd + 6..off + nd + 6 + key.len()].copy_from_slice(key);
                wr_u64(
                    &mut self.mmap[off + nd + 6 + key.len()..off + nd + 6 + key.len() + 8],
                    *ovfl_page,
                );
            }
        }
        Ok(())
    }

    // -- leaf split (with truncated separator, no overflow materialization) --

    fn leaf_split(
        &mut self,
        off: usize,
        page: u64,
        key: &[u8],
        value: &[u8],
    ) -> io::Result<Option<PageSplit>> {
        let count = rd_u16(&self.mmap[off + 2..off + 4]) as usize;

        // Collect existing entries without materializing overflow data.
        let mut entries: Vec<(Vec<u8>, SplitVal)> = Vec::with_capacity(count + 1);
        for i in 0..count {
            let sp = off + HEADER_SIZE + i * SLOT_SIZE;
            let eo = off + rd_u16(&self.mmap[sp..sp + 2]) as usize;
            let kl = rd_u16(&self.mmap[eo..eo + 2]) as usize;
            let raw_vl = rd_u32(&self.mmap[eo + 2..eo + 6]);
            let k = self.mmap[eo + 6..eo + 6 + kl].to_vec();
            if raw_vl & OVFL_FLAG != 0 {
                let ovfl = rd_u64(&self.mmap[eo + 6 + kl..eo + 6 + kl + 8]);
                entries.push((k, SplitVal::Overflow { page: ovfl, real_len: raw_vl & !OVFL_FLAG }));
            } else {
                let vl = raw_vl as usize;
                entries.push((k, SplitVal::Inline(self.mmap[eo + 6 + kl..eo + 6 + kl + vl].to_vec())));
            }
        }

        // Insert new entry in sorted position.
        let ip = entries
            .binary_search_by(|(k, _)| k.as_slice().cmp(key))
            .unwrap_or_else(|i| i);
        entries.insert(ip, (key.to_vec(), SplitVal::Inline(value.to_vec())));

        // Size-based split point using the correct formula.
        let mut used = 0usize;
        let mid = entries
            .iter()
            .position(|(k, v)| {
                let entry_inline = match v {
                    SplitVal::Inline(val) => 6 + k.len() + val.len() <= MAX_INLINE,
                    SplitVal::Overflow { .. } => false,
                };
                let payload = match v {
                    SplitVal::Inline(val) if entry_inline => val.len(),
                    _ => 8, // overflow pointer
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
            // All fit — just rebuild the page.
            wr_u16(&mut self.mmap[off + 2..off + 4], 0);
            wr_u32(&mut self.mmap[off + 4..off + 8], PAGE_SIZE as u32);
            for (k, v) in entries {
                self.leaf_insert_split_entry(off, page, &k, v)?;
            }
            return Ok(None);
        }
        let mid = mid.max(1);

        let sep = truncated_separator(&entries[mid - 1].0, &entries[mid].0);

        // Clear left page and fill with entries[..mid].
        wr_u16(&mut self.mmap[off + 2..off + 4], 0);
        wr_u32(&mut self.mmap[off + 4..off + 8], PAGE_SIZE as u32);
        let right = self.alloc_page()?;
        self.init_leaf(right, false);

        let entries_right: Vec<(Vec<u8>, SplitVal)> = entries.split_off(mid);
        for (k, v) in entries {
            self.leaf_insert_split_entry(off, page, &k, v)?;
        }
        let ro = page_offset(right);
        for (k, v) in entries_right {
            self.leaf_insert_split_entry(ro, right, &k, v)?;
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

    /// Insert a single entry during a split, reusing existing overflow pages
    /// for `SplitVal::Overflow` entries instead of allocating new ones.
    fn leaf_insert_split_entry(
        &mut self,
        off: usize,
        _page: u64,
        key: &[u8],
        val: SplitVal,
    ) -> io::Result<()> {
        let count = rd_u16(&self.mmap[off + 2..off + 4]) as usize;
        let data_off = rd_u32(&self.mmap[off + 4..off + 8]) as usize;

        let leaf_es = match &val {
            SplitVal::Inline(v) if 6 + key.len() + v.len() <= MAX_INLINE => 6 + key.len() + v.len(),
            _ => 6 + key.len() + 8,
        };
        let is_ovfl = matches!(&val, SplitVal::Overflow { .. })
            || matches!(&val, SplitVal::Inline(v) if 6 + key.len() + v.len() > MAX_INLINE);

        // Entries arrive in sorted order during rebuild, so append at end.
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
        // Shift slot array left to close the gap.
        let ss = off + HEADER_SIZE;
        for j in pos + 1..count {
            let v = rd_u16(&self.mmap[ss + j * SLOT_SIZE..ss + j * SLOT_SIZE + 2]);
            wr_u16(
                &mut self.mmap[ss + (j - 1) * SLOT_SIZE..ss + (j - 1) * SLOT_SIZE + 2],
                v,
            );
        }
        // Shift data region up to reclaim the deleted entry's space.
        let dlen = eo_rel - data_off;
        self.mmap.copy_within(
            off + data_off..off + data_off + dlen,
            off + data_off + es,
        );
        // Adjust all remaining slot offsets that pointed into the moved region.
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
        // Binary search: first separator strictly greater than key.
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

    // -- overflow pages --

    fn write_overflow(&mut self, value: &[u8]) -> io::Result<u64> {
        let mut remaining = value;
        let mut first: u64 = 0;
        let mut prev: u64 = 0;
        while !remaining.is_empty() {
            let page = self.alloc_page()?;
            if first == 0 {
                first = page;
            }
            let off = page_offset(page);
            let chunk = remaining.len().min(OVFL_DATA);
            self.mmap[off..off + chunk].copy_from_slice(&remaining[..chunk]);
            wr_u64(&mut self.mmap[off + OVFL_DATA..off + PAGE_SIZE], 0);
            if prev != 0 {
                let po = page_offset(prev);
                wr_u64(&mut self.mmap[po + OVFL_DATA..po + PAGE_SIZE], page);
            }
            prev = page;
            remaining = &remaining[chunk..];
        }
        Ok(first)
    }

    fn read_overflow(&self, mut page: u64, total: usize) -> Vec<u8> {
        let mut result = Vec::with_capacity(total);
        while page != 0 {
            let off = page_offset(page);
            let next = rd_u64(&self.mmap[off + OVFL_DATA..off + PAGE_SIZE]);
            let chunk = if next != 0 {
                OVFL_DATA
            } else {
                total - result.len()
            };
            result.extend_from_slice(&self.mmap[off..off + chunk]);
            page = next;
        }
        result
    }

    fn free_overflow(&mut self, mut page: u64) {
        while page != 0 {
            let off = page_offset(page);
            let next = rd_u64(&self.mmap[off + OVFL_DATA..off + PAGE_SIZE]);
            self.free_page(page);
            page = next;
        }
    }

    // -- page allocation --

    fn free_head(&self) -> u64 {
        rd_u64(&self.mmap[16..24])
    }
    fn set_free_head(&mut self, p: u64) {
        wr_u64(&mut self.mmap[16..24], p);
    }
    fn pages(&self) -> u64 {
        rd_u64(&self.mmap[24..32])
    }
    fn set_pages(&mut self, n: u64) {
        wr_u64(&mut self.mmap[24..32], n);
    }

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

    fn free_page(&mut self, page: u64) {
        let head = self.free_head();
        wr_u64(
            &mut self.mmap[page_offset(page)..page_offset(page) + 8],
            head,
        );
        self.set_free_head(page);
    }

    fn grow(&mut self, extra: u64) -> io::Result<()> {
        let np = (self.pages() + extra) * PAGE_SIZE as u64;
        self.file.set_len(np)?;
        self.mmap = unsafe { MmapMut::map_mut(&self.file)? };
        Ok(())
    }

    fn init_leaf(&mut self, page: u64, root: bool) {
        let off = page_offset(page);
        self.mmap[off] = PAGE_LEAF;
        self.mmap[off + 1] = if root { FLAG_ROOT } else { 0 };
        wr_u16(&mut self.mmap[off + 2..off + 4], 0);
        wr_u32(&mut self.mmap[off + 4..off + 8], PAGE_SIZE as u32);
        wr_u64(&mut self.mmap[off + 8..off + 16], 0);
    }

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

pub struct BTreeIter<'a> {
    tree: &'a BPlusTree,
    page: u64,
    slot: usize,
}

impl<'a> Iterator for BTreeIter<'a> {
    type Item = (Vec<u8>, Vec<u8>);
    fn next(&mut self) -> Option<Self::Item> {
        while self.page != 0 {
            let off = page_offset(self.page);
            let count = rd_u16(&self.tree.mmap[off + 2..off + 4]) as usize;
            if self.slot < count {
                let sp = off + HEADER_SIZE + self.slot * SLOT_SIZE;
                let eo = off + rd_u16(&self.tree.mmap[sp..sp + 2]) as usize;
                let kl = rd_u16(&self.tree.mmap[eo..eo + 2]) as usize;
                let key = self.tree.mmap[eo + 6..eo + 6 + kl].to_vec();
                let raw_vl = rd_u32(&self.tree.mmap[eo + 2..eo + 6]);
                let val = if raw_vl & OVFL_FLAG != 0 {
                    let real_len = (raw_vl & !OVFL_FLAG) as usize;
                    let ovfl = rd_u64(&self.tree.mmap[eo + 6 + kl..eo + 6 + kl + 8]);
                    self.tree.read_overflow(ovfl, real_len)
                } else {
                    let vl = raw_vl as usize;
                    self.tree.mmap[eo + 6 + kl..eo + 6 + kl + vl].to_vec()
                };
                self.slot += 1;
                return Some((key, val));
            }
            self.page = rd_u64(&self.tree.mmap[off + 8..off + 16]);
            self.slot = 0;
        }
        None
    }
}

pub struct BTreeRange<'a> {
    tree: &'a BPlusTree,
    page: u64,
    slot: usize,
    end: &'a [u8],
}

impl<'a> Iterator for BTreeRange<'a> {
    type Item = (Vec<u8>, Vec<u8>);
    fn next(&mut self) -> Option<Self::Item> {
        while self.page != 0 {
            let off = page_offset(self.page);
            let count = rd_u16(&self.tree.mmap[off + 2..off + 4]) as usize;
            if self.slot < count {
                let sp = off + HEADER_SIZE + self.slot * SLOT_SIZE;
                let eo = off + rd_u16(&self.tree.mmap[sp..sp + 2]) as usize;
                let kl = rd_u16(&self.tree.mmap[eo..eo + 2]) as usize;
                let key = &self.tree.mmap[eo + 6..eo + 6 + kl];
                if key >= self.end {
                    return None;
                }
                let key = key.to_vec();
                let raw_vl = rd_u32(&self.tree.mmap[eo + 2..eo + 6]);
                let val = if raw_vl & OVFL_FLAG != 0 {
                    let real_len = (raw_vl & !OVFL_FLAG) as usize;
                    let ovfl = rd_u64(&self.tree.mmap[eo + 6 + kl..eo + 6 + kl + 8]);
                    self.tree.read_overflow(ovfl, real_len)
                } else {
                    let vl = raw_vl as usize;
                    self.tree.mmap[eo + 6 + kl..eo + 6 + kl + vl].to_vec()
                };
                self.slot += 1;
                return Some((key, val));
            }
            self.page = rd_u64(&self.tree.mmap[off + 8..off + 16]);
            self.slot = 0;
        }
        None
    }
}

impl Drop for BPlusTree {
    fn drop(&mut self) {
        let _ = self.mmap.flush();
    }
}

impl fmt::Debug for BPlusTree {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BPlusTree")
            .field("entries", &self.len())
            .field("pages", &self.pages())
            .finish()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    fn tmp(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("zendb_btree_{}", name))
    }

    #[test]
    fn create_and_open() {
        let p = tmp("co");
        BPlusTree::create(&p).unwrap().flush().unwrap();
        BPlusTree::open(&p).unwrap();
        fs::remove_file(&p).ok();
    }

    #[test]
    fn insert_and_get() {
        let p = tmp("ig");
        let mut t = BPlusTree::create(&p).unwrap();
        t.insert(b"hello", b"world").unwrap();
        t.insert(b"foo", b"bar").unwrap();
        t.flush().unwrap();
        assert_eq!(t.get(b"hello"), Some(b"world".to_vec()));
        assert_eq!(t.get(b"foo"), Some(b"bar".to_vec()));
        assert_eq!(t.get(b"nope"), None);
        fs::remove_file(&p).ok();
    }

    #[test]
    fn overwrite() {
        let p = tmp("ow");
        let mut t = BPlusTree::create(&p).unwrap();
        t.insert(b"k", b"v1").unwrap();
        t.insert(b"k", b"v2").unwrap();
        assert_eq!(t.get(b"k"), Some(b"v2".to_vec()));
        assert_eq!(t.len(), 1);
        fs::remove_file(&p).ok();
    }

    #[test]
    fn delete() {
        let p = tmp("del");
        let mut t = BPlusTree::create(&p).unwrap();
        t.insert(b"a", b"1").unwrap();
        t.insert(b"b", b"2").unwrap();
        assert!(t.delete(b"a").unwrap());
        assert!(!t.delete(b"a").unwrap());
        assert_eq!(t.get(b"a"), None);
        assert_eq!(t.get(b"b"), Some(b"2".to_vec()));
        assert_eq!(t.len(), 1);
        fs::remove_file(&p).ok();
    }

    #[test]
    fn empty_ops() {
        let p = tmp("e");
        let t = BPlusTree::create(&p).unwrap();
        assert!(t.is_empty());
        assert_eq!(t.get(b"x"), None);
        assert!(t.iter().next().is_none());
        fs::remove_file(&p).ok();
    }

    #[test]
    fn iterator() {
        let p = tmp("it");
        let mut t = BPlusTree::create(&p).unwrap();
        t.insert(b"c", b"3").unwrap();
        t.insert(b"a", b"1").unwrap();
        t.insert(b"b", b"2").unwrap();
        let v: Vec<_> = t.iter().collect();
        assert_eq!(v.len(), 3);
        assert_eq!(v[0].0, b"a");
        assert_eq!(v[1].0, b"b");
        assert_eq!(v[2].0, b"c");
        fs::remove_file(&p).ok();
    }

    #[test]
    fn range_query() {
        let p = tmp("rq");
        let mut t = BPlusTree::create(&p).unwrap();
        for i in 0u32..20 {
            let key = format!("k{:04}", i);
            t.insert(key.as_bytes(), &i.to_le_bytes()).unwrap();
        }
        let keys: Vec<_> = t
            .range(b"k0005", b"k0010")
            .map(|(k, _)| String::from_utf8(k).unwrap())
            .collect();
        assert_eq!(keys, ["k0005", "k0006", "k0007", "k0008", "k0009"]);
        fs::remove_file(&p).ok();
    }

    #[test]
    fn range_empty_and_bounds() {
        let p = tmp("rqb");
        let mut t = BPlusTree::create(&p).unwrap();
        t.insert(b"b", b"2").unwrap();
        t.insert(b"d", b"4").unwrap();
        // Range that matches nothing.
        assert_eq!(t.range(b"e", b"z").count(), 0);
        // Range that starts before first key.
        let keys: Vec<_> = t.range(b"a", b"c").map(|(k, _)| k).collect();
        assert_eq!(keys, [b"b".to_vec()]);
        fs::remove_file(&p).ok();
    }

    #[test]
    fn reopen() {
        let p = tmp("ro");
        {
            let mut t = BPlusTree::create(&p).unwrap();
            t.insert(b"p", b"d").unwrap();
            t.flush().unwrap();
        }
        assert_eq!(BPlusTree::open(&p).unwrap().get(b"p"), Some(b"d".to_vec()));
        fs::remove_file(&p).ok();
    }

    #[test]
    fn split_leaf() {
        let p = tmp("sl");
        let mut t = BPlusTree::create(&p).unwrap();
        for i in 0u32..500 {
            t.insert(format!("k{:04}", i).as_bytes(), &i.to_le_bytes())
                .unwrap();
        }
        for i in 0u32..500 {
            let key = format!("k{:04}", i);
            assert_eq!(t.get(key.as_bytes()), Some(i.to_le_bytes().to_vec()));
        }
        assert_eq!(t.len(), 500);
        fs::remove_file(&p).ok();
    }

    #[test]
    fn large_value_overflow() {
        let p = tmp("ovfl");
        let mut t = BPlusTree::create(&p).unwrap();
        let big = vec![0xABu8; 10_000];
        t.insert(b"big", &big).unwrap();
        t.flush().unwrap();
        assert_eq!(t.get(b"big"), Some(big.clone()));
        let bigger = vec![0xCDu8; 15_000];
        t.insert(b"big", &bigger).unwrap();
        assert_eq!(t.get(b"big"), Some(bigger.clone()));
        assert!(t.delete(b"big").unwrap());
        assert_eq!(t.get(b"big"), None);
        assert_eq!(t.len(), 0);
        fs::remove_file(&p).ok();
    }

    #[test]
    fn truncated_separator_basic() {
        assert_eq!(truncated_separator(b"abc", b"abd"), b"abd".to_vec());
        assert_eq!(
            truncated_separator(b"abc", b"abcd"),
            vec![b'a', b'b', b'c', 0]
        );
        assert_eq!(truncated_separator(b"hello", b"world"), b"w".to_vec());
        assert_eq!(truncated_separator(b"key_a", b"key_b"), b"key_b".to_vec());
        assert_eq!(truncated_separator(b"aaa", b"aab"), b"aab".to_vec());
    }

    #[test]
    fn very_large_value() {
        let p = tmp("vlarge");
        let mut t = BPlusTree::create(&p).unwrap();
        let huge = vec![0xEFu8; 50_000];
        t.insert(b"huge", &huge).unwrap();
        t.flush().unwrap();
        assert_eq!(t.get(b"huge"), Some(huge));
        fs::remove_file(&p).ok();
    }

    #[test]
    fn delete_counter_correct() {
        let p = tmp("dcnt");
        let mut t = BPlusTree::create(&p).unwrap();
        for i in 0u32..10 {
            t.insert(format!("k{}", i).as_bytes(), b"v").unwrap();
        }
        assert_eq!(t.len(), 10);
        for i in 0u32..5 {
            t.delete(format!("k{}", i).as_bytes()).unwrap();
        }
        assert_eq!(t.len(), 5);
        fs::remove_file(&p).ok();
    }

    // --- Regression tests for fixed bugs ---

    // Old bug: dec_entries used inc_entries(u64::MAX) — wrapping_add(u64::MAX)
    // is wrapping_sub(1), which technically works but is semantically wrong and
    // dangerous.  This test verifies the counter stays exact after many
    // insert/delete cycles and never wraps to a huge number.
    #[test]
    fn delete_counter_never_wraps() {
        let p = tmp("nowrap");
        let mut t = BPlusTree::create(&p).unwrap();
        for i in 0u32..100 {
            t.insert(format!("k{:04}", i).as_bytes(), b"val").unwrap();
        }
        assert_eq!(t.len(), 100);
        for i in 0u32..100 {
            assert!(t.delete(format!("k{:04}", i).as_bytes()).unwrap());
        }
        // If the counter wrapped (old bug under different conditions), this
        // would be u64::MAX or some huge value instead of 0.
        assert_eq!(t.len(), 0, "counter wrapped: got {}", t.len());
        // Re-insert and delete again — second pass also must be exact.
        for i in 0u32..50 {
            t.insert(format!("k{:04}", i).as_bytes(), b"val").unwrap();
        }
        assert_eq!(t.len(), 50);
        for i in 0u32..50 {
            t.delete(format!("k{:04}", i).as_bytes()).unwrap();
        }
        assert_eq!(t.len(), 0);
        fs::remove_file(&p).ok();
    }

    // Old bug: leaf_split size formula used `v.len().min(8)` for all values,
    // underestimating inline values > 8 bytes.  A 50-byte value would be
    // counted as 8 bytes, meaning the split point would be calculated as if
    // 42 bytes of per-entry space don't exist — placing too many entries on
    // the left page, potentially overflowing it.
    #[test]
    fn split_medium_inline_values() {
        let p = tmp("split_med");
        let mut t = BPlusTree::create(&p).unwrap();
        // 50-byte values: inline (< MAX_INLINE), but old formula counted them
        // as 8 bytes each — 42-byte underestimate per entry.
        let value = vec![0xAAu8; 50];
        for i in 0u32..300 {
            t.insert(format!("key{:06}", i).as_bytes(), &value).unwrap();
        }
        for i in 0u32..300 {
            let key = format!("key{:06}", i);
            assert_eq!(
                t.get(key.as_bytes()),
                Some(value.clone()),
                "missing key {}",
                key
            );
        }
        assert_eq!(t.len(), 300);
        // Iterator must also visit all 300 in order.
        let count = t.iter().count();
        assert_eq!(count, 300);
        fs::remove_file(&p).ok();
    }

    // Old bug: leaf_split materialised ALL values — for overflow entries this
    // read the entire overflow chain just to write it back.  Worse, it created
    // NEW overflow pages for the same data and orphaned the old ones (page
    // leak).  The new code carries the overflow page pointer through the split
    // without touching the data.
    #[test]
    fn overflow_entries_survive_split() {
        let p = tmp("ovfl_split");
        let mut t = BPlusTree::create(&p).unwrap();
        // 8 000-byte values: well above the inline threshold, so every entry
        // goes through an overflow chain.  Inserting 30 of them forces multiple
        // leaf splits.
        let big = vec![0xBBu8; 8_000];
        for i in 0u32..30 {
            t.insert(format!("ovk{:04}", i).as_bytes(), &big).unwrap();
        }
        for i in 0u32..30 {
            let key = format!("ovk{:04}", i);
            assert_eq!(t.get(key.as_bytes()), Some(big.clone()), "key {}", key);
        }
        assert_eq!(t.len(), 30);
        // Range scan must also work across page boundaries.
        let range_count = t.range(b"ovk0000", b"ovk0030").count();
        assert_eq!(range_count, 30);
        fs::remove_file(&p).ok();
    }

    // Verify binary search in internal_search and leaf_find_slot produces the
    // same results as the old linear scan would for a large, randomly-ordered
    // dataset that exercises many internal page levels.
    #[test]
    fn binary_search_agrees_with_expected_values() {
        let p = tmp("bsearch");
        let mut t = BPlusTree::create(&p).unwrap();
        // Insert in reverse order so the tree has to do real sorted insertion.
        for i in (0u32..1000).rev() {
            let k = format!("zz{:06}", i);
            t.insert(k.as_bytes(), &i.to_le_bytes()).unwrap();
        }
        // Forward lookup of every key.
        for i in 0u32..1000 {
            let k = format!("zz{:06}", i);
            assert_eq!(t.get(k.as_bytes()), Some(i.to_le_bytes().to_vec()));
        }
        // Iterator order must be ascending.
        let keys: Vec<_> = t.iter().map(|(k, _)| k).collect();
        assert_eq!(keys.len(), 1000);
        for w in keys.windows(2) {
            assert!(w[0] < w[1], "out of order: {:?} >= {:?}", w[0], w[1]);
        }
        fs::remove_file(&p).ok();
    }

    // Range query must correctly handle the edge where `start` falls between
    // two leaf pages (i.e. the binary-search entry point lands in the right
    // leaf from the start rather than scanning from the leftmost leaf).
    #[test]
    fn range_mid_dataset() {
        let p = tmp("range_mid");
        let mut t = BPlusTree::create(&p).unwrap();
        for i in 0u32..500 {
            t.insert(format!("r{:06}", i).as_bytes(), &i.to_le_bytes()).unwrap();
        }
        let got: Vec<u32> = t
            .range(b"r000200", b"r000210")
            .map(|(_, v)| u32::from_le_bytes(v.try_into().unwrap()))
            .collect();
        assert_eq!(got, (200u32..210).collect::<Vec<_>>());
        fs::remove_file(&p).ok();
    }
}
