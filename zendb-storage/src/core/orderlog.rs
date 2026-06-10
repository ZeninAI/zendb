//! OrderLog - in-memory ordered KV store with WAL durability.
//!
//! # Architecture
//!
//! Keys and values live in an arena-backed skip list. The mmap'd file is
//! only a write-ahead log used to reconstruct the skip list on the next
//! `open`; reads never deserialize from the mmap after replay.
//!
//! # Public surface
//!
//! Aside from [`OrderLog::create`] and [`OrderLog::open`], operations are
//! exposed through [`Backend`] and [`OrderedBackend`].
//!
//! # Writing
//!
//! Writes append either a live record or tombstone to the WAL, then mutate
//! the in-memory skip list.
//!
//! # Compaction
//!
//! Dead bytes from overwrites and tombstones trigger in-place compaction when
//! the configured ratio is reached. Live skip-list records are copied by WAL
//! offset to the front of the existing file, followed by `SENTINEL` so the
//! next `replay_wal` terminates at the new live tail.
//!
//! # File format
//!
//! Header: `[MAGIC: u32 LE = 0x474F4C4F ("OLOG")]`
//! Live: `[value_size: u32 LE][V bytes][key_size: u32 LE][K bytes]`
//! Tombstone: `[0xFFFF_FFFF: u32][key_size: u32 LE][K bytes]`

use std::{
    borrow::Cow,
    fmt,
    fs::{File, OpenOptions},
    hash::Hash,
    io::{self},
    path::Path,
};

use bincode::{Decode, Encode};
use memmap2::MmapMut;

use crate::core::backend::{Backend, OrderedBackend};
use crate::utils::{
    fast_rand,
    serdes::{deserialize_from, read_u32_le, with_two_scratches},
};

const DEFAULT_INITIAL_CAPACITY: u64 = 1024 * 1024;
const DEFAULT_COMPACTION_RATIO: f64 = 0.5;
const MAGIC: u32 = 0x474F4C4F;
const HEADER_SIZE: usize = 4;
const TOMBSTONE: u32 = u32::MAX;
/// Replay terminator written at the live tail by `create`, every record
/// write, `clear`, and `compact`. Distinct from `value_size == 0` (a
/// legitimate empty-value encoding) so a `V` whose bincode representation
/// is 0 bytes (e.g. the unit type) round-trips correctly. Mirrors
/// KeyDir's `SENTINEL`.
const SENTINEL: u32 = u32::MAX - 1;
const MAX_LEVEL: usize = 16;

type Heads = [Option<usize>; MAX_LEVEL];

/// Offset and total on-disk size of a live record in the WAL. Mirrors
/// KeyDir's `EntryMeta` so the two backends speak the same vocabulary
/// when authoring tombstones and computing dead bytes. Individual
/// `value_size` and `key_size` live on disk and are re-read from the
/// mmap when the tombstone writer needs to locate the encoded key
/// inside the record.
#[derive(Debug, Clone)]
struct EntryMeta {
    offset: u64,
    /// Total bytes the live record occupies on disk: two u32 length
    /// prefixes (8) plus the encoded key and value bytes.
    record_size: u32,
}

struct SkipNode<K, V> {
    key: K,
    value: V,
    meta: EntryMeta,
    prev: Option<usize>,
    level: usize,
}

/// Skip-list arena with a flat next-pointer table.
///
/// `nexts` stores each node's forward pointers in a single contiguous
/// `Vec<Option<usize>>`, laid out as a row-major `[node_idx][level]`
/// grid of stride `MAX_LEVEL`. Each node owns a full row regardless of
/// its actual `level`; entries past `level` are unused but cheap and
/// avoid the per-node `Box<[Option<usize>]>` heap allocation the
/// previous design paid on every insert.
struct OrderIndex<K: Ord, V> {
    arena: Vec<SkipNode<K, V>>,
    /// Flat forward-pointer table. Size = `arena.len() * MAX_LEVEL`.
    /// `nexts[node_idx * MAX_LEVEL + level]` is `node_idx`'s next pointer at `level`.
    nexts: Vec<Option<usize>>,
    free: Vec<usize>,
    heads: Heads,
    height: usize,
    len: usize,
}

struct Finger {
    update: Heads,
    cursor: Option<usize>,
}

impl<K: Ord, V> OrderIndex<K, V> {
    fn new() -> Self {
        OrderIndex {
            arena: Vec::new(),
            nexts: Vec::new(),
            free: Vec::new(),
            heads: [None; MAX_LEVEL],
            height: 0,
            len: 0,
        }
    }

    /// Forward pointer of `node` at `level`. Always indexes a real entry
    /// in `nexts` — the row is allocated to MAX_LEVEL even when the
    /// node's `level` is smaller. Pointers past `node.level` are unused
    /// but read as `None`.
    #[inline]
    fn next_at(&self, node: usize, level: usize) -> Option<usize> {
        self.nexts[node * MAX_LEVEL + level]
    }

    #[inline]
    fn set_next(&mut self, node: usize, level: usize, value: Option<usize>) {
        self.nexts[node * MAX_LEVEL + level] = value;
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

    /// Standard skip-list search that fills `update[]` at every level.
    /// Returns `(predecessors, found_idx)`. Callers that need to
    /// unlink (remove) or link in (insert at not-found) rely on the
    /// complete `update[]` and pay the full O(height) descent.
    fn search(&self, key: &K) -> (Heads, Option<usize>) {
        let mut update: Heads = [None; MAX_LEVEL];
        let mut found: Option<usize> = None;
        let mut prev: Option<usize> = None;

        for i in (0..self.height).rev() {
            let mut x = match prev {
                Some(p) => self.next_at(p, i),
                None => self.heads[i],
            };
            while let Some(idx) = x {
                match self.arena[idx].key.cmp(key) {
                    std::cmp::Ordering::Less => {
                        prev = Some(idx);
                        x = self.next_at(idx, i);
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

    /// Short-circuiting find — returns as soon as the key matches at
    /// any level. Used by read-only callers (`get`, `contains`) where
    /// the predecessor path is irrelevant. Saves the descent through
    /// the levels below the first match.
    fn find(&self, key: &K) -> Option<usize> {
        if self.is_empty() {
            return None;
        }
        let mut prev: Option<usize> = None;
        for i in (0..self.height).rev() {
            let mut x = match prev {
                Some(p) => self.next_at(p, i),
                None => self.heads[i],
            };
            while let Some(idx) = x {
                match self.arena[idx].key.cmp(key) {
                    std::cmp::Ordering::Less => {
                        prev = Some(idx);
                        x = self.next_at(idx, i);
                    }
                    std::cmp::Ordering::Equal => return Some(idx),
                    std::cmp::Ordering::Greater => break,
                }
            }
        }
        None
    }

    fn get(&self, key: &K) -> Option<&V> {
        self.find(key).map(|idx| &self.arena[idx].value)
    }

    fn contains(&self, key: &K) -> bool {
        self.find(key).is_some()
    }

    fn alloc_node(&mut self, key: K, value: V, meta: EntryMeta, level: usize) -> usize {
        if let Some(idx) = self.free.pop() {
            self.arena[idx] = SkipNode {
                key,
                value,
                meta,
                prev: None,
                level,
            };
            // Re-initialize this node's forward-pointer row to None;
            // remove_found does this too but we reset defensively in
            // case a future caller frees a node without clearing.
            let base = idx * MAX_LEVEL;
            for slot in &mut self.nexts[base..base + MAX_LEVEL] {
                *slot = None;
            }
            idx
        } else {
            let idx = self.arena.len();
            self.arena.push(SkipNode {
                key,
                value,
                meta,
                prev: None,
                level,
            });
            // Grow the flat next-array by one row. New entries default
            // to None so we don't need to touch them after extending.
            self.nexts.resize(self.nexts.len() + MAX_LEVEL, None);
            idx
        }
    }

    /// Insert or overwrite. Returns the previous record's `EntryMeta`
    /// if `key` was already present (so the caller can charge its
    /// size to `dead_bytes`); returns `None` for a fresh insert.
    fn insert(&mut self, key: K, value: V, meta: EntryMeta) -> Option<EntryMeta> {
        let (update, found) = self.search(&key);
        if let Some(idx) = found {
            self.arena[idx].value = value;
            return Some(std::mem::replace(&mut self.arena[idx].meta, meta));
        }
        self.insert_with_update(key, value, meta, update);
        None
    }

    fn insert_with_update(&mut self, key: K, value: V, meta: EntryMeta, update: Heads) {
        let level = self.random_level();
        if level > self.height {
            self.height = level;
        }

        let new_idx = self.alloc_node(key, value, meta, level);
        self.link_new_node(new_idx, &update);
        self.len += 1;
    }

    fn link_new_node(&mut self, new_idx: usize, update: &Heads) {
        let level = self.arena[new_idx].level;
        self.arena[new_idx].prev = update[0];
        for (i, prev) in update.iter().copied().enumerate().take(level) {
            if let Some(p) = prev {
                let pn = self.next_at(p, i);
                self.set_next(new_idx, i, pn);
                self.set_next(p, i, Some(new_idx));
            } else {
                self.set_next(new_idx, i, self.heads[i]);
                self.heads[i] = Some(new_idx);
            }
        }
        if let Some(next) = self.next_at(new_idx, 0) {
            self.arena[next].prev = Some(new_idx);
        }
    }

    /// Remove `key`. Returns the removed entry's `EntryMeta` so the
    /// caller can append a tombstone reusing the already-encoded key
    /// bytes still living at `meta.offset`.
    fn remove_drop(&mut self, key: &K) -> Option<EntryMeta> {
        if self.is_empty() {
            return None;
        }
        let (update, found) = self.search(key);
        found.map(|idx| self.remove_found(update, idx))
    }

    fn remove_found(&mut self, update: Heads, idx: usize) -> EntryMeta {
        let level = self.arena[idx].level;
        let next0 = self.next_at(idx, 0);
        let prev0 = self.arena[idx].prev;

        for (i, prev) in update.iter().copied().enumerate().take(level) {
            let nxt = self.next_at(idx, i);
            if let Some(p) = prev {
                self.set_next(p, i, nxt);
            } else {
                self.heads[i] = nxt;
            }
        }
        if let Some(next) = next0 {
            self.arena[next].prev = prev0;
        }
        while self.height > 0 && self.heads[self.height - 1].is_none() {
            self.height -= 1;
        }
        let meta = self.arena[idx].meta.clone();
        // Clear this node's forward-pointer row so a future alloc_node
        // recycling this slot starts from a clean state.
        let base = idx * MAX_LEVEL;
        for slot in &mut self.nexts[base..base + MAX_LEVEL] {
            *slot = None;
        }
        self.free.push(idx);
        self.len -= 1;
        meta
    }

    fn clear(&mut self) {
        self.arena.clear();
        self.nexts.clear();
        self.free.clear();
        self.heads = [None; MAX_LEVEL];
        self.height = 0;
        self.len = 0;
    }

    fn iter(&self) -> OrderIndexIter<'_, K, V> {
        OrderIndexIter {
            arena: &self.arena,
            nexts: &self.nexts,
            current: self.heads[0],
        }
    }

    fn range_owned_end(&self, start: &K, end: K) -> OrderIndexRangeIter<'_, K, V> {
        let first = if self.is_empty() {
            None
        } else {
            let (update, found) = self.search(start);
            found.or_else(|| match update[0] {
                None => self.heads[0],
                Some(p) => self.next_at(p, 0),
            })
        };
        OrderIndexRangeIter {
            arena: &self.arena,
            nexts: &self.nexts,
            current: first,
            end,
        }
    }

    fn first_idx(&self) -> Option<usize> {
        self.heads[0]
    }

    fn last_idx(&self) -> Option<usize> {
        if self.is_empty() {
            return None;
        }
        let mut idx: Option<usize> = None;
        for i in (0..self.height).rev() {
            let mut cur = match idx {
                Some(p) => self.next_at(p, i),
                None => self.heads[i],
            };
            while let Some(c) = cur {
                idx = Some(c);
                cur = self.next_at(c, i);
            }
        }
        idx
    }

    fn predecessor_of(&self, key: &K) -> Option<usize> {
        if self.is_empty() {
            None
        } else {
            self.search(key).0[0]
        }
    }

    fn iter_rev(&self) -> OrderIndexRevIter<'_, K, V> {
        OrderIndexRevIter {
            arena: &self.arena,
            current: self.last_idx(),
        }
    }

    fn range_rev(&self, start: K, end: &K) -> OrderIndexRevRangeIter<'_, K, V> {
        OrderIndexRevRangeIter {
            arena: &self.arena,
            current: self.predecessor_of(end),
            start,
        }
    }

    fn finger_at_start(&self) -> Finger {
        Finger {
            update: [None; MAX_LEVEL],
            cursor: self.heads[0],
        }
    }

    fn advance_finger_to(&self, finger: &mut Finger, target: &K) {
        while let Some(idx) = finger.cursor {
            if self.arena[idx].key >= *target {
                break;
            }
            let level = self.arena[idx].level;
            for i in 0..level {
                finger.update[i] = Some(idx);
            }
            finger.cursor = self.next_at(idx, 0);
        }
    }

    fn finger_matches(&self, finger: &Finger, key: &K) -> bool {
        finger.cursor.is_some_and(|idx| self.arena[idx].key == *key)
    }

    fn insert_at_finger(&mut self, finger: &mut Finger, key: K, value: V, meta: EntryMeta) {
        let level = self.random_level();
        if level > self.height {
            self.height = level;
        }
        let new_idx = self.alloc_node(key, value, meta, level);
        self.link_new_node(new_idx, &finger.update);
        for i in 0..level {
            finger.update[i] = Some(new_idx);
        }
        // Pin the cursor on the newly inserted node so a duplicate of
        // `key` on the next iteration of `bulk_put_sorted` is caught
        // by `finger_matches` and routed through `overwrite_at_finger`.
        // Without this, the cursor stays at the first node strictly
        // greater than `key`, and the duplicate silently lands as a
        // second arena slot unreachable from `get`.
        finger.cursor = Some(new_idx);
        self.len += 1;
    }

    fn overwrite_at_finger(&mut self, finger: &Finger, value: V, meta: EntryMeta) -> EntryMeta {
        let idx = finger.cursor.expect("finger cursor must point at a node");
        self.arena[idx].value = value;
        std::mem::replace(&mut self.arena[idx].meta, meta)
    }

    /// Remove the node the finger currently points at. Returns the
    /// removed entry's `EntryMeta` so the caller can write a tombstone
    /// reusing the encoded key bytes still living at `meta.offset`.
    fn remove_at_finger(&mut self, finger: &mut Finger) -> EntryMeta {
        let idx = finger.cursor.expect("finger cursor must point at a node");
        let next0 = self.next_at(idx, 0);
        let prev0 = self.arena[idx].prev;
        let level = self.arena[idx].level;

        for i in 0..level {
            let nxt = self.next_at(idx, i);
            if let Some(p) = finger.update[i] {
                self.set_next(p, i, nxt);
            } else {
                self.heads[i] = nxt;
            }
        }
        if let Some(next) = next0 {
            self.arena[next].prev = prev0;
        }
        while self.height > 0 && self.heads[self.height - 1].is_none() {
            self.height -= 1;
        }
        finger.cursor = next0;
        let meta = self.arena[idx].meta.clone();
        // Clear this node's forward-pointer row before recycling.
        let base = idx * MAX_LEVEL;
        for slot in &mut self.nexts[base..base + MAX_LEVEL] {
            *slot = None;
        }
        self.free.push(idx);
        self.len -= 1;
        meta
    }
}

struct OrderIndexIter<'a, K, V> {
    arena: &'a [SkipNode<K, V>],
    nexts: &'a [Option<usize>],
    current: Option<usize>,
}

impl<'a, K, V> Iterator for OrderIndexIter<'a, K, V> {
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        let idx = self.current?;
        let node = &self.arena[idx];
        self.current = self.nexts[idx * MAX_LEVEL];
        Some((&node.key, &node.value))
    }
}

struct OrderIndexRangeIter<'a, K: Ord, V> {
    arena: &'a [SkipNode<K, V>],
    nexts: &'a [Option<usize>],
    current: Option<usize>,
    end: K,
}

impl<'a, K: Ord, V> Iterator for OrderIndexRangeIter<'a, K, V> {
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        let idx = self.current?;
        let node = &self.arena[idx];
        if node.key >= self.end {
            return None;
        }
        self.current = self.nexts[idx * MAX_LEVEL];
        Some((&node.key, &node.value))
    }
}

struct OrderIndexRevIter<'a, K, V> {
    arena: &'a [SkipNode<K, V>],
    current: Option<usize>,
}

impl<'a, K, V> Iterator for OrderIndexRevIter<'a, K, V> {
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        let idx = self.current?;
        let node = &self.arena[idx];
        self.current = node.prev;
        Some((&node.key, &node.value))
    }
}

struct OrderIndexRevRangeIter<'a, K: Ord, V> {
    arena: &'a [SkipNode<K, V>],
    current: Option<usize>,
    start: K,
}

impl<'a, K: Ord, V> Iterator for OrderIndexRevRangeIter<'a, K, V> {
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        let idx = self.current?;
        let node = &self.arena[idx];
        if node.key < self.start {
            return None;
        }
        self.current = node.prev;
        Some((&node.key, &node.value))
    }
}

pub struct ConsumeValIter<'a, K, V> {
    mmap: &'a [u8],
    cursor: usize,
    _marker: std::marker::PhantomData<(K, V)>,
}

impl<'a, K, V> Iterator for ConsumeValIter<'a, K, V>
where
    K: Decode<()>,
    V: Decode<()>,
{
    type Item = io::Result<(K, Option<V>)>;

    fn next(&mut self) -> Option<Self::Item> {
        let value_size = read_u32_le(self.mmap, self.cursor)?;
        if value_size == SENTINEL {
            return None;
        }

        let is_tombstone = value_size == TOMBSTONE;
        let key_size_off = if is_tombstone {
            self.cursor + 4
        } else {
            self.cursor + 4 + value_size as usize
        };
        let key_size =
            read_u32_le(self.mmap, key_size_off).expect("key_size header within mmap bounds");
        let key_start = key_size_off + 4;
        let entry_end = key_start + key_size as usize;

        let key: K = match deserialize_from(&self.mmap[key_start..entry_end]) {
            Ok(k) => k,
            Err(e) => return Some(Err(e)),
        };

        let item = if is_tombstone {
            (key, None)
        } else {
            let value_start = self.cursor + 4;
            let value_end = value_start + value_size as usize;
            let value: V = match deserialize_from(&self.mmap[value_start..value_end]) {
                Ok(v) => v,
                Err(e) => return Some(Err(e)),
            };
            (key, Some(value))
        };

        self.cursor = entry_end;
        Some(Ok(item))
    }
}

// Field-level helpers let methods hold a skip-list search result while
// writing to disjoint mmap/file/stats fields.

fn grow_into(mmap: &mut MmapMut, file: &mut File, desired: u64) -> io::Result<()> {
    let new_capacity = ((mmap.len() as u64) * 2).max(desired);
    file.set_len(new_capacity)?;
    *mmap = unsafe { MmapMut::map_mut(&*file)? };
    Ok(())
}

/// Append `[vlen u32][V][klen u32][K]` at the current write tail
/// (`stats.data_size`). Writes the trailing `SENTINEL` so a subsequent
/// `replay_wal` terminates correctly. Returns the new record's
/// `EntryMeta`. Mirrors KeyDir's `write_entry_into`.
fn write_entry_into<K, V>(
    mmap: &mut MmapMut,
    file: &mut File,
    stats: &mut OrderLogStats,
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
/// the already-encoded key bytes that still live in the doomed record's
/// slot — no bincode encode, no scratch `Vec<u8>`. A single
/// `copy_within` moves the key into the tombstone payload. Charges the
/// tombstone's own bytes to `dead_bytes` internally; the caller charges
/// the displaced live record. Mirrors KeyDir's `write_tombstone_into`.
fn write_tombstone_into(
    mmap: &mut MmapMut,
    file: &mut File,
    stats: &mut OrderLogStats,
    old: &EntryMeta,
) -> io::Result<()> {
    let value_size = read_u32_le(mmap, old.offset as usize)
        .expect("live record header within mmap bounds") as usize;
    let key_size = old.record_size as usize - 8 - value_size;
    let total = 8 + key_size;

    let new_offset = stats.data_size as usize;
    let end = new_offset + total;
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

#[derive(Debug, Clone, Encode, Decode)]
pub struct OrderLogConfig {
    pub initial_capacity: u64,
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

#[derive(Debug, Clone, Default, PartialEq, Eq, Encode, Decode)]
pub struct OrderLogStats {
    pub data_size: u64,
    pub dead_bytes: u64,
}

pub struct OrderLog<K: Ord, V> {
    index: OrderIndex<K, V>,
    mmap: MmapMut,
    file: File,
    config: OrderLogConfig,
    stats: OrderLogStats,
}

impl<K, V> OrderLog<K, V>
where
    K: Encode + Decode<()> + Hash + Eq + Clone + Ord,
    V: Encode + Decode<()> + Clone,
{
    fn replay_wal(&mut self) -> io::Result<()> {
        let mut cursor = HEADER_SIZE;
        self.stats.dead_bytes = 0;

        while let Some(value_size) = read_u32_le(&self.mmap, cursor) {
            // SENTINEL marks the live tail. value_size == 0 is a
            // legitimate empty-value encoding (e.g. V = ()).
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
                if let Some(old) = self.index.remove_drop(&key) {
                    self.stats.dead_bytes += old.record_size as u64;
                }
                self.stats.dead_bytes += 8 + key_size as u64;
            } else {
                let value_start = cursor + 4;
                let value_end = value_start + value_size as usize;
                let value: V = deserialize_from(&self.mmap[value_start..value_end])?;
                let meta = EntryMeta {
                    offset: cursor as u64,
                    record_size: 8 + value_size + key_size,
                };
                if let Some(old) = self.index.insert(key, value, meta) {
                    self.stats.dead_bytes += old.record_size as u64;
                }
            }
            cursor = entry_end;
        }

        self.stats.data_size = cursor as u64;
        Ok(())
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

    pub fn consume_val(&self) -> ConsumeValIter<'_, K, V> {
        ConsumeValIter {
            mmap: &self.mmap[..],
            cursor: HEADER_SIZE,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<K, V> Backend<K, V> for OrderLog<K, V>
where
    K: Encode + Decode<()> + Hash + Eq + Clone + Ord,
    V: Encode + Decode<()> + Clone,
{
    type Stats<'a>
        = &'a OrderLogStats
    where
        Self: 'a;
    type Config = OrderLogConfig;

    fn create(path: &Path, config: Self::Config) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(path)?;

        file.set_len(config.initial_capacity)?;
        let mut mmap = unsafe { MmapMut::map_mut(&file)? };
        mmap[0..4].copy_from_slice(&MAGIC.to_le_bytes());
        mmap[HEADER_SIZE..HEADER_SIZE + 4].copy_from_slice(&SENTINEL.to_le_bytes());

        Ok(OrderLog {
            index: OrderIndex::new(),
            mmap,
            file,
            config,
            stats: OrderLogStats {
                data_size: HEADER_SIZE as u64,
                dead_bytes: 0,
            },
        })
    }

    fn open(path: &Path, config: Self::Config) -> io::Result<Self> {
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
            config,
            stats: OrderLogStats::default(),
        };
        this.replay_wal()?;
        Ok(this)
    }

    fn get(&self, key: &K) -> Option<Cow<'_, V>> {
        self.index.get(key).map(Cow::Borrowed)
    }

    fn contains(&self, key: &K) -> bool {
        self.index.contains(key)
    }

    fn put(&mut self, key: K, value: V) -> io::Result<()> {
        let meta = write_entry_into(
            &mut self.mmap,
            &mut self.file,
            &mut self.stats,
            &key,
            &value,
        )?;
        if let Some(old) = self.index.insert(key, value, meta) {
            self.stats.dead_bytes += old.record_size as u64;
        }
        self.maybe_compact()
    }

    fn put_if_absent(&mut self, key: K, value: V) -> io::Result<bool> {
        let (update, found) = self.index.search(&key);
        if found.is_some() {
            return Ok(false);
        }
        let meta = write_entry_into(
            &mut self.mmap,
            &mut self.file,
            &mut self.stats,
            &key,
            &value,
        )?;
        self.index.insert_with_update(key, value, meta, update);
        self.maybe_compact()?;
        Ok(true)
    }

    /// The returned `Cow` is always `Owned`: the old value is replaced
    /// in the skip list, so there's nothing for the caller to borrow.
    fn replace(&mut self, key: K, value: V) -> io::Result<Option<Cow<'_, V>>> {
        let (update, found) = self.index.search(&key);
        let prev = found.map(|idx| Cow::Owned(self.index.arena[idx].value.clone()));
        let meta = write_entry_into(
            &mut self.mmap,
            &mut self.file,
            &mut self.stats,
            &key,
            &value,
        )?;
        match found {
            Some(idx) => {
                let old = std::mem::replace(&mut self.index.arena[idx].meta, meta);
                self.index.arena[idx].value = value;
                self.stats.dead_bytes += old.record_size as u64;
            }
            None => self.index.insert_with_update(key, value, meta, update),
        }
        self.maybe_compact()?;
        Ok(prev)
    }

    fn bulk_put_sorted<I>(&mut self, sorted: I) -> io::Result<()>
    where
        I: IntoIterator<Item = (K, V)>,
    {
        let mut first_err = None;
        let mut finger = self.index.finger_at_start();
        for (k, v) in sorted {
            let meta =
                match write_entry_into(&mut self.mmap, &mut self.file, &mut self.stats, &k, &v) {
                    Ok(meta) => meta,
                    Err(e) => {
                        first_err = Some(e);
                        break;
                    }
                };
            self.index.advance_finger_to(&mut finger, &k);
            if self.index.finger_matches(&finger, &k) {
                let old = self.index.overwrite_at_finger(&finger, v, meta);
                self.stats.dead_bytes += old.record_size as u64;
            } else {
                self.index.insert_at_finger(&mut finger, k, v, meta);
            }
        }

        if let Err(e) = self.maybe_compact() {
            if first_err.is_none() {
                first_err = Some(e);
            }
        }
        match first_err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    fn delete(&mut self, key: &K) -> io::Result<bool> {
        let Some(old) = self.index.remove_drop(key) else {
            return Ok(false);
        };
        // The removed node's record bytes are still intact in the mmap
        // (`remove_drop` only touches in-memory state). Reuse the
        // already-encoded key bytes at `old.offset` for the tombstone.
        self.stats.dead_bytes += old.record_size as u64;
        write_tombstone_into(&mut self.mmap, &mut self.file, &mut self.stats, &old)?;
        self.maybe_compact()?;
        Ok(true)
    }

    fn bulk_delete_sorted<'a, I>(&mut self, sorted: I) -> io::Result<usize>
    where
        I: IntoIterator<Item = &'a K>,
        K: 'a,
    {
        let mut removed = 0;
        let mut first_err = None;
        let mut finger = self.index.finger_at_start();
        for k in sorted {
            self.index.advance_finger_to(&mut finger, k);
            if !self.index.finger_matches(&finger, k) {
                continue;
            }
            // Snapshot the doomed record's metadata before
            // `remove_at_finger` frees the arena slot — the mmap bytes
            // stay live and the tombstone writer copies the encoded key
            // from there.
            let cur_idx = finger.cursor.expect("finger matches a node");
            let old = self.index.arena[cur_idx].meta.clone();
            self.stats.dead_bytes += old.record_size as u64;
            if let Err(e) =
                write_tombstone_into(&mut self.mmap, &mut self.file, &mut self.stats, &old)
            {
                first_err = Some(e);
                break;
            }
            let _ = self.index.remove_at_finger(&mut finger);
            removed += 1;
        }

        if let Err(e) = self.maybe_compact() {
            if first_err.is_none() {
                first_err = Some(e);
            }
        }
        match first_err {
            Some(e) => Err(e),
            None => Ok(removed),
        }
    }

    fn update<F>(&mut self, key: &K, f: F) -> io::Result<()>
    where
        F: FnOnce(Option<V>) -> Option<V>,
    {
        let (update, found) = self.index.search(key);
        let current = found.map(|idx| self.index.arena[idx].value.clone());
        let Some(new_state) = f(current) else {
            if let Some(idx) = found {
                // Snapshot the existing record's metadata before the
                // removal frees the arena slot — the tombstone writer
                // copies the encoded key out of the mmap at this offset.
                let old = self.index.arena[idx].meta.clone();
                self.stats.dead_bytes += old.record_size as u64;
                write_tombstone_into(&mut self.mmap, &mut self.file, &mut self.stats, &old)?;
                let _ = self.index.remove_found(update, idx);
                self.maybe_compact()?;
            }
            return Ok(());
        };

        let meta = write_entry_into(
            &mut self.mmap,
            &mut self.file,
            &mut self.stats,
            key,
            &new_state,
        )?;
        match found {
            Some(idx) => {
                let old = std::mem::replace(&mut self.index.arena[idx].meta, meta);
                self.index.arena[idx].value = new_state;
                self.stats.dead_bytes += old.record_size as u64;
            }
            None => self
                .index
                .insert_with_update(key.clone(), new_state, meta, update),
        }
        self.maybe_compact()?;
        Ok(())
    }

    fn clear(&mut self) -> io::Result<()> {
        self.index.clear();
        self.stats = OrderLogStats {
            data_size: HEADER_SIZE as u64,
            dead_bytes: 0,
        };
        self.mmap[0..4].copy_from_slice(&MAGIC.to_le_bytes());
        // SENTINEL at the live tail so a subsequent reopen replays as
        // empty. initial_capacity is always >> HEADER_SIZE + 4 so no
        // length check is needed.
        self.mmap[HEADER_SIZE..HEADER_SIZE + 4].copy_from_slice(&SENTINEL.to_le_bytes());
        Ok(())
    }

    fn compact(&mut self) -> io::Result<()> {
        // Walk the level-0 list (key order) then sort by record offset
        // so the `copy_within` sweep always shifts records forward into
        // the freed gap — `dst` is always ≤ `src`, so the memmove never
        // destructively overlaps an unmoved record.
        let mut nodes: Vec<usize> = Vec::with_capacity(self.index.len());
        let mut current = self.index.heads[0];
        while let Some(idx) = current {
            nodes.push(idx);
            current = self.index.next_at(idx, 0);
        }
        nodes.sort_unstable_by_key(|idx| self.index.arena[*idx].meta.offset);

        let mut cursor: usize = HEADER_SIZE;
        for idx in nodes {
            let node = &mut self.index.arena[idx];
            let src = node.meta.offset as usize;
            let len = node.meta.record_size as usize;
            if src != cursor {
                self.mmap.copy_within(src..src + len, cursor);
            }
            node.meta.offset = cursor as u64;
            cursor += len;
        }

        // SENTINEL at the new tail tells `replay_wal` to stop here on
        // next open, even if we don't shrink the file. No length check
        // is needed: every writer maintains `mmap.len() >= data_size + 4`
        // and `cursor <= data_size` after the pack loop, so
        // `cursor + 4 <= mmap.len()` always holds.
        self.mmap[cursor..cursor + 4].copy_from_slice(&SENTINEL.to_le_bytes());

        self.stats.data_size = cursor as u64;
        self.stats.dead_bytes = 0;

        // Release the physical slack past the new tail. Keep at least
        // `initial_capacity` so the next writes don't immediately re-grow
        // the file. Windows refuses to shrink a file while a mapped
        // section is open, so swap in a small anonymous mmap as a
        // placeholder before the truncation, then remap onto the
        // resized file.
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
        self.index.iter().map(|(k, _)| Cow::Borrowed(k))
    }

    fn values<'a>(&'a self) -> impl Iterator<Item = Cow<'a, V>> + 'a
    where
        V: 'a,
    {
        self.index.iter().map(|(_, v)| Cow::Borrowed(v))
    }

    fn entries<'a>(&'a self) -> impl Iterator<Item = (Cow<'a, K>, Cow<'a, V>)> + 'a
    where
        K: 'a,
        V: 'a,
    {
        self.index
            .iter()
            .map(|(k, v)| (Cow::Borrowed(k), Cow::Borrowed(v)))
    }

    fn size(&self) -> usize {
        self.index.len()
    }

    fn is_empty(&self) -> bool {
        self.index.is_empty()
    }

    fn stats(&self) -> Self::Stats<'_> {
        &self.stats
    }

    fn config(&self) -> &Self::Config {
        &self.config
    }

    fn flush(&self) -> io::Result<()> {
        self.mmap.flush_async()
    }

    fn sync(&self) -> io::Result<()> {
        self.mmap.flush()
    }
}

impl<K, V> OrderedBackend<K, V> for OrderLog<K, V>
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
        self.index
            .range_owned_end(start, end.clone())
            .map(|(k, v)| (Cow::Borrowed(k), Cow::Borrowed(v)))
    }

    fn first<'a>(&'a self) -> Option<(Cow<'a, K>, Cow<'a, V>)>
    where
        K: 'a,
        V: 'a,
    {
        let idx = self.index.first_idx()?;
        let node = &self.index.arena[idx];
        Some((Cow::Borrowed(&node.key), Cow::Borrowed(&node.value)))
    }

    fn last<'a>(&'a self) -> Option<(Cow<'a, K>, Cow<'a, V>)>
    where
        K: 'a,
        V: 'a,
    {
        let idx = self.index.last_idx()?;
        let node = &self.index.arena[idx];
        Some((Cow::Borrowed(&node.key), Cow::Borrowed(&node.value)))
    }

    fn entries_rev<'a>(&'a self) -> impl Iterator<Item = (Cow<'a, K>, Cow<'a, V>)> + 'a
    where
        K: 'a,
        V: 'a,
    {
        self.index
            .iter_rev()
            .map(|(k, v)| (Cow::Borrowed(k), Cow::Borrowed(v)))
    }

    fn range_rev<'a>(
        &'a self,
        start: &K,
        end: &K,
    ) -> impl Iterator<Item = (Cow<'a, K>, Cow<'a, V>)> + 'a
    where
        K: 'a,
        V: 'a,
    {
        self.index
            .range_rev(start.clone(), end)
            .map(|(k, v)| (Cow::Borrowed(k), Cow::Borrowed(v)))
    }
}

impl<K: Ord, V> Drop for OrderLog<K, V> {
    fn drop(&mut self) {
        let _ = self.mmap.flush_async();
    }
}

impl<K: Ord, V> fmt::Debug for OrderLog<K, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.stats, f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::backend::{Backend, OrderedBackend};
    use crate::utils::serdes::serialized_size;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    /// Test helper — read through the Cow-returning Backend::get API
    /// and immediately materialize to an owned value.
    fn get<K, V>(ol: &OrderLog<K, V>, key: &K) -> Option<V>
    where
        K: Encode + Decode<()> + Hash + Eq + Clone + Ord,
        V: Encode + Decode<()> + Clone,
    {
        Backend::get(ol, key).map(Cow::into_owned)
    }

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

    fn create_with(path: &Path, config: OrderLogConfig) -> OrderLog<TestKey, TestVal> {
        OrderLog::create(path, config).unwrap()
    }

    fn open(path: &Path) -> OrderLog<TestKey, TestVal> {
        OrderLog::open(path, OrderLogConfig::default()).unwrap()
    }

    #[test]
    fn put_then_get() {
        let p = tmp_path("put_get");
        let mut ol = create(&p);
        ol.put(k("alice"), v("alice", 1)).unwrap();
        assert_eq!(get(&ol, &k("alice")), Some(v("alice", 1)));
    }

    #[test]
    fn missing_returns_none() {
        let p = tmp_path("missing");
        let ol = create(&p);
        assert!(get(&ol, &k("ghost")).is_none());
    }

    #[test]
    fn entries_are_ordered() {
        let p = tmp_path("ordered");
        let mut ol = create(&p);
        for s in ["delta", "alpha", "echo", "bravo", "charlie"] {
            ol.put(k(s), v(s, 0)).unwrap();
        }
        assert_eq!(
            ol.keys().map(Cow::into_owned).collect::<Vec<_>>(),
            vec![k("alpha"), k("bravo"), k("charlie"), k("delta"), k("echo")]
        );
    }

    #[test]
    fn range_returns_subrange() {
        let p = tmp_path("range");
        let mut ol = create(&p);
        for s in ["a", "b", "c", "d", "e", "f"] {
            ol.put(k(s), v(s, 0)).unwrap();
        }
        let got: Vec<_> = ol
            .range(&k("b"), &k("e"))
            .map(|(k, _)| k.into_owned())
            .collect();
        assert_eq!(got, vec![k("b"), k("c"), k("d")]);
    }

    #[test]
    fn first_last_endpoints() {
        let p = tmp_path("endpoints");
        let mut ol = create(&p);
        for s in ["m", "a", "z", "f"] {
            ol.put(k(s), v(s, 0)).unwrap();
        }
        assert_eq!(ol.first().map(|(k, _)| k.into_owned()), Some(k("a")));
        assert_eq!(ol.last().map(|(k, _)| k.into_owned()), Some(k("z")));
    }

    #[test]
    fn delete_removes_and_persists() {
        let p = tmp_path("delete");
        {
            let mut ol = create(&p);
            ol.put(k("a"), v("a", 1)).unwrap();
            ol.put(k("b"), v("b", 2)).unwrap();
            assert!(ol.delete(&k("a")).unwrap());
            assert!(!ol.delete(&k("a")).unwrap());
            ol.sync().unwrap();
        }
        let ol = open(&p);
        assert!(get(&ol, &k("a")).is_none());
        assert_eq!(get(&ol, &k("b")), Some(v("b", 2)));
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
        assert_eq!(get(&open(&p), &k("x")), Some(v("v2", 2)));
    }

    #[test]
    fn update_paths_work() {
        let p = tmp_path("update");
        let mut ol: OrderLog<TestKey, u32> =
            OrderLog::create(&p, OrderLogConfig::default()).unwrap();
        ol.update(&k("counter"), |cur| Some(cur.unwrap_or(0) + 1))
            .unwrap();
        ol.update(&k("counter"), |cur| Some(cur.unwrap_or(0) + 5))
            .unwrap();
        assert_eq!(get(&ol, &k("counter")), Some(6));
        ol.update(&k("counter"), |_| None).unwrap();
        assert!(get(&ol, &k("counter")).is_none());
        ol.update(&k("ghost"), |_| None).unwrap();
    }

    #[test]
    fn update_combines_existing() {
        let p = tmp_path("update_combine");
        let mut ol: OrderLog<TestKey, u32> =
            OrderLog::create(&p, OrderLogConfig::default()).unwrap();
        ol.put(k("counter"), 1).unwrap();
        let inc = 5;
        ol.update(&k("counter"), move |cur| Some(cur.unwrap_or(0) + inc))
            .unwrap();
        let inc2 = 4;
        ol.update(&k("counter"), move |cur| Some(cur.unwrap_or(0) + inc2))
            .unwrap();
        assert_eq!(get(&ol, &k("counter")), Some(10));
    }

    #[test]
    fn update_inserts_when_absent() {
        let p = tmp_path("update_absent_insert");
        let mut ol: OrderLog<TestKey, u32> =
            OrderLog::create(&p, OrderLogConfig::default()).unwrap();
        ol.update(&k("fresh"), |cur| Some(cur.unwrap_or(0) + 7))
            .unwrap();
        assert_eq!(get(&ol, &k("fresh")), Some(7));
    }

    #[test]
    fn update_returning_none_deletes() {
        let p = tmp_path("update_delete");
        let mut ol: OrderLog<TestKey, u32> =
            OrderLog::create(&p, OrderLogConfig::default()).unwrap();
        ol.put(k("x"), 42).unwrap();
        ol.update(&k("x"), |_| None).unwrap();
        assert!(get(&ol, &k("x")).is_none());
        assert_eq!(ol.size(), 0);
    }

    #[test]
    fn update_absent_returning_none_is_noop() {
        let p = tmp_path("update_noop");
        let mut ol: OrderLog<TestKey, u32> =
            OrderLog::create(&p, OrderLogConfig::default()).unwrap();
        ol.update(&k("ghost"), |_| None).unwrap();
        assert!(get(&ol, &k("ghost")).is_none());
        assert_eq!(ol.size(), 0);
    }

    #[test]
    fn update_can_move_owned_captures() {
        let p = tmp_path("update_move");
        let mut ol: OrderLog<TestKey, Vec<u32>> =
            OrderLog::create(&p, OrderLogConfig::default()).unwrap();
        ol.put(k("acc"), vec![1, 2, 3]).unwrap();
        let extras = vec![10, 20, 30];
        ol.update(&k("acc"), move |cur| {
            let mut v = cur.unwrap();
            v.extend(extras);
            Some(v)
        })
        .unwrap();
        assert_eq!(get(&ol, &k("acc")), Some(vec![1, 2, 3, 10, 20, 30]));
    }

    #[test]
    fn stats_track_data_size_and_dead_bytes() {
        let p = tmp_path("stats");
        let mut ol = create_with(
            &p,
            OrderLogConfig {
                initial_capacity: 8 * 1024,
                compaction_ratio: 1.0,
            },
        );
        assert_eq!(
            *ol.stats(),
            OrderLogStats {
                data_size: HEADER_SIZE as u64,
                dead_bytes: 0,
            }
        );
        assert_eq!(ol.size(), 0);
        ol.put(k("a"), v("first", 1)).unwrap();
        assert_eq!(ol.size(), 1);
        assert_eq!(ol.stats().dead_bytes, 0);
        ol.put(k("a"), v("second", 2)).unwrap();
        assert_eq!(ol.size(), 1);
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
            let mut ol = create_with(
                &p,
                OrderLogConfig {
                    initial_capacity: 8 * 1024,
                    compaction_ratio: 1.0,
                },
            );
            ol.put(key.clone(), v1).unwrap();
            ol.put(key.clone(), v2).unwrap();
            assert_eq!(ol.stats().dead_bytes, rec1);
            ol.delete(&key).unwrap();
            assert_eq!(ol.stats().dead_bytes, rec1 + rec2 + tombstone);
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
        assert_eq!(ol.stats().dead_bytes, rec1 + rec2 + tombstone);
    }

    #[test]
    fn compact_keeps_live_drops_dead_and_reopens() {
        let p = tmp_path("compact");
        {
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
            let pre_size = fs::metadata(&p).unwrap().len();
            ol.compact().unwrap();
            assert!(!p.with_extension("compact").exists());
            assert!(fs::metadata(&p).unwrap().len() <= pre_size);
            assert_eq!(ol.stats().dead_bytes, 0);
            ol.sync().unwrap();
        }
        let ol = open(&p);
        assert_eq!(ol.size(), 25);
        for i in 25..50 {
            assert_eq!(get(&ol, &k(&format!("k{:02}", i))), Some(v("update", i)));
        }
    }

    #[test]
    fn clear_resets_and_survives_reopen() {
        let p = tmp_path("clear");
        {
            let mut ol = create(&p);
            for s in ["a", "b", "c"] {
                ol.put(k(s), v(s, 0)).unwrap();
            }
            ol.clear().unwrap();
            ol.sync().unwrap();
        }
        let ol = open(&p);
        assert_eq!(ol.size(), 0);
        assert!(ol.is_empty());
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
        assert_eq!(
            ol.keys().map(Cow::into_owned).collect::<Vec<_>>(),
            vec![k("a"), k("c"), k("d")]
        );
    }

    #[test]
    fn reads_dont_touch_disk_after_open() {
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
        assert_eq!(ol.first().unwrap().0.into_owned(), k("k00"));
        assert_eq!(ol.last().unwrap().0.into_owned(), k("k19"));
        let middle: Vec<_> = ol
            .range(&k("k05"), &k("k08"))
            .map(|(k, _)| k.into_owned())
            .collect();
        assert_eq!(middle, vec![k("k05"), k("k06"), k("k07")]);
    }

    #[test]
    fn put_if_absent_and_replace_paths() {
        let p = tmp_path("pia_replace");
        let mut ol = create(&p);
        assert!(ol.put_if_absent(k("a"), v("first", 1)).unwrap());
        assert!(!ol.put_if_absent(k("a"), v("second", 2)).unwrap());
        assert_eq!(get(&ol, &k("a")), Some(v("first", 1)));
        assert_eq!(
            ol.replace(k("a"), v("third", 3))
                .unwrap()
                .map(Cow::into_owned),
            Some(v("first", 1))
        );
        assert_eq!(ol.replace(k("b"), v("new", 4)).unwrap(), None);
        assert_eq!(get(&ol, &k("a")), Some(v("third", 3)));
        assert_eq!(get(&ol, &k("b")), Some(v("new", 4)));
    }

    #[test]
    fn bulk_put_and_delete() {
        let p = tmp_path("bulk_auto");
        let mut ol = create(&p);
        let items: Vec<_> = (0..100u32)
            .map(|i| (format!("k{:03}", i).into_bytes(), v("p", i)))
            .collect();
        ol.bulk_put(items).unwrap();
        assert_eq!(ol.size(), 100);
        let keys: Vec<_> = (0..50u32)
            .map(|i| format!("k{:03}", i).into_bytes())
            .collect();
        let removed = ol.bulk_delete(keys.iter()).unwrap();
        assert_eq!(removed, 50);
        assert_eq!(ol.size(), 50);
    }

    #[test]
    fn bulk_unsorted_keeps_ordered_iteration() {
        let p = tmp_path("bulk_unsorted_sort");
        let mut ol = create(&p);
        let items = vec![
            (k("d"), v("d", 4)),
            (k("a"), v("a", 1)),
            (k("c"), v("c", 3)),
            (k("b"), v("b", 2)),
        ];
        ol.bulk_put(items).unwrap();
        assert_eq!(
            ol.keys().map(Cow::into_owned).collect::<Vec<_>>(),
            vec![k("a"), k("b"), k("c"), k("d")]
        );

        let keys = vec![k("c"), k("missing"), k("a")];
        assert_eq!(ol.bulk_delete(keys.iter()).unwrap(), 2);
        assert_eq!(
            ol.keys().map(Cow::into_owned).collect::<Vec<_>>(),
            vec![k("b"), k("d")]
        );
    }

    #[test]
    fn bulk_put_sorted_cases() {
        let p = tmp_path("bulk_sorted");
        let mut ol = create(&p);
        let first: Vec<_> = (0..10u32)
            .map(|i| (format!("k{:02}", i * 2).into_bytes(), v("even", i)))
            .collect();
        ol.bulk_put_sorted(first).unwrap();
        let interleaved: Vec<_> = (0..10u32)
            .map(|i| (format!("k{:02}", i * 2 + 1).into_bytes(), v("odd", i)))
            .collect();
        ol.bulk_put_sorted(interleaved).unwrap();
        let overwrite = vec![(k("k04"), v("new", 44)), (k("k25"), v("tail", 25))];
        ol.bulk_put_sorted(overwrite).unwrap();
        assert_eq!(ol.size(), 21);
        assert_eq!(get(&ol, &k("k04")), Some(v("new", 44)));
        assert_eq!(ol.last().map(|(k, _)| k.into_owned()), Some(k("k25")));
        let keys: Vec<_> = ol.keys().map(Cow::into_owned).collect();
        assert!(keys.windows(2).all(|w| w[0] < w[1]));
    }

    /// Regression: duplicate keys inside a single `bulk_put_sorted`
    /// batch used to produce orphaned arena slots because the finger
    /// cursor didn't move onto the freshly-inserted node, so the
    /// duplicate would re-enter `insert_at_finger` instead of going
    /// through the overwrite path. Last-write-wins is the expected
    /// behavior (matches looping `put`).
    #[test]
    fn bulk_put_sorted_dedupes_within_batch() {
        let p = tmp_path("bulk_sorted_dupes");
        let mut ol = create(&p);
        ol.bulk_put_sorted([
            (k("a"), v("first", 1)),
            (k("a"), v("second", 2)),
            (k("a"), v("third", 3)),
            (k("b"), v("b", 4)),
            (k("b"), v("b_again", 5)),
        ])
        .unwrap();
        assert_eq!(ol.size(), 2);
        assert_eq!(get(&ol, &k("a")), Some(v("third", 3)));
        assert_eq!(get(&ol, &k("b")), Some(v("b_again", 5)));
        // Iteration order must still be by key, with one entry per key.
        let keys: Vec<TestKey> = ol.keys().map(Cow::into_owned).collect();
        assert_eq!(keys, vec![k("a"), k("b")]);
    }

    /// Tombstones written via `write_tombstone_into` must survive a
    /// reopen — the WAL bytes carry the encoded key over the
    /// `copy_within` boundary the same way a serialized tombstone
    /// would, and replay must mark the key as deleted.
    #[test]
    fn tombstone_copy_within_round_trips_through_reopen() {
        let p = tmp_path("tombstone_copy_within");
        {
            let mut ol = create(&p);
            ol.put(k("alpha"), v("a", 1)).unwrap();
            ol.put(k("beta"), v("b", 2)).unwrap();
            ol.put(k("gamma"), v("c", 3)).unwrap();
            assert!(ol.delete(&k("beta")).unwrap());
            ol.sync().unwrap();
        }
        let ol = open(&p);
        assert_eq!(ol.size(), 2);
        assert_eq!(get(&ol, &k("alpha")), Some(v("a", 1)));
        assert!(get(&ol, &k("beta")).is_none());
        assert_eq!(get(&ol, &k("gamma")), Some(v("c", 3)));
    }

    #[test]
    fn bulk_delete_sorted_removes_hits_skips_misses() {
        let p = tmp_path("bulk_delete_sorted");
        let mut ol = create(&p);
        for i in 0..10u32 {
            ol.put(format!("k{:02}", i).into_bytes(), v("p", i))
                .unwrap();
        }
        let keys = vec![k("ghost"), k("k00"), k("k03"), k("k09"), k("zzz")];
        let removed = ol.bulk_delete_sorted(keys.iter()).unwrap();
        assert_eq!(removed, 3);
        assert_eq!(ol.size(), 7);
        assert!(!ol.contains(&k("k00")));
        assert!(!ol.contains(&k("k03")));
        assert!(!ol.contains(&k("k09")));
    }

    #[test]
    fn reverse_iteration_and_ranges() {
        let p = tmp_path("reverse");
        let mut ol = create(&p);
        for s in ["a", "b", "c", "d", "e"] {
            ol.put(k(s), v(s, 0)).unwrap();
        }
        let rev: Vec<_> = ol.entries_rev().map(|(k, _)| k.into_owned()).collect();
        assert_eq!(rev, vec![k("e"), k("d"), k("c"), k("b"), k("a")]);
        let forward: Vec<(TestKey, TestVal)> = ol
            .range(&k("b"), &k("e"))
            .map(|(k, v)| (k.into_owned(), v.into_owned()))
            .collect();
        let rev_range: Vec<(TestKey, TestVal)> = ol
            .range_rev(&k("b"), &k("e"))
            .map(|(k, v)| (k.into_owned(), v.into_owned()))
            .collect();
        assert_eq!(rev_range, forward.into_iter().rev().collect::<Vec<_>>());
        assert!(ol.range_rev(&k("c"), &k("c")).next().is_none());
        ol.delete(&k("c")).unwrap();
        let rev_after: Vec<_> = ol.entries_rev().map(|(k, _)| k.into_owned()).collect();
        assert_eq!(rev_after, vec![k("e"), k("d"), k("b"), k("a")]);
    }

    #[test]
    fn prev_link_correct_after_bulk_put_sorted_overwrite() {
        let p = tmp_path("prev_overwrite");
        let mut ol = create(&p);
        ol.bulk_put_sorted([
            (k("a"), v("a", 1)),
            (k("b"), v("b", 1)),
            (k("c"), v("c", 1)),
        ])
        .unwrap();
        ol.bulk_put_sorted([(k("b"), v("b2", 2))]).unwrap();
        assert_eq!(
            ol.entries_rev()
                .map(|(k, _)| k.into_owned())
                .collect::<Vec<_>>(),
            vec![k("c"), k("b"), k("a")]
        );
    }

    #[test]
    fn range_streams_for_large_skip() {
        let p = tmp_path("range_stream");
        let mut ol = create(&p);
        for i in 0..5000u32 {
            ol.put(format!("k{:05}", i).into_bytes(), v("p", i))
                .unwrap();
        }
        let got: Vec<_> = ol
            .range(&k("k01000"), &k("k05000"))
            .take(3)
            .map(|(k, _)| k.into_owned())
            .collect();
        assert_eq!(got, vec![k("k01000"), k("k01001"), k("k01002")]);
    }

    /// OrderLog holds keys/values in its in-memory skip list, so every
    /// retrieval method should return `Cow::Borrowed`. Verifies that
    /// no read path materializes through `Cow::Owned`.
    #[test]
    fn retrieval_methods_return_borrowed() {
        let p = tmp_path("cow_borrowed");
        let mut ol = create(&p);
        for s in ["a", "b", "c"] {
            ol.put(k(s), v(s, 0)).unwrap();
        }

        assert!(matches!(Backend::get(&ol, &k("a")), Some(Cow::Borrowed(_))));
        assert!(Backend::keys(&ol).all(|c| matches!(c, Cow::Borrowed(_))));
        assert!(Backend::values(&ol).all(|c| matches!(c, Cow::Borrowed(_))));
        assert!(Backend::entries(&ol)
            .all(|(k, v)| { matches!(k, Cow::Borrowed(_)) && matches!(v, Cow::Borrowed(_)) }));

        assert!(OrderedBackend::range(&ol, &k("a"), &k("c"))
            .all(|(k, v)| { matches!(k, Cow::Borrowed(_)) && matches!(v, Cow::Borrowed(_)) }));
        let (fk, fv) = OrderedBackend::first(&ol).unwrap();
        assert!(matches!(fk, Cow::Borrowed(_)));
        assert!(matches!(fv, Cow::Borrowed(_)));
        let (lk, lv) = OrderedBackend::last(&ol).unwrap();
        assert!(matches!(lk, Cow::Borrowed(_)));
        assert!(matches!(lv, Cow::Borrowed(_)));
    }

    /// Callers that need owned values must be able to materialize them
    /// via `Cow::into_owned()`. Exercises both `get` and `entries`
    /// alongside the ordered surface.
    #[test]
    fn into_owned_yields_correct_values() {
        let p = tmp_path("cow_into_owned");
        let mut ol = create(&p);
        ol.put(k("a"), v("alpha", 1)).unwrap();
        ol.put(k("b"), v("beta", 2)).unwrap();

        let owned: TestVal = Backend::get(&ol, &k("a")).unwrap().into_owned();
        assert_eq!(owned, v("alpha", 1));

        let owned_entries: Vec<(TestKey, TestVal)> = Backend::entries(&ol)
            .map(|(k, v)| (k.into_owned(), v.into_owned()))
            .collect();
        assert_eq!(
            owned_entries,
            vec![(k("a"), v("alpha", 1)), (k("b"), v("beta", 2))]
        );
    }

    /// A `V` whose bincode encoding is 0 bytes (the unit type here)
    /// used to be silently truncated by the old `value_size == 0`
    /// replay terminator. `SENTINEL = u32::MAX - 1` separates the
    /// empty-value case from the live-tail case.
    #[test]
    fn zero_byte_value_round_trips() {
        let p = tmp_path("zero_byte_value");
        {
            let mut ol: OrderLog<TestKey, ()> =
                OrderLog::create(&p, OrderLogConfig::default()).unwrap();
            ol.put(k("alpha"), ()).unwrap();
            ol.put(k("beta"), ()).unwrap();
            ol.put(k("gamma"), ()).unwrap();
            ol.delete(&k("beta")).unwrap();
            ol.sync().unwrap();
        }
        let ol: OrderLog<TestKey, ()> = OrderLog::open(&p, OrderLogConfig::default()).unwrap();
        assert_eq!(ol.size(), 2);
        assert_eq!(
            Backend::get(&ol, &k("alpha")).map(Cow::into_owned),
            Some(())
        );
        assert!(Backend::get(&ol, &k("beta")).is_none());
        assert_eq!(
            Backend::get(&ol, &k("gamma")).map(Cow::into_owned),
            Some(())
        );
    }
}
