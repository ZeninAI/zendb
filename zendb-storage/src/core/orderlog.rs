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
//! the in-memory skip list. During `open_tx` / `close_tx`, writes are encoded
//! into a pooled staging buffer and copied into the mmap once on close. This
//! is batching, not atomicity: no rollback, no isolation, no crash recovery.
//!
//! # Compaction
//!
//! Dead bytes from overwrites and tombstones trigger in-place compaction when
//! the configured ratio is reached. Live skip-list entries are re-encoded in
//! ascending key order at the front of the existing file, followed by a zero
//! sentinel for replay.
//!
//! # File format
//!
//! Header: `[MAGIC: u32 LE = 0x474F4C4F ("OLOG")]`
//! Live: `[value_size: u32 LE][V bytes][key_size: u32 LE][K bytes]`
//! Tombstone: `[0xFFFF_FFFF: u32][key_size: u32 LE][K bytes]`

use std::{
    fmt,
    fs::{File, OpenOptions},
    hash::Hash,
    io::{self},
    path::{Path, PathBuf},
};

use bincode::{Decode, Encode};
use memmap2::MmapMut;

use crate::core::backend::{Backend, OrderedBackend};
use crate::utils::{
    fast_rand,
    reusables::PooledBuf,
    serdes::{deserialize_from, read_u32_le, with_scratch, with_two_scratches},
};

const DEFAULT_INITIAL_CAPACITY: u64 = 1024 * 1024;
const DEFAULT_COMPACTION_RATIO: f64 = 0.5;
const MAGIC: u32 = 0x474F4C4F;
const HEADER_SIZE: usize = 4;
const TOMBSTONE: u32 = u32::MAX;
const MAX_LEVEL: usize = 16;

type Heads = [Option<usize>; MAX_LEVEL];

struct SkipNode<K, V> {
    key: K,
    value: V,
    record_size: u64,
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

    fn get(&self, key: &K) -> Option<&V> {
        if self.is_empty() {
            return None;
        }
        self.search(key).1.map(|idx| &self.arena[idx].value)
    }

    fn contains(&self, key: &K) -> bool {
        self.get(key).is_some()
    }

    fn alloc_node(&mut self, key: K, value: V, record_size: u64, level: usize) -> usize {
        if let Some(idx) = self.free.pop() {
            self.arena[idx] = SkipNode {
                key,
                value,
                record_size,
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
                record_size,
                prev: None,
                level,
            });
            // Grow the flat next-array by one row. New entries default
            // to None so we don't need to touch them after extending.
            self.nexts.resize(self.nexts.len() + MAX_LEVEL, None);
            idx
        }
    }

    fn insert(&mut self, key: K, value: V, record_size: u64) -> Option<u64> {
        let (update, found) = self.search(&key);
        if let Some(idx) = found {
            self.arena[idx].value = value;
            return Some(std::mem::replace(
                &mut self.arena[idx].record_size,
                record_size,
            ));
        }
        self.insert_with_update(key, value, record_size, update);
        None
    }

    fn insert_with_update(&mut self, key: K, value: V, record_size: u64, update: Heads) {
        let level = self.random_level();
        if level > self.height {
            self.height = level;
        }

        let new_idx = self.alloc_node(key, value, record_size, level);
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

    fn remove_drop(&mut self, key: &K) -> Option<u64> {
        if self.is_empty() {
            return None;
        }
        let (update, found) = self.search(key);
        found.map(|idx| self.remove_found(update, idx))
    }

    fn remove_found(&mut self, update: Heads, idx: usize) -> u64 {
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
        let record_size = self.arena[idx].record_size;
        // Clear this node's forward-pointer row so a future alloc_node
        // recycling this slot starts from a clean state.
        let base = idx * MAX_LEVEL;
        for slot in &mut self.nexts[base..base + MAX_LEVEL] {
            *slot = None;
        }
        self.free.push(idx);
        self.len -= 1;
        record_size
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

    fn iter_with_nodes(&self) -> OrderIndexNodeIter<'_, K, V> {
        OrderIndexNodeIter {
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

    fn insert_at_finger(&mut self, finger: &mut Finger, key: K, value: V, record_size: u64) {
        let level = self.random_level();
        if level > self.height {
            self.height = level;
        }
        let new_idx = self.alloc_node(key, value, record_size, level);
        self.link_new_node(new_idx, &finger.update);
        for i in 0..level {
            finger.update[i] = Some(new_idx);
        }
        self.len += 1;
    }

    fn overwrite_at_finger(&mut self, finger: &Finger, value: V, record_size: u64) -> u64 {
        let idx = finger.cursor.expect("finger cursor must point at a node");
        let old = self.arena[idx].record_size;
        self.arena[idx].value = value;
        self.arena[idx].record_size = record_size;
        old
    }

    fn remove_at_finger(&mut self, finger: &mut Finger) -> u64 {
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
        let record_size = self.arena[idx].record_size;
        // Clear this node's forward-pointer row before recycling.
        let base = idx * MAX_LEVEL;
        for slot in &mut self.nexts[base..base + MAX_LEVEL] {
            *slot = None;
        }
        self.free.push(idx);
        self.len -= 1;
        record_size
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

struct OrderIndexNodeIter<'a, K, V> {
    arena: &'a [SkipNode<K, V>],
    nexts: &'a [Option<usize>],
    current: Option<usize>,
}

impl<'a, K, V> Iterator for OrderIndexNodeIter<'a, K, V> {
    type Item = (&'a K, &'a SkipNode<K, V>);

    fn next(&mut self) -> Option<Self::Item> {
        let idx = self.current?;
        let node = &self.arena[idx];
        self.current = self.nexts[idx * MAX_LEVEL];
        Some((&node.key, node))
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
        if value_size == 0 {
            return None;
        }

        let is_tombstone = value_size == TOMBSTONE;
        let key_size_off = if is_tombstone {
            self.cursor + 4
        } else {
            self.cursor + 4 + value_size as usize
        };
        let key_size = match read_u32_le(self.mmap, key_size_off) {
            Some(s) => s,
            None => {
                return Some(Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "truncated WAL entry",
                )));
            }
        };
        let key_start = key_size_off + 4;
        let entry_end = key_start + key_size as usize;
        if entry_end > self.mmap.len() {
            return Some(Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "truncated WAL entry",
            )));
        }

        let key: K = match deserialize_from(&self.mmap[key_start..entry_end]) {
            Ok(k) => k,
            Err(e) => return Some(Err(e)),
        };

        let item = if is_tombstone {
            (key, None)
        } else {
            let value_start = self.cursor + 4;
            let value_end = value_start + value_size as usize;
            if value_end > self.mmap.len() {
                return Some(Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "truncated WAL entry",
                )));
            }
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

struct TxState {
    staging: PooledBuf,
    tx_base: u64,
}

// Field-level helpers let methods hold a skip-list search result while
// writing to disjoint mmap/file/tx/stats fields.

fn grow_into(mmap: &mut MmapMut, file: &mut File, desired: u64) -> io::Result<()> {
    let new_capacity = ((mmap.len() as u64) * 2).max(desired);
    file.set_len(new_capacity)?;
    *mmap = unsafe { MmapMut::map_mut(&*file)? };
    Ok(())
}

fn append_entry_into<K, V>(
    mmap: &mut MmapMut,
    file: &mut File,
    tx: &mut Option<TxState>,
    stats: &mut OrderLogStats,
    key: &K,
    value: &V,
) -> io::Result<u64>
where
    K: Encode,
    V: Encode,
{
    // Encode key + value once; `with_two_scratches` returns the slice
    // lengths via `.len()` so we never re-run bincode just to count bytes.
    with_two_scratches(key, value, |kb, vb| {
        let total = 8usize
            .checked_add(vb.len())
            .and_then(|n| n.checked_add(kb.len()))
            .ok_or_else(|| io::Error::new(io::ErrorKind::OutOfMemory, "OrderLog offset overflow"))?;

        if let Some(tx_state) = tx.as_mut() {
            let local = tx_state.staging.len();
            tx_state.staging.resize(local + total, 0);
            encode_live_record(&mut tx_state.staging[local..local + total], kb, vb);
            return Ok(total as u64);
        }

        let offset = stats.data_size as usize;
        let end = offset.checked_add(total).ok_or_else(|| {
            io::Error::new(io::ErrorKind::OutOfMemory, "OrderLog offset overflow")
        })?;
        if end > mmap.len() {
            grow_into(mmap, file, end as u64)?;
        }
        encode_live_record(&mut mmap[offset..end], kb, vb);
        stats.data_size = end as u64;
        Ok(total as u64)
    })
}

fn append_tombstone_into<K>(
    mmap: &mut MmapMut,
    file: &mut File,
    tx: &mut Option<TxState>,
    stats: &mut OrderLogStats,
    key: &K,
) -> io::Result<u64>
where
    K: Encode,
{
    with_scratch(key, |kb| {
        let total = 8usize.checked_add(kb.len()).ok_or_else(|| {
            io::Error::new(io::ErrorKind::OutOfMemory, "OrderLog offset overflow")
        })?;

        if let Some(tx_state) = tx.as_mut() {
            let local = tx_state.staging.len();
            tx_state.staging.resize(local + total, 0);
            encode_tombstone_record(&mut tx_state.staging[local..local + total], kb);
            return Ok(total as u64);
        }

        let offset = stats.data_size as usize;
        let end = offset.checked_add(total).ok_or_else(|| {
            io::Error::new(io::ErrorKind::OutOfMemory, "OrderLog offset overflow")
        })?;
        if end > mmap.len() {
            grow_into(mmap, file, end as u64)?;
        }
        encode_tombstone_record(&mut mmap[offset..end], kb);
        stats.data_size = end as u64;
        Ok(total as u64)
    })
}

fn append_entry_raw<K, V>(
    mmap: &mut MmapMut,
    file: &mut File,
    cursor: u64,
    key: &K,
    value: &V,
) -> io::Result<u64>
where
    K: Encode,
    V: Encode,
{
    with_two_scratches(key, value, |kb, vb| {
        let total = 8usize
            .checked_add(vb.len())
            .and_then(|n| n.checked_add(kb.len()))
            .ok_or_else(|| io::Error::new(io::ErrorKind::OutOfMemory, "OrderLog offset overflow"))?;
        let offset = cursor as usize;
        let end = offset.checked_add(total).ok_or_else(|| {
            io::Error::new(io::ErrorKind::OutOfMemory, "OrderLog offset overflow")
        })?;
        if end > mmap.len() {
            grow_into(mmap, file, end as u64)?;
        }
        encode_live_record(&mut mmap[offset..end], kb, vb);
        Ok(end as u64)
    })
}

/// Pack `[vlen u32][value][klen u32][key]` from pre-encoded byte slices.
/// Callers pre-encode via `with_two_scratches` (or peer scratch buffers)
/// so neither key nor value is re-serialized inside this routine.
fn encode_live_record(dst: &mut [u8], key_bytes: &[u8], value_bytes: &[u8]) {
    let v_size = value_bytes.len();
    let k_size = key_bytes.len();
    dst[0..4].copy_from_slice(&(v_size as u32).to_le_bytes());
    dst[4..4 + v_size].copy_from_slice(value_bytes);
    let k_len_off = 4 + v_size;
    dst[k_len_off..k_len_off + 4].copy_from_slice(&(k_size as u32).to_le_bytes());
    dst[k_len_off + 4..k_len_off + 4 + k_size].copy_from_slice(key_bytes);
}

fn encode_tombstone_record(dst: &mut [u8], key_bytes: &[u8]) {
    let k_size = key_bytes.len();
    dst[0..4].copy_from_slice(&TOMBSTONE.to_le_bytes());
    dst[4..8].copy_from_slice(&(k_size as u32).to_le_bytes());
    dst[8..8 + k_size].copy_from_slice(key_bytes);
}

#[derive(Debug, Clone, Copy, Encode, Decode)]
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

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Encode, Decode)]
pub struct OrderLogStats {
    pub data_size: u64,
    pub dead_bytes: u64,
}

pub struct OrderLog<K: Ord, V> {
    index: OrderIndex<K, V>,
    mmap: MmapMut,
    file: File,
    path: PathBuf,
    config: OrderLogConfig,
    stats: OrderLogStats,
    tx: Option<TxState>,
}

impl<K, V> OrderLog<K, V>
where
    K: Encode + Decode<()> + Hash + Eq + Clone + Ord,
    V: Encode + Decode<()> + Clone,
{
    pub fn create(path: &Path, config: OrderLogConfig) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(path)?;

        let capacity = config.initial_capacity.max(HEADER_SIZE as u64);
        file.set_len(capacity)?;
        let mut mmap = unsafe { MmapMut::map_mut(&file)? };
        mmap[0..4].copy_from_slice(&MAGIC.to_le_bytes());

        Ok(OrderLog {
            index: OrderIndex::new(),
            mmap,
            file,
            path: path.to_path_buf(),
            config,
            stats: OrderLogStats {
                data_size: HEADER_SIZE as u64,
                dead_bytes: 0,
            },
            tx: None,
        })
    }

    pub fn open(path: &Path, config: OrderLogConfig) -> io::Result<Self> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        if file.metadata()?.len() < HEADER_SIZE as u64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "not an OrderLog file",
            ));
        }
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
            tx: None,
        };
        this.replay_wal()?;
        Ok(this)
    }

    fn replay_wal(&mut self) -> io::Result<()> {
        let mut cursor = HEADER_SIZE;
        self.stats.dead_bytes = 0;

        while let Some(value_size) = read_u32_le(&self.mmap, cursor) {
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

            let key: K = deserialize_from(&self.mmap[key_start..entry_end])?;
            if is_tombstone {
                if let Some(old) = self.index.remove_drop(&key) {
                    self.stats.dead_bytes += old;
                }
                self.stats.dead_bytes += 8 + key_size as u64;
            } else {
                let value_start = cursor + 4;
                let value_end = value_start + value_size as usize;
                if value_end > self.mmap.len() {
                    break;
                }
                let value: V = deserialize_from(&self.mmap[value_start..value_end])?;
                let record_size = 8 + value_size as u64 + key_size as u64;
                if let Some(old) = self.index.insert(key, value, record_size) {
                    self.stats.dead_bytes += old;
                }
            }
            cursor = entry_end;
        }

        self.stats.data_size = cursor as u64;
        Ok(())
    }

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

    pub fn consume_val(&self) -> ConsumeValIter<'_, K, V> {
        ConsumeValIter {
            mmap: &self.mmap[..],
            cursor: HEADER_SIZE,
            _marker: std::marker::PhantomData,
        }
    }

    fn grow(&mut self, desired: u64) -> io::Result<()> {
        grow_into(&mut self.mmap, &mut self.file, desired)
    }
}

impl<K, V> Backend<K, V> for OrderLog<K, V>
where
    K: Encode + Decode<()> + Hash + Eq + Clone + Ord,
    V: Encode + Decode<()> + Clone,
{
    type Stats = OrderLogStats;
    type Config = OrderLogConfig;

    fn open_tx(&mut self) -> io::Result<()> {
        if self.tx.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "OrderLog tx already open",
            ));
        }
        self.tx = Some(TxState {
            staging: PooledBuf::acquire(),
            tx_base: self.stats.data_size,
        });
        Ok(())
    }

    fn close_tx(&mut self) -> io::Result<()> {
        let Some(tx) = self.tx.take() else {
            return Ok(());
        };
        let staged_len = tx.staging.len();
        if staged_len > 0 {
            let end = tx.tx_base.checked_add(staged_len as u64).ok_or_else(|| {
                io::Error::new(io::ErrorKind::OutOfMemory, "OrderLog offset overflow")
            })?;
            if end > self.mmap.len() as u64 {
                self.grow(end)?;
            }
            let start = tx.tx_base as usize;
            self.mmap[start..start + staged_len].copy_from_slice(&tx.staging);
            self.stats.data_size = end;
        }
        drop(tx);
        self.maybe_compact()
    }

    fn get(&self, key: &K) -> Option<V> {
        self.index.get(key).cloned()
    }

    fn contains(&self, key: &K) -> bool {
        self.index.contains(key)
    }

    fn put(&mut self, key: K, value: V) -> io::Result<()> {
        let encoded = append_entry_into(
            &mut self.mmap,
            &mut self.file,
            &mut self.tx,
            &mut self.stats,
            &key,
            &value,
        )?;
        if let Some(old) = self.index.insert(key, value, encoded) {
            self.stats.dead_bytes += old;
        }
        self.maybe_compact()
    }

    fn put_if_absent(&mut self, key: K, value: V) -> io::Result<bool> {
        let (update, found) = self.index.search(&key);
        if found.is_some() {
            return Ok(false);
        }
        let Self {
            index,
            mmap,
            file,
            tx,
            stats,
            ..
        } = self;
        let encoded = append_entry_into(mmap, file, tx, stats, &key, &value)?;
        index.insert_with_update(key, value, encoded, update);
        self.maybe_compact()?;
        Ok(true)
    }

    fn replace(&mut self, key: K, value: V) -> io::Result<Option<V>> {
        let (update, found) = self.index.search(&key);
        let prev = found.map(|idx| self.index.arena[idx].value.clone());
        let Self {
            index,
            mmap,
            file,
            tx,
            stats,
            ..
        } = self;
        let encoded = append_entry_into(mmap, file, tx, stats, &key, &value)?;
        match found {
            Some(idx) => {
                let old = index.arena[idx].record_size;
                index.arena[idx].value = value;
                index.arena[idx].record_size = encoded;
                stats.dead_bytes += old;
            }
            None => index.insert_with_update(key, value, encoded, update),
        }
        self.maybe_compact()?;
        Ok(prev)
    }

    fn bulk_put<I>(&mut self, items: I) -> io::Result<()>
    where
        I: IntoIterator<Item = (K, V)>,
    {
        let mut sorted: Vec<(K, V)> = items.into_iter().collect();
        sorted.sort_by(|(a, _), (b, _)| a.cmp(b));
        self.bulk_put_sorted(sorted)
    }

    fn bulk_put_sorted<I>(&mut self, sorted: I) -> io::Result<()>
    where
        I: IntoIterator<Item = (K, V)>,
    {
        let opened_here = self.tx.is_none();
        if opened_here {
            self.open_tx()?;
        }

        let mut first_err = None;
        {
            let Self {
                index,
                mmap,
                file,
                tx,
                stats,
                ..
            } = self;
            let mut finger = index.finger_at_start();
            for (k, v) in sorted {
                let encoded = match append_entry_into(mmap, file, tx, stats, &k, &v) {
                    Ok(size) => size,
                    Err(e) => {
                        first_err = Some(e);
                        break;
                    }
                };
                index.advance_finger_to(&mut finger, &k);
                if index.finger_matches(&finger, &k) {
                    let old = index.overwrite_at_finger(&finger, v, encoded);
                    stats.dead_bytes += old;
                } else {
                    index.insert_at_finger(&mut finger, k, v, encoded);
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
            None => Ok(()),
        }
    }

    fn delete(&mut self, key: &K) -> io::Result<bool> {
        let Some(old) = self.index.remove_drop(key) else {
            return Ok(false);
        };
        let tombstone = append_tombstone_into(
            &mut self.mmap,
            &mut self.file,
            &mut self.tx,
            &mut self.stats,
            key,
        )?;
        self.stats.dead_bytes += old + tombstone;
        self.maybe_compact()?;
        Ok(true)
    }

    fn bulk_delete<'a, I>(&mut self, keys: I) -> io::Result<usize>
    where
        I: IntoIterator<Item = &'a K>,
        K: 'a,
    {
        let mut sorted: Vec<&K> = keys.into_iter().collect();
        sorted.sort();
        self.bulk_delete_sorted(sorted)
    }

    fn bulk_delete_sorted<'a, I>(&mut self, sorted: I) -> io::Result<usize>
    where
        I: IntoIterator<Item = &'a K>,
        K: 'a,
    {
        let opened_here = self.tx.is_none();
        if opened_here {
            self.open_tx()?;
        }

        let mut removed = 0;
        let mut first_err = None;
        {
            let Self {
                index,
                mmap,
                file,
                tx,
                stats,
                ..
            } = self;
            let mut finger = index.finger_at_start();
            for k in sorted {
                index.advance_finger_to(&mut finger, k);
                if !index.finger_matches(&finger, k) {
                    continue;
                }
                let tombstone = match append_tombstone_into(mmap, file, tx, stats, k) {
                    Ok(size) => size,
                    Err(e) => {
                        first_err = Some(e);
                        break;
                    }
                };
                let old = index.remove_at_finger(&mut finger);
                stats.dead_bytes += old + tombstone;
                removed += 1;
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
                let tombstone = append_tombstone_into(
                    &mut self.mmap,
                    &mut self.file,
                    &mut self.tx,
                    &mut self.stats,
                    key,
                )?;
                let old = self.index.remove_found(update, idx);
                self.stats.dead_bytes += old + tombstone;
                self.maybe_compact()?;
            }
            return Ok(());
        };

        let encoded = append_entry_into(
            &mut self.mmap,
            &mut self.file,
            &mut self.tx,
            &mut self.stats,
            key,
            &new_state,
        )?;
        match found {
            Some(idx) => {
                let old = self.index.arena[idx].record_size;
                self.index.arena[idx].value = new_state;
                self.index.arena[idx].record_size = encoded;
                self.stats.dead_bytes += old;
            }
            None => self
                .index
                .insert_with_update(key.clone(), new_state, encoded, update),
        }
        self.maybe_compact()?;
        Ok(())
    }

    fn clear(&mut self) -> io::Result<()> {
        debug_assert!(self.tx.is_none(), "clear() called inside an open tx");
        self.index.clear();
        self.stats = OrderLogStats {
            data_size: HEADER_SIZE as u64,
            dead_bytes: 0,
        };
        self.mmap[0..4].copy_from_slice(&MAGIC.to_le_bytes());
        if self.mmap.len() >= HEADER_SIZE + 4 {
            self.mmap[HEADER_SIZE..HEADER_SIZE + 4].copy_from_slice(&0u32.to_le_bytes());
        }
        Ok(())
    }

    fn compact(&mut self) -> io::Result<()> {
        debug_assert!(self.tx.is_none(), "compact() called inside an open tx");
        let Self {
            index,
            mmap,
            file,
            config,
            stats,
            ..
        } = self;

        mmap[0..4].copy_from_slice(&MAGIC.to_le_bytes());
        let mut cursor = HEADER_SIZE as u64;
        for (key, node) in index.iter_with_nodes() {
            cursor = append_entry_raw(mmap, file, cursor, key, &node.value)?;
        }
        if (cursor as usize) + 4 <= mmap.len() {
            let p = cursor as usize;
            mmap[p..p + 4].copy_from_slice(&0u32.to_le_bytes());
        }
        stats.data_size = cursor;
        stats.dead_bytes = 0;

        let new_capacity = cursor.max(config.initial_capacity);
        *mmap = MmapMut::map_anon(1)?;
        file.set_len(new_capacity)?;
        *mmap = unsafe { MmapMut::map_mut(&*file)? };
        Ok(())
    }

    fn keys(&self) -> impl Iterator<Item = K> + '_ {
        self.index.iter().map(|(k, _)| k.clone())
    }

    fn values(&self) -> impl Iterator<Item = V> + '_ {
        self.index.iter().map(|(_, v)| v.clone())
    }

    fn entries(&self) -> impl Iterator<Item = (K, V)> + '_ {
        self.index.iter().map(|(k, v)| (k.clone(), v.clone()))
    }

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
    fn range(&self, start: &K, end: &K) -> impl Iterator<Item = (K, V)> + '_ {
        self.index
            .range_owned_end(start, end.clone())
            .map(|(k, v)| (k.clone(), v.clone()))
    }

    fn first(&self) -> Option<(K, V)> {
        let idx = self.index.first_idx()?;
        let node = &self.index.arena[idx];
        Some((node.key.clone(), node.value.clone()))
    }

    fn last(&self) -> Option<(K, V)> {
        let idx = self.index.last_idx()?;
        let node = &self.index.arena[idx];
        Some((node.key.clone(), node.value.clone()))
    }

    fn entries_rev(&self) -> impl Iterator<Item = (K, V)> + '_
    where
        K: 'static,
        V: 'static,
    {
        self.index.iter_rev().map(|(k, v)| (k.clone(), v.clone()))
    }

    fn range_rev(&self, start: &K, end: &K) -> impl Iterator<Item = (K, V)> + '_
    where
        K: 'static,
        V: 'static,
    {
        self.index
            .range_rev(start.clone(), end)
            .map(|(k, v)| (k.clone(), v.clone()))
    }
}

impl<K: Ord, V> Drop for OrderLog<K, V> {
    fn drop(&mut self) {
        let _ = self.mmap.flush_async();
    }
}

impl<K: Ord, V> fmt::Debug for OrderLog<K, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OrderLog")
            .field("path", &self.path)
            .field("stats", &self.stats)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::backend::{Backend, OrderedBackend};
    use crate::utils::serdes::serialized_size;
    use std::fs;
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
        assert_eq!(
            ol.keys().collect::<Vec<_>>(),
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
        let got: Vec<_> = ol.range(&k("b"), &k("e")).map(|(k, _)| k).collect();
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
        {
            let mut ol = create(&p);
            ol.put(k("a"), v("a", 1)).unwrap();
            ol.put(k("b"), v("b", 2)).unwrap();
            assert!(ol.delete(&k("a")).unwrap());
            assert!(!ol.delete(&k("a")).unwrap());
            ol.sync().unwrap();
        }
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
        assert_eq!(open(&p).get(&k("x")), Some(v("v2", 2)));
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
        assert_eq!(ol.get(&k("counter")), Some(6));
        ol.update(&k("counter"), |_| None).unwrap();
        assert!(ol.get(&k("counter")).is_none());
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
        assert_eq!(ol.get(&k("counter")), Some(10));
    }

    #[test]
    fn update_inserts_when_absent() {
        let p = tmp_path("update_absent_insert");
        let mut ol: OrderLog<TestKey, u32> =
            OrderLog::create(&p, OrderLogConfig::default()).unwrap();
        ol.update(&k("fresh"), |cur| Some(cur.unwrap_or(0) + 7))
            .unwrap();
        assert_eq!(ol.get(&k("fresh")), Some(7));
    }

    #[test]
    fn update_returning_none_deletes() {
        let p = tmp_path("update_delete");
        let mut ol: OrderLog<TestKey, u32> =
            OrderLog::create(&p, OrderLogConfig::default()).unwrap();
        ol.put(k("x"), 42).unwrap();
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
        assert_eq!(ol.get(&k("acc")), Some(vec![1, 2, 3, 10, 20, 30]));
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
            assert_eq!(ol.get(&k(&format!("k{:02}", i))), Some(v("update", i)));
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
        assert_eq!(ol.keys().collect::<Vec<_>>(), vec![k("a"), k("c"), k("d")]);
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
        assert_eq!(ol.first().unwrap().0, k("k00"));
        assert_eq!(ol.last().unwrap().0, k("k19"));
        let middle: Vec<_> = ol.range(&k("k05"), &k("k08")).map(|(k, _)| k).collect();
        assert_eq!(middle, vec![k("k05"), k("k06"), k("k07")]);
    }

    #[test]
    fn put_if_absent_and_replace_paths() {
        let p = tmp_path("pia_replace");
        let mut ol = create(&p);
        assert!(ol.put_if_absent(k("a"), v("first", 1)).unwrap());
        assert!(!ol.put_if_absent(k("a"), v("second", 2)).unwrap());
        assert_eq!(ol.get(&k("a")), Some(v("first", 1)));
        assert_eq!(
            ol.replace(k("a"), v("third", 3)).unwrap(),
            Some(v("first", 1))
        );
        assert_eq!(ol.replace(k("b"), v("new", 4)).unwrap(), None);
        assert_eq!(ol.get(&k("a")), Some(v("third", 3)));
        assert_eq!(ol.get(&k("b")), Some(v("new", 4)));
    }

    #[test]
    fn tx_bulk_put_visible_after_close_and_reopen() {
        let p = tmp_path("tx_bulk_put");
        {
            let mut ol = create(&p);
            ol.open_tx().unwrap();
            for i in 0..250u32 {
                ol.put(format!("k{:04}", i).into_bytes(), v("payload", i))
                    .unwrap();
            }
            ol.close_tx().unwrap();
            ol.sync().unwrap();
        }
        let ol = open(&p);
        assert_eq!(ol.size(), 250);
        assert_eq!(ol.get(&k("k0249")), Some(v("payload", 249)));
    }

    #[test]
    fn tx_read_your_own_writes_and_delete_survive() {
        let p = tmp_path("tx_ryow");
        {
            let mut ol = create(&p);
            ol.put(k("doomed"), v("old", 1)).unwrap();
            ol.open_tx().unwrap();
            ol.put(k("staged"), v("fresh", 42)).unwrap();
            assert_eq!(ol.get(&k("staged")), Some(v("fresh", 42)));
            assert!(ol.delete(&k("doomed")).unwrap());
            assert!(!ol.contains(&k("doomed")));
            ol.close_tx().unwrap();
            ol.sync().unwrap();
        }
        let ol = open(&p);
        assert_eq!(ol.get(&k("staged")), Some(v("fresh", 42)));
        assert!(!ol.contains(&k("doomed")));
    }

    #[test]
    fn tx_overwrite_within_tx() {
        let p = tmp_path("tx_overwrite");
        let mut ol = create(&p);
        ol.open_tx().unwrap();
        ol.put(k("a"), v("first", 1)).unwrap();
        ol.put(k("a"), v("second", 2)).unwrap();
        assert_eq!(ol.get(&k("a")), Some(v("second", 2)));
        ol.close_tx().unwrap();
        assert_eq!(ol.get(&k("a")), Some(v("second", 2)));
    }

    #[test]
    fn nested_open_tx_errors_and_close_noop() {
        let p = tmp_path("tx_nested");
        let mut ol = create(&p);
        ol.close_tx().unwrap();
        ol.open_tx().unwrap();
        assert_eq!(
            ol.open_tx().unwrap_err().kind(),
            io::ErrorKind::AlreadyExists
        );
        ol.close_tx().unwrap();
        ol.close_tx().unwrap();
    }

    #[test]
    fn compaction_does_not_fire_during_tx() {
        let p = tmp_path("tx_compact_deferred");
        let mut ol = create_with(
            &p,
            OrderLogConfig {
                initial_capacity: 16 * 1024,
                compaction_ratio: 0.0,
            },
        );
        ol.put(k("hot"), v("seed", 0)).unwrap();
        assert_eq!(ol.stats().dead_bytes, 0);
        ol.open_tx().unwrap();
        for i in 1..20u32 {
            ol.put(k("hot"), v("staged", i)).unwrap();
        }
        assert!(ol.stats().dead_bytes > 0);
        ol.close_tx().unwrap();
        assert_eq!(ol.stats().dead_bytes, 0);
        assert_eq!(ol.get(&k("hot")), Some(v("staged", 19)));
    }

    #[test]
    fn bulk_put_and_delete_auto_tx() {
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
        ol.open_tx().unwrap();
        ol.close_tx().unwrap();
    }

    #[test]
    fn bulk_unsorted_routes_through_sorted_paths() {
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
            ol.keys().collect::<Vec<_>>(),
            vec![k("a"), k("b"), k("c"), k("d")]
        );

        let keys = vec![k("c"), k("missing"), k("a")];
        assert_eq!(ol.bulk_delete(keys.iter()).unwrap(), 2);
        assert_eq!(ol.keys().collect::<Vec<_>>(), vec![k("b"), k("d")]);
    }

    #[test]
    fn bulk_put_within_existing_tx_does_not_close_it() {
        let p = tmp_path("bulk_existing_tx");
        let mut ol = create(&p);
        ol.open_tx().unwrap();
        ol.bulk_put([(k("a"), v("a", 1)), (k("b"), v("b", 2))])
            .unwrap();
        assert_eq!(
            ol.open_tx().unwrap_err().kind(),
            io::ErrorKind::AlreadyExists
        );
        ol.put(k("c"), v("c", 3)).unwrap();
        ol.close_tx().unwrap();
        assert_eq!(ol.size(), 3);
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
        assert_eq!(ol.get(&k("k04")), Some(v("new", 44)));
        assert_eq!(ol.last().map(|(k, _)| k), Some(k("k25")));
        let keys: Vec<_> = ol.keys().collect();
        assert!(keys.windows(2).all(|w| w[0] < w[1]));
    }

    #[test]
    fn bulk_put_sorted_within_existing_tx_does_not_close_it() {
        let p = tmp_path("bulk_sorted_tx");
        let mut ol = create(&p);
        ol.open_tx().unwrap();
        ol.bulk_put_sorted([(k("a"), v("a", 1)), (k("b"), v("b", 2))])
            .unwrap();
        assert_eq!(
            ol.open_tx().unwrap_err().kind(),
            io::ErrorKind::AlreadyExists
        );
        ol.close_tx().unwrap();
        assert_eq!(ol.size(), 2);
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
    fn bulk_delete_sorted_within_existing_tx_does_not_close_it() {
        let p = tmp_path("bulk_delete_sorted_tx");
        let mut ol = create(&p);
        for i in 0..5u32 {
            ol.put(format!("k{:02}", i).into_bytes(), v("p", i))
                .unwrap();
        }
        ol.open_tx().unwrap();
        let keys = vec![k("k01"), k("k03")];
        assert_eq!(ol.bulk_delete_sorted(keys.iter()).unwrap(), 2);
        assert_eq!(
            ol.open_tx().unwrap_err().kind(),
            io::ErrorKind::AlreadyExists
        );
        ol.close_tx().unwrap();
        assert_eq!(ol.size(), 3);
    }

    #[test]
    fn reverse_iteration_and_ranges() {
        let p = tmp_path("reverse");
        let mut ol = create(&p);
        for s in ["a", "b", "c", "d", "e"] {
            ol.put(k(s), v(s, 0)).unwrap();
        }
        let rev: Vec<_> = ol.entries_rev().map(|(k, _)| k).collect();
        assert_eq!(rev, vec![k("e"), k("d"), k("c"), k("b"), k("a")]);
        let forward: Vec<_> = ol.range(&k("b"), &k("e")).collect();
        let rev_range: Vec<_> = ol.range_rev(&k("b"), &k("e")).collect();
        assert_eq!(rev_range, forward.into_iter().rev().collect::<Vec<_>>());
        assert!(ol.range_rev(&k("c"), &k("c")).next().is_none());
        ol.delete(&k("c")).unwrap();
        let rev_after: Vec<_> = ol.entries_rev().map(|(k, _)| k).collect();
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
            ol.entries_rev().map(|(k, _)| k).collect::<Vec<_>>(),
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
            .map(|(k, _)| k)
            .collect();
        assert_eq!(got, vec![k("k01000"), k("k01001"), k("k01002")]);
    }

    #[test]
    fn open_rejects_truncated_file_without_panicking() {
        let p = tmp_path("truncated");
        fs::write(&p, [1u8, 2, 3]).unwrap();
        let err = OrderLog::<TestKey, TestVal>::open(&p, OrderLogConfig::default()).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }
}
