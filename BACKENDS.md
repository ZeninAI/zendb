# ZenDB Storage Backends — Technical Reference

This document describes every function in every backend implementation at the
algorithmic level.  It is organised by backend, then by logical group
(construction, reads, writes, bulk operations, iteration, maintenance).

---

## Table of Contents

1. [Common Infrastructure](#1-common-infrastructure)
   - [Trait Hierarchy](#11-trait-hierarchy)
   - [Serialisation Primitives (`serdes.rs`)](#12-serialisation-primitives)
   - [Thread-Local Buffer Pool (`reusables.rs`)](#13-thread-local-buffer-pool)
2. [BPlusTree (`btree.rs`)](#2-bplustree)
   - [On-Disk Format](#21-on-disk-format)
   - [Construction](#22-construction)
   - [Read Path](#23-read-path)
   - [Write Path](#24-write-path)
   - [Bulk Operations](#25-bulk-operations)
   - [Iteration](#26-iteration)
   - [Ordered Operations](#27-ordered-operations)
   - [Maintenance](#28-maintenance)
3. [KeyDir (`keydir.rs`)](#3-keydir)
   - [On-Disk Format](#31-on-disk-format)
   - [Construction](#32-construction)
   - [Read Path](#33-read-path)
   - [Write Path](#34-write-path)
   - [Bulk Operations](#35-bulk-operations)
   - [Iteration](#36-iteration)
   - [Maintenance](#37-maintenance)
4. [SkipList (`skiplist.rs`)](#4-skiplist)
   - [Data Structure](#41-data-structure)
   - [Construction](#42-construction)
   - [Read Path](#43-read-path)
   - [Write Path](#44-write-path)
   - [Bulk Operations](#45-bulk-operations)
   - [Iteration & Ordered Operations](#46-iteration--ordered-operations)
   - [Maintenance](#47-maintenance)
5. [Topic (`topic.rs`)](#5-topic)
   - [Architecture](#51-architecture)
   - [Construction](#52-construction)
   - [Append Path](#53-append-path)
   - [Read Path](#54-read-path)
   - [Consumer Management](#55-consumer-management)
   - [Compaction](#56-compaction)
   - [Durability](#57-durability)
6. [State (`state.rs`)](#6-state)
   - [Runtime Dispatch](#61-runtime-dispatch)
   - [Method Dispatch Table](#62-method-dispatch-table)
7. [Trait Definitions](#7-trait-definitions)
   - [`Backend<K, V>`](#71-backendk-v)
   - [`FileBackedBackend<K, V>`](#72-filebackedbackendk-v)
   - [`OrderedBackend<K, V>`](#73-orderedbackendk-v)

---

## 1. Common Infrastructure

### 1.1 Trait Hierarchy

```
Backend<K, V>                  ← common CRUD + iteration + flush/sync
├── FileBackedBackend<K, V>    ← create(path) + open(path)
└── OrderedBackend<K, V>       ← range, first, last, entries_rev, range_rev
```

- **`Backend`** is the universal contract.  Every backend (BPlusTree, KeyDir,
  SkipList) implements it.
- **`FileBackedBackend`** is implemented by BPlusTree and KeyDir (mmap'd
  files).  SkipList is purely in-memory and does not implement it.
- **`OrderedBackend`** is implemented by BPlusTree and SkipList.  KeyDir is
  unordered (hash-indexed) and does not implement it.
- **`State`** is a runtime enum that wraps one of the three and implements all
  three traits, dispatching each call to the active variant.

All keys (`K`) and values (`V`) are serialised through **bincode 2** with a
shared configuration: little-endian, fixed-int encoding, no decode limit.
The trait bounds on `Backend<K, V>` are:

```rust
K: Encode + Decode<()> + Hash + Eq + Clone + Ord
V: Encode + Decode<()> + Clone
```

### 1.2 Serialisation Primitives

File: `zendb-storage/src/utils/serdes.rs`

| Function | Purpose | Allocation? |
|----------|---------|-------------|
| `serialized_size(value)` | Measures bincode output size via `SizeWriter`. Zero allocation. | No |
| `serialize_into(value, dst)` | Writes bincode directly into `&mut [u8]` (typically an mmap slice). Returns bytes written. | No |
| `serialize_into_std(value, writer)` | Writes bincode into any `io::Write`. | No (uses writer) |
| `serialize_to_vec(value)` | Encodes into a freshly-allocated `Vec<u8>`. | Yes |
| `deserialize_from(src)` | Decodes a value from `&[u8]`. Returns owned `T`. | Yes (decoded value) |
| `with_scratch(value, f)` | Acquires a `PooledBuf`, encodes `value` into it, passes slice to `f`. Buffer is returned to pool after `f` returns. | No (buffer reused) |
| `with_two_scratches(a, b, f)` | Same as `with_scratch` but for two values simultaneously. | No |

Byte-level helpers (`rd_u16`, `rd_u32`, `rd_u64`, `wr_u16`, `wr_u32`, `wr_u64`)
read/write little-endian integers directly from `&[u8]` / `&mut [u8]` slices.

### 1.3 Thread-Local Buffer Pool

File: `zendb-storage/src/utils/reusables.rs`

`PooledBuf` wraps a `Vec<u8>` stored in a per-thread `RefCell<Vec<Vec<u8>>>`
LIFO stack.

- **`acquire()`**: pops a buffer from the stack, or allocates a fresh empty
  `Vec<u8>`. The returned buffer has `len() == 0` but retains its previous
  capacity.
- **`drop()`**: clears the buffer (`len = 0`) and pushes it back onto the
  stack.  Capacity is preserved so the next acquirer gets a warm allocation.
- **Recursion-safe**: acquiring a second `PooledBuf` while one is already
  held simply pops the next buffer (or allocates).  No `RefCell::borrow_mut`
  conflict because the inner `Vec<u8>` is moved out of the pool on acquire.

This pool is the backbone of `with_scratch` / `with_two_scratches` — the hot
path for key encoding in BPlusTree navigation and value encoding in all write
paths never calls `Vec::new()` after warm-up.

---

## 2. BPlusTree

File: `zendb-storage/src/core/btree.rs`

A persistent, ordered B+ tree backed by a single memory-mapped file.  All
values live in leaf pages; internal pages carry only separator keys for
navigation.  The tree orders entries by **lexicographic comparison of
serialised key bytes**, not by `K::Ord`.

### 2.1 On-Disk Format

#### Page Layout

Every page is `PAGE_SIZE` bytes (4096).  Page 0 is the **meta page**; pages
≥1 are either **leaf** or **internal** pages.

##### Meta Page (page 0)

| Offset | Size | Field |
|--------|------|-------|
| 0 | 4 | `magic` (`u32` LE, `0x54524542` = "BERT") |
| 4 | 8 | `root` page number |
| 12 | 8 | `free_head` (freelist head page number, 0 = empty) |
| 20 | 8 | `pages` (total allocated pages) |
| 28 | 8 | `entries` (live key count) |
| 36 | 8 | `rightmost_leaf` page number |
| 44 | 8 | `leaf_pages` count |
| 52 | 8 | `leaf_entry_bytes` (sum of entry byte costs across all leaves) |
| 60 | 8 | `free_pages` count |

##### Leaf Page

| Offset | Size | Field |
|--------|------|-------|
| 0 | 1 | `type` (`PAGE_LEAF = 1`) |
| 1 | 1 | `flags` (`FLAG_ROOT = 1` if root, else 0) |
| 2 | 2 | `count` (`u16` LE, number of live slots) |
| 4 | 4 | `data_off` (`u32` LE, next free byte growing up from bottom) |
| 8 | 8 | `next_leaf` page number (0 = none) |
| 16 | 8 | `prev_leaf` page number (0 = none) |

After the 24-byte header:
- **Slot array** grows **down** from `data_off`.  Each slot is `SLOT_SIZE`
  (2) bytes, holding a `u16` LE offset into the page data area.
- **Entry data** grows **up** from the page bottom (`PAGE_SIZE`).

##### Internal Page

| Offset | Size | Field |
|--------|------|-------|
| 0 | 1 | `type` (`PAGE_INTERNAL = 2`) |
| 1 | 1 | `flags` |
| 2 | 2 | `count` (`u16` LE) |
| 4 | 4 | `data_off` (`u32` LE) |
| 8 | 8 | `leftmost_child` page number |

Slot array starts at `HEADER_SIZE` (16).  Each slot is 2 bytes pointing to an
internal entry.

#### Entry Formats

**Leaf, inline** (value fits in `MAX_INLINE` = page minus overhead):

```
[key_len: u16 LE][value_len: u32 LE][key_bytes][value_bytes]
```

**Leaf, extent** (value exceeds `MAX_INLINE`):

```
[key_len: u16 LE][value_len: u32 LE | OVFL_FLAG][key_bytes][extent_start: u64 LE]
```

The `OVFL_FLAG` (`0x8000_0000`) is OR'd into `value_len`.  The real byte
length is `value_len & !OVFL_FLAG`.  `extent_start` is the first page of a
contiguous page run holding the raw value bytes.

**Internal**:

```
[key_len: u16 LE][child_page: u64 LE][separator_key]
```

#### Extents

Values that don't fit inline are stored in a **contiguous run of pages**
allocated at the file's tail.  The leaf entry stores only the first page
number and true byte length.  Extent pages are **not** formatted as B+ tree
pages — they are raw byte arrays.

Extent allocation:
1. Scan the **freelist** (a singly-linked list of freed pages, head at
   `free_head` in the meta page) for a contiguous run of the required length.
2. If no run is found, grow the file with `ensure_capacity` and allocate from
   the new tail.

#### Suffix Truncation

When a page splits, the separator key pushed to the parent is the **shortest
prefix** of the new page's first key that is strictly greater than the old
page's last key.  This is computed by `truncated_separator(left_last,
right_first)`:

1. Find the first byte position `i` where `right_first[i] > left_last[i]`.
2. Return `right_first[..=i]`.
3. If `right_first` is a prefix of `left_first`, append a `0x00` byte to
   `left_first`.

### 2.2 Construction

#### `BPlusTree::create(path, config)`

**Algorithm:**
1. Opens (or creates) the file at `path` with `truncate(true)`, `read(true)`,
   `write(true)`.
2. Sizes the file to `max(2, config.initial_capacity_pages) * 4096` bytes.
3. Maps the file with `MmapMut::map_mut`.
4. Initialises **page 0** (meta page): writes magic, `root=1`, `pages=2`,
   `entries=0`, `leaf_pages=1`, all other counters to 0.
5. Initialises **page 1** as an empty root leaf: `PAGE_LEAF | FLAG_ROOT`,
   `count=0`, `data_off=4096`, `next_leaf=0`, `prev_leaf=0`.
6. Returns `BPlusTree { mmap, file, config, stats }`.

**Config fields used:** `initial_capacity_pages` (minimum 2).

#### `BPlusTree::open(path, config)`

**Algorithm:**
1. Opens the existing file with `read(true)`, `write(true)` (no truncate).
2. Maps the file.
3. Reads and validates the 4-byte magic at offset 0.  Returns
   `InvalidData` error on mismatch.
4. Constructs a `BPlusTree` with default-zeroed stats.
5. Calls `refresh_stats_from_meta()` to populate `stats` from the meta page
   counters.

**Note:** Does **not** replay or validate the tree structure.  Trusts the
meta page counters.  If they are corrupted (e.g., crash mid-write), the tree
may reference garbage pages.

#### `refresh_stats_from_meta()`

Reads `entries`, `pages`, `leaf_pages`, `leaf_entry_bytes`, and `free_pages`
from the meta page and copies them into `self.stats`.  Called once at `open`.

### 2.3 Read Path

#### `get(key)`

**Algorithm:**
1. `with_scratch(key, |kb| ...)` — encodes key into a pooled scratch buffer.
2. Calls `value_bytes_for(kb)` to navigate to the leaf and find the value.
3. If found, calls `deserialize_from::<V>(slice)` to decode the value.
4. Returns `Some(Cow::Owned(v))`.

**Complexity:** O(log n) page probes + binary search within leaf.

#### `contains(key)`

**Algorithm:**
1. Encodes key via `with_scratch`.
2. Calls `value_bytes_for(kb).is_some()`.
3. **No value deserialization.**  Pure presence check.

**Complexity:** O(log n) — identical navigation cost to `get`, but skips the
decode step.

#### `value_bytes_for(key_bytes)`

The core lookup primitive:

1. `find_leaf(root, key_bytes)` — descends from root through internal pages.
2. Reads `count` from the leaf header.
3. `leaf_find_slot(leaf, count, key_bytes)` — binary search within the leaf.
4. If `Ok(i)`, returns `value_bytes_at(leaf, i)`.
5. If `Err(_)`, returns `None`.

#### `find_leaf(root, key_bytes)`

Iterates starting at `root`:

```
while page is internal:
    child = internal_search(page, key_bytes)
    page = child
return page  // must be a leaf
```

#### `internal_search(page, key_bytes)`

Binary search through an internal page's separator keys:

1. Reads `count` and `leftmost_child` from the page header.
2. Binary search on separators (slot entry at offset `eo` → key at
   `eo+10..eo+10+kl`).
3. If `key < separator[mid]`, search left half.
4. Returns the child page pointer from the matching slot (or `leftmost_child`
   if the key is smaller than all separators).

**Byte comparison only** — no key deserialisation during navigation.

#### `leaf_find_slot(page, count, key_bytes)`

Binary search within a leaf:

```
lo = 0, hi = count
while lo < hi:
    mid = (lo + hi) / 2
    slot_key = key_bytes_at(page, mid)
    match slot_key.cmp(key_bytes):
        Equal  → return Ok(mid)
        Less   → lo = mid + 1
        Greater → hi = mid
return Err(lo)  // insertion point
```

Returns `Ok(index)` for exact match, `Err(insertion_point)` otherwise.

#### `value_bytes_at(leaf_page, slot)`

Reads the raw value bytes for a slot:

1. Reads slot offset → entry offset `eo`.
2. Reads `key_len` (`u16`) and `raw_value_len` (`u32`).
3. If `OVFL_FLAG` is set: reads `extent_start` (`u64`), returns a slice from
   the extent page run (`mmap[page_offset(extent_start) .. + real_len]`).
4. Otherwise: returns a slice from within the leaf page
   (`leaf[eo+6+kl .. eo+6+kl+vl]`).

**Zero-copy** — returns a borrow directly from the mmap.

#### `key_bytes_at(leaf_page, slot)`

Reads raw key bytes: slot offset → entry offset → key at `eo+6..eo+6+kl`.
Always borrowed from the mmap.

### 2.4 Write Path

All writes mutate the mmap in place.  There is no WAL, no dirty page cache,
no rollback.

#### `put(key, value)`

**Algorithm:**
1. `with_two_scratches(&key, &value, |kb, vb| ...)` — encodes both into
   pooled buffers.
2. Calls `insert_bytes(kb, vb, None)`.
3. Calls `maybe_compact()`.

#### `put_if_absent(key, value)`

**Algorithm:**
1. Encodes key + value via `with_two_scratches`.
2. `self.descend_to_leaf(kb)` — single descent from root to leaf.
3. If `hint.slot` is `Ok(_)` (key exists): return `false` immediately —
   **no write, no mmap mutation**.
4. Otherwise: `insert_bytes(kb, vb, Some(hint))` reuses the descent hint to
   avoid a second `leaf_find_slot`.
5. If inserted, calls `maybe_compact()`.

**Single descent + single hash** — avoids the default trait impl's
`contains` + `put` (two lookups).

#### `replace(key, value)`

**Algorithm:**
1. Encodes key + value.
2. `descend_to_leaf(kb)` — single descent.
3. If key exists: deserialises the old value from the mmap **before**
   overwriting, then calls `insert_bytes`.
4. If key is absent: calls `insert_bytes`, returns `None`.
5. Calls `maybe_compact()`.

**The old value is deserialised and returned as `Cow::Owned`.**  The slot on
disk is about to be overwritten, so borrowing is impossible.

#### `delete(key)`

**Algorithm:**
1. `with_scratch(key, |kb| self.delete_bytes(kb))`.
2. If deleted, calls `maybe_compact()`.

#### `delete_bytes(key_bytes)`

1. `descend_to_leaf(key_bytes)`.
2. If `hint.slot` is `Err(_)`, key not found → return `false`.
3. Otherwise: free any extent, call `leaf_remove_entry`, call `dec_entries`.

#### `update(key, f)`

**Algorithm** (single-descent read-modify-write):

1. `with_scratch(key, |kb| ...)` — encode key once.
2. `descend_to_leaf(kb)` — single descent.
3. If key exists: deserialise current value via `value_bytes_at` +
   `deserialize_from`.
4. Call `f(current)`:
   - `(had_value, Some(new_v))` → encode `new_v` via nested `with_scratch`,
     call `insert_bytes(kb, vb, Some(hint))`, then `maybe_compact()`.
   - `(true, None)` → call `delete_at(leaf_page, slot)`, then
     `maybe_compact()`.
   - `(false, None)` → no-op.

**Key is encoded exactly once.**  The descent hint is reused for the
subsequent insert/delete, avoiding a second binary search.

#### `insert_bytes(key_bytes, value_bytes, hint)`

The central write primitive — called by `put`, `put_if_absent`, `replace`,
and `update`.

1. Unwraps or computes the `DescentHint` (leaf page + parent path + slot
   result).
2. Calls `leaf_insert_bytes(leaf_page, key_bytes, value_bytes, Some(slot))`.
3. If the key did not previously exist (`slot` was `Err`), calls
   `inc_entries(1)`.
4. If `leaf_insert_bytes` returns a `PageSplit`, calls
   `cascade_split(path, split)` to propagate the split upward.

#### `leaf_insert_bytes(page, key_bytes, value_bytes, slot_hint)`

**Algorithm:**

1. Reads current `count` and `data_off` from the page header.
2. Determines whether the value is an extent (`OVFL_FLAG`) by checking
   `key.len() + value.len() + 6 > MAX_INLINE`.
3. Computes `leaf_es` (entry size in page) and `needed` (entry + slot).

**Case A — exact match (`slot_hint = Ok(i)`):**
- Reads the old entry's `raw_vl`, `kl`, `eo`, and extent pointer.
- Frees any old extent.
- **Fast path:** if old entry was inline, new entry is inline, and byte
  lengths match → overwrite value bytes in place, return `None` (no split).
- Otherwise: removes the old entry via `leaf_remove_entry`, then recurses
  with `Some(Err(i))` to insert at the freed position.

**Case B — insert at position (`slot_hint = Err(pos)`):**
- If `free_start + needed > data_off` → page is full, return
  `leaf_split(page, key_bytes, value_bytes)`.
- Otherwise: shifts slots `[pos..count)` right by one, writes the new entry
  at `data_off - leaf_es` via `write_leaf_entry_raw`, updates `count` and
  `data_off`, calls `add_leaf_entry_bytes`.

#### `leaf_split(page, key_bytes, value_bytes)`

**Algorithm** (balanced split):

1. Snapshots the original page into a `PooledBuf` scratch buffer (one
   allocation, not N per-entry `Vec<u8>`s).
2. Computes the insertion position `ip` via binary search in the snapshot.
3. Builds an `order: Vec<usize>` of slot offsets in ascending key order, with
   `NEW_SLOT` (a sentinel = `usize::MAX`) at position `ip`.
4. Computes `entry_size` and `key_bytes_of` closures that handle both real
   slots and the `NEW_SLOT` sentinel.
5. Checks if the combined set fits in a single page — if so, repacks without
   splitting (avoids unnecessary splits for small overwrites).
6. Otherwise: finds the split midpoint by cumulative byte cost, splitting at
   `total_bytes / 2` so both halves are ~50% loaded.
7. Resets the original page header (`count = 0`, `data_off = PAGE_SIZE`).
8. Allocates a new right page via `alloc_page()`, initialises as leaf,
   links `next_leaf`/`prev_leaf`.
9. **Left page** (original): append-only repack of entries `0..mid`.
10. **Right page** (new): append-only repack of entries `mid..`.
11. If the new entry is in the right page, writes the extent (if needed)
    before the right page fills.
12. Computes the separator key via
    `truncated_separator(last_key_of_left, first_key_of_right)`.
13. Returns `Some(PageSplit { left_page, right_page, separator_key })`.

#### `cascade_split(path, split)`

Propagates a split upward through the parent chain:

1. Unpacks `(separator_key, left_page, right_page)` from the split.
2. Iterates the parent path in reverse (bottom-up): for each parent page,
   calls `internal_insert(parent, separator_key, right_page)`.
   - If the internal page has room → insert succeeds, return.
   - If the internal page splits → the split becomes the new
     `(separator_key, left, right)` for the next iteration.
3. If the split reaches the root (path exhausted):
   - Allocates a new root page.
   - Initialises it as internal with `leftmost_child = left_page`.
   - Inserts `(separator_key, right_page)`.
   - Sets `FLAG_ROOT` on the new root, clears it on the old root.
   - Updates `set_root(new_root)`.

#### `write_leaf_entry_raw(page, nd, key_bytes, value_bytes, is_ovfl)`

Writes a new leaf entry at offset `nd` within the page:

1. If `is_ovfl`: allocates and writes extent pages via `write_extent`, then
   writes `[key_len][value_len|OVFL_FLAG][key_bytes][extent_start]`.
2. If inline: writes `[key_len][value_len][key_bytes][value_bytes]`.

#### `leaf_remove_entry(page, slot, count, data_off)`

Removes an entry from a leaf:

1. Reads the entry offset `eo` from the slot array.
2. Computes the entry's byte size from `key_len` + `value_len` (or extent
   pointer size).
3. Shifts all data **above** the removed entry down by `entry_size` bytes
   using `copy_within`.
4. Adjusts all slot offsets that pointed above the removed entry.
5. Shifts slots `[slot+1..count)` left by one.
6. Updates `count` (decrement) and `data_off` (increase by `entry_size`).
7. Calls `sub_leaf_entry_bytes`.

#### `internal_insert(page, key_bytes, child)`

Inserts a `(separator_key, child_page)` pair into an internal page:

1. Reads `count` and `data_off`.
2. If no room (`free_start + entry_size + SLOT_SIZE > data_off`), returns
   `internal_split(page, key_bytes, child)`.
3. Binary-searches for the insertion position.
4. Shifts higher slots right by one.
5. Writes `[key_len][child_page][separator_key]` at the data area.
6. Updates `count` and `data_off`.

#### `internal_split(page, key_bytes, child)`

Mirrors `leaf_split` for internal pages:

1. Snapshots the original page into a `PooledBuf`.
2. Computes insertion position and builds an ordered slot list.
3. Splits at the byte midpoint.
4. Separator key = `truncated_separator(last_key_of_left, first_key_of_right)`.
5. The entry at the split boundary is "lifted" — its child becomes the right
   page's `leftmost_child`, and only its separator key moves up.

### 2.5 Bulk Operations

#### `bulk_put(items)`

Simple loop: `for (k, v) in items { self.put(k, v)? }`.

Each `put` descends from root independently.

#### `bulk_put_sorted(sorted)`

**Two paths:**

1. **Tree is empty:** bottom-up bulk build.
   - Streams the iterator, encoding each `(K, V)` on the fly via
     `serialize_to_vec`.
   - Passes the `io::Result<(Vec<u8>, Vec<u8>)>` stream to
     `bottom_up_build_raw`.
   - Peak memory = one encoded pair.

2. **Tree is non-empty:** falls back to `bulk_put` (loop `put`).

#### `bottom_up_build_raw(items)`

Builds a tree from sorted raw byte pairs:

1. **Leaf packing:** streams items into leaves sequentially.  When a leaf is
   full, finalises it (records first/last keys), allocates a new leaf, links
   `next_leaf`/`prev_leaf`.
2. Tracks `leaf_first_keys` and `leaf_last_keys` for each completed leaf.
3. After all leaves are built, sets `rightmost_leaf` and `inc_entries`.
4. **Internal level construction:** calls `build_internal_levels(leaves,
   first_keys, last_keys)`.

**Complexity:** O(n) — each item is written exactly once, each page is split
at most O(log n) times.

#### `build_internal_levels(children, first_keys, last_keys)`

Builds internal pages bottom-up:

1. Iterates through children in groups that fit in one internal page.
2. For each group: allocates a new internal page, sets `leftmost_child` to
   the group's first child, then appends `(separator, child)` pairs for
   subsequent children.
3. Separator keys are computed as `truncated_separator(last_keys[i-1],
   first_keys[i])` — using the **last** key of the previous child ensures
   the separator is strictly greater than every key in that child.
4. If the resulting level has >1 pages, recurses to build the next level up.
5. The final level becomes the new root.

#### `bulk_delete(keys)`

Simple loop: `for k in keys { if self.delete(k)? { n += 1 } }`.

#### `bulk_delete_sorted(sorted)`

**Algorithm** (single-pass leaf-chain sweep):

1. **Pre-encoding:** serialises all target keys into `Vec<Vec<u8>>`.  This is
   an upfront allocation proportional to the number of keys to delete.

2. **Leaf-chain walk:** walks `first_leaf()` → `next_leaf` chain.  For each
   leaf:
   - Advances the target index past keys smaller than the current entry key.
   - On exact match: reads the entry's extent pointer (if any), frees the
     extent, calls `leaf_remove_entry`, calls `dec_entries`, increments
     `removed` counter.  Does **not** increment the entry index (slots shifted
     left).
   - On mismatch (entry key < target): advances to next entry.
   - On mismatch (target key < entry key): advances target index.

3. **Cleanup:** if any entries were removed, calls
   `rebuild_internal_from_leaves()` to rebuild internal levels and free
   emptied leaves.

**Complexity:** O(n) single pass over leaves, plus O(n) rebuild of internal
levels.  Much better than N × O(log n) for large deletion sets.

#### `rebuild_internal_from_leaves()`

After bulk deletion empties some leaves:

1. Walks the leaf chain.  Emptied leaves are unlinked from the chain and
   freed via `free_page`.  Non-empty leaves have their first/last keys
   recorded.
2. If no leaves survive: allocates a fresh empty root leaf (matching
   `do_clear` state).
3. Frees all old internal pages via DFS, filtering children so leaves are
   never pushed onto the stack (avoids wasteful pop-and-discard).
4. If one leaf survives: promotes it to root.
5. If multiple leaves survive: calls `build_internal_levels` to construct
   fresh internal pages.

### 2.6 Iteration

All typed iterators yield `(Cow<'a, K>, Cow<'a, V>)` — always `Owned` because
bincode decode materialises new heap values.

#### `entries()`

Creates a `BTreeIter` starting at `first_leaf()`, slot 0.

#### `BTreeIter::next()`

```
while page != 0:
    if slot < count:
        k = deserialize_from(key_bytes_at(page, slot))
        v = deserialize_from(value_bytes_at(page, slot))
        slot += 1
        return Some((Cow::Owned(k), Cow::Owned(v)))
    // Advance to next leaf.
    page = next_leaf(page)
    slot = 0
    count = page.count
return None
```

**Cached `count`:** the entry count for the current page is read once on page
transition, avoiding a per-`next()` header read.

#### `entries_rev()`

Creates a `BTreeIterRev` starting at `rightmost_leaf()`, walking backward via
`prev_leaf`.  Yields entries in descending key order.

#### `BTreeIterRev::next()`

Same structure as `BTreeIter` but:
- Yields `count - 1 - slot` (from the last entry backward).
- Advances to `prev_leaf` on page exhaustion.

#### `keys()` / `values()`

Both are **proxy iterators**: `keys()` calls `self.entries().map(|(k, _)|
k)`, and `values()` calls `self.entries().map(|(_, v)| v)`.  This means both
deserialise the **full (K, V) pair** and then discard one half.

#### `RawBTreeEntries`

An internal zero-copy iterator yielding `(&'a [u8], &'a [u8])` — raw key and
value byte slices borrowed directly from the mmap.  Used by `do_compact` to
stream entries into the shadow tree without decoding.  **Not exposed through
the `Backend` trait.**

### 2.7 Ordered Operations

#### `range(start, end)`

1. Serialises `start` and `end` into `Vec<u8>`.
2. Finds the leaf containing `start` via `find_leaf`.
3. Finds the insertion slot via `leaf_find_slot` (if `start` matches an
   existing key, starts at that slot; otherwise at the insertion point).
4. Returns a `BTreeRange` iterator.

#### `BTreeRange::next()`

Like `BTreeIter` but with a **bound check**: before yielding, compares the
raw key bytes against the serialised `end` bytes.  If `key_bytes >=
end_bytes`, returns `None`.

#### `range_rev(start, end)`

1. Serialises `start` and `end`.
2. Finds the leaf containing `end` via `find_leaf`.
3. Finds the insertion slot `i` for `end`.  Starts from slot `i-1` of the
   leaf (or the last slot of `prev_leaf` if `i == 0`).
4. Returns a `BTreeRangeRev` iterator.

#### `BTreeRangeRev::next()`

Walks backward through slots, comparing against serialised `start` bytes:
returns `None` when `key_bytes < start_bytes`.

#### `first()`

1. Checks `size() == 0` → `None`.
2. Calls `leftmost_nonempty_leaf()` — walks right from `first_leaf()` skipping
   leaves whose `count == 0` (emptied by per-entry `delete` but not yet
   unlinked).
3. Deserialises K and V from slot 0.

#### `last()`

Mirrors `first()` but calls `rightmost_nonempty_leaf()` and reads the last
slot.

### 2.8 Maintenance

#### `compact()`

Calls `do_compact()`.

#### `do_compact()`

**Algorithm** (shadow rebuild):

1. Creates a temporary file at `compact_temp_path()` — a path in
   `env::temp_dir()` named `zendb-btree-compact-{pid}-{nanos}.tmp`.
2. Creates a fresh `BPlusTree` in the temp file with `initial_capacity_pages=2`
   and `compaction_ratio=1.0` (disable auto-compaction during rebuild).
3. Streams all live entries via `RawBTreeEntries` into the shadow tree via
   `bottom_up_build_raw`.  **Zero deserialisation** — raw byte slices are
   memcpy'd.
4. Copies the compacted pages from the shadow mmap into `self.mmap` via
   `copy_from_slice`.
5. Updates `self.stats` from the shadow tree.
6. Truncates the backing file to the compacted size (clamped to
   `initial_capacity_pages`).  On Windows, swaps in an anonymous mmap first
   to release the file mapping before `set_len`.
7. Removes the temp file.

#### `clear()`

Calls `do_clear()`.

#### `do_clear()`

Resets to post-`create` state without file truncation:

1. Resets meta page counters: `root=1`, `pages=2`, `entries=0`,
   `leaf_pages=1`, all others to 0.
2. Resets `self.stats` to match.
3. Re-initialises page 1 as an empty root leaf.

**The file retains its allocated size.**  Pages beyond page 1 are
inaccessible (no path from root reaches them) but not freed.

#### `maybe_compact()`

Checks `self.fragmentation_ratio()` against `config.compaction_ratio`.  If
the ratio is ≥ threshold, calls `do_compact()`.  If `threshold >= 1.0`,
compaction is disabled.

#### `fragmentation_ratio()`

```
allocated_pages = pages - 1  // exclude meta page
packed_leaf_pages = ceil(leaf_entry_bytes / (PAGE_SIZE - LEAF_HEADER_SIZE))
reclaimable = free_pages + max(0, leaf_pages - packed_leaf_pages)
ratio = reclaimable / allocated_pages
```

#### `flush()` / `sync()`

- `flush()`: `mmap.flush_async()` — schedules OS writeback, returns
  immediately.
- `sync()`: `mmap.flush()` — blocks until writeback completes.

---

## 3. KeyDir

File: `zendb-storage/src/core/keydir.rs`

A persistent key-value store using the **Bitcask** model: an in-memory
`HashMap<K, EntryMeta>` indexes into an append-only memory-mapped data file.
Every write appends; nothing is mutated in place.

### 3.1 On-Disk Format

#### File Layout

| Offset | Size | Description |
|--------|------|-------------|
| 0 | 4 | `MAGIC` (`u32` LE, `0x4452494B` = "KIRD") |
| 4 | … | Records |
| … | 4 | `SENTINEL` (`u32` LE, `0xFFFF_FFFF`) marks the live tail |

#### Live Record

```
[value_size: u32 LE][V bytes][key_size: u32 LE][K bytes]
```

#### Tombstone

```
[TOMBSTONE: u32 LE = 0xFFFF_FFFF][key_size: u32 LE][K bytes]
```

A tombstone marks a key as deleted.  During `rebuild_index`, it removes the
key from the index.  During compaction, tombstones are stripped.

#### `EntryMeta` (in-memory)

```rust
struct EntryMeta {
    offset: u64,       // byte offset in the mmap where the record starts
    record_size: u32,  // total on-disk bytes (8 + v_size + k_size)
}
```

Only `offset` and `record_size` are kept in memory.  Individual `value_size`
and `key_size` are read from the mmap on demand (from the same cache line as
the value bytes — essentially free).

### 3.2 Construction

#### `KeyDir::create(path, config)`

**Algorithm:**
1. Opens the file with `create(true)`, `truncate(true)`, `read(true)`,
   `write(true)`.
2. Sets file length to `config.initial_capacity` bytes.
3. Maps the file.
4. Writes `MAGIC` at offset 0, `SENTINEL` at offset `HEADER_SIZE` (4).
5. Returns `KeyDir { index: HashMap::new(), mmap, file, config, stats }`.
6. `stats.data_size` is initialised to `HEADER_SIZE` (4) — the first write
   will append immediately after the sentinel.

#### `KeyDir::open(path, config)`

**Algorithm:**
1. Opens the existing file with `read(true)`, `write(true)`.
2. Maps the file.
3. Validates the 4-byte magic at offset 0.
4. Constructs a `KeyDir` with an empty `HashMap` and default stats.
5. Calls `rebuild_index()` to replay the append log and populate the index.

#### `rebuild_index()`

Replays the entire append log to reconstruct the in-memory index:

```
cursor = HEADER_SIZE (4)
dead_bytes = 0

while value_size = read_u32_le(mmap, cursor):
    if value_size == SENTINEL:       // reached logical end
        break
    if value_size == TOMBSTONE:      // deleted key
        key_bytes = read from mmap
        key = deserialize_from(key_bytes)
        if key in index:
            dead_bytes += old.record_size
            remove from index
        dead_bytes += 8 + key_size   // tombstone itself is dead
    else:                            // live record
        key_bytes = read from mmap
        key = deserialize_from(key_bytes)
        meta = EntryMeta { offset: cursor, record_size: 8 + value_size + key_size }
        if key in index:
            dead_bytes += old.record_size  // overwritten record is dead
            update index entry
        else:
            insert into index

    cursor = end of this record

data_size = cursor
```

**Complexity:** O(n) in file size.  Every record is read and every key is
**deserialised** (necessary to populate the typed `HashMap<K, EntryMeta>`).
Tombstones and overwritten entries accumulate in `dead_bytes`.

**Self-healing:** stops at the first `SENTINEL`.  Any garbage past that point
(from a crash mid-write) is invisible.

### 3.3 Read Path

#### `get(key)`

**Algorithm:**
1. `self.index.get(key)` — O(1) HashMap probe.
2. If found: `value_bytes_in(mmap, meta)` reads the raw value slice → calls
   `deserialize_from::<V>(slice)`.
3. Returns `Some(Cow::Owned(v))`.

**Value is always deserialised.**  There is no zero-copy borrow path for
typed values because bincode decode materialises.

#### `contains(key)`

**Algorithm:** `self.index.contains_key(key)` — pure HashMap probe.  **No
mmap access, no deserialisation.**  Faster than `get(key).is_some()`.

### 3.4 Write Path

Every write **appends** a new record at `stats.data_size`.  The old record
(if overwriting) becomes dead bytes.  The `Entry::Occupied` path increments
`dead_bytes` by the old record size; the `Entry::Vacant` path does not.

#### `put(key, value)`

**Algorithm:**
1. `write_entry_into(mmap, file, stats, &key, &value)` — appends
   `[vlen][V][klen][K]` at the current tail, writes trailing `SENTINEL`.
2. `self.index.entry(key)`:
   - `Occupied`: increments `dead_bytes` by old `record_size`, updates meta.
   - `Vacant`: inserts new meta.
3. Calls `maybe_compact()`.

**One hash per `put`** — the `Entry` API combines lookup and insertion.

#### `write_entry_into(mmap, file, stats, key, value)`

**Algorithm** (field-level free function for disjoint borrows):

1. `with_two_scratches(key, value, |kb, vb| ...)` — encodes both into pooled
   buffers.  The encoded lengths (`kb.len()`, `vb.len()`) give the on-disk
   sizes.
2. Computes `total = 8 + v_size + k_size`.
3. If `stats.data_size + total + 4 > mmap.len()`, calls
   `grow_into(mmap, file, new_capacity)` which sets the file length to
   `max(current * 2, needed)` and remaps.
4. Writes `[v_size: u32 LE][vb][k_size: u32 LE][kb][SENTINEL: u32 LE]`
   using `copy_from_slice`.
5. Returns `EntryMeta { offset, record_size: total }`.

#### `delete(key)`

**Algorithm:**
1. `self.index.remove(key)` — if not found, returns `Ok(false)`.
2. Increments `dead_bytes` by old `record_size`.
3. `write_tombstone_into(mmap, file, stats, &old)` — appends a tombstone
   that reuses the **already-encoded key bytes** from the dead entry's mmap
   slot via `copy_within`.  **No bincode encode, no scratch `Vec<u8>`.**
4. Calls `maybe_compact()`.

#### `write_tombstone_into(mmap, file, stats, old)`

1. Reads `value_size` from the old record's header (at `old.offset`).
2. Computes `key_size = old.record_size - 8 - value_size`.
3. Computes `key_src_start = old.offset + 8 + value_size` — where the
   already-encoded key bytes live.
4. Grows the file if needed.
5. Writes `[TOMBSTONE: u32][key_size: u32]` at the new tail.
6. `mmap.copy_within(key_src_start .. key_src_start + key_size, new_offset + 8)`.
7. Writes trailing `SENTINEL`.
8. Increments `dead_bytes` by the tombstone size.

#### `put_if_absent(key, value)`

**Algorithm:**
1. `self.index.entry(key)` — one hash.
   - `Occupied` → `false` (no-op, no mmap access).
   - `Vacant` → `write_entry_into` + insert meta → `true`.
2. If inserted, calls `maybe_compact()`.

#### `replace(key, value)`

**Algorithm:**
1. `self.index.entry(key)` — one hash.
   - `Occupied`: reads and deserialises the old value from the mmap
     **before** overwriting, increments `dead_bytes` by old size, writes new
     record, updates meta.
   - `Vacant`: writes new record, inserts meta, returns `None`.
2. Calls `maybe_compact()`.

#### `update(key, f)`

**Algorithm** (single hash, single append):

1. `self.index.raw_entry_mut().from_key(key)` — **one hash** via hashbrown's
   raw entry API.
   - `RawEntryMut::Occupied`:
     - Deserialises current value.
     - Calls `f(Some(current))`:
       - `Some(new_v)` → `write_entry_into` + update meta + `dead_bytes += old_size`.
       - `None` → `write_tombstone_into` + `dead_bytes += old_size` + remove entry.
   - `RawEntryMut::Vacant`:
     - Calls `f(None)`:
       - `Some(new_v)` → `write_entry_into` + `insert(key.clone(), meta)`.
       - `None` → no-op.
2. If mutated, calls `maybe_compact()`.

The `RawEntryMut` is held open across the mmap write and entry mutation
because the field-level free functions (`write_entry_into`,
`write_tombstone_into`) take `&mut mmap`, `&mut file`, `&mut stats` — these
are **disjoint** from the borrow on `self.index` that `RawEntryMut` holds.

### 3.5 Bulk Operations

KeyDir does **not** override `bulk_put`, `bulk_put_sorted`, `bulk_delete`, or
`bulk_delete_sorted`.  All use the trait defaults which loop the
single-entry methods.  Each individual call to `put`/`delete` calls
`maybe_compact()`, so a large bulk operation may compact multiple times.

### 3.6 Iteration

#### `keys()`

**Algorithm:** `self.index.keys().map(Cow::Borrowed)`.

Keys are borrowed directly from the HashMap.  Zero-cost — no mmap access, no
deserialisation.

#### `values()`

**Algorithm:** `self.index.values().map(|meta| deserialize_from(value_bytes_in(mmap, meta)))`.

Every value is deserialised from the mmap.  Always `Cow::Owned`.

#### `entries()`

**Algorithm:** `self.index.iter().map(|(k, meta)| (Cow::Borrowed(k), deserialize_from(...)))`.

Keys borrowed, values deserialised.

### 3.7 Maintenance

#### `compact()`

**Algorithm** (in-place forward sweep):

1. Collects `&mut EntryMeta` references from the HashMap into a vector.
2. Sorts by `meta.offset` ascending — this gives entries in on-disk order.
3. Iterates with a write cursor starting at `HEADER_SIZE`:
   - For each live entry: if `src != cursor`, calls
     `mmap.copy_within(src..src+len, cursor)`.  Since `cursor ≤ src` at all
     times (forward sweep), `copy_within` (memmove semantics) never
     destructively overlaps.
   - Updates `meta.offset = cursor`.
   - Advances `cursor += len`.
4. Writes `SENTINEL` at the new tail.
5. Resets `data_size = cursor`, `dead_bytes = 0`.
6. Truncates the file to `max(cursor + 4, config.initial_capacity)`.
   - On Windows: swaps in anonymous mmap, calls `file.set_len`, remaps.

**No temp file, no `fs::rename`.**  One pass over live entries.

#### `clear()`

**Algorithm:**
1. `self.index.clear()`.
2. `stats.data_size = HEADER_SIZE`, `stats.dead_bytes = 0`.
3. Rewrites `MAGIC` at offset 0 and `SENTINEL` at offset 4 in the mmap.

The file is **not truncated**.  Subsequent writes reuse the existing file
capacity.

#### `maybe_compact()`

```
threshold = config.compaction_ratio
if threshold >= 1.0 → return (disabled)
if threshold == 0.0 or dead_bytes / data_size >= threshold → compact()
```

Special-cases `0.0` to mean "compact after every write."

#### `flush()` / `sync()`

Same as BPlusTree: `mmap.flush_async()` / `mmap.flush()`.

---

## 4. SkipList

File: `zendb-storage/src/core/skiplist.rs`

An entirely in-memory probabilistic skip list.  No file backing, no
serialisation for storage (keys and values live in native Rust types in a
`Vec<Node<K, V>>` arena).  Implements both `Backend` and `OrderedBackend`.

### 4.1 Data Structure

```
const MAX_LEVEL: usize = 16

struct Node<K, V> {
    key: K,
    value: V,
    next: [Option<usize>; MAX_LEVEL],  // forward pointers per level
    prev: Option<usize>,                // level-0 back pointer
    level: usize,                       // actual height of this node (1..MAX_LEVEL)
}

struct SkipList<K: Ord, V> {
    arena: Vec<Node<K, V>>,    // all nodes (indexed by usize)
    free: Vec<usize>,          // freed indices available for reuse
    heads: [Option<usize>; MAX_LEVEL],  // entry points per level
    height: usize,             // current max level (0..MAX_LEVEL)
    len: usize,                // live entry count
    config: SkipListConfig,
    stats: SkipListStats,
}
```

Nodes are stored in a flat `Vec`.  "Pointers" are `usize` indices into
`arena`.  The `free` list reuses slots from deleted nodes (LIFO).

**Bounded capacity:** when `SkipListCapacity::Bounded { max_entries }`, the
arena preallocates capacity and `alloc` returns `StorageFull` if all slots
are occupied and no freed slot is available.

### 4.2 Construction

#### `SkipList::new(config)`

Creates an empty skip list:

```
arena = Vec::with_capacity(initial_capacity)  // 0 for Unbounded, max_entries for Bounded
free = Vec::with_capacity(initial_capacity)
heads = [None; MAX_LEVEL]
height = 0
len = 0
```

No file I/O.  No `create`/`open` on `FileBackedBackend`.

### 4.3 Read Path

#### `get(key)`

**Algorithm:**
1. `search(key)` returns `(update, found)`.
2. If `found` is `Some(index)`: returns `Cow::Borrowed(&self.arena[index].value)`.
3. If `None`: returns `None`.

**Zero-copy borrow** — the value lives in the arena Vec.

#### `contains(key)`

**Algorithm:** `self.search(key).1.is_some()` — same search, discards the
update array.

#### `search(key)`

**Algorithm** (standard skip list search with update tracking):

```
update = [None; MAX_LEVEL]
previous = None
found = None

for level in (height-1)..=0 (descending):
    current = previous.next[level] or heads[level]
    while current exists:
        match current.key.cmp(key):
            Less    → previous = current, current = current.next[level]
            Equal   → found = current, break
            Greater → break
    update[level] = previous

return (update, found)
```

Returns:
- `update`: for each level, the node *before* the insertion/deletion point.
  Used by `insert_new` and `remove_found`.
- `found`: the node index if key exists, else `None`.

**Complexity:** O(log n) expected, O(n) worst case.

**Uses `K::Ord`** (not serialised bytes) — native comparison.

### 4.4 Write Path

#### `put(key, value)`

**Algorithm:**
1. `search(&key)` → `(update, found)`.
2. If `found` exists: `arena[index].value = value` (in-place overwrite).
3. Otherwise: `insert_new(key, value, update)`.

**No key clone on overwrite** — the existing key is reused.

#### `put_if_absent(key, value)`

**Algorithm:**
1. `search(&key)` → `(_, found)`.
2. If `found` exists → `Ok(false)`.
3. Otherwise → `insert_new(key, value, update)`.

#### `replace(key, value)`

**Algorithm:**
1. `search(&key)` → `(update, found)`.
2. If found: `mem::replace(&mut arena[index].value, value)` swaps out the old
   value and returns it as `Cow::Owned(old_value)`.  **No clone of the old
   value.**
3. If not found: `insert_new(key, value, update)` → `Ok(None)`.

#### `delete(key)`

**Algorithm:**
1. `search(key)` → `(update, found)`.
2. If `found` is `None` → `Ok(false)`.
3. Otherwise: `remove_found(update, index)`.

#### `update(key, f)`

**Algorithm:**
1. `search(key)` → `(update, found)`.
2. `current = found.map(|idx| arena[idx].value.clone())` — **clones the
   value** to pass to the closure.
3. Calls `f(current)`:
   - `Some(new_value)`: if key existed, overwrites in place; otherwise
     `insert_new(key.clone(), new_value, update)`.
   - `None`: if key existed, `remove_found(update, index)`.

**The clone on line 2 is the cost of the `FnOnce(Option<V>)` API** — the
closure takes ownership.  This could be avoided with `V: Default` and
`mem::take`, but SkipList does not impose that bound.

#### `insert_new(key, value, update)`

**Algorithm:**
1. Determines the new node's level probabilistically:
   ```
   level = 1
   while level < MAX_LEVEL and fast_rand() & 1 == 0:
       level += 1
   ```
   (Each additional level has 50% probability, capped at `MAX_LEVEL`.)
2. `alloc(key, value, level)` — gets an arena index (from free list or push).
3. Sets `prev = update[0]`.
4. For each level `0..level`:
   - Links `node.next[level] = predecessor.next[level]`.
   - Updates `predecessor.next[level] = Some(index)`.
5. Updates `prev.next`'s back pointer.
6. `len += 1`, `height = max(height, level)`.

#### `remove_found(update, index)`

**Algorithm:**
1. For each level `0..arena[index].level`:
   - If predecessor exists: `predecessor.next[level] = node.next[level]`.
   - Otherwise: `heads[level] = node.next[level]`.
2. Updates the next node's `prev` pointer to skip the removed node.
3. Decrements `height` while `heads[height-1]` is `None`.
4. Clears the node's `next` and `prev`, pushes `index` onto `free`.
5. `len -= 1`.

### 4.5 Bulk Operations

SkipList does **not** override `bulk_put`, `bulk_put_sorted`, `bulk_delete`,
or `bulk_delete_sorted`.  All use the trait defaults (loop `put`/`delete`).

### 4.6 Iteration & Ordered Operations

All iterators yield `(&K, &V)` — borrowed directly from the arena Vec.  The
trait methods wrap these with `Cow::Borrowed`.

#### `Iter` (forward, all entries)

```
current = heads[0]
next(): node = &arena[current?], current = node.next[0], return (&node.key, &node.value)
```

#### `RangeIter` (forward, bounded)

```
current = first_at_or_after(start), end = end.clone()
next(): node = &arena[current?], if node.key >= end → None, current = node.next[0]
```

#### `RevIter` (backward, all entries)

```
current = last_index()
next(): node = &arena[current?], current = node.prev, return (&node.key, &node.value)
```

#### `RevRangeIter` (backward, bounded)

Like `RevIter` with a `start` bound: stops when `node.key < start`.

#### `first()`

Returns `heads[0]` → `(&arena[idx].key, &arena[idx].value)` as `Cow::Borrowed`.

#### `last()`

Calls `last_index()` — walks down from the highest level, always taking the
rightmost path at each level.  Returns the last node.

#### `first_at_or_after(key)`

`search(key)` — if found, returns the found index; otherwise returns
`update[0].next[0]` (the successor of the insertion point).

#### `last_index()`

```
current = None
for level in (height-1)..=0:
    next = current.next[level] or heads[level]
    while next exists:
        current = next
        next = current.next[level]
return current
```

Walks as far right as possible at each level.  O(log n) expected.

### 4.7 Maintenance

#### `clear()`

Resets all state: `arena.clear()`, `free.clear()`, `heads = [None; MAX_LEVEL]`,
`height = 0`, `len = 0`, `stats.entries = 0`.

#### `compact()`

Uses the trait default: `Ok(())` (no-op).  SkipList has no reclaimable waste.

#### `flush()` / `sync()`

Both return `Ok(())` (no-op).  SkipList is in-memory with no persistence.

---

## 5. Topic

File: `zendb-storage/src/concurrent/topic.rs`

A persistent single-writer, multiple-reader append-only log.  Designed for
event streaming: records are immutable once written, readers track their own
cursors, and old segments are compacted when all consumers have passed them.

### 5.1 Architecture

```
TopicWriter (exclusive owner)
├── active: File               ← current append target
├── segments: Vec<Arc<Segment>> ← all segments (active + sealed)
├── manifest: KeyDir<TopicOffset, PersistedSegment>  ← segment index
└── topic: Arc<Topic>

Topic (shared)
├── segments: ArcSwap<Vec<Arc<Segment>>>  ← published for readers
├── next_offset: AtomicU64               ← global tail
├── consumers: Mutex<HashMap<String, Arc<ConsumerState>>>
├── offsets: Mutex<KeyDir<String, TopicOffset>>  ← committed offsets
└── _value: PhantomData<T>

TopicReader (per-consumer)
├── topic: Arc<Topic>
├── consumer: Arc<ConsumerState>   ← committed + volatile cursors
└── current: Option<SegmentCursor> ← open file + position
```

**Key design points:**
- `TopicWriter` owns the active segment file and the segment manifest.
- `Topic` is `Arc`-shared, holding an `ArcSwap` of the segment list for
  lock-free reader access.
- `ArcSwap::load_full()` gives readers a consistent snapshot of segments
  without blocking the writer.
- Consumer state is `Arc<ConsumerState>` so readers can update volatile
  offsets atomically without locking.

### 5.2 Construction

#### `Topic::create(path, config)`

**Algorithm:**
1. `fs::create_dir_all(path)` — ensures the directory exists.
2. Creates a `KeyDir` for consumer offsets at `path/offsets`.
3. Creates a `KeyDir` segment manifest at `path/segments`.
4. Creates the first segment file (`00000000000000000000.log`):
   - Opens with `create_new(true)` — errors if the file exists.
   - Writes 4-byte `MAGIC` header.
5. Inserts the segment into the manifest.
6. Opens the segment for append via `open_segment_writer`.
7. Constructs `Topic` with an empty consumer map and `next_offset = 0`.
8. Returns `(TopicWriter, Arc<Topic>)`.

#### `Topic::open(path, config)`

**Algorithm:**
1. Opens the offsets `KeyDir` and segment manifest `KeyDir`.
2. Reads all persisted segments from the manifest, sorts by base offset.
3. **Scans the active segment** (last segment) via `scan_active_segment`:
   - Reads header, then iterates records checking `cursor + 4 + size ≤
     file_len`.  Stops at the first incomplete record (truncated write).
   - If the file is longer than the last complete record, truncates it with
     `file.set_len`.
   - Updates the manifest with the corrected `end_offset` and `byte_len`.
4. Reconstructs `Segment` structs from persisted metadata.
5. Reconstructs consumer states from the offsets `KeyDir` — each consumer
   gets `committed` and `volatile` cursors set to the persisted offset.
6. Opens the active segment for append.
7. Returns `(TopicWriter, Arc<Topic>)`.

### 5.3 Append Path

#### `TopicWriter::append(value)`

**Algorithm:**
1. `with_scratch(value, |encoded| ...)` — serialises the value once into a
   pooled buffer.
2. Computes `record_size = 4 + encoded.len()` (4-byte size prefix + data).
3. If the active segment already has records AND adding this record would
   exceed `config.max_segment_bytes` → calls `rotate_segment()`.
4. Writes `[size: u32 LE][encoded bytes]` to the active segment file.
5. Updates segment metadata: `byte_len += record_size`,
   `end_offset += 1`.
6. Updates writer stats: `next_offset += 1`, `records += 1`,
   `retained_bytes += record_size`.
7. Publishes `next_offset` to `topic.next_offset` (atomic store).

#### `rotate_segment()`

**Algorithm:**
1. Flushes the active file.
2. Persists the active segment's metadata to the manifest.
3. Creates a new segment file at the current `next_offset` via
   `create_segment`.
4. Inserts the new segment into the manifest.
5. Opens the new segment for append.
6. Pushes the segment onto the writer's segment list.
7. Calls `publish_segments()` to update the `ArcSwap`.
8. Triggers `compact()` to remove segments all consumers have passed.

#### `publish_segments()`

**Algorithm:** `topic.segments.store(Arc::new(self.segments.clone()))`.

Atomically swaps the reader-visible segment list.  Readers calling
`load_full()` on their next `position()` call see the new list.

### 5.4 Read Path

#### `Topic::reader(consumer)`

**Algorithm:**
1. Acquires the consumer map lock.
2. Looks up or creates the consumer state:
   - New consumer: registered at `next_offset`, persisted to offsets KeyDir.
   - Existing consumer: returns existing `Arc<ConsumerState>`.
3. Checks `reader_active` — if already `true`, returns `AlreadyExists` error
   (only one reader per consumer).
4. Sets `reader_active = true` (AcqRel).
5. Returns `TopicReader` with no current cursor (lazy positioning).

#### `TopicReader::position()`

**Algorithm** (called lazily, or on segment transition):

1. Reads `consumer.volatile` offset.
2. If `offset >= topic.next_offset` → no data available, returns `false`.
3. Loads the segment snapshot via `topic.segments.load_full()`.
4. Finds the segment containing `offset` (linear scan of segments by base
   offset range).
5. Opens the segment file.
6. **Scans from `base_offset` to `offset`:** for each record, reads the
   4-byte size prefix, computes `byte_offset += 4 + size`, seeks to the next
   record.  This is O(offset - base_offset) syscall pairs.
7. Seeks to the computed `byte_offset` — this is the start of the next
   record to read.
8. Creates `SegmentCursor { file, segment, logical_offset: offset }`.

#### `TopicReader::next()`

**Algorithm:**
1. Checks if the cursor needs (re)positioning:
   - If `logical_offset >= segment.end_offset` AND `< topic.next_offset` →
     segment transition, calls `position()`.
   - If `logical_offset >= topic.next_offset` → no more data, returns `None`.
2. Reads 4-byte size prefix from the current file position.
3. Acquires a `PooledBuf`, resizes to `value_size`, reads the value bytes.
4. Calls `deserialize_from::<T>(&bytes)` to decode.
5. Increments `logical_offset`.
6. Stores the new offset to `consumer.volatile` (Release ordering).
7. Returns `Some(Ok(decoded_value))`.

#### `SegmentCursor`

Holds the open file handle, the `Arc<Segment>` (keeps the segment alive even
if compaction removes it from the writer's list), and the current logical
offset.

### 5.5 Consumer Management

#### `commit_consumer(consumer)`

1. Acquires consumer map lock, looks up or creates consumer state.
2. Reads `volatile` offset.
3. Persists `(consumer_name, volatile_offset)` to the offsets KeyDir.
4. Stores `volatile` into `committed` (Release ordering).

#### `reset_consumer(consumer)`

Resets `volatile` to `committed`.  Refuses if a reader is active.

#### `remove_consumer(consumer)`

Deletes from offsets KeyDir and consumer map.  Refuses if a reader is active.

### 5.6 Compaction

#### `TopicWriter::compact()`

**Algorithm:**
1. Finds the minimum committed offset across all consumers.
   - If no consumers exist → nothing to compact.
2. Counts removable segments: all segments except the active one whose
   `end_offset ≤ min_committed`.
3. Syncs the offsets KeyDir (so committed offsets survive a crash).
4. Removes compacted segments from the manifest.
5. Calls `publish_segments()` to update the reader-visible list.
6. Sets `delete_on_drop = true` on each removed segment's `Arc`.
   - The segment file is deleted when the last `Arc<Segment>` is dropped
     (i.e., when all readers that opened the segment have finished).
7. Updates writer stats: `earliest_offset`, `records`, `retained_bytes`.

**Segments are not immediately deleted** — they persist until all readers
holding an `Arc` to them have dropped their cursors.  This means a slow
reader does not block compaction for other readers.

### 5.7 Durability

#### `flush()`

1. Flushes the active segment file.
2. Flushes the offsets KeyDir.
3. Flushes the segment manifest.

#### `sync()`

1. Syncs the active segment file (`sync_all`).
2. Syncs the offsets KeyDir.
3. Syncs the segment manifest.

#### `Drop for TopicWriter`

Flushes the active file, offsets KeyDir, and manifest on drop.  Best-effort;
errors are silently discarded (`let _ = ...`).

---

## 6. State

File: `zendb-storage/src/core/state.rs`

A runtime dispatch enum that wraps one of the three backends and delegates
every trait method to the active variant.

### 6.1 Runtime Dispatch

```rust
pub enum State<K: Ord, V> {
    Ordered   { backend: BPlusTree<K, V>, config: StateConfig },
    Unordered { backend: KeyDir<K, V>,     config: StateConfig },
    InMemory  { backend: SkipList<K, V>,   config: StateConfig },
}

pub enum StateConfig {
    Ordered(BPlusTreeConfig),
    Unordered(KeyDirConfig),
    InMemory(SkipListConfig),
}

pub enum StateStats {
    Ordered(BPlusTreeStats),
    Unordered(KeyDirStats),
    InMemory(SkipListStats),
}
```

### 6.2 Method Dispatch Table

| Method | Ordered (BPlusTree) | Unordered (KeyDir) | InMemory (SkipList) |
|--------|---------------------|--------------------|--------------------|
| `create` | `BPlusTree::create` | `KeyDir::create` | `SkipList::new` |
| `open` | `BPlusTree::open` | `KeyDir::open` | `SkipList::new` |
| `get` | delegated | delegated | delegated |
| `contains` | delegated | delegated | delegated |
| `put` | delegated | delegated | delegated |
| `put_if_absent` | delegated | delegated | delegated |
| `replace` | delegated | delegated | delegated |
| `delete` | delegated | delegated | delegated |
| `update` | delegated | delegated | delegated |
| `clear` | delegated | delegated | delegated |
| `compact` | delegated | delegated | delegated |
| `keys` | Box'd | Box'd | Box'd |
| `values` | Box'd | Box'd | Box'd |
| `entries` | Box'd | Box'd | Box'd |
| `size` | delegated | delegated | delegated |
| `is_empty` | delegated | delegated | delegated |
| `stats` | Cloned into `StateStats::Ordered` | Cloned into `StateStats::Unordered` | Cloned into `StateStats::InMemory` |
| `config` | Returns `&StateConfig` | `&StateConfig` | `&StateConfig` |
| `flush` | delegated | delegated | delegated |
| `sync` | delegated | delegated | delegated |
| `range` | Box'd | **panics** | Box'd |
| `first` | delegated | **panics** | delegated |
| `last` | delegated | **panics** | delegated |
| `entries_rev` | Box'd | **panics** | Box'd |
| `range_rev` | Box'd | **panics** | Box'd |

**Box'd iterators:** `keys()`, `values()`, `entries()`, `range()`,
`entries_rev()`, and `range_rev()` all box the inner iterator via `Box::new()
as Box<dyn Iterator>`.  This adds one heap allocation and vtable dispatch per
iterator creation.  The underlying iterator's `next()` call goes through a
dynamic dispatch.

**Ordered operations on Unordered:** `range`, `first`, `last`,
`entries_rev`, and `range_rev` panic with `"ordered operation requires an
ordered state backend"`.  There is no compile-time prevention — the panic is
runtime-only.

**`stats()` cloning:** Unlike the three backends which return `&Stats`,
`State::stats()` returns `StateStats` (an owned enum).  It clones the inner
stats via `.clone()`, allocating a new struct on every call.  This is forced
by the `type Stats<'a> = StateStats where Self: 'a` associated type — the
lifetime can't name the inner backend's borrow because the match arm
temporaries don't live long enough.

---

## 7. Trait Definitions

File: `zendb-storage/src/core/backend.rs`

### 7.1 `Backend<K, V>`

The universal contract.

```rust
pub trait Backend<K, V>
where
    K: Encode + Decode<()> + Hash + Eq + Clone + Ord,
    V: Encode + Decode<()> + Clone,
{
    type Stats<'a> where Self: 'a;
    type Config: Clone + Default + Encode + Decode<()>;

    // Reads
    fn get(&self, key: &K) -> Option<Cow<'_, V>>;
    fn contains(&self, key: &K) -> bool { self.get(key).is_some() }

    // Writes
    fn put(&mut self, key: K, value: V) -> io::Result<()>;
    fn put_if_absent(&mut self, key: K, value: V) -> io::Result<bool> { /* default: contains + put */ }
    fn replace(&mut self, key: K, value: V) -> io::Result<Option<Cow<'_, V>>> { /* default: get + put */ }
    fn delete(&mut self, key: &K) -> io::Result<bool>;
    fn update<F>(&mut self, key: &K, f: F) -> io::Result<()>
        where F: FnOnce(Option<V>) -> Option<V> { /* default: get + f + put/delete */ }

    // Bulk
    fn bulk_put<I>(&mut self, items: I) -> io::Result<()> { /* default: loop put */ }
    fn bulk_put_sorted<I>(&mut self, sorted: I) -> io::Result<()> { /* default: loop put */ }
    fn bulk_delete<'a, I>(&mut self, keys: I) -> io::Result<usize> { /* default: loop delete */ }
    fn bulk_delete_sorted<'a, I>(&mut self, sorted: I) -> io::Result<usize> { /* default: loop delete */ }

    // Maintenance
    fn clear(&mut self) -> io::Result<()>;
    fn compact(&mut self) -> io::Result<()> { Ok(()) }

    // Iteration (RPITIT — not object-safe)
    fn keys<'a>(&'a self) -> impl Iterator<Item = Cow<'a, K>> + 'a;
    fn values<'a>(&'a self) -> impl Iterator<Item = Cow<'a, V>> + 'a;
    fn entries<'a>(&'a self) -> impl Iterator<Item = (Cow<'a, K>, Cow<'a, V>)> + 'a;

    // Bookkeeping
    fn size(&self) -> usize;
    fn is_empty(&self) -> bool { self.size() == 0 }
    fn stats(&self) -> Self::Stats<'_>;
    fn config(&self) -> &Self::Config;

    // Durability
    fn flush(&self) -> io::Result<()>;
    fn sync(&self) -> io::Result<()>;
}
```

**Default implementations** that backends commonly override:

| Method | Default | Overridden by |
|--------|---------|---------------|
| `contains` | `get(key).is_some()` | BPlusTree, KeyDir, SkipList |
| `put_if_absent` | `contains` + `put` (two lookups) | BPlusTree, KeyDir, SkipList |
| `replace` | `get` + `put` (two lookups) | BPlusTree, KeyDir, SkipList |
| `update` | `get` + `f` + `put`/`delete` | BPlusTree, KeyDir, SkipList |
| `bulk_put_sorted` | `bulk_put` (loop `put`) | BPlusTree |
| `bulk_delete_sorted` | `bulk_delete` (loop `delete`) | BPlusTree |
| `compact` | `Ok(())` | BPlusTree, KeyDir |
| `is_empty` | `size() == 0` | KeyDir |

### 7.2 `FileBackedBackend<K, V>`

```rust
pub trait FileBackedBackend<K, V>: Backend<K, V> {
    fn create(path: &Path, config: Self::Config) -> io::Result<Self>;
    fn open(path: &Path, config: Self::Config) -> io::Result<Self>;
}
```

Implemented by: `BPlusTree`, `KeyDir`, `State` (delegates).

Not implemented by: `SkipList` (in-memory).

### 7.3 `OrderedBackend<K, V>`

```rust
pub trait OrderedBackend<K, V>: Backend<K, V> {
    fn range<'a>(&'a self, start: &K, end: &K)
        -> impl Iterator<Item = (Cow<'a, K>, Cow<'a, V>)> + 'a;
    fn first<'a>(&'a self) -> Option<(Cow<'a, K>, Cow<'a, V>)> { self.entries().next() }
    fn last<'a>(&'a self) -> Option<(Cow<'a, K>, Cow<'a, V>)> { self.entries().last() }
    fn entries_rev<'a>(&'a self)
        -> impl Iterator<Item = (Cow<'a, K>, Cow<'a, V>)> + 'a
    { /* default: collect → reverse → iterate */ }
    fn range_rev<'a>(&'a self, start: &K, end: &K)
        -> impl Iterator<Item = (Cow<'a, K>, Cow<'a, V>)> + 'a
    { /* default: collect range → reverse → iterate */ }
}
```

Implemented by: `BPlusTree`, `SkipList`, `State` (delegates, panics on
Unordered).

Not implemented by: `KeyDir` (unordered).

**Default `first`/`last`:** calls `.entries().next()` / `.entries().last()` —
O(n) for `last()` if the iterator must be exhausted.  Both BPlusTree and
SkipList override with O(1) or O(log n) implementations.

**Default `entries_rev`/`range_rev`:** materialises the full forward iteration
into a `Vec`, reverses it, and iterates.  O(n) memory.  Both BPlusTree and
SkipList override with true streaming reverse iteration via `prev_leaf` /
`prev` pointers.

---

*End of document.*
