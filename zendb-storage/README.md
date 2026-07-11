# zendb-storage

**Reusable persistent and in-memory key-value storage backends for ZeninDB.**

This crate provides general-purpose ordered and unordered data structures
backed by memory-mapped files or in-memory state. It is independent of
ZeninDB's CRDT types — backends operate on generic `K: Encode, V: Encode`.
It also remains independent of identity, authorization, operator placement,
leases, and networking; those concerns belong to higher crates.

---

## Backends

Three KV backends, each implementing the common `Backend<K, V>` trait:

| Backend | Persistent | Ordered | Best for |
|---|---|---|---|
| `BPlusTree` | ✅ (mmap) | ✅ | Ordered state, range scans, large datasets |
| `KeyDir` | ✅ (mmap) | ❌ | Unordered lookups, catalogs, metadata |
| `SkipList` | ❌ (in-memory) | ✅ | Caches, working sets, buffers |

Plus one append-only log:

| Type | Persistent | Best for |
|---|---|---|
| `Topic<T>` | ✅ (segmented files) | Event streaming, change logs, consumer offsets |

---

## Trait Hierarchy

```text
Storage                         — stats + config introspection
├── DurableStorage              — create/open + flush/sync/compact
├── Backend<K, V>               — CRUD, bulk ops, iteration
│   └── OrderedBackend<K, V>    — range, first, last, reverse iteration
```

- **`Backend<K, V>`** — the universal contract. `get`, `put`, `delete`,
  `update`, `bulk_put`, `bulk_delete`, `keys`, `values`, `entries`, `clear`,
  `flush`, `sync`.
- **`DurableStorage`** — `create(path, config)` + `open(path, config)`.
  Implemented by `BPlusTree`, `KeyDir`, `Topic`. Not by `SkipList`.
- **`OrderedBackend<K, V>`** — `range`, `first`, `last`, `entries_rev`,
  `range_rev`. Implemented by `BPlusTree`, `SkipList`. Not by `KeyDir`.

Keys and values are serialized with **bincode 2** (little-endian, fixed-int,
no decode limit). Key ordering in `BPlusTree` is by **lexicographic comparison
of serialized key bytes**, not `K::Ord`.

---

## `BPlusTree<K, V>`

Persistent ordered B+ tree in a single memory-mapped file.

- 4096-byte pages (meta, leaf, internal)
- In-place mutation — no WAL, no dirty page cache
- Overflow extents for large values (> ~4 KB)
- Page freelist for reuse after deletes
- Suffix-truncated separator keys
- Bottom-up sorted bulk load on empty trees
- Forward and reverse range iteration via leaf sibling links
- Compaction by shadow rebuild to a temp file

```rust
let mut tree = BPlusTree::<String, Vec<u8>>::create(path, BPlusTreeConfig::default())?;
tree.put("key".into(), b"value".to_vec())?;
assert_eq!(tree.get(&"key".into())?.unwrap().as_ref(), b"value");
```

**Config:** `BPlusTreeConfig { initial_capacity_pages: u64, compaction_ratio: f64 }`
— defaults to 4096 pages (16 MiB) with compaction at 50% reclaimable waste.

---

## `KeyDir<K, V>`

Bitcask-style unordered persistent KV store.

- In-memory `HashMap<K, EntryMeta>` index
- Single append-only mmap'd data file
- Overwrites append new records; old records become dead bytes
- Deletes append tombstones
- Compaction slides live records forward in place
- Replay on open rebuilds the hash index

```rust
let mut dir = KeyDir::<String, String>::create(path, KeyDirConfig::default())?;
dir.put("key".into(), "value".into())?;
assert_eq!(dir.get(&"key".into())?.unwrap().as_ref(), "value");
```

**Config:** `KeyDirConfig { initial_capacity: u64, compaction_ratio: f64 }`
— defaults to 16 MiB with compaction at 50% dead bytes.

---

## `SkipList<K, V>`

Entirely in-memory ordered skip list.

- Arena-backed (all nodes in a single `Vec`)
- Probabilistic level assignment (max 16 levels)
- Freelist for deleted node reuse
- Optional bounded capacity
- Zero-copy value borrows on read

```rust
let mut list = SkipList::<i64, String>::new(SkipListConfig::default());
list.put(1, "one".into())?;
assert_eq!(list.first()?.unwrap().1.as_ref(), "one");
```

**Config:** `SkipListConfig { capacity: SkipListCapacity }` — `Unbounded` by default.

---

## `Topic<T>`

Persistent single-writer, multiple-reader segmented append-only log.

- Data split into `.log` segment files (default 64 MiB each)
- `TopicWriter` appends records; `TopicConsumer` reads them
- Per-consumer committed + volatile offsets
- Compaction discards segments all consumers have passed
- Readers hold `Arc<Segment>` refs — slow readers don't block compaction

```rust
let (mut writer, topic) = Topic::<String>::create(path, TopicConfig::default())?;
writer.append(&"event".into())?;

let consumer = topic.consumer("my-consumer")?;
for record in consumer {
    println!("{:?}", record?);
}
```

---

## Frontend Types

### `State<K, V>`

Runtime dispatch enum wrapping one of the three backends:

```rust
pub enum State<K: Ord, V> {
    Ordered   { backend: BPlusTree<K, V>, config: StateConfig },
    Unordered { backend: KeyDir<K, V>,     config: StateConfig },
    InMemory  { backend: SkipList<K, V>,   config: StateConfig },
}
```

All trait methods delegate to the active variant. Ordered operations
(`range`, `first`, `last`, etc.) **panic** on `Unordered`.

**Config:** `StateConfig::{ Ordered(BPlusTreeConfig), Unordered(KeyDirConfig), InMemory(SkipListConfig) }`.

### `Table`

ZeninDB-aware table facade composing materialized `State`, an in-memory
skip-list cache, and a `Topic<Change>` for durable change streaming.

- Reads merge cache over state (cache shadows state)
- `insert_event` applies an event, caches the resolved cell, publishes a `Change`
- Auto-drains cache to state at `max_buffered_records`
- Cache rebuilt from topic on open

```rust
let mut table = Table::create(path, TableConfig::default())?;
table.insert_event(event)?;
```

---

## Serialization Utilities

Located in `src/utils/serdes.rs`. Zero-allocation hot-path helpers:

- `serialized_size(value)` — measure without allocating
- `serialize_into(value, &mut [u8])` — write directly into an mmap slice
- `with_scratch(value, |bytes| ...)` — encode into a pooled buffer, use it, return buffer
- `deserialize_from(&[u8])` — decode from bytes

Thread-local `PooledBuf` (in `reusables.rs`) keeps warm `Vec<u8>` buffers
per thread so the hot encoding path never allocates after warm-up.

---

## Module Structure

```
src/
├── lib.rs             # Crate root
├── core/
│   ├── mod.rs         # Re-exports
│   ├── traits.rs      # Storage, DurableStorage, Backend, OrderedBackend
│   ├── btree.rs       # BPlusTree
│   ├── keydir.rs      # KeyDir
│   ├── skiplist.rs    # SkipList
│   └── topic.rs       # Topic, TopicConsumer
├── frontend/
│   ├── mod.rs         # Re-exports
│   ├── state.rs       # State enum
│   └── table.rs       # Table, TableConfig, TableStats, Change
└── utils/
    ├── serdes.rs      # bincode helpers, byte I/O
    └── reusables.rs   # PooledBuf thread-local buffer pool
```
