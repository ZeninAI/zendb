# zendb-storage

`zendb-storage` owns persistence mechanics. Its algorithms are independent of
Catalog, peer identity, roles, and network policy.

## Storage Types

- `BPlusTree<K, V>`: ordered durable storage.
- `KeyDir<K, V>`: unordered durable storage.
- `SkipList<K, V>`: ordered in-memory storage.
- `State<K, V>`: runtime selection across those backends.
- `Topic<T>`: segmented append-only log with named durable consumers.

`ReadBackend`, `WriteBackend`, `OrderedReadBackend`, `Storage`, and
`DurableStorage` define the contracts. Binary helpers live in `zendb-types`, so
backend pages, Topic records, Blob metadata, and workspace metadata use one
bincode configuration.

## Table

`Table` is the invariant-preserving storage facade:

```text
Table
  State<PrimaryKey, Cell>
  SkipList<PrimaryKey, Cell> write cache
  Topic<Change>
  recovery consumer
```

`TableConfig` is accepted by `DurableStorage::create` and `open`; Table stores
no name, identifier, or duplicate full configuration value. The workspace
Catalog owns persisted table configuration.

`Table::insert(Event)` is its only mutation path. It applies the operation,
returns `Ignored` when state does not change, or appends
`Change { event, previous, current }` before updating the cache.

Table implements `ReadBackend` and `OrderedReadBackend`, but not
`WriteBackend`. Ordered state and cache rows use a lazy two-way merge where the
cache wins equal keys. Plain unordered reads stream rows while filtering keys
shadowed by the cache. Ordered views over an unordered backend are the one
case that requires temporary sorting.

Reverse iteration is explicit on every ordered backend; the trait no longer
provides a hidden collect-and-reverse default. Lifecycle writeback remains on
`DurableStorage`, while trait flush and `Drop` share Table's single inherent
flush implementation.

## Topic

Named Topic consumers decode one record per iterator step and commit their
cursor explicitly. Table recovery and workspace consumers preserve that
streaming behavior.

The retained tests belong only to B+ tree, KeyDir, SkipList, State, and Topic.
