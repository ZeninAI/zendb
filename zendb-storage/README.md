# zendb-storage

Generic persistent and in-memory key/value mechanics. This crate knows nothing
about CRDTs, devices, authorization, workspaces, or networking.

## Backends

| Type | Durable | Ordered | Purpose |
|---|---:|---:|---|
| `BPlusTree<K, V>` | yes | yes | range-oriented persistent state |
| `KeyDir<K, V>` | yes | no | fast persistent point lookup |
| `SkipList<K, V>` | no | yes | bounded or unbounded in-memory state |
| `Topic<T>` | yes | append order | segmented change logs and consumer offsets |
| `State<K, V>` | configurable | configurable | runtime wrapper over the three KV backends |

## Contracts

- `Storage`: config and statistics.
- `DurableStorage`: create/open/flush/sync/physical compact.
- `ReadBackend`: read-only lookup and iteration for invariant-preserving
  facades and raw stores.
- `WriteBackend`: raw CRUD and bulk mutation; it extends `ReadBackend`.
- `OrderedReadBackend`: read-only ordered extension.

The split lets a Table expose the complete getter surface without implementing
raw writes that bypass CRDT and replication invariants. Concrete B+ tree,
KeyDir, SkipList, and State backends implement both read and write contracts;
only genuinely ordered implementations expose `OrderedReadBackend`.

Backends serialize keys and values using bincode 2. B+ tree ordering follows
serialized key bytes; SkipList ordering follows `K::Ord`. Generic callers must
use key encodings for which those orders agree when switching backends.

`Topic` persists consumer offsets independently, permits one active reader per
consumer, and retains compacted segments while a reader still references them.
