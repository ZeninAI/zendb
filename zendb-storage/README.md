# zendb-storage

`zendb-storage` owns persistence mechanics. Its backends are independent of
workspace membership, roles, and network policy.

## Storage Types

- `BPlusTree<K, V>`: ordered durable storage.
- `KeyDir<K, V>`: unordered durable storage.
- `SkipList<K, V>`: ordered in-memory storage.
- `State<K, V>`: runtime selection across those backends.
- `Topic<T>`: segmented append-only log with named durable consumers.

`ReadBackend`, `WriteBackend`, `OrderedReadBackend`, `Storage`, and
`DurableStorage` define the shared contracts.

## Table

`Table` is the only table mutation facade:

```text
Table
  State<PrimaryKey, Cell>
  State<InstallationId, ReceiptWindow>
  Topic<Change>
  recovery consumer
```

`Table::insert(Event)` assigns the next sequence for the event author on that
table and applies a local operation. A CRDT no-op returns
`InsertOutcome::Ignored` without changing the topic or causal state. A changed
operation appends before its cell is written to State and advances the durable
receipt window. `Table::observe(Event)` preserves a remote sequence, ignores
duplicates, and records novel remote observations even when the local CRDT
does not change; those no-ops are intentionally absent from the change topic
until the replication protocol gains its no-op response.

The durable `Change` record stores the event, previous cell, and resulting
current cell. `TableConfig::causal` selects the backend for per-installation
sequence and receipt records and defaults to unordered `KeyDir`.

Table implements `ReadBackend` and `OrderedReadBackend`, but not
`WriteBackend`. Reads and writes interact directly with the configured State;
the table facade remains the only path that can append a row change topic.
Change-log access is exposed through `Table::reader()` and
`Table::consumer(name)`; the underlying `Topic<Change>` is private to the table.

## Topics And Retention

Named consumers decode records incrementally and persist cursors only when
`commit` is called. Anonymous `TopicReader` cursors start at the earliest
retained offset and support lookup without creating retention state.

Because `Change` starts with `event` and `Event` starts with `stamp`, the first
28 bytes of every encoded table change are the fixed-width `EventStamp`.
`TopicReader::seek` resolves offset, stamp-predicate, earliest, and latest
targets, while `TopicConsumer::seek` delegates to its reader. Stamp predicates
scan only that prefix and skip payload bodies. During iteration, stale offsets
below retained data advance to the earliest available record and offsets beyond
the tail move to the tail.

Each topic segment keeps a sparse logical-offset-to-byte-position index. A
random offset seek starts at the nearest sparse entry and scans no more than
one stride. Retention-based trimming removes only fully consumed segments, so
repair can still resume from retained offsets without key compaction. Table
topics are not trimmed from local consumer offsets because replication
retention eventually requires a workspace-wide watermark.
