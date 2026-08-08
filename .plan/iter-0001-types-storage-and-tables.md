# Iteration 0001: Portable Types, Storage, and Tables

Status: implemented and reflected from the current source.

This iteration is the persistence foundation. It has no workspace policy,
networking, catalog, or authorization concerns. The types crate defines the
portable model and the storage crate provides durable primitives and the typed
table facade.

## Portable model

`zendb-types` owns the data that can cross crate and replication boundaries:

- Opaque `InstallationId` and `WorkspaceId` values.
- `EventId` as `(author installation, sequence)`.
- `EventTime` and `EventStamp`, ordered by physical time, logical time,
  author, and sequence. The fixed encoded stamp is the prefix of every event.
- `Event`, containing its stamp, primary key, path, and CRDT operation.
- Paths, scalar and container values, cells, and CRDT operations.
- Installation records, permissions, and lifecycle states used by the
  workspace layer.
- The `PeerIdentity` input boundary and shared fixed little-endian bincode
  serialization helpers.

The model remains portable: storage algorithms do not depend on workspace
catalogs, installation policy, networking, or authorization.

## Storage primitives

`zendb-storage` defines the backend traits, statistics/configuration types,
durability barriers, read/write traits, and ordered-read traits. The current
implementations are:

- `BPlusTree`: ordered durable storage.
- `KeyDir`: unordered durable storage backed by an append-only memory-mapped
  file and an in-memory key directory.
- `SkipList`: in-memory ordered storage.
- `State`: the backend-selection facade used by typed state and table data.

Durable backends expose flush, sync, persist, recovery, and compaction
boundaries. The storage layer intentionally keeps validation and per-record
defense limited to the format invariants required for recovery.

## Topic

`Topic<T>` is a persistent, segmented, append-only log with one writer and
multiple readers/consumers:

- The active writer serializes into a pooled buffer, writes a length-delimited
  record, and rotates segments at the configured size.
- Segments have a small header, a base logical offset, and a sparse logical
  offset to byte-position index.
- Readers use the published segment list and sparse index to seek and scan.
- A named consumer persists its committed logical offset in a `KeyDir`; an
  anonymous reader starts at the earliest retained record.
- Compaction removes segments fully consumed by registered consumers. Table
  topics are retained according to the table's replication/recovery needs.
- Opening an active segment scans and truncates an incomplete trailing record
  so a crash cannot leave an unreadable tail.

The topic is an internal table mechanism. `Table::reader()` and
`Table::consumer(name)` expose the supported read paths; the `Topic` itself is
not part of the public table API.

## Table

`Table<PrimaryKey>` composes three storage concerns:

```text
State<PrimaryKey, Cell>       current projected CRDT cells
State<InstallationId, ReceiptWindow> per-table causal receipts
Topic<Change>                 durable change log
```

`insert` allocates the next sequence for the local author on that table,
prepares the CRDT change, appends an applied change to the topic, updates the
projected state, and records the receipt. A local no-op is ignored.

`observe` preserves a remote event's sequence, rejects invalid zero sequences,
ignores duplicates, and applies novel changes. A remote no-op records causal
receipt state but does not create a topic change. The recovery consumer replays
the topic into projected state when a table opens.

The table implements read and ordered-read behavior. It is deliberately not a
generic `WriteBackend`; writes must pass through event and CRDT invariants.

## Invariants and boundaries

- Event identity and ordering are portable and stable.
- Causal receipts are scoped to a table, not the whole workspace.
- The topic is the durable change source; projected state is a recoverable
  materialization.
- Backend mechanics are independent of workspace policy.
- No network protocol, workspace lifecycle, role system, global causal index,
  or signed event envelope belongs in this iteration.
