# ZeninDB Product Requirements and Architectural Design

> **Document status:** Living product and architecture specification
> **Repository status reflected:** Current working tree, including the in-progress `Database` implementation
> **Primary audience:** ZeninDB maintainers and contributors designing the type system, storage engine, table engine, replication layer, and future streaming consumer runtime

---

## 1. Executive Summary

ZeninDB is an embedded, local-first, eventually consistent database for applications that need rich collaborative data structures, offline mutation, deterministic convergence, and local materialized views.

The central abstraction is a table containing rows addressed by primary keys. Each row is a recursive `Cell` tree. Cells contain typed values, structural Hybrid Logical Clock metadata, and an optional synchronization policy. All collaborative writes are represented as self-contained `Delta` values that identify a table, row, recursive path, operation, HLC, sync decision, and opaque signature.

The current repository implements three major layers:

1. **`zendb-types`** implements the recursive cell model, HLCs, deltas, paths, operations, generated type dispatch, scalar values, records, LWW sets, RGA-style lists, and RGA-style collaborative text.
2. **`zendb-storage`** implements reusable persistent storage backends: an unordered Bitcask-style `KeyDir`, an ordered memory-mapped `BPlusTree`, and an in-memory ordered `OrderLog` with a durable write-ahead log.
3. **`zendb-engine`** implements a `Table` that combines materialized row state with an in-flight delta journal and an in-memory resolved-row cache. An in-progress `Database` implementation owns a persistent table catalog and lazily opened tables.

The next major product capability is a **stateful streaming consumer runtime**. External owners of a database must be able to register computations that:

- observe accepted deltas or resolved state changes;
- filter events by table, row, path, operation, or resolved state;
- bootstrap from an existing table snapshot and then continue incrementally;
- maintain private durable state;
- publish derived state into normal ZeninDB tables;
- pause, resume, restart, fail, recover, and be removed at runtime;
- support multiple independently progressing consumers;
- eventually support dynamically supplied logic through a sandboxed runtime such as WebAssembly.

This PRD carefully distinguishes:

- **Implemented:** behavior present in the current working tree.
- **Partial:** behavior present but with important limitations.
- **Planned:** agreed target architecture not yet implemented.
- **Deferred:** explicitly outside the immediate implementation sequence.

That distinction is mandatory. ZeninDB's current storage writeback and table journal provide useful durability, but they do not yet provide database transactions, exactly-once stream processing, anti-entropy replication, or a durable multi-consumer commit log.

---

## 2. Product Vision

ZeninDB should make collaborative and reactive local applications possible without forcing every application to rebuild the same difficult primitives:

- recursive conflict-free data;
- offline writes;
- deterministic merge behavior;
- durable local storage;
- compact materialized state;
- low-latency local queries;
- live reactive computations;
- stateful derived views;
- eventual peer-to-peer convergence.

An application should be able to embed ZeninDB, create tables, apply local or remote deltas, query resolved rows, and attach long-running computations to those tables. Those computations should behave like small local streaming jobs: they first process existing state, then continuously process new changes, and optionally maintain or publish derived state.

ZeninDB is not intended to require a central coordination service. A server may be used as a transport relay, backup target, or authority in a particular deployment, but the data model and merge semantics must remain usable by independently operating replicas.

---

## 3. Goals

### 3.1 Core data goals

- Represent every addressable row or nested value as a `Cell`.
- Support typed scalar and collaborative values.
- Allow operations to target arbitrary nested paths.
- Preserve enough metadata in compacted state for deterministic convergence.
- Support out-of-order delivery and delete-before-create behavior.
- Ensure repeated application and repeated merge are idempotent where required by each CRDT.
- Allow local-only and synchronized subtrees to coexist in a row.

### 3.2 Storage goals

- Provide embedded, file-backed storage without requiring an external service.
- Provide both ordered and unordered state backends.
- Make reads available from resolved table state before journal materialization.
- Recover pending table deltas after reopening.
- Support compaction and bounded in-flight table journals.
- Keep storage primitives generic and reusable outside the ZeninDB table abstraction.

### 3.3 Reactive computation goals

- Let database owners register independently progressing consumers.
- Provide correct handling of stale HLC operations and resolved state changes.
- Support stateless triggers and stateful aggregations.
- Support initial batch bootstrap followed by incremental stream processing.
- Support private managed state and public derived tables.
- Support runtime lifecycle operations.
- Define a credible path from compiled Rust consumers to dynamically loaded sandboxed consumers.

### 3.4 Replication goals

- Keep `Delta` transport-independent.
- Accept local and remote deltas through the same conflict-resolution path.
- Support eventual state-based convergence without requiring infinite delta retention.
- Preserve signatures as opaque data at the type layer.

Replication orchestration, signing, verification, transport, anti-entropy, and cold sync are planned but are not implemented in the current repository.

---

## 4. Non-Goals and Explicit Boundaries

The following are not immediate product requirements:

- A distributed consensus protocol.
- Linearizable multi-device transactions.
- A centralized cloud control plane.
- A SQL-compatible query language.
- Arbitrary native Rust code loaded safely at runtime.
- Exactly-once external side effects before the database has a transactional outbox or equivalent commit protocol.
- Infinite historical event retention.
- Using HLC ordering as a substitute for a local stream cursor.

ZeninDB may later add some of these capabilities, but the current architecture must not accidentally promise them.

---

## 5. Repository and Crate Organization

| Crate | Current responsibility | Status |
|---|---|---|
| `zendb-types` | Pure data model, HLC, cells, deltas, operations, paths, CRDT types, generated dispatch | Implemented |
| `zendb-storage` | Generic persistent key/value and ordered storage backends | Implemented |
| `zendb-engine` | Tables, materialized state, in-flight delta journal, resolved cache, database catalog | Implemented / partial |
| `zendb-replication` | Transport-independent synchronization, signatures, anti-entropy, cold sync | Planned |
| consumer runtime module or crate | Durable subscriptions, managed state, lifecycle, scheduling | Planned |

### 5.1 Dependency direction

The intended dependency direction is:

```text
zendb-types
    ^
    |
zendb-storage     (generic; should not depend on ZeninDB types)
    ^
    |
zendb-engine
    ^
    |
replication / consumers / application adapters
```

`zendb-types` must remain free of storage, I/O, networking, and runtime dependencies. `zendb-storage` must remain generic over encoded key and value types. `zendb-engine` is responsible for composing the generic backends with ZeninDB semantics.

---

## 6. Current Capability Matrix

| Area | Capability | Status | Notes |
|---|---|---|---|
| Type system | Recursive `Cell` tree | Implemented | `Cell.value: Option<Value>` represents live values and tombstones |
| Type system | 16-byte HLC | Implemented | 48-bit physical time, 16-bit logical counter, 64-bit device ID |
| Type system | Nested typed paths | Implemented | Paths carry expected container tags |
| Type system | Scalar types | Implemented | Bool, Int, String, Timestamp, Blob |
| Type system | Record container | Implemented | Named child cells |
| Type system | LWW set | Implemented | Per-element add and remove clocks |
| Type system | RGA-style list | Implemented | Stable HLC element IDs and immutable placement |
| Type system | Collaborative text | Implemented | Stable `(Hlc, offset)` character IDs |
| Type system | Delta signatures | Data field only | Verification is not implemented |
| Storage | Generic backend contract | Implemented | CRUD, bulk operations, iteration, stats, flush, sync |
| Storage | Unordered `KeyDir` | Implemented | Hash index plus append-only mmap data file |
| Storage | Ordered `BPlusTree` | Implemented | mmap, page splits, extents, ranges, reverse iteration |
| Storage | Ordered `OrderLog` | Implemented | In-memory skip list plus durable WAL |
| Storage | Database transactions | Missing | No atomic multi-backend transaction boundary |
| Engine | Materialized table state | Implemented | Ordered or unordered backend |
| Engine | In-flight table delta journal | Implemented | Ordered by row then HLC |
| Engine | Resolved pending-row cache | Implemented | Rebuilt from the journal on open |
| Engine | Automatic materialization | Implemented | Manual or event-count threshold |
| Engine | Persistent table catalog | Partial | Current working tree includes `Database` |
| Engine | Observable-only mutation API | Missing | Direct backend writes can bypass deltas |
| Engine | Durable consumer commit log | Missing | Required before durable consumers |
| Engine | Stateful consumer runtime | Missing | Designed in this PRD |
| Replication | Live delta sync | Missing | Planned |
| Replication | State-based anti-entropy | Missing | Planned |
| Replication | Sync-policy enforcement | Missing | Metadata exists, policy behavior does not |

---

## 7. Core Data Model

### 7.1 Cell

**Status: Implemented**

`Cell` is the universal addressable state unit:

```rust
pub struct Cell {
    pub value: Option<Value>,
    pub hlc: Hlc,
    pub sync: Option<bool>,
}
```

Semantics:

- `value = Some(value)` means the cell is live.
- `value = None` means the cell is a tombstone.
- `hlc` is the structural clock for that cell.
- `sync = None` means no explicit local sync policy is attached at that cell.
- `sync = Some(true)` or `Some(false)` stores an explicit local sync decision.

A cell's structural HLC covers direct structural operations against that cell:

- creation;
- deletion;
- replacement;
- type replacement;
- direct type operation where the type implementation changes state;
- sync metadata changes.

Descendant-only writes do not automatically bump ancestor HLCs. This is necessary so an independent newer write to one field does not prevent valid writes to another field.

`Cell::max_hlc()` recursively returns the greatest HLC represented by the cell and its nested value. It is used when deciding whether a newer operation may repair or replace an incompatible target type.

### 7.2 Tombstones

**Status: Implemented**

Deletion is represented by `Cell { value: None, hlc, ... }`, not by physically removing a child from a collaborative container.

Tombstones are required to prevent an old peer or out-of-order delta from resurrecting deleted state. Containers such as records retain tombstoned child cells as part of compacted state. Sets, lists, and text maintain their own type-specific deletion metadata.

Physical garbage collection of tombstones is not implemented. Any future tombstone compaction mechanism must prove that removed causal metadata can no longer be needed by any replica.

### 7.3 Hybrid Logical Clock

**Status: Implemented**

`Hlc` is a 16-byte lexicographically ordered timestamp:

```text
Bytes 0-5:   physical_ms, unsigned 48-bit big-endian
Bytes 6-7:   logical, unsigned 16-bit big-endian
Bytes 8-15:  device_id, unsigned 64-bit identity bytes
```

Comparison order is:

1. physical milliseconds;
2. logical counter;
3. device ID.

The all-zero value is `Hlc::ZERO`. Every valid non-zero clock beats it.

The current implementation derives the process-global device ID by hashing the operating system machine UID with BLAKE3 and truncating it to 64 bits. Initialization is explicit and idempotent.

Important limitations:

- The repository defines HLC representation and comparison, but it does not yet implement a complete clock generator that safely advances logical counters for local events and observes remote clocks.
- The current global device-ID storage uses mutable process-global state. This should be hardened before exposing concurrent initialization or runtime reconfiguration.
- HLC is a conflict-resolution timestamp and event identity component. It must not be treated as the stream-processing cursor for consumers.

### 7.4 Primary keys

**Status: Implemented**

Primary keys are generated by the type registration macro and currently support:

- `Bool`
- `Int`
- `String`
- `Timestamp`
- `Blob`

Primary-key variants are also used as set members.

### 7.5 Delta

**Status: Implemented as a data structure**

```rust
pub struct Delta {
    pub table_id: TableId,
    pub primary_key: PrimaryKey,
    pub path: Path,
    pub op: Op,
    pub hlc: Hlc,
    pub sync: bool,
    pub signature: Signature,
}
```

A delta is intended to contain everything required to apply one mutation:

- destination table;
- destination row;
- recursive path;
- operation;
- HLC;
- resolved synchronization decision;
- opaque signature bytes.

Current engine limitations:

- `Table::insert_delta` does not validate that `delta.table_id` matches the owning table.
- Signature verification is not implemented.
- The current table event identity is `(primary_key, hlc)`. This assumes one unique HLC per row mutation. Two different operations for the same row with the same HLC collide even if their paths differ.

Before replication and consumers become production features, event identity must be made explicit. Options include guaranteeing globally unique HLCs per delta or introducing a dedicated `DeltaId`.

### 7.6 Path

**Status: Implemented**

A path is a sequence of typed traversal steps:

```rust
pub struct PathStep {
    pub container_tag: TypeTag,
    pub segment: Segment,
}

pub struct Path {
    pub steps: Vec<PathStep>,
}
```

An empty path addresses the row root. Each step says:

- what container type is expected at the current position;
- which child segment to enter.

This type information makes operations self-healing. If a required intermediate cell is absent or tombstoned and the incoming operation beats the represented state, the apply walk can construct an empty container of the required type.

### 7.7 Operations

**Status: Implemented**

Cross-cutting `Op` variants:

- `Type(TypeOp)`: dispatch a type-specific operation.
- `SetSync { sync: Option<bool> }`: set local sync metadata.
- `Delete`: tombstone the target cell.
- `Replace { value }`: replace the target value.
- `Merge { cell }`: merge a remote cell state.

Scalar values currently have no type-specific operations. Scalar changes use `Replace`, and scalar merges use cell HLC ordering.

---

## 8. Type System and CRDT Semantics

### 8.1 Generated dispatch

**Status: Implemented**

The `register_types!` macro is the central type registry. It generates:

- `TypeTag`;
- `PrimaryKey`;
- `TypeError`;
- `Value`;
- `TypeOp`;
- `Segment`;
- dispatch implementations for `Type` and `ContainerType`.

The current registration is:

```text
Key-capable: Bool, Int, String, Timestamp, Blob
Leaf values: Bool, Int, String, Timestamp, Blob, Set, Text
Containers: Record, List
```

Adding a type requires:

1. defining its value representation;
2. defining its operation and error types;
3. implementing `Type`;
4. optionally implementing `ContainerType`;
5. registering it in the macro invocation.

### 8.2 `Type` contract

**Status: Implemented**

```rust
pub trait Type: Sized + Encode + Decode<()> {
    type Op: Encode + Decode<()>;
    type Error: std::error::Error;

    fn apply(
        &mut self,
        op: &Self::Op,
        local_hlc: Hlc,
        op_hlc: Hlc,
    ) -> Result<bool, Self::Error>;

    fn merge(
        &mut self,
        remote: &Self,
        local_hlc: Hlc,
        remote_hlc: Hlc,
    ) -> Result<bool, Self::Error>;

    fn max_hlc(&self) -> Hlc;
}
```

`apply` and `merge` return whether represented CRDT state changed. This does not necessarily mean the user-visible value changed. For example, a stale set add may advance the stored add clock while the element remains hidden by a newer remove clock.

That distinction is essential for the future consumer runtime.

### 8.3 `ContainerType` contract

**Status: Implemented**

Containers provide mutable child traversal:

```rust
fn child_or_default(
    &mut self,
    segment: &Segment,
    child_tag: Option<TypeTag>,
) -> Result<&mut Cell, Error>;
```

`child_tag = Some(tag)` requests a live empty child of the specified type when absent. `child_tag = None` requests a tombstone placeholder when no target value type is known, such as an out-of-order delete.

### 8.4 Scalar values

**Status: Implemented**

Bool, Int, String, Timestamp, and Blob are scalar LWW values. They have no type-specific mutation operations. A newer remote scalar replaces an older local scalar during merge.

### 8.5 Record

**Status: Implemented**

`Record` is a `BTreeMap<String, Cell>`.

Properties:

- each field is independently addressable;
- fields retain tombstone cells;
- merge recursively combines corresponding cells;
- missing fields are cloned from the remote record;
- `max_hlc` recursively covers all child cells.

Record field deletion uses the field cell's tombstone rather than a separate record-level tombstone map.

### 8.6 Set

**Status: Implemented**

`Set` is a `BTreeMap<PrimaryKey, Meta>`, where each member stores:

```rust
pub struct Meta {
    pub updated: Hlc,
    pub deleted: Hlc,
}
```

Membership is visible when `updated > deleted`.

Operations:

- `Add { key }` advances `updated` if the operation HLC beats it.
- `Remove { key }` advances `deleted` if the operation HLC beats it.

Important semantic consequence:

- A set operation may change CRDT metadata without changing visible membership.
- Consumers interested in assignments must compare resolved membership before and after the operation, not merely match `SetOp::Add`.

### 8.7 List

**Status: Implemented**

`List` is an RGA-style ordered container:

- each element ID is the HLC of its insert operation;
- each entry stores an immutable `after` anchor;
- concurrent siblings are ordered by descending ID;
- delete creates or merges a tombstone cell;
- delete-before-insert is supported through placeholders;
- list path traversal addresses elements by stable ID, not visible index.

Visible numeric indices are read helpers only. They must never be persisted as mutation addresses because concurrent edits can change visible positions.

### 8.8 Text

**Status: Implemented**

Collaborative text is an RGA-style Unicode scalar sequence:

- each insert operation has one HLC;
- each inserted character ID is `(operation_hlc, character_offset)`;
- inserts record immutable anchors and character content;
- deletes maintain a latest deletion HLC;
- delete-before-insert is supported;
- conflicting content for the same stable insert identity is rejected;
- visible text is derived by deterministic traversal.

The unit is a Rust `char`, meaning a Unicode scalar value, not a grapheme cluster. Applications that need user-perceived character operations must layer grapheme-aware editing behavior above the CRDT.

---

## 9. Cell Apply and Merge Semantics

### 9.1 Apply walk

**Status: Implemented**

`Cell::apply(&Delta) -> bool`:

1. starts at the row root;
2. walks each path step;
3. ensures the current cell has the required container type;
4. creates or repairs intermediate containers when the incoming HLC beats the represented state;
5. obtains or creates the addressed child;
6. applies the leaf operation.

If a required type cannot be established because existing state wins, the operation is rejected as a no-op.

### 9.2 Type repair

When the target has the wrong type, the incoming operation may replace it with an empty value of the expected type only when its HLC beats `cursor.max_hlc()`.

Using `max_hlc()` rather than only the cursor's structural HLC prevents a newer nested descendant from being erased by an older type-repair operation.

### 9.3 Merge

**Status: Implemented**

`Cell::merge` follows these rules:

- tombstone versus tombstone: newer structural HLC wins;
- live versus tombstone: newer structural HLC wins;
- differing live types: newer structural HLC wins;
- matching live types: delegate recursively to the type implementation;
- after same-type merge, advance the cell structural HLC if the remote HLC beats it.

The local cell's `sync` metadata is intentionally preserved when the full remote cell replaces local state. Sync policy is local metadata and must not silently change because a remote replica has a different policy.

### 9.4 Current result limitation

`Cell::apply` currently returns only `bool`.

That is insufficient for advanced engine behavior because it cannot distinguish:

- duplicate event rejected before application;
- accepted event that did not change CRDT state;
- CRDT metadata change;
- user-visible state change;
- row creation;
- row deletion;
- target path before and after values;
- type errors or rejected path traversal.

The engine should eventually introduce a richer internal mutation result while preserving a convenient boolean API where appropriate.

---

## 10. Synchronization Model

### 10.1 Intended semantics

**Status: Planned; metadata fields are implemented**

`Cell.sync` is local policy:

| Value | Intended meaning |
|---|---|
| `None` | Inherit effective policy from an ancestor or database default |
| `Some(true)` | Synchronize this subtree by default |
| `Some(false)` | Keep this subtree local by default |

`Delta.sync` is a resolved boolean captured when the delta is created. It answers whether that delta should be offered to replication.

### 10.2 Required behavior

The intended write-time resolution is:

1. locate the target path;
2. walk from target toward root;
3. use the nearest explicit sync policy;
4. fall back to table or database default;
5. store the result in `Delta.sync`.

`SetSync` is expected to remain local-only because sync policy belongs to the local replica.

### 10.3 Current limitation

The repository currently stores and merges sync metadata but does not implement:

- inherited policy resolution;
- rejection of incoming deltas for local-only subtrees;
- replicated-state views;
- state hashing;
- anti-entropy;
- broadcast queues.

The PRD must therefore treat these behaviors as planned, not current.

---

## 11. Generic Storage Layer

### 11.1 Backend contract

**Status: Implemented**

`Backend<K, V>` defines:

- point reads and presence checks;
- put, put-if-absent, replace, update, and delete;
- bulk put and delete operations;
- iteration over keys, values, and entries;
- clear and compact;
- size, stats, and immutable configuration;
- asynchronous writeback scheduling through `flush`;
- blocking writeback through `sync`.

`OrderedBackend<K, V>` adds:

- ascending half-open range scans;
- first and last entries;
- reverse iteration;
- reverse range scans.

The backend layer does not provide:

- isolation;
- rollback;
- cross-backend atomicity;
- corruption repair;
- a formal crash-consistency protocol.

`flush` and `sync` are writeback boundaries, not database transactions.

### 11.2 Serialization

**Status: Implemented**

The storage layer uses bincode 2 with:

- little-endian encoding;
- fixed integer encoding;
- no decode limit for trusted internal data.

Reusable thread-local scratch buffers reduce hot-path allocation. Bincode is not zero-copy for decoded values, so backends generally return owned values where decoding is required.

### 11.3 KeyDir

**Status: Implemented**

`KeyDir` is an unordered persistent key/value store modeled after Bitcask:

- an in-memory hash map indexes live keys;
- records append to a memory-mapped file;
- overwrites leave dead records;
- deletes append tombstones;
- compaction slides live records forward in place;
- opening replays the file to rebuild the hash index.

Best use cases:

- unordered lookup-heavy state;
- catalogs and metadata;
- private consumer state that does not require range scans.

### 11.4 BPlusTree

**Status: Implemented**

`BPlusTree` is an ordered persistent mmap-backed B+ tree:

- 4096-byte pages;
- slotted internal and leaf pages;
- leaf sibling links for forward and reverse iteration;
- overflow extents for large values;
- freelist page reuse;
- suffix-truncated separator keys;
- sorted bottom-up bulk loading on empty trees;
- range and reverse-range scans;
- compaction by rebuilding a denser tree.

Critical ordering rule:

> The B+ tree orders keys by lexicographic serialized key bytes, not by Rust `K::Ord`.

Callers must choose a serialization whose byte order matches the desired semantic order. This is particularly important for numeric keys and any future stream cursor encoding.

### 11.5 OrderLog

**Status: Implemented**

`OrderLog` combines:

- an in-memory ordered skip-list index;
- a memory-mapped append-only write-ahead log;
- key ordering based on Rust `K::Ord`;
- overwrite and tombstone replay;
- compaction;
- efficient ordered and reverse iteration.

It is suitable for durable ordered working sets and journals where values should remain immediately available in memory after replay.

The current table uses `OrderLog<EventKey, Delta>` as an in-flight delta journal. That journal is not a durable consumer stream because materialization clears it.

---

## 12. Table Engine

### 12.1 Table responsibilities

**Status: Implemented**

A `Table` owns:

```text
state   = materialized PrimaryKey -> Cell backend
events  = durable in-flight EventKey -> Delta journal
cache   = in-memory PrimaryKey -> fully resolved pending Cell
```

The state backend is configurable:

- ordered `BPlusTree`;
- unordered `KeyDir`.

The event journal is an `OrderLog`.

### 12.2 EventKey

**Status: Implemented with limitations**

```rust
pub struct EventKey {
    pub primary_key: PrimaryKey,
    pub hlc: Hlc,
}
```

This ordering groups pending events by row and then orders each row's events by HLC. It is useful for rebuilding a row's resolved pending state.

It does not provide:

- global arrival order;
- global commit order;
- ordering across rows by HLC;
- independent consumer cursors;
- multiple operations for the same row and same HLC.

These limitations are acceptable for the current materialization journal but make `EventKey` unsuitable as the key for the future consumer commit log.

### 12.3 Resolved cache

**Status: Implemented**

The cache stores the fully resolved row state after applying all pending events over materialized state.

Each cache entry also remembers whether the row existed in materialized state when the cache entry was created. `novel_pending` counts pending rows that are new relative to state, allowing table size to remain O(1).

On open, the table replays the in-flight event journal into the cache one row at a time.

### 12.4 Delta insertion

**Status: Implemented**

`insert_delta`:

1. builds an `EventKey`;
2. inserts the delta only if the key is absent;
3. applies the delta to the resolved cache entry or a state-seeded cell;
4. avoids making an absent row visible when the only operation is ineffective;
5. conditionally materializes according to flush configuration.

`bulk_insert_delta` sorts and deduplicates events, then performs equivalent insert-if-absent behavior.

### 12.5 Materialization

**Status: Implemented with crash-consistency limitations**

Materialization:

1. writes every cached resolved row into materialized state;
2. syncs state;
3. clears and syncs the event journal;
4. clears the in-memory cache.

The cache is retained if a state write fails mid-loop, preserving reads in the current process.

However, state writes and journal clearing are not part of one atomic transaction. A process or storage failure between these steps can leave ambiguous recovery conditions. Production durability requires a formal commit protocol.

### 12.6 Reads and iteration

**Status: Implemented**

Reads resolve cache over materialized state:

- cache wins for matching keys;
- ordered state merges with ordered cache for ascending and descending scans;
- unordered state filters keys shadowed by cache and chains pending entries.

Ordered table methods panic when the table uses an unordered state backend. A future public API should prefer an explicit error or distinct ordered-table type over runtime panic.

### 12.7 Direct backend writes

**Status: Implemented but architecturally unsafe**

Because `Table` implements `Backend<PrimaryKey, Cell>`, callers can use `put`, `replace`, `delete`, `update`, and bulk methods directly against materialized state.

These methods:

- bypass the delta journal;
- bypass deduplication;
- bypass future replication;
- bypass future consumer events;
- can make resolved cache entries stale.

This is the largest current API integrity issue.

Required direction:

- define a clearly named internal or administrative direct-state API;
- make normal observable table mutation delta-based;
- prevent consumers and replication from silently missing normal application writes.

### 12.8 Table configuration

**Status: Implemented**

`TableConfig` controls:

- `sync`: currently stored but not behaviorally enforced;
- flush policy;
- state backend configuration;
- event journal configuration.

Flush policies:

- `Manual`;
- `EventCount { max_events }`.

The default materializes after 1,000 pending events.

---

## 13. Database Layer

### 13.1 Current implementation

**Status: Partial; present in the current working tree**

`Database` owns:

- the database root path;
- a persistent `KeyDir<String, TableConfig>` catalog in `_meta`;
- a map of lazily opened in-memory `Table` objects.

Supported operations:

- create a database;
- open a database;
- create a table and persist its configuration;
- lazily open a cataloged table;
- borrow currently loaded tables.

Disk layout:

```text
<database>/
    _meta
    <table-name>/
        state
        events
```

### 13.2 Required evolution

The database must become the ownership boundary for cross-table behavior:

- durable commit sequence allocation;
- durable consumer change log;
- consumer definitions and lifecycle;
- private consumer state;
- derived-table writes;
- replication coordination;
- table identifier validation;
- database-wide synchronization and shutdown.

Consumers must be owned by `Database`, not individual `Table` values, because a consumer may observe and update multiple tables.

---

## 14. Reactive and Stateful Consumer Runtime

### 14.1 Product requirement

**Status: Planned; next major architecture**

An external owner of a ZeninDB database must be able to attach a computation to table activity.

Examples:

- Trigger an application action when user `X` becomes assigned to a note.
- Maintain the count of notes currently assigned to each user.
- Maintain a reverse index from user ID to note IDs.
- Publish a derived table containing note summaries.
- Chain a second consumer from the output of a first consumer.

The runtime must support both stateless and stateful computations. It must process existing state when first created and then continue from live changes.

### 14.2 Architectural boundary

The runtime must not execute arbitrary callbacks directly inside `Table::insert_delta`.

Direct callback execution would create:

- reentrant table mutation;
- difficult Rust borrow interactions;
- unpredictable write latency;
- failure coupling between storage and user code;
- no independent consumer checkpoint;
- no durable replay after restart;
- no clean pause or resume behavior;
- no safe route to dynamically supplied logic.

Instead, table mutation and consumer processing are separate stages:

```text
incoming Delta
    -> resolve against table state
    -> append durable ChangeRecord with CommitSeq
    -> make change available to independently scheduled consumers
    -> consumer updates private state and/or queues derived writes
```

### 14.3 HLC versus CommitSeq

This is the most important consumer design decision.

**HLC decides conflict resolution. `CommitSeq` decides stream progress.**

HLC cannot safely serve as a consumer cursor because:

- remote deltas can arrive late;
- an older HLC may still change an independent nested field;
- HLC order is causal/conflict metadata, not local arrival order;
- multiple rows need one stable database stream order;
- consumers need an unambiguous checkpoint after every processed change.

Introduce a monotonic local commit sequence:

```rust
pub struct CommitSeq(pub u64);
```

Requirements:

- allocated by the database for every unique accepted mutation record;
- monotonically increasing within one database history;
- never reused after durable assignment;
- encoded in a byte-order-preserving form when stored in `BPlusTree`;
- independent from the delta's HLC;
- suitable as the durable checkpoint for every consumer.

### 14.4 Accepted events versus effective changes

The runtime must distinguish three concepts:

1. **Duplicate:** the event identity already exists and is not accepted again.
2. **Accepted delta:** the event is new to this database, even if it does not alter resolved state.
3. **Resolved change:** application changes represented or user-visible state.

A stale operation can be accepted but ineffective. A set add can update metadata but not visible membership. A root replacement can change membership without containing a set add operation.

Therefore consumers cannot be correct if they only receive raw deltas.

### 14.5 ChangeRecord

The initial durable stream record should favor correctness and simplicity:

```rust
pub struct ChangeRecord {
    pub cursor: CommitSeq,
    pub table_id: TableId,
    pub primary_key: PrimaryKey,
    pub delta: Delta,
    pub effect: ChangeEffect,
}

pub enum ChangeEffect {
    NoEffect,
    RowChanged {
        before: Option<Cell>,
        after: Option<Cell>,
    },
}
```

`before` and `after` represent resolved row state around the accepted delta.

Whole-row snapshots may be expensive, but they establish unambiguous semantics. Later optimizations may store:

- changed path before/after values;
- compact semantic diffs;
- references to versioned row snapshots;
- lazy before/after reconstruction.

Such optimizations must preserve the same observable semantics.

### 14.6 Delivery modes

Consumers should declare their required event semantics:

```rust
pub enum DeliveryMode {
    AcceptedDeltas,
    ResolvedChanges,
}
```

`AcceptedDeltas`:

- receives each unique accepted delta;
- includes `NoEffect` changes;
- useful for auditing, metrics, and replication-related tooling.

`ResolvedChanges`:

- receives only changes that alter the relevant resolved state;
- should be the default for business triggers and materialized views.

Future modes may include metadata-only changes or explicit row lifecycle events, but the first implementation should avoid excessive surface area.

### 14.7 Correct assignment trigger

For a note row whose front matter contains a set of users, a correct "user X became assigned" consumer must evaluate:

```text
before contains X = false
after contains X  = true
```

It must not merely match:

```text
delta.op == SetOp::Add { key: X }
```

Reasons:

- the add may be stale;
- a newer remove may still dominate;
- a root or front-matter replacement may add X;
- a merge may add X;
- a type replacement may remove or add the set;
- a delete may remove X without a set remove operation.

This before/after rule also provides the correct basis for incremental counts.

### 14.8 Subscription filtering

Filtering must be split into declarative runtime filtering and consumer logic.

Proposed declarative subscription:

```rust
pub struct Subscription {
    pub tables: Vec<TableId>,
    pub primary_keys: Option<KeyFilter>,
    pub path_prefixes: Vec<PathPattern>,
    pub operations: Option<OpFilter>,
    pub delivery: DeliveryMode,
}
```

Runtime filtering should handle inexpensive structural predicates:

- source table;
- exact row key or key range where supported;
- path prefix;
- operation category;
- delivery mode.

Consumer code should handle semantic predicates:

- whether a particular user is visible in the resolved set;
- whether a count-relevant condition transitioned;
- whether a field value crossed a threshold;
- whether multiple fields jointly satisfy a condition.

Why use both layers:

- runtime filtering avoids needless deserialization and callback dispatch;
- consumer filtering remains expressive without designing a query language immediately;
- the durable cursor remains independent from filter complexity.

Critical cursor rule:

> A filtered-out record still advances the consumer's processed cursor.

Otherwise a consumer can become permanently blocked on an event it intentionally ignores.

### 14.9 Consumer API

The first native API should support compiled Rust implementations:

```rust
pub trait Consumer: Send {
    fn on_snapshot(
        &mut self,
        row: SnapshotRow,
        ctx: &mut ConsumerContext,
    ) -> Result<(), ConsumerError>;

    fn on_change(
        &mut self,
        change: &ChangeRecord,
        ctx: &mut ConsumerContext,
    ) -> Result<(), ConsumerError>;
}
```

The runtime should also expose lifecycle hooks only where they have defined recovery semantics. Avoid adding hooks that are impossible to replay deterministically.

### 14.10 ConsumerContext

Consumers must not receive unrestricted `&mut Database`.

An unrestricted mutable database reference would allow:

- nested mutation while processing the source event;
- bypassing output ordering;
- deadlocks once concurrency is added;
- reads that are inconsistent with the processing checkpoint;
- accidental recursive event chains;
- non-deterministic recovery.

Provide a capability-limited context:

```rust
pub trait ConsumerContext {
    fn state(&mut self) -> &mut dyn StateStore;
    fn emit_delta(&mut self, delta: Delta) -> Result<(), ConsumerError>;
    fn emit_action(&mut self, action: ExternalAction) -> Result<(), ConsumerError>;
}
```

Outputs are queued while the callback executes. They are applied only after the callback returns successfully.

### 14.11 Private managed state

Each consumer instance may own a private durable key/value state store.

Use cases:

- counts;
- last-seen values;
- deduplication identities;
- partial aggregates;
- join state;
- bootstrap progress;
- pending external actions.

Private managed state should use direct key/value updates, not CRDT deltas.

Reasons:

- it is owned by one local runtime;
- it does not require collaborative conflict resolution;
- CRDT metadata would add cost and ambiguity;
- checkpointing it with the consumer cursor is the important consistency requirement.

Likely initial backend:

- `KeyDir` for unordered state;
- optionally `BPlusTree` where ordered scans are necessary.

### 14.12 Public derived state

If another consumer or application must observe a consumer's output, that output should be published to a normal ZeninDB table through normal deltas.

Examples:

- `user_note_counts`;
- `notes_by_user`;
- `search_index_metadata`;
- `notification_queue`.

Public derived tables:

- participate in the normal event stream;
- may be observed by other consumers;
- may eventually replicate;
- retain the same CRDT and HLC semantics as application tables.

This produces a clear rule:

> Private state is directly managed and checkpointed. Shared observable state is written as normal table deltas.

### 14.13 Consumer chaining

Consumers may subscribe to tables produced by other consumers.

The runtime must:

- identify the originating consumer on derived writes;
- detect or constrain obvious cycles;
- define maximum chain depth or loop protection;
- preserve commit order;
- expose lag and failure information per consumer.

Cycles are not automatically invalid. Some iterative computations intentionally form cycles. However, the initial implementation should reject direct or indirect dependency cycles unless an explicit iterative processing model is designed.

### 14.14 Bootstrap: snapshot then stream

A newly created consumer must first process existing state and then process every later relevant change exactly according to its delivery guarantee.

Proposed protocol:

1. Persist the consumer definition with status `Bootstrapping`.
2. Establish a source fence cursor `F`.
3. Read a resolved snapshot corresponding to `F`.
4. Deliver snapshot rows to `on_snapshot`.
5. Persist private state and snapshot progress.
6. Replay durable change records with cursor greater than `F`.
7. Catch up to the live head.
8. Transition to `Running`.

Snapshot rows are not fake deltas:

```rust
pub struct SnapshotRow {
    pub table_id: TableId,
    pub primary_key: PrimaryKey,
    pub value: Cell,
}
```

The first implementation may briefly serialize writes while capturing the fence and snapshot. That is acceptable for correctness. Later versions may add:

- MVCC;
- copy-on-write snapshots;
- versioned table roots;
- incremental snapshot checkpoints.

The bootstrap protocol must explicitly define behavior for rows deleted after the fence but before the live tail is consumed. Replaying post-fence changes after the snapshot resolves this naturally.

### 14.15 Consumer lifecycle

Proposed durable states:

```rust
pub enum ConsumerStatus {
    Created,
    Bootstrapping,
    Running,
    Paused,
    Failed,
    Stopped,
}
```

Required operations:

- create;
- inspect;
- start;
- pause;
- resume;
- restart;
- stop;
- drop;
- reset and rebuild from source.

Semantics:

- **Pause:** stop processing but retain definition, private state, and cursor.
- **Resume:** continue from retained cursor.
- **Restart:** reconstruct implementation and resume from durable state.
- **Stop:** terminate execution while retaining durable metadata.
- **Drop:** remove definition and private state after validating dependency impact.
- **Reset:** intentionally discard checkpoint/state and bootstrap again.

Failures must persist:

- last successful cursor;
- failing cursor;
- error information;
- retry count;
- next retry time where applicable.

### 14.16 Runtime-created instances versus runtime-created logic

Rust supports dynamic creation and removal of instances whose implementation was compiled into the application:

```rust
runtime.register_factory(
    "count-user-notes",
    || Box::new(CountUserNotes::new()),
);

runtime.create_consumer(
    "count-alice-notes",
    "count-user-notes",
    config,
);
```

This supports:

- runtime configuration;
- many instances of one implementation;
- pause/resume/drop;
- persisted instance definitions.

It does not support arbitrary new native Rust logic without recompilation.

For runtime-supplied logic, the preferred later architecture is WebAssembly:

- serialized `SnapshotRow` and `ChangeRecord` inputs;
- controlled host functions;
- bounded memory and execution;
- no unrestricted filesystem or network access;
- explicit versioned ABI;
- deterministic state and output APIs.

Dynamic native libraries should not be the default because they are unsafe, platform-specific, difficult to upgrade, and not sandboxed.

### 14.17 Scheduling and backpressure

Each consumer progresses independently.

The runtime must track:

- current durable cursor;
- current stream head;
- lag in records and, optionally, wall-clock time;
- batch size;
- processing duration;
- failure and retry state.

The first scheduler may be cooperative and single-threaded. It should process bounded batches so one slow consumer cannot monopolize the database owner thread.

Later concurrency must preserve:

- per-consumer input order;
- deterministic output application order;
- safe access to private state;
- bounded resource usage.

Backpressure policy must be explicit. A slow durable consumer cannot silently force infinite commit-log retention. The runtime needs configurable policies:

- retain until all durable consumers advance;
- cap retention and fail/reset lagging consumers;
- archive old records;
- treat some consumers as ephemeral and non-blocking.

### 14.18 Durable consumer log

The table's current `events` log cannot serve consumers because:

- it is cleared during materialization;
- it is ordered by row then HLC, not commit sequence;
- it contains only pending table events;
- it has no independent consumer checkpoints.

Add a database-level durable commit log:

```text
CommitSeq -> ChangeRecord
```

This log must be separate from the table materialization journal.

Retention is governed by:

- minimum durable consumer cursor;
- replication requirements, if shared;
- configured retention limits;
- snapshot/rebuild capability.

### 14.19 Delivery guarantees

The current storage architecture cannot provide exactly-once stateful processing because source change records, private state writes, derived outputs, and cursor advancement cannot be committed atomically across backends.

Initial supported guarantee:

> At-least-once consumer delivery with durable checkpoints and idempotent processing requirements.

Consumers may see the same input again after failure. Therefore:

- private state mutations should be idempotent or guarded by processed event identity;
- derived deltas should have deterministic identities;
- external actions should use a durable outbox and idempotency key.

Future exactly-once local processing requires a database transaction or commit batch that atomically records:

- source `ChangeRecord`;
- private state mutations;
- derived table deltas;
- external outbox entries;
- consumer checkpoint advancement.

### 14.20 External side effects

Consumers must not directly perform irreversible external actions inside the processing callback if correctness depends on exactly-once behavior.

Instead:

1. consumer writes an `ExternalAction` to a durable outbox;
2. consumer checkpoint and outbox entry commit together when transactional support exists;
3. an action dispatcher executes the action using an idempotency key;
4. completion is recorded durably.

Until atomic commit exists, external side effects are at-least-once and must be explicitly documented as such.

### 14.21 Example: count notes assigned to user X

Bootstrap:

```text
count = 0
for every snapshot note:
    if resolved note.front_matter.users contains X:
        count += 1
persist count in private state
```

Incremental processing:

```text
before_assigned = before note exists and users contains X
after_assigned  = after note exists and users contains X

if !before_assigned && after_assigned:
    count += 1
if before_assigned && !after_assigned:
    count -= 1
otherwise:
    no visible count change
```

Publishing:

- keep the count only in private state if no other component needs it;
- emit a delta into `user_note_counts` if applications or consumers must observe it.

### 14.22 Consumer API decisions summary

| Decision | Chosen direction | Reason |
|---|---|---|
| Ownership | Database-level runtime | Consumers may span and update multiple tables |
| Callback location | Outside table mutation | Isolation, replay, lifecycle, failure handling |
| Stream cursor | Monotonic `CommitSeq` | HLC is not arrival/commit order |
| Default input | Resolved before/after change | Correct business semantics |
| Filtering | Runtime structural + consumer semantic | Efficiency without premature query language |
| Private state | Direct managed KV | Local ownership; no CRDT overhead |
| Shared state | Normal derived tables through deltas | Observable and chainable |
| Bootstrap | Snapshot at fence, then replay tail | No gaps between batch and stream |
| Initial guarantee | At-least-once | No atomic cross-backend transaction yet |
| Runtime-native logic | Registered compiled factories | Idiomatic and safe |
| Arbitrary runtime logic | Future sandboxed WASM | Dynamic without unsafe native loading |

---

## 15. Replication Architecture

### 15.1 Intended model

**Status: Planned**

ZeninDB replication should combine:

- low-latency delta propagation;
- state-based anti-entropy for authoritative repair;
- cold sync for a new replica;
- transport independence.

### 15.2 Live delta propagation

Local deltas whose resolved `sync` flag is true may be broadcast to reachable peers. Remote deltas enter the same table conflict-resolution path as local deltas.

Live delivery is best effort. It must not be the only convergence mechanism.

### 15.3 State-based anti-entropy

Compacted state must contain enough causal metadata to converge without retaining every historical delta forever.

Planned anti-entropy:

- hash synchronized compacted state;
- compare hashes between peers;
- exchange differing chunks;
- merge remote cells.

The exact hash tree, chunk boundaries, sync-policy interaction, and tombstone collection rules remain to be designed.

### 15.4 Transport boundary

ZeninDB should not own peer discovery or network connection policy. The embedder provides transport and identity integration.

### 15.5 Relationship to consumers

Consumers observe local commit order, including accepted remote deltas after they arrive locally.

Consequences:

- different replicas can process the same replicated changes in different local `CommitSeq` order;
- private consumer state is local unless explicitly published;
- deterministic replicated derived tables require CRDT-safe output semantics;
- external side effects should generally run on one designated replica or use application-level deduplication.

---

## 16. Durability, Recovery, and Consistency

### 16.1 Current guarantees

Current backends provide file-backed persistence and mmap writeback operations. The table can rebuild its pending cache from the in-flight event journal after reopening.

### 16.2 Guarantees not currently provided

- atomic table state plus journal commit;
- atomic database catalog plus table creation rollback;
- atomic source change plus consumer output;
- snapshot isolation;
- concurrent writer isolation;
- automatic corruption detection and repair;
- exactly-once stream processing.

### 16.3 Required durability direction

Introduce an explicit database commit protocol before claiming production-grade crash consistency.

Potential designs include:

- one database-level WAL containing typed commit batches;
- copy-on-write roots with atomic root publication;
- transactional staging plus commit markers;
- a single append log from which materialized structures recover.

The design must coordinate table state, table journals, consumer logs, consumer state, and catalog metadata.

---

## 17. Concurrency Model

### 17.1 Current model

The current engine is fundamentally single-owner and mutable:

- mutation APIs require `&mut self`;
- backends mutate mmap state in place;
- `Database` returns mutable table references;
- there is no locking or transaction scheduler.

### 17.2 Near-term requirement

Keep the first consumer runtime cooperative and database-owned. Avoid introducing background mutation threads until commit ordering and ownership rules are explicit.

### 17.3 Future model

Potential future architecture:

- one serialized database writer;
- concurrent immutable/read snapshots;
- consumers scheduled around committed change batches;
- background flush and compaction workers;
- explicit shutdown and sync coordination.

Concurrency must not compromise deterministic commit sequence assignment or consumer checkpoint semantics.

---

## 18. API Design Principles

1. **Observable writes use deltas.** Direct state mutation is administrative or internal.
2. **Conflict time and stream time are separate.** HLC and `CommitSeq` have different jobs.
3. **Compacted state is authoritative.** Journals may be bounded when snapshots can rebuild consumers and peers.
4. **Status is explicit.** APIs and documentation must distinguish implemented guarantees from planned semantics.
5. **Ownership boundaries are visible.** Table owns row resolution; Database owns cross-table coordination.
6. **Private and public state are distinct.** Private consumer state is managed KV; public derived state is a normal table.
7. **Errors should be data, not panics.** Runtime misuse should increasingly return typed errors.
8. **Encoding order is deliberate.** Ordered backends require order-preserving key encodings.
9. **Dynamic execution is capability-limited.** Runtime code receives controlled context, not unrestricted database access.
10. **Recovery behavior is designed before guarantees are named.**

---

## 19. Observability Requirements

### 19.1 Current observability

Backends and tables expose basic stats:

- data size;
- dead bytes;
- pages;
- free pages;
- leaf utilization;
- entry counts;
- state backend stats;
- event journal stats.

### 19.2 Consumer observability

The consumer runtime must expose:

- status;
- current cursor;
- stream head;
- lag;
- bootstrap progress;
- processed record count;
- filtered record count;
- processing failures;
- retry count;
- private state size;
- queued output count;
- last processing duration;
- last successful processing time.

### 19.3 Operational inspection

Future tooling should inspect:

- database catalog;
- table configurations;
- table state and pending journal sizes;
- consumer definitions and dependency graph;
- retained commit-log range;
- replication state.

---

## 20. Security Requirements

### 20.1 Current status

Signatures are opaque bytes in `Delta`. No verification, authorization, encryption, or sandbox exists.

### 20.2 Required direction

- Verify replicated delta signatures before acceptance.
- Define table- and path-level authorization outside the pure type layer.
- Treat WASM consumers as untrusted code.
- Restrict consumer host capabilities.
- Prevent consumers from reading or writing undeclared tables unless explicitly authorized.
- Bound CPU, memory, output volume, and state size for dynamic consumers.
- Never expose raw unrestricted filesystem access to dynamic consumers.

---

## 21. Testing Strategy

### 21.1 Current strengths

The repository has extensive unit tests for:

- scalar LWW merge behavior;
- record recursive merge;
- set convergence;
- list ordering, placeholders, deletion, and convergence;
- text ordering, deletion, conflicts, and convergence;
- cell apply and merge behavior;
- backend persistence, compaction, ranges, and bulk operations;
- table cache, materialization, recovery, and merged iteration;
- database catalog behavior in the current working tree.

### 21.2 Required consumer tests

The consumer runtime must include:

- no-gap snapshot-to-stream bootstrap tests;
- stale delta versus resolved transition tests;
- set metadata change without visible membership change;
- duplicate delivery and recovery tests;
- filtered-event cursor advancement;
- pause/resume/restart;
- failure at every checkpoint boundary;
- derived-table chaining;
- cycle rejection;
- retention pressure from lagging consumers;
- deterministic replay;
- database reopen during bootstrap;
- output idempotency;
- external outbox retry.

### 21.3 Required property tests

Property testing should validate:

- CRDT merge commutativity, associativity, and idempotence;
- replay equivalence between journaled and materialized states;
- consumer incremental result equivalence to full recomputation;
- snapshot-plus-tail equivalence to a full snapshot at the final cursor.

---

## 22. Performance Requirements

### 22.1 Current performance direction

The storage layer already emphasizes:

- mmap-backed storage;
- reusable scratch buffers;
- direct serialized writes;
- bottom-up sorted bulk loading;
- reverse leaf traversal;
- in-memory indexes;
- compaction thresholds;
- O(1) table size accounting.

### 22.2 Consumer performance principles

- Filter structurally before cloning full rows where possible.
- Process records in bounded batches.
- Persist checkpoints at configurable batch boundaries.
- Avoid forcing every consumer to retain its own copy of source events.
- Allow consumers to request only the delivery detail they need.
- Measure bootstrap throughput and live lag separately.
- Do not optimize away before/after correctness before semantics are stable.

### 22.3 Future optimization path

After the whole-row `ChangeRecord` semantics are proven:

- compute compact path-level effects;
- reuse resolved row snapshots;
- use reference-counted immutable cells;
- coalesce changes only where consumer semantics permit;
- parallelize independent consumers;
- introduce indexed subscriptions.

---

## 23. Roadmap

### Phase 1: Harden the current table boundary

Priority: immediate

- Make the normal public mutation path delta-based.
- Separate direct state administration from observable writes.
- Validate table identity.
- Define explicit delta identity.
- Replace boolean-only internal mutation results with a richer effect result.
- Document and test crash windows around materialization.
- Complete and stabilize `Database`.

### Phase 2: Durable commit stream

- Add database-level `CommitSeq`.
- Add durable `CommitSeq -> ChangeRecord` storage.
- Emit accepted and resolved-change records.
- Add cursor range reads and retention metadata.
- Keep the commit log separate from table materialization journals.

### Phase 3: Native consumer runtime

- Add persistent consumer catalog.
- Add registered native consumer factories.
- Add lifecycle management.
- Add private state stores.
- Add structural subscriptions.
- Implement cooperative bounded-batch scheduling.
- Provide at-least-once replay.

### Phase 4: Snapshot-plus-stream bootstrap

- Capture source fence.
- Deliver resolved snapshot rows.
- Persist bootstrap progress.
- Replay tail without gaps.
- Support reset and rebuild.

### Phase 5: Derived tables and outbox

- Queue consumer-emitted deltas.
- Apply outputs after successful callbacks.
- Track origin consumer.
- Add dependency graph and cycle restrictions.
- Add durable external-action outbox.

### Phase 6: Transactional processing

- Design database-level atomic commit batches.
- Atomically commit state updates, outputs, and checkpoints.
- Upgrade supported delivery guarantees where justified.

### Phase 7: Replication

- Implement sync-policy resolution.
- Implement signature verification.
- Add live delta transport integration.
- Add state hashes, anti-entropy, and cold sync.

### Phase 8: Dynamic sandboxed consumers

- Define versioned WASM ABI.
- Add capability-limited host API.
- Add resource limits.
- Persist module identity and configuration.
- Define upgrade and state-migration semantics.

---

## 24. Open Design Questions

The following decisions require prototypes or additional design work:

1. Should `DeltaId` be distinct from HLC, and what is its encoded form?
2. Should `CommitSeq` be database-wide or partitioned while preserving a database-wide logical cursor?
3. What exact durability protocol makes state, commit log, and journal recovery atomic?
4. How should before/after effects be represented without cloning large rows?
5. What snapshot mechanism provides a stable fence without long writer pauses?
6. How should commit-log retention interact with paused durable consumers?
7. Which consumers are durable versus ephemeral?
8. How should consumer definitions be versioned when compiled code changes?
9. How are private state migrations performed?
10. How are derived-write cycles represented and controlled?
11. Should consumer output HLCs be allocated from a dedicated logical device identity?
12. How should replicated derived tables avoid duplicate computation on every replica?
13. What tombstone garbage-collection proof is compatible with offline replicas?
14. How should authorization apply to dynamic consumer subscriptions and outputs?
15. Should ordered table APIs use a distinct type instead of runtime panic on unordered state?

---

## 25. Acceptance Criteria for the Consumer Milestone

The first consumer-runtime milestone is complete when all of the following are true:

- A database owner can register a compiled Rust consumer instance at runtime.
- The consumer can subscribe to one or more tables.
- A new consumer scans existing resolved rows and then consumes all later changes without a gap.
- The runtime provides a monotonic durable cursor independent from HLC.
- The consumer can distinguish accepted no-op deltas from resolved row changes.
- The user-assignment example triggers only on a resolved absent-to-present transition.
- A stateful consumer can durably maintain a count.
- A consumer can publish a derived delta after successful processing.
- Consumer state and cursor survive database reopen.
- Pause, resume, restart, reset, and drop have documented and tested semantics.
- Duplicate replay does not corrupt the example aggregate.
- Failures are persisted and visible.
- The table materialization journal can be cleared without losing consumer replay data.
- Documentation clearly states that the initial delivery guarantee is at-least-once.

---

## 26. Final Architectural Summary

ZeninDB's architecture should preserve a strict separation of concerns:

```text
zendb-types
    Defines deterministic recursive CRDT state and conflict resolution.

zendb-storage
    Persists generic ordered and unordered data structures.

Table
    Resolves row deltas into materialized state and emits precise mutation effects.

Database
    Owns tables, commit order, durable change records, consumers, and cross-table outputs.

Consumer runtime
    Turns durable changes into private managed state, derived table deltas, and outbox actions.

Replication
    Moves deltas and compacted state between replicas without redefining local semantics.
```

The foundational rule for the next phase is:

> **HLC determines whether and how collaborative state changes. A durable local `CommitSeq` determines what a consumer processes next.**

Keeping those two notions of time separate allows ZeninDB to remain correct under stale deltas, late remote delivery, materialization, replay, bootstrap, independently paced consumers, and future replication.
