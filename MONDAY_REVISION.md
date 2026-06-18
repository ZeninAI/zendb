# Monday Revision

This document describes the current ZendDB engine model after the table/topic,
computation runtime, and typed computation-state changes.

## Storage Model

ZendDB storage has two different primitives with different jobs:

- **Backends** are key-value stores used for table materialized state and
  computation state.
- **Topics** are append-only logs used to publish table changes to
  computations.

Tables still implement the backend traits so they can be queried like a
materialized key-value store, but table changes are now published through the
table's topic. The topic is not a backend because it is not key-value storage.

## Table Model

A table owns:

- Persistent materialized state: `State<PrimaryKey, Cell>`
- In-memory resolved cache: `SkipList<PrimaryKey, (Cell, bool)>`
- Durable change topic: `Topic<Change>` plus a single `TopicWriter<Change>`

The table write path is:

1. Caller obtains a table write guard.
2. Caller invokes `insert_event(event)`.
3. The table resolves the event against the in-memory cache first.
4. If the row is not cached, the table reads the current row from state.
5. The event is applied to the resolved `Cell`.
6. If the event changes the cell, the cache is updated.
7. The resulting `Change` is appended to the table topic.
8. If the cache exceeds `max_buffered_records`, cached rows are materialized
   into state.

The table does not expose direct public event-log APIs. The only event mutation
path is `insert_event`.

Read APIs merge materialized state and cache, with cache entries shadowing
state entries. This preserves the table view before cache materialization.

## Topic Model

The topic is the durable change stream for a table.

The current concurrency assumption is:

- Single writer
- Multiple readers

The writer is exclusive and owned by the table. Readers are created per
consumer. A computation normally has one topic reader for each subscribed
table.

Consumer offsets have two layers:

- **Volatile offset**: in-memory progress made by an active reader.
- **Committed offset**: durable progress persisted after computation success.

Dropping a reader does not revert volatile progress. Reopening the database
does revert to committed offsets because volatile offsets are intentionally
not durable.

Compaction is based on committed offsets. Segments older than the minimum
committed consumer offset may be discarded, while active readers keep segment
references alive long enough to finish reading safely.

## Database Ownership

The database is the lifecycle owner of:

- Catalog
- Tables
- Shared computation states
- Local computation states
- Computation workers

Public table handles are direct concurrent handles:

```rust
pub type Table = Arc<RwLock<RawTable>>;
```

The database does not wrap table operations in a proxy API. Application code
and computations explicitly acquire read or write guards:

```rust
let table = db.table("users")?;
table.write().insert_event(event)?;
```

This keeps lock ownership visible and avoids hiding long-lived read/write
guards behind fluent wrapper methods.

## Catalog Model

The catalog stores durable resource metadata:

- `Table(TableConfig)`
- `Computation(ComputationConfig)`
- `SharedState { owner, implementation, config }`

Tables and shared states are reopened from the catalog when the database opens.
Computations in the catalog are also restarted on database open.

Local computation states are not catalog entries by themselves. They are owned
through the computation configuration and opened from the computation state
directory when the computation is opened.

## Computation Model

A computation is registered by implementation name in `ComputationRegistry`.
The catalog stores the implementation name and configuration bytes. On database
open, the registry is used to recreate the computation instance.

A computation declares subscriptions:

- `Subscription::Table(name)`
- `Subscription::AllTables`

When a computation is created or reopened:

1. The database validates subscribed tables.
2. The computation instance is created from the registry.
3. A topic reader is created for every subscribed table.
4. A `ComputationWorker` is spawned on the application-provided executor.

The worker loop is poll based:

1. Poll subscribed topic readers up to `computation_poll_size`.
2. If no changes are available, call `Executor::idle().await`.
3. Pass the batch to `Computation::process`.
4. If processing succeeds with `Continue`, commit topic offsets.
5. If processing fails, reset readers to committed offsets.
6. If processing returns `Finish`, drop the computation and its owned states.

The database does not require Tokio. The application supplies the executor:

```rust
pub trait Executor: Send + Sync + 'static {
    fn spawn(&self, future: RuntimeFuture);
    fn idle(&self) -> RuntimeFuture;
}
```

This keeps runtime selection application-specific.

## Computation State Model

Computation states are now typed. They are not forced through
`State<Vec<u8>, Vec<u8>>`.

The storage layer still provides:

```rust
State<K, V>
```

The engine stores heterogeneous typed states internally using erased state
handles, and computations recover the real type by requesting:

```rust
context.local_state::<K, V>("name")?;
context.shared_state::<K, V>("name")?;
```

The state type is selected by a stable state implementation name registered in
`ComputationRegistry`:

```rust
registry.register_state::<String, u64>("string-u64");
```

The computation configuration refers to that implementation:

```rust
StateDefinition {
    name: "totals".into(),
    visibility: StateVisibility::Shared,
    implementation: "string-u64".into(),
    config: StateConfig::default(),
}
```

At runtime, the state is accessed as:

```rust
let totals = context.shared_state::<String, u64>("totals")?;
totals.put("users".into(), 42)?;
```

`StateRef<K, V>` exposes:

- `read()` for direct read-guard access to `State<K, V>`
- `write()` for direct write-guard access to `State<K, V>`
- `get`
- `put`
- `delete`

Wrong typed lookup fails with a type mismatch error instead of returning an
incorrect state.

## State Visibility

There are two computation-state visibility modes:

- **Local**: owned by one computation and stored under that computation.
- **Shared**: cataloged database resource, owned by the creating computation,
  and accessible by name.

When a computation is dropped or finishes, its declared local and shared states
are removed from the database registries and marked inactive. Physical deletion
is delayed until outstanding state handles are dropped.

## Concurrency Model

The current concurrency model is deliberately explicit:

- Storage backends are single-threaded values.
- Tables are shared through `Arc<RwLock<RawTable>>`.
- Computation states are shared through typed state refs backed by `RwLock`.
- Topic writing is single-writer.
- Topic reading supports multiple readers.
- The computation worker owns its topic readers.

The important rule is that users and computations must not hold lock guards
across `.await`. If async work is required, copy or compute the minimal data
needed, drop the guard, then await.

## Current Flow Summary

Normal application write:

```text
application
  -> db.table("users")
  -> table.write()
  -> insert_event(event)
  -> table resolves event into cache/state
  -> table appends Change to topic
```

Computation processing:

```text
ComputationWorker
  -> poll table topic readers
  -> collect Change batch
  -> computation.process(batch, context).await
  -> commit offsets on success
  -> reset readers on failure
```

Computation writing back:

```text
computation.process
  -> context.table("target")
  -> table.write()
  -> insert_event(event)
  -> target table topic receives another Change
  -> subscribed computations eventually poll it
```

Feedback loops are allowed. The system does not attempt to detect or prevent
infinite computation loops.

## Compatibility Note

The catalog now stores state implementation names for shared states. Databases
created with the older computation-state catalog shape are not compatible
without a migration.
