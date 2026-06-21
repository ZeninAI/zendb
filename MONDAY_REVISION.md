# Monday Revision

Note: the current engine API uses `operator` terminology, not `computation`.
Operators receive `Weak<Database>` in `open/process/finish`, and database
resources use fluent get-or-create calls:

- `database.table(name, Option<TableConfig>)`
- `database.state::<K, V>(name, Option<StateConfig>)`
- `database.register_operator(name, config)`

State ownership is no longer tracked by the database catalog. An operator that
creates temporary states is responsible for dropping them explicitly in
`finish()`.

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
- States
- Computation workers

Public table handles are direct concurrent handles:

```rust
pub type Table = Arc<RwLock<RawTable>>;
```

State handles follow the same shape:

```rust
pub type State<K, V> = Arc<RwLock<RawState<K, V>>>;
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
- `State { owner: Option<String>, config: StateConfig }`

Tables are reopened from the catalog when the database opens. State catalog
entries store backend configuration plus optional computation ownership; the
typed state handle is opened lazily when application or computation code asks
for it. Computations in the catalog are also restarted on database open.

The database stores tables under `tables/` and states under `states/`.

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
5. The worker calls `Computation::open` with a context so the computation can
   obtain tables and create/open states.

The worker loop is poll based:

1. Poll subscribed topic readers up to `computation_poll_size`.
2. If no changes are available, call `Executor::idle().await`.
3. Pass the batch to `Computation::process`.
4. If processing succeeds with `Continue`, commit topic offsets.
5. If processing fails, reset readers to committed offsets.
6. If processing returns `Finish`, call `Computation::finish`, then drop the
   computation registration and topic consumers.
7. If the database externally drops a computation, the worker is signaled to
   stop, runs `finish`, cleans up its consumers and owned states, and only then
   does `drop_computation` return.

The database does not require Tokio. The application supplies the executor:

```rust
pub trait Executor: Send + Sync + 'static {
    fn spawn(&self, future: RuntimeFuture);
    fn idle(&self) -> RuntimeFuture;
}
```

This keeps runtime selection application-specific.

## State Model

States are typed. They are not forced through
`State<Vec<u8>, Vec<u8>>`.

The storage layer still provides the raw backend:

```rust
RawState<K, V>
```

The engine exposes guarded state handles, matching the table handle pattern:

```rust
pub type State<K, V> = Arc<RwLock<RawState<K, V>>>;
```

State types and state names are no longer registered in
`ComputationRegistry`, and `ComputationConfig` no longer declares states.
Instead, computations create/open resources in `open()` and store the returned
guarded handles:

```rust
struct Totals {
    totals: Option<State<String, u64>>,
    output: Option<Table>,
}

impl Computation for Totals {
    fn open<'a>(&'a mut self, context: ComputationContext) -> BoxFuture<'a, io::Result<()>> {
        Box::pin(async move {
            self.totals = Some(context.state("totals", StateConfig::default())?);
            self.output = Some(context.table("summary")?);
            Ok(())
        })
    }

    fn process<'a>(
        &'a mut self,
        changes: Vec<Change>,
        context: ComputationContext,
    ) -> BoxFuture<'a, io::Result<ComputationStatus>> {
        Box::pin(async move {
            let totals = self.totals.as_ref().unwrap();
            totals.write().put("users".into(), changes.len() as u64)?;
            let output = self.output.as_ref().unwrap();
            let _guard = output.write();
            Ok(ComputationStatus::Continue)
        })
    }
}
```

The same `ComputationContext` API shape is available to `open`, `process`, and
`finish`, so a computation can either reuse stored handles or perform late
lookups when that is appropriate.

When a computation creates a state through the context, the catalog records
that computation as the state owner. `drop_computation` removes the computation
registration synchronously and drops any states owned by that computation.

Wrong typed lookup fails with a type mismatch error instead of returning an
incorrect state when a state is already open in the current process as another
`K, V`. After a database restart, there is no persisted Rust type identity or
schema registry; the first typed lookup opens the durable bytes as the caller's
requested `K, V`, so application code is responsible for using a compatible
type for existing data.

## Concurrency Model

The current concurrency model is deliberately explicit:

- Storage backends are single-threaded values.
- Tables are shared through `Arc<RwLock<RawTable>>`.
- States are shared through `Arc<RwLock<RawState<K, V>>>`.
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

The catalog now stores `State { owner, config }` entries and tables live under
the `tables/` directory. Databases created with previous table/state catalog or
path shapes are not compatible without a migration.
