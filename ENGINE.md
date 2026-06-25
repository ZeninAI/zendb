# ZendDB Engine

The engine API uses `operator` terminology. All operator lifecycle methods
receive an `OperatorContext`:

- `ctx.table(name, Option<TableConfig>) -> TableHandle`
- `ctx.state::<K, V>(name, Option<StateConfig>) -> StateHandle<K, V>`
- `ctx.register_timer(fire_at_ms, payload: Vec<u8>)`
- `ctx.register_timer_typed(fire_at_ms, &T)` — bincode-encoded convenience
- `ctx.cancel_timer(fire_at_ms)`
- `ctx.decode_timer::<T>(payload) -> T`
- `ctx.database() -> Option<Arc<Database>>`

Database-level APIs (callable from outside operators):

- `db.table(name, Option<TableConfig>) -> TableHandle`
- `db.state::<K, V>(name, Option<StateConfig>) -> StateHandle<K, V>`
- `db.register_operator(name, config)`
- `db.register_timer(operator, fire_at_ms, payload)`
- `db.cancel_timer(operator, fire_at_ms)`

State ownership is not tracked by the database catalog. An operator that
creates temporary states is responsible for dropping them explicitly in
`finish()`.

This document describes the current ZendDB engine model after the table/topic,
computation runtime, typed computation-state changes, the single-root ownership
model, and the processing-time timer subsystem.

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

The database is the **single lifecycle root** and the only strong owner of:

- Catalog
- Tables
- States
- Operator workers (each worker owns its operator instance)
- The shared timer store and the timer scheduler loop

`Database` is one `Arc<Database>` (no inner-Arc indirection). When the last
`Arc<Database>` is dropped, every table, state, operator worker, and the timer
scheduler is torn down deterministically.

Public table and state handles are **weak**:

```rust
pub struct TableHandle { /* Weak<RwLock<Table>> */ }
pub struct StateHandle<K, V> { /* Weak<RwLock<State<K, V>>> */ }
```

A handle never keeps a table, state, or the database alive. To run an operation
you upgrade the handle for that operation only:

```rust
let table = db.table("users", Some(TableConfig::default()))?; // TableHandle (weak)
table.get()?.write().insert_event(event)?;                    // strong only here
```

If the owning database has been dropped, `get()` fails with
`io::ErrorKind::NotConnected` instead of resurrecting a detached resource. This
removes the previous asymmetry where a table (strong `Arc`) could outlive the
database while operators (receiving `OperatorContext` backed by a `Weak<Database>`) could not.

`drop_table` / `drop_state` no longer require the application to release its
handles, because handles are weak. They still refuse to delete a resource while
an operation is **in flight**: an upgraded `get()` holds a strong reference for
its duration, so an overlapping drop returns `io::ErrorKind::WouldBlock` and the
caller may retry. Durably held handles do not block teardown.

### Why database-rooted ownership (not table-rooted)

Operators may subscribe to many tables or to `Subscription::all()`, and replication-style
operators subscribe to everything and may own no single table. Such operators
have no natural single owning table, so "the table owns the computation" cannot
express them. Rooting all ownership at the database matches Flink (a job is
owned by the runtime, not by its sources) and gives one uniform teardown rule.

## Catalog Model

The catalog stores durable resource metadata as `CatalogEntry`:

- `Table(TableConfig)`
- `Operator(OperatorConfig)`
- `State(StateConfig)`

Tables are reopened eagerly from the catalog when the database opens, so the
in-memory table map is the exhaustive set of existing tables. State catalog
entries store backend configuration only (no owner field); the typed state
handle is opened lazily when application or operator code first asks for it.
Operators in the catalog are restarted on database open. The shared timer store
is not a catalog entry — it is a fixed database-owned resource under `timers/`.

The database stores tables under `tables/`, states under `states/`, and the
shared timer store under `timers/`.

## Operator (Computation) Model

An operator is registered by implementation name in `OperatorRegistry`. The
catalog stores the implementation name and configuration bytes. On database
open, the registry recreates the operator instance.

An operator declares subscriptions as **glob patterns**:

```rust
Subscription::pattern("users")    // exact table name
Subscription::pattern("wiki-*")   // all tables starting with "wiki-"
Subscription::pattern("*-log")    // all tables ending with "-log"
Subscription::all()               // every table (equivalent to "*")
```

Multiple `*` wildcards are allowed; the pattern is not a regex. A
non-wildcard subscription (no `*`) is validated against the existing table
map when the operator is registered. A wildcard subscription is not validated
— the operator will be auto-subscribed to any matching table created later.

### Worker owns the operator

There is a single runtime entity per operator: `OperatorWorker`. The worker owns
the operator instance (`Mutex<Option<Box<dyn Operator>>>`) along with its
subscriptions, topic inputs, timer inbox, and control flags. `build_worker`
returns one `Arc<OperatorWorker>`; `spawn` takes the operator out of the worker
and drives it on the executor. There is no separate "worker handle plus detached
operator" split.

When an operator is created or reopened:

1. The database validates subscribed tables (the table map is exhaustive after
   `open`, so this is a membership check, not a lazy reopen).
2. The operator instance is created from the registry and placed in the worker.
3. A topic reader is created for every subscribed table (non-wildcard only at
   registration time; wildcard subscriptions pick up new tables automatically).
4. The worker is spawned on the application-provided executor.
5. The worker calls `Operator::open(ctx)`, where the operator obtains
   table/state handles and may register timers.

The worker loop is poll-based:

1. Poll subscribed topic readers up to `operator_poll_size` and drain the timer
   inbox.
2. If there are neither changes nor timers, call `Executor::idle().await`.
3. Fire each due timer through `Operator::on_timer(payload, ctx)`.
4. Pass any change batch to `Operator::process`.
5. On `Continue`, commit topic offsets.
6. On `process` failure: increment attempt counter, compute an exponential
   back-off with jitter (capped at `retry.max_delay_ms`), reset readers to
   committed offsets, and sleep via `Executor::sleep(delay)`. If
   `retry.max_attempts > 0` and the counter reaches the limit, run `finish`,
   then retire the operator.
7. On `Finish`, run `finish`, then retire the operator.
8. If the database externally drops an operator, the worker is signaled to stop,
   runs `finish`, cleans up its consumers/owned states/timers, and only then
   does `drop_operator` return.

The database does not require Tokio. The application supplies the executor:

```rust
pub trait Executor: Send + Sync + 'static {
    fn spawn(&self, future: RuntimeFuture);
    fn idle(&self) -> RuntimeFuture;
    fn sleep(&self, duration: Duration) -> RuntimeFuture;
}
```

This keeps runtime selection application-specific. `sleep` is used by the
retry policy in the worker loop; in a thread-per-task executor it may
delegate to `std::thread::sleep`.

## TypedOperator

`TypedOperator` is a convenience super-trait that lets an operator declare its
configuration and timer payload as associated types, eliminating manual bincode
boilerplate:

```rust
struct Totals { /* ... */ }

impl TypedOperator for Totals {
    type Config = TotalsConfig;   // serde / bincode, not ad-hoc bytes
    type Timer = TotalsTimer;     // decoded before handle_timer is called

    fn process<'a>(&'a mut self, changes: Vec<Change>, ctx: OperatorContext)
        -> BoxFuture<'a, io::Result<OperatorStatus>>
    { /* ... */ }

    fn handle_timer<'a>(&'a mut self, payload: TotalsTimer, ctx: OperatorContext)
        -> BoxFuture<'a, io::Result<()>>
    { /* ... */ }
}
```

A blanket `impl<T: TypedOperator> Operator for T` provides the `on_timer`
decode step and forwards to the typed `handle_timer`. Register with the
registry:

```rust
registry.register_operator::<Totals>("totals", |config: TotalsConfig| {
    Ok(Totals::new(config))
});
```

Build the `OperatorConfig` with the associated type inferred:

```rust
OperatorConfig::for_operator::<Totals>(
    "totals",
    &TotalsConfig { /* ... */ },
    vec![Subscription::pattern("users")],
    RetryConfig::default(),
)?
```

Use `type Timer = ()` for operators that never register timers (the default
`handle_timer` is a no-op).



Operators register Flink-style **processing-time** timers through
`OperatorContext`:

```rust
ctx.register_timer(fire_at_ms, payload);               // raw bytes
ctx.register_timer_typed(fire_at_ms, &value);          // bincode-encoded
ctx.cancel_timer(fire_at_ms);                          // cancellation
```

All operators share **one** ordered (B+ tree) timer store, keyed by
`TimerKey { fire_at_ms, operator }` with value `TimerEntry { payload }`.
The composite key gives one global firing order across every operator.
Within the same operator there is exactly **one** slot per
`(operator, fire_at_ms)` pair — registering again at the same time
overwrites the previous payload (last-write-wins; no FIFO for equal times on
the same operator). The store is persistent, so timers that have not yet come
due survive a restart and are re-armed on the next `open`.

A single timer **scheduler loop**, owned by the database lifecycle, uses a
condvar to sleep until the next timer's `fire_at_ms`, waking early when a
nearer timer is registered. It pops the due head of the store
(`fire_at_ms <= now`) and delivers each payload to the owning operator's
worker inbox. The worker fires `Operator::on_timer` from its own run loop, so
operator instances stay owned by exactly one task. When an operator is retired,
its pending timers are swept from the store.

Durability is at-most-once across a crash: a timer that has been removed from
the store but not yet fired is lost if the process dies in that window. This
matches the engine's existing "feedback loops allowed, no exactly-once" stance.

## Device reassignment (future)

An operator is fully described by `(implementation, configuration,
subscriptions)` in the catalog. Reassigning a running operator from device X to
device Y (a replica of the same database) is orchestration over that
description: remove the catalog entry and worker on X, recreate it on Y. No new
engine abstraction is required; the registry-by-name plus catalog-config model
already supports relocation. Operator state is either rebuilt from inputs or
shipped alongside the config.

## State Model

States are typed. They are not forced through `State<Vec<u8>, Vec<u8>>`.

The storage frontend provides the typed state primitive, re-exported by the
engine without renaming:

```rust
// zendb-engine
pub use zendb_storage::frontend::state::State; // State<K, V>
pub type ConcurrentState<K, V> = Arc<RwLock<State<K, V>>>;
```

The database owns each `ConcurrentState<K, V>` and hands out weak handles;
upgrade with `get()` for the duration of one operation:

```rust
pub struct StateHandle<K, V> { /* Weak<RwLock<State<K, V>>> */ }
pub struct TableHandle { /* Weak<RwLock<Table>> */ }
```

State types and state names are not registered in `OperatorRegistry`, and
`OperatorConfig` does not declare states. Instead, operators create/open
resources in `open()` through `OperatorContext` and store the returned weak
handles:

```rust
struct Totals {
    totals: Option<StateHandle<String, u64>>,
    output: Option<TableHandle>,
}

impl Operator for Totals {
    fn open<'a>(&'a mut self, ctx: OperatorContext) -> BoxFuture<'a, io::Result<()>> {
        Box::pin(async move {
            self.totals = Some(ctx.state("totals", Some(StateConfig::default()))?);
            self.output = Some(ctx.table("summary", None)?);
            Ok(())
        })
    }

    fn process<'a>(
        &'a mut self,
        changes: Vec<Change>,
        _ctx: OperatorContext,
    ) -> BoxFuture<'a, io::Result<OperatorStatus>> {
        Box::pin(async move {
            self.totals.as_ref().unwrap().get()?
                .write().put("users".into(), changes.len() as u64)?;
            let _guard = self.output.as_ref().unwrap().get()?.write();
            Ok(OperatorStatus::Continue)
        })
    }
}
```

`open`, `process`, `on_timer`, and `finish` all receive an `OperatorContext`,
which provides scoped access to tables, states, and timers. The context carries
the operator's own name so `register_timer` no longer needs an explicit name
argument, and the upgrade boilerplate (`Weak::upgrade().ok_or(...)`) is hidden
inside the context methods. Use `ctx.database()` for the rare case that requires
the raw `Arc<Database>`.

Wrong typed lookup fails with a type mismatch error instead of returning an
incorrect state when a state is already open in the current process as another
`K, V`. After a database restart, there is no persisted Rust type identity or
schema registry; the first typed lookup opens the durable bytes as the caller's
requested `K, V`, so application code is responsible for using a compatible
type for existing data.

## Concurrency Model

The current concurrency model is deliberately explicit:

- Storage backends are single-threaded values.
- The database holds the only durable strong references to tables and states
  (`Arc<RwLock<Table>>` / `Arc<RwLock<State<K, V>>>`); application and operator
  code hold weak `TableHandle` / `StateHandle` and upgrade per operation.
- Topic writing is single-writer.
- Topic reading supports multiple readers.
- The operator worker owns its topic readers, its operator instance, and its
  timer inbox.
- One shared ordered timer store is written by `register_timer` and by the
  single scheduler loop that drains due timers.

The important rule is that users and operators must not hold lock guards
across `.await`. If async work is required, copy or compute the minimal data
needed, drop the guard, then await.

## Current Flow Summary

Normal application write:

```text
application
  -> db.table("users", cfg)        // TableHandle (weak)
  -> handle.get()?                 // upgrade for this operation
  -> .write().insert_event(event)
  -> table resolves event into cache/state
  -> table appends Change to topic
```

Operator processing:

```text
OperatorWorker
  -> poll table topic readers + drain timer inbox
  -> fire on_timer for each due timer via ctx
  -> collect Change batch
  -> operator.process(batch, ctx).await
  -> commit offsets on success
  -> reset readers + exponential sleep on failure
  -> retire on Finish or max_attempts exceeded
```

Operator writing back:

```text
operator.process
  -> ctx.table("target", None)?.get()?
  -> .write().insert_event(event)
  -> target table topic receives another Change
  -> subscribed operators eventually poll it
```

Timer flow:

```text
ctx.register_timer(fire_at_ms, payload)   // insert into shared B+ tree + signal condvar
scheduler loop  -> condvar.wait_for(next_due - now)
                -> pop due head -> deliver to worker inbox
worker loop     -> operator.on_timer(payload, ctx)
ctx.cancel_timer(fire_at_ms)              // delete from B+ tree
```

Feedback loops are allowed. The system does not attempt to detect or prevent
infinite operator loops.

## Future Tasks

The ownership model, worker/operator unification, weak handles, shared timer
subsystem, OperatorContext, glob subscriptions, retry policy with exponential
back-off, condvar-based timer scheduler sleep, and timer cancellation are all in
place and tested. The items below are prioritized next steps.

### Correctness & safety

1. **Close the drop-vs-handle race in `finish_drop_table` / `delete_state`.**
   Teardown decides to delete by checking `Arc::strong_count(...) == 1` under the
   resource-map lock, but `TableHandle::get()` / `StateHandle::get()` upgrade the
   `Weak` *without* taking the lifecycle lock. A `get()` that upgrades between the
   count check and the `fs::remove_dir_all` can leave an operation running against
   files that were just deleted. Tighten this with a generation/tombstone marker,
   or have `get()` participate in the lifecycle lock so the check-then-act is
   atomic across both synchronization domains.

2. **Prevent operator self-teardown deadlock.** If an operator's `process`
   synchronously calls `db.drop_table` (or `drop_operator`) on its *own* only
   subscription, `prepare_drop_table` marks the worker orphaned and `drop_table`
   then blocks in `worker.wait_finished()` — but the worker is the current thread
   inside `process`, so it never reaches `mark_finished()`. This deadlocks. Detect
   reentrant teardown from inside a worker and either reject it or defer the
   teardown until after the current batch returns.

3. **Crash consistency for create/drop.** Resource creation does `Table::create`
   (writes files) *then* `catalog.put`; drop does `catalog.delete` *then*
   `fs::remove_dir_all`. A crash between the two steps leaves an orphan directory
   or a dangling catalog entry. Reconcile stray resource directories against the
   catalog on `open`, making the catalog the source of truth.

4. **Resilient `open`.** A single corrupt table or operator currently fails the
   whole `Database::open`. Add quarantine/skip-with-report semantics so one bad
   resource does not make the entire database unopenable.

5. **Persist a typed-state fingerprint.** After restart the first typed lookup
   blindly reinterprets the durable bytes as the caller's `K, V`; a wrong type
   silently misreads data. Store a type fingerprint in the `State` catalog entry
   and reject mismatched reopen with a clear error.

### Scheduling & performance

6. **Event-driven worker wakeups.** Workers poll on `Executor::idle()` (fixed
   ~1ms). With many operators this is N threads spinning. Signal a worker when
   its subscribed topic is appended to. Requires touching `zendb-storage`'s
   `Table` to add a write-notification hook, or introducing a new engine-level
   write API that the engine can intercept.

7. **Backpressure & consumer-lag visibility.** A slow operator pins topic
   segments (compaction is bounded by the minimum committed offset), so disk can
   grow unbounded with no signal. Expose per-consumer lag and add a
   max-lag/backpressure policy.

8. **Bound the timer inbox.** `OperatorWorker::timer_inbox` is an unbounded
   `VecDeque`; a slow operator with many due timers can accumulate without limit.
   Add a bound or coalescing strategy.

9. **Monotonic-aware processing time.** `now_ms()` reads wall-clock
   `SystemTime`, which can jump backward (NTP/DST), delaying or early-firing
   timers. Consider a monotonic base anchored to a wall-clock reference.

### Public API & observability

10. **Introspection / listing API.** There is no public way to enumerate tables,
    states, or operators — tests reach into `pub(crate)` maps. Add
    `db.tables()`, `db.operators()`, `db.states()` and per-resource metadata
    accessors.

11. **Metrics & health surface.** Everything is `log::error!`. Expose structured
    per-operator health: processed counts, last error, retry attempt count, lag,
    state size, pending timer count, and running/stopped status.

12. **Event-time timers.** Only *processing-time* timers exist. Consider
    *event-time* timers driven by watermarks for deterministic replay.

### Feature operators (the motivating use cases)

13. **Scripted operators (Rhai / DSL).** A `ScriptOperator` that accepts source
    text and runs the operator lifecycle inside an embedded scripting engine.
    `OperatorContext` is the natural capability-scoped sandbox to expose to the
    script.

14. **Index operators (BM25 / vector).** Materialized-index operators that
    subscribe to a text table, maintain an index in operator state (using
    `OperatorContext::state`), and expose a fast typed query path.

15. **Replication operator.** A `Subscription::all()` operator
    that ships change deltas to peer replicas and ingests remote deltas as local
    inserts. Needs a network transport abstraction, debounce/batching, and CRDT
    merge — events already carry `Hlc` + `device_id` + a `sync` flag to build on.

16. **External-API operators.** Side-effecting operators that call out to external
    services with the retry/backoff policy applied to network calls.

### Distribution

17. **Device reassignment handoff.** Implement the orchestration sketched in
    [Device reassignment](#device-reassignment-future): remove the catalog entry
    and worker on the source device, recreate from `(implementation,
    configuration, subscriptions)` on the target, and decide per operator whether
    state is rebuilt from inputs or shipped alongside the config.

## Compatibility Note

The catalog stores `Table` / `Operator` / `State` entries; tables live under
`tables/`, states under `states/`, and the shared timer store under `timers/`.
Databases created with previous catalog or path shapes are not compatible
without a migration.
