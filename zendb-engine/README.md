# zendb-engine

**Workspace engine with tables, a streaming operator runtime, processing-time
timers, and workspace-rooted ownership.**

---

## Overview

`zendb-engine` composes the generic storage backends from `zendb-storage` into
a workspace that owns tables, states, operators, and timers. Operators are
streaming computations that subscribe to tables, process changes incrementally,
maintain private state, and publish derived results.

---

## Workspace

`Workspace<D>` is the **single lifecycle root**. It holds strong references to
every table, state, operator worker, and the shared timer store. When the last
`Arc<Workspace>` is dropped, everything is torn down deterministically.

### Lifecycle

```rust
let workspace = Workspace::<MyOps>::create("/path/to/workspace", executor, WorkspaceConfig::default())?;
// or
let workspace = Workspace::<MyOps>::open("/path/to/workspace", executor, WorkspaceConfig::default())?;
```

### Catalog

Durable catalogs (each backed by a `KeyDir`):

| Catalog | File | Stores |
|---|---|---|
| Tables | `_tables` | `String → TableConfig` |
| States | `_states` | `String → StateConfig` |
| Operators | `_operators` | Local native execution adapter entries |

Disk layout:

```text
<workspace>/
├── _tables           # table catalog
├── _states           # state catalog
├── _operators        # operator catalog
├── _timers           # shared timer store (B+tree)
├── tables/
│   └── <name>/       # per-table directory
└── states/
    └── <name>/       # per-state directory
```

---

## Tables

```rust
let handle = workspace.table("users", Some(TableConfig::default()))?; // TableHandle (weak)
let table = handle.get()?;                                       // upgrade for one operation
table.write().insert_event(event)?;
```

- `workspace.table(name, config)` — open or create; returns a weak `TableHandle`
- `workspace.close_table(name)` — evict from memory (durable catalog entry remains)
- `workspace.delete_table(name)` — evict + remove from catalog + delete files
- `workspace.contains_table(name)` / `workspace.is_table_open(name)` / `workspace.list_tables()`

**`TableHandle`** is a `Weak<RwLock<Table>>`. It never keeps a table or workspace
alive. `get()` upgrades for one operation and fails with `NotConnected` if the
workspace was dropped.

A `Table` owns:
- **Materialized state** — `State<PrimaryKey, Cell>` (ordered or unordered backend)
- **Resolved cache** — `SkipList<PrimaryKey, Cell>` shadowing pending rows
- **Change topic** — `Topic<Change>` with a single writer, multiple consumer readers

---

## States

```rust
let handle = workspace.state::<String, u64>("totals", Some(StateConfig::default()))?;
let state = handle.get()?;
state.write().put("count".into(), 42)?;
```

- `workspace.state::<K, V>(name, config)` — open or create typed state
- `workspace.close_state(name)` — evict from memory
- `workspace.delete_state(name)` — evict + remove from catalog + delete files

States are **typed** (`State<K, V>`), not forced through `Vec<u8>`.
Type safety is runtime-only — there is no persisted schema registry.

---

## Operators

Operators are streaming computations that subscribe to table changes. The
client-side control model separates the durable desired object from its local
worker:

- `zendb_types::OperatorSpec` is desired state: class, source, inputs,
  placement, permissions, approvals, retries, and outputs.
- `OperatorObservation` is status: generation, condition, worker, lease, and
  error information.
- `Workspace` owns the local operator catalog, worker lifecycle, observations,
  jobs, checkpoints, and lease records. These are workspace-owned state, not
  interchangeable public service traits.
- `LeaseConsistency` records whether a lease is advisory or authoritative;
  the reconciler still validates fencing epochs before shared writes.
- `plan_reconciliation` computes actions; it does not silently mutate desired
  state or assume a central scheduler.
- `AuthorizationEvaluator` and the Workspace effect path authorize exact
  writes, capability invocations, jobs, and shared publication.
- `CapabilityHost` combines local capability descriptors and execution.

The current `Operator` trait is the native execution ABI used by the local
runner. It is not itself the control-plane object. It implements:
Compiled Rust operators are trusted application extensions. Scripted or
externally supplied operators use the `CapabilityHost` and Workspace
authorization boundary instead of receiving this native Workspace reference.

```rust
pub trait Operator: Send + 'static {
    type Config: Debug + Clone + PartialEq + Encode + Decode<()> + 'static;
    type Timer: Encode + Decode<()> + 'static;
    type Facet: Send + Sync + 'static;

    fn create(db, name, config) -> impl Future<Output = io::Result<Self>> + Send;
    fn facet(&self) -> Self::Facet;
    fn process(&mut self, changes, db, name, config) -> impl Future<Output = io::Result<OperatorDirective>> + Send;
    fn on_timer(&mut self, payload, fire_at_ms, db, name, config) -> impl Future<Output = io::Result<OperatorDirective>> + Send;
    fn on_input_opened(&mut self, table, db, name, config) -> impl Future<Output = io::Result<OperatorDirective>> + Send;
    fn on_input_closed(&mut self, table, db, name, config) -> impl Future<Output = io::Result<OperatorDirective>> + Send;
    fn teardown(&mut self, phase, db, name, config) -> impl Future<Output = io::Result<()>> + Send;
}
```

Each method receives `&Arc<Workspace<D>>` directly because this is the current
native engine ABI. It is not a permission bypass: table writes, shared event
publication, jobs, and capabilities must be checked against the active
operator policy. User-authored Rhai code does not receive this Rust object.

### Dispatching

Operators are registered through the `define_operator_set!` macro, which
generates a dispatch enum:

```rust
define_operator_set! {
    pub mod ops {
        FullTextIndex(FullTextIndexOperator),
        MerkleTree(MerkleTreeOperator),
        Rhai(RhaiOperator),
        MyCustom(MyCustomOperator),
    }
}

// Type alias for convenience
type MyWorkspace = Workspace<ops::OperatorInstance>;
```

Then dispatch locally for embedded/test use:

```rust
workspace.dispatch_operator::<MyCustom>(
    "my-op",
    MyCustomConfig { /* ... */ },
    OperatorRuntimeConfig {
        subscriptions: vec![Subscription::pattern("users")],
        poll_size: 128,
    },
)?;
```

`workspace.dispatch_operator` is a low-level immediate realization API. A
cluster-aware application should persist a `zendb_types::OperatorSpec` and let
the local reconciler decide whether to start a worker, acquire a lease, or
remain stopped on this device.

### Subscriptions

Operators declare which tables they read using glob patterns:

```rust
Subscription::pattern("users")     // exact match
Subscription::pattern("wiki-*")    // prefix
Subscription::pattern("*-log")     // suffix
Subscription::pattern("*")         // all tables
```

### Lifecycle

```text
create  →  on_input_opened (for each matching table)
         →  ACTIVE LOOP:
              process(changes)  |  on_timer(payload)  |  on_input_opened / _closed
         →  teardown(phase)     ← reason: Active/Finished/Failed/Cancelled
```

- **`Active`** — operator suspended (no open inputs); may be respawned later
- **`Finished`** — operator returned `OperatorDirective::Finish`
- **`Failed { error }`** — unrecoverable error
- **`Cancelled`** — permanently cancelled by `workspace.cancel_operator(name)`

Errors in `process`, `on_timer`, or lifecycle callbacks transition the operator
to `Failed` and retire it.

### Cancel and Delete

```rust
workspace.cancel_operator("my-op")?;
workspace.delete_terminal_operator("my-op")?;
```

### Facets

Operators expose a typed query interface via the `Facet` associated type.
The facet is produced after `create()` and stored in the worker, retrievable
with `workspace.facet::<F>(name)`. It typically wraps `StateHandle` clones so
queries read directly from operator state without locking the processing loop:

```rust
impl Operator for IndexerOp {
    type Facet = IndexerFacet;
    fn facet(&self) -> IndexerFacet { IndexerFacet { index: self.index.clone() } }
}

let facet = workspace.facet::<IndexerFacet>("indexer")?;
let results = facet.lookup("hello")?;
```

Use `type Facet = ()` for operators without a query interface.

---

## Processing-Time Timers

All operators share one ordered `BPlusTree` timer store keyed by
`(fire_at_ms, operator)`. Operators register timers through the database:

```rust
workspace.register_timer("my-op", fire_at_ms, &payload)?;
workspace.cancel_timer("my-op", fire_at_ms)?;
```

A background scheduler loop sleeps until the next due time (via condvar),
delivers payloads to operator worker inboxes, and the worker fires
`on_timer` from its run loop.

Timers are **persistent** — they survive restart. Durability is at-most-once:
a timer removed from the store but not yet fired is lost across a crash.

---

## Concurrency Model

- Storage backends are single-threaded values behind `RwLock`
- The workspace holds strong `Arc<RwLock<Table>>` / `Arc<RwLock<State<K, V>>>`
- Application and operator code hold weak `TableHandle` / `StateHandle`
- Topic writing is single-writer; reading supports multiple concurrent consumers
- Each operator worker owns its inputs, timer inbox, and event queue
- **Rule:** never hold a lock guard across `.await`

---

## Executor Abstraction

The engine does not depend on Tokio. The application provides an executor:

```rust
pub trait Executor: Send + Sync + 'static {
    fn spawn(&self, future: RuntimeFuture);
    fn idle(&self) -> RuntimeFuture;
    fn sleep(&self, duration: Duration) -> RuntimeFuture;
}
```

---

## Module Structure

```
src/
├── lib.rs                # Crate root, re-exports
├── runtime.rs            # Executor trait
├── workspace/
│   ├── mod.rs            # Workspace implementation, config, handles, catalogs
│   ├── tables.rs         # table(), close_table(), delete_table()
│   ├── states.rs         # state(), close_state(), delete_state()
│   ├── operators.rs      # dispatch_operator(), cancel_operator(), retire_operator()
│   └── timers.rs         # register_timer(), cancel_timer(), scheduler loop
└── operator/
    ├── mod.rs            # Architecture docs, re-exports
    ├── config.rs         # Subscription, OperatorRuntimeConfig
    ├── control.rs        # Workspace operator control and effect interfaces
    ├── lifecycle.rs      # OperatorPhase, OperatorDirective
    ├── traits.rs         # Operator, DispatchOperator, DispatchConfig traits
    ├── macros.rs         # define_operator_set! macro
    ├── worker.rs         # OperatorWorker: inputs, timers, events, spawn
    ├── run_loop.rs       # Async run loop driving the operator lifecycle
    └── prelude/          # Built-in operators (FullTextIndex, MerkleTree, Rhai)
```
