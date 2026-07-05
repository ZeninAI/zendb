# zendb-engine

**Table engine with a streaming operator runtime, processing-time timers,
and a database-rooted ownership model.**

---

## Overview

`zendb-engine` composes the generic storage backends from `zendb-storage` into
a database that owns tables, states, operators, and timers. Operators are
streaming computations that subscribe to tables, process changes incrementally,
maintain private state, and publish derived results.

---

## Database

`Database<D>` is the **single lifecycle root**. It holds strong references to
every table, state, operator worker, and the shared timer store. When the last
`Arc<Database>` is dropped, everything is torn down deterministically.

### Lifecycle

```rust
let db = Database::<MyOps>::create("/path/to/db", executor, DatabaseConfig::default())?;
// or
let db = Database::<MyOps>::open("/path/to/db", executor, DatabaseConfig::default())?;
```

### Catalog

Durable catalogs (each backed by a `KeyDir`):

| Catalog | File | Stores |
|---|---|---|
| Tables | `_tables` | `String → TableConfig` |
| States | `_states` | `String → StateConfig` |
| Operators | `_operators` | `String → OperatorEntry<Config>` |

Disk layout:

```text
<database>/
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
let handle = db.table("users", Some(TableConfig::default()))?;   // TableHandle (weak)
let table = handle.get()?;                                       // upgrade for one operation
table.write().insert_event(event)?;
```

- `db.table(name, config)` — open or create; returns a weak `TableHandle`
- `db.close_table(name)` — evict from memory (durable catalog entry remains)
- `db.delete_table(name)` — evict + remove from catalog + delete files
- `db.contains_table(name)` / `db.is_table_open(name)` / `db.list_tables()`

**`TableHandle`** is a `Weak<RwLock<Table>>`. It never keeps a table or database
alive. `get()` upgrades for one operation and fails with `NotConnected` if the
database was dropped.

A `Table` owns:
- **Materialized state** — `State<PrimaryKey, Cell>` (ordered or unordered backend)
- **Resolved cache** — `SkipList<PrimaryKey, Cell>` shadowing pending rows
- **Change topic** — `Topic<Change>` with a single writer, multiple consumer readers

---

## States

```rust
let handle = db.state::<String, u64>("totals", Some(StateConfig::default()))?;
let state = handle.get()?;
state.write().put("count".into(), 42)?;
```

- `db.state::<K, V>(name, config)` — open or create typed state
- `db.close_state(name)` — evict from memory
- `db.delete_state(name)` — evict + remove from catalog + delete files

States are **typed** (`State<K, V>`), not forced through `Vec<u8>`.
Type safety is runtime-only — there is no persisted schema registry.

---

## Operators

Operators are streaming computations that subscribe to table changes.
They implement the `Operator` trait:

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

Each method receives `&Arc<Database<D>>` directly — there is no separate context
object. Operators open tables/states, register timers, and read/write data
through the database reference.

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
type MyDb = Database<ops::OperatorInstance>;
```

Then dispatch at runtime:

```rust
db.dispatch_operator::<MyCustom>(
    "my-op",
    MyCustomConfig { /* ... */ },
    OperatorRuntimeConfig {
        subscriptions: vec![Subscription::pattern("users")],
        poll_size: 128,
    },
)?;
```

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
- **`Cancelled`** — permanently cancelled by `db.cancel_operator(name)`

Errors in `process`, `on_timer`, or lifecycle callbacks transition the operator
to `Failed` and retire it.

### Cancel and Delete

```rust
db.cancel_operator("my-op")?;
db.delete_terminal_operator("my-op")?;
```

### Facets

Operators expose a typed query interface via the `Facet` associated type.
The facet is produced after `create()` and stored in the worker, retrievable
with `db.facet::<F>(name)`. It typically wraps `StateHandle` clones so
queries read directly from operator state without locking the processing loop:

```rust
impl Operator for IndexerOp {
    type Facet = IndexerFacet;
    fn facet(&self) -> IndexerFacet { IndexerFacet { index: self.index.clone() } }
}

let facet = db.facet::<IndexerFacet>("indexer")?;
let results = facet.lookup("hello")?;
```

Use `type Facet = ()` for operators without a query interface.

---

## Processing-Time Timers

All operators share one ordered `BPlusTree` timer store keyed by
`(fire_at_ms, operator)`. Operators register timers through the database:

```rust
db.register_timer("my-op", fire_at_ms, &payload)?;
db.cancel_timer("my-op", fire_at_ms)?;
```

A background scheduler loop sleeps until the next due time (via condvar),
delivers payloads to operator worker inboxes, and the worker fires
`on_timer` from its run loop.

Timers are **persistent** — they survive restart. Durability is at-most-once:
a timer removed from the store but not yet fired is lost across a crash.

---

## Concurrency Model

- Storage backends are single-threaded values behind `RwLock`
- The database holds strong `Arc<RwLock<Table>>` / `Arc<RwLock<State<K, V>>>`
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
├── database/
│   ├── mod.rs            # Database struct, DatabaseConfig, handles, catalogs
│   ├── tables.rs         # table(), close_table(), delete_table()
│   ├── states.rs         # state(), close_state(), delete_state()
│   ├── operators.rs      # dispatch_operator(), cancel_operator(), retire_operator()
│   └── timers.rs         # register_timer(), cancel_timer(), scheduler loop
└── operator/
    ├── mod.rs            # Architecture docs, re-exports
    ├── config.rs         # Subscription, OperatorRuntimeConfig
    ├── lifecycle.rs      # OperatorPhase, OperatorDirective
    ├── traits.rs         # Operator, DispatchOperator, DispatchConfig traits
    ├── macros.rs         # define_operator_set! macro
    ├── worker.rs         # OperatorWorker: inputs, timers, events, spawn
    ├── run_loop.rs       # Async run loop driving the operator lifecycle
    └── prelude/          # Built-in operators (FullTextIndex, MerkleTree, Rhai)
```
