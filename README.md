# ZeninDB

**An embedded, local-first, eventually consistent database with CRDT types,
offline mutation, streaming operators, and typed query facets.**

---

## Table of Contents

1. [What Is ZeninDB?](#what-is-zenindb)
2. [Architecture](#architecture)
3. [Crate Map](#crate-map)
4. [Quick Start](#quick-start)
5. [Core Concepts](#core-concepts)
   - [The Database Facet](#the-database-facet)
   - [Tables and Events](#tables-and-events)
   - [Operators](#operators)
   - [States](#states)
   - [Timers](#timers)
6. [Operator Lifecycle](#operator-lifecycle)
7. [Walkthrough: A Document Indexer](#walkthrough-a-document-indexer)
8. [Storage Backends](#storage-backends)
9. [Building and Testing](#building-and-testing)

---

## What Is ZeninDB?

ZeninDB is an embedded Rust database designed for applications that need:

- **Conflict-free collaborative data structures** — counters, sets, lists,
  registers, priority queues, and collaborative text, all nested inside
  typed records
- **Offline-first writes** with deterministic CRDT merge when devices
  reconnect
- **Streaming operators** — attach long-running computations to tables that
  process changes incrementally, maintain private durable state, and
  publish derived results back into tables
- **Typed query facets** — operators expose a public read-only query API
  that applications use directly, without locking the operator or blocking
  its processing loop
- **File-backed durability** with ordered (B+tree) and unordered (Bitcask)
  backends, plus a segmented append-only topic log
- **No external service** — link it into your application binary

ZeninDB is *not* a SQL database, a distributed consensus system, or a
cloud control plane. It is a local engine that you embed, configure with
your own operator types, and query through typed handles.

---

## Architecture

```text
┌──────────────────────────────────────────────────────────┐
│                    Your Application                       │
│                                                          │
│  • Define operator types (define_operator_set!)           │
│  • Create/open Database<YourOps>                          │
│  • Insert events into tables                              │
│  • Query facets from running operators                    │
│  • Cancel / retire operators at runtime                   │
└────────────┬──────────────────────────────┬──────────────┘
             │                              │
    ┌────────▼────────┐            ┌────────▼────────┐
    │   Table Handles  │            │   Facet Queries  │
    │  (weak, upgrade  │            │  (typed, direct  │
    │   per operation) │            │   state reads)   │
    └────────┬────────┘            └────────┬────────┘
             │                              │
┌────────────▼──────────────────────────────▼──────────────┐
│                     zendb-engine                          │
│                                                          │
│  ┌──────────┐  ┌───────────────┐  ┌───────────────────┐  │
│  │ Database │  │ OperatorWorker│  │     RunLoop       │  │
│  │          │  │               │  │                   │  │
│  │ • tables │──│ • inputs      │──│ • event queue     │  │
│  │ • states │  │ • timer inbox │  │ • shutdown FSM    │  │
│  │ • ops    │  │ • facet       │  │ • poll + commit   │  │
│  │ • timers │  │ • spawn       │  │ • idle / wake     │  │
│  └──────────┘  └───────────────┘  └────────┬──────────┘  │
│                                            │              │
│                                     ┌──────▼──────┐      │
│                                     │  Operator   │      │
│                                     │ (your code) │      │
│                                     └─────────────┘      │
└──────────────────────────┬───────────────────────────────┘
                           │
┌──────────────────────────▼───────────────────────────────┐
│                    zendb-storage                          │
│                                                          │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐ │
│  │ BPlusTree│  │  KeyDir  │  │ SkipList │  │  Topic   │ │
│  │ (ordered)│  │(unordered)│  │(in-memory)│  │(app-log) │ │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘ │
└──────────────────────────┬───────────────────────────────┘
                           │
┌──────────────────────────▼───────────────────────────────┐
│                    zendb-types                            │
│                                                          │
│  Cell, Event, HLC, Path, Op, Value, CRDT types,          │
│  register_types! macro, generated dispatch                │
└──────────────────────────────────────────────────────────┘
```

The dependency direction is strict: `zendb-types` → `zendb-storage` → `zendb-engine`.
Types know nothing about storage; storage knows nothing about ZeninDB semantics;
the engine composes them together.

---

## Crate Map

| Crate | Purpose | Key Exports |
|---|---|---|
| [`zendb-types`](zendb-types/) | Pure data model — CRDTs, HLCs, cells, events | `Cell`, `Event`, `Hlc`, `Value`, `Op`, `Path`, `Type` trait, `register_types!` |
| [`zendb-storage`](zendb-storage/) | Generic KV backends and topic log | `BPlusTree`, `KeyDir`, `SkipList`, `Topic`, `State`, `Table`, `Backend` trait |
| [`zendb-engine`](zendb-engine/) | Tables, database catalog, streaming operators, timers, facets | `Database`, `Operator` trait, `define_operator_set!`, `Subscription`, `Executor` |
| [`zendb-testing`](zendb-testing/) | Integration test harness (document pipeline) | Test operators: `IndexerOp`, `ArchiverOp` |

---

## Quick Start

Add to your `Cargo.toml`:

```toml
[dependencies]
zendb-engine = { path = "zendb-engine" }
zendb-types = { path = "zendb-types" }
zendb-storage = { path = "zendb-storage" }
```

Define your operator set — this is your **database facet**:

```rust
use zendb_engine::define_operator_set;

define_operator_set! {
    pub mod my_ops {
        MyIndexer(MyIndexerOperator),
        MyArchiver(MyArchiverOperator),
    }
}

// Your database type is parameterized by this set:
type MyDb = Database<my_ops::OperatorInstance>;
```

Create or open a database:

```rust
use zendb_engine::{Database, DatabaseConfig, Executor};
use std::sync::Arc;

let executor: Arc<dyn Executor> = /* your executor */;

// Create a fresh database
let db = MyDb::create("./my_db", executor.clone(), DatabaseConfig::default())?;

// Or open an existing one
let db = MyDb::open("./my_db", executor, DatabaseConfig::default())?;
```

Create a table and insert an event:

```rust
use zendb_engine::TableConfig;
use zendb_types::{Event, Hlc, Op, PrimaryKey, Value, Path};

let table = db.table("documents", Some(TableConfig::default()))?;
let guard = table.get()?;

let event = Event {
    table_id: "documents".into(),
    primary_key: PrimaryKey::String("doc-1".into()),
    path: Path::new(),
    op: Op::Replace { value: Value::String("hello world".into()) },
    hlc: Hlc::with_device_id(1000, 0, device_id()).unwrap(),
    sync: false,
    signature: vec![],
};
guard.write().insert_event(event)?;
```

Dispatch an operator:

```rust
use zendb_engine::{OperatorRuntimeConfig, Subscription};

db.dispatch_operator::<MyIndexer>(
    "indexer",
    MyIndexerConfig::default(),
    OperatorRuntimeConfig {
        subscriptions: vec![Subscription::pattern("documents")],
        poll_size: 128,
    },
)?;
```

Query the operator's facet:

```rust
// Operators can expose a typed query interface while they run
let facet = db.facet::<MyIndexerFacet>("indexer")?;
let index = facet.lookup("hello")?;
println!("Documents containing 'hello': {:?}", index);
```

---

## Core Concepts

### The Database Facet

The `Database<D>` type is parameterized by `D`, which is the **operator set**
generated by `define_operator_set!`. This means:

- At compile time, the database knows exactly which operator types exist.
- Operator configs are type-safe — `dispatch_operator::<MyOp>` ensures the
  config type matches.
- The generated `OperatorInstance` enum implements `DispatchOperator`,
  providing the type-erased dispatch layer that the run loop calls.

Each operator can also expose a **query facet** — a typed, read-only struct
that applications use to query the operator's accumulated state:

```rust
// In your operator impl:
impl Operator for MyIndexer {
    type Config = MyIndexerConfig;
    type Timer = ();
    type Facet = MyIndexerFacet;  // <-- the facet type

    fn create(db, name, config) -> BoxFuture<io::Result<Self>> {
        // open state handles, store them on self
    }

    fn facet(&self) -> MyIndexerFacet {
        // return a lightweight clone of the state handles
        MyIndexerFacet { index: self.index.clone() }
    }

    fn process(&mut self, changes, db, name, config) -> BoxFuture<io::Result<OperatorDirective>> {
        // update self.index as changes arrive
    }
}

// The facet provides public query methods:
impl MyIndexerFacet {
    pub fn lookup(&self, word: &str) -> io::Result<HashSet<String>> {
        let state = self.index.get()?;
        Ok(state.read().get(&word.into())?.unwrap_or_default())
    }
}
```

The facet is stored in the operator worker and retrieved via
`db.facet::<MyIndexerFacet>("indexer")`. It reads directly from the
operator's persisted state — no lock contention with the processing loop.

Use `type Facet = ()` and `fn facet(&self) {}` for operators that don't
expose queries.

### Tables and Events

A table owns three things:

| Component | Backend | Purpose |
|---|---|---|
| Materialized state | `State<PrimaryKey, Cell>` (B+tree or KeyDir) | Durable resolved row state |
| Resolved cache | `SkipList<PrimaryKey, Cell>` | In-memory shadow of pending rows |
| Change topic | `Topic<Change>` | Durable append-only change stream |

The write path:

1. `insert_event(event)` — apply the event to the resolved cache (or
   materialized state if not cached)
2. If the event changes the cell, update the cache
3. Append a `Change` (before/after snapshot) to the topic
4. When the cache exceeds `max_buffered_records`, drain to materialized state

Reads merge cache over state — cache entries shadow materialized entries.
This means reads see the latest state even before materialization.

Table handles are **weak** (`Weak<RwLock<Table>>`). They never keep a table
or database alive. Upgrade with `.get()` for one operation:

```rust
let handle = db.table("users", None)?;         // TableHandle (weak)
let table = handle.get()?;                      // Arc<RwLock<Table>> for this op
table.write().insert_event(event)?;
// table dropped here — lock released
```

### Operators

Operators are streaming computations. They implement the `Operator` trait
with five lifecycle hooks and one query hook:

| Method | When Called |
|---|---|
| `create(db, name, config)` | Once on spawn — open state handles, create output tables |
| `facet(&self)` | Once after create — return the query facet |
| `on_input_opened(table, db, name, config)` | A matching table was opened |
| `on_input_closed(table, db, name, config)` | A matching table was closed |
| `process(changes, db, name, config)` | A batch of changes is available |
| `on_timer(payload, fire_at_ms, db, name, config)` | A registered timer fired |
| `teardown(phase, db, name, config)` | Operator is stopping |

Each method receives `&Arc<Database<D>>` directly — no separate context
object. Operators open tables, create states, register timers, and read/write
data through the database reference.

**Return directives:**

- `OperatorDirective::Continue` — keep processing (commit offsets)
- `OperatorDirective::Finish` — clean shutdown, retire the operator

**Subscriptions** use glob patterns:

```rust
Subscription::pattern("users")      // exact table name
Subscription::pattern("wiki-*")     // all tables starting with "wiki-"
Subscription::pattern("*-log")      // all tables ending with "-log"
Subscription::pattern("*")          // every table
```

### States

States are typed key-value stores owned by the database. Operators typically
open states in `create()` and store weak `StateHandle<K, V>` references:

```rust
struct MyIndexer {
    index: StateHandle<String, HashSet<String>>,  // weak handle
}

impl Operator for MyIndexer {
    fn create(db, name, config) -> BoxFuture<io::Result<Self>> {
        Box::pin(async move {
            // Open (or create) a typed state
            let index = db.state("my-index", Some(StateConfig::default()))?;
            Ok(Self { index })
        })
    }

    fn process(&mut self, changes, db, name, config) -> BoxFuture<io::Result<OperatorDirective>> {
        Box::pin(async move {
            let state = self.index.get()?;   // upgrade for this operation
            state.write().put("hello".into(), doc_ids)?;
            Ok(OperatorDirective::Continue)
        })
    }
}
```

State backends are configurable: `Ordered` (B+tree), `Unordered` (KeyDir),
or `InMemory` (SkipList). The first typed lookup on open establishes the
`K, V` types — there is no persisted schema registry, so application code
is responsible for type compatibility across restarts.

### Timers

All operators share one ordered B+tree timer store keyed by
`(fire_at_ms, operator)`. Operators register timers through the database:

```rust
// Inside an operator method:
db.register_timer("my-op", fire_at_ms, &payload)?;
db.cancel_timer("my-op", fire_at_ms)?;
```

A background scheduler loop sleeps until the next timer is due (condvar),
delivers payloads to operator worker inboxes, and the worker fires
`on_timer` from its main loop.

Timers persist across restarts. Durability is at-most-once: a timer removed
from the store but not yet fired is lost if the process crashes.

---

## Operator Lifecycle

```text
┌──────────┐
│  create  │  ← open state handles, create output tables, register initial timers
└────┬─────┘
     │  facet() called — query interface becomes available
     │  (for each matching table already open)
     ▼
┌────────────────┐
│on_input_opened │  ← notified of each initial subscription match
└───────┬────────┘
        │
        ▼
┌──────────────────────────────────────────────────────────┐
│                     ACTIVE LOOP                            │
│                                                           │
│  ┌──────────┐   ┌──────────┐   ┌──────────────────────┐  │
│  │ process  │   │ on_timer │   │ on_input_opened /     │  │
│  │ changes  │   │  fires   │   │ _closed               │  │
│  └──────────┘   └──────────┘   └──────────────────────┘  │
│                                                           │
│  Return Finish from any method → teardown                 │
│  Error from any method → Failed, then teardown            │
│  No inputs remain → teardown(Active), suspend             │
└──────────────────────────────┬────────────────────────────┘
                               │
                               ▼
                      ┌────────────────┐
                      │   teardown     │ ← Active (suspend) / Finished / Failed / Cancelled
                      └────────────────┘
```

---

## Walkthrough: A Document Indexer

Let's build a complete document indexing pipeline — the same one tested in
`zendb-testing`.

### Step 1: Define the operator set

```rust
use zendb_engine::define_operator_set;

define_operator_set! {
    pub mod doc_ops {
        Indexer(IndexerOp),
        Archiver(ArchiverOp),
    }
}

type DocDb = Database<doc_ops::OperatorInstance>;
```

### Step 2: The Indexer operator

```rust
use std::{collections::HashSet, io, sync::Arc};
use zendb_engine::{
    BoxFuture, Change, Database, DispatchOperator, Operator,
    OperatorDirective, StateHandle, Subscription,
};
use zendb_storage::frontend::state::StateConfig;
use bincode::{Decode, Encode};

#[derive(Debug, Clone, PartialEq, Encode, Decode)]
struct IndexerConfig;

/// Query facet — allows the application to search the index.
struct IndexerFacet {
    index: StateHandle<String, HashSet<String>>,
}

impl IndexerFacet {
    pub fn search(&self, word: &str) -> io::Result<HashSet<String>> {
        let state = self.index.get()?;
        Ok(state.read().get(&word.to_lowercase())?.unwrap_or_default())
    }

    pub fn doc_count(&self) -> io::Result<usize> {
        Ok(self.index.get()?.read().size())
    }
}

struct IndexerOp {
    index: StateHandle<String, HashSet<String>>,  // word → set of doc IDs
    docs:  StateHandle<String, u64>,              // doc ID → word count
}

impl Operator for IndexerOp {
    type Config = IndexerConfig;
    type Timer = ();
    type Facet = IndexerFacet;

    fn create<'a, D>(
        db: &'a Arc<Database<D>>,
        _name: &'a str,
        _config: &'a Self::Config,
    ) -> BoxFuture<'a, io::Result<Self>>
    where D: DispatchOperator
    {
        Box::pin(async move {
            let index = db.state("indexer/index", Some(StateConfig::default()))?;
            let docs  = db.state("indexer/docs", Some(StateConfig::default()))?;
            Ok(Self { index, docs })
        })
    }

    fn facet(&self) -> IndexerFacet {
        IndexerFacet { index: self.index.clone() }
    }

    fn process<'a, D>(
        &'a mut self,
        changes: Vec<Change>,
        db: &'a Arc<Database<D>>,
        _name: &'a str,
        _config: &'a Self::Config,
    ) -> BoxFuture<'a, io::Result<OperatorDirective>>
    where D: DispatchOperator
    {
        Box::pin(async move {
            for change in changes {
                match (&change.previous, &change.current) {
                    // New or updated document
                    (_, Some(cell)) if !cell.is_dummy() => {
                        let doc_id = /* extract from primary key */;
                        let text = /* extract text from cell */;
                        let words = tokenize(&text);

                        let index_state = self.index.get()?;
                        let docs_state = self.docs.get()?;

                        for word in &words {
                            index_state.write().update(&word.into(), |entry| {
                                let mut set = entry.unwrap_or_default();
                                set.insert(doc_id.clone());
                                Some(set)
                            })?;
                        }
                        docs_state.write().put(doc_id.clone(), words.len() as u64)?;
                    }
                    // Deleted document
                    (Some(_), None) => {
                        // Remove from index...
                    }
                    _ => {}
                }
            }
            Ok(OperatorDirective::Continue)
        })
    }
}
```

### Step 3: Put it all together

```rust
fn main() -> io::Result<()> {
    let executor = Arc::new(ThreadExecutor);
    let db = DocDb::create("./my_db", executor, DatabaseConfig::default())?;

    // Create the documents table
    let docs = db.table("documents", Some(TableConfig::default()))?;

    // Dispatch the indexer
    db.dispatch_operator::<IndexerOp>(
        "indexer",
        IndexerConfig,
        OperatorRuntimeConfig {
            subscriptions: vec![Subscription::pattern("documents")],
            poll_size: 128,
        },
    )?;

    // Insert some documents
    {
        let table = docs.get()?;
        table.write().insert_event(make_event("doc-1", "hello world"))?;
        table.write().insert_event(make_event("doc-2", "hello rust"))?;
    }

    // Wait a moment for the operator to process...
    std::thread::sleep(std::time::Duration::from_millis(50));

    // Query the index through the facet — no locking the operator!
    let facet = db.facet::<IndexerFacet>("indexer")?;
    let results = facet.search("hello")?;
    assert_eq!(results.len(), 2);  // doc-1 and doc-2

    println!("Documents containing 'hello': {:?}", results);
    Ok(())
}
```

---

## Storage Backends

ZeninDB's storage layer provides three KV backends plus an append-only log:

| Backend | Persistent | Ordered | Best For |
|---|---|---|---|
| `BPlusTree<K, V>` | ✅ mmap file | ✅ | Ordered state, range scans, large datasets |
| `KeyDir<K, V>` | ✅ mmap file | ❌ | Unordered lookups, catalogs, metadata |
| `SkipList<K, V>` | ❌ in-memory | ✅ | Caches, working sets, event buffers |
| `Topic<T>` | ✅ segmented files | N/A | Event streaming, change logs, consumer offsets |

All backends implement the `Backend<K, V>` trait (CRUD, bulk ops, iteration,
flush, sync). Ordered backends add `range`, `first`, `last`, and reverse
iteration. The `State<K, V>` enum provides runtime backend dispatch so you
can switch between ordered and unordered storage by changing config.

All serialization uses **bincode 2** (little-endian, fixed-int encoding).
Hot-path encoding uses thread-local pooled `Vec<u8>` buffers — zero
allocation after warm-up.

---

## Building and Testing

```sh
# Build everything
cargo build

# Run all tests (unit + integration)
cargo test

# Run only engine tests
cargo test -p zendb-engine

# Run only storage tests
cargo test -p zendb-storage

# Run integration tests (document search pipeline)
cargo test -p zendb-testing
```

The engine is executor-agnostic. Tests use a trivial `ThreadExecutor` that
spawns each future onto a dedicated OS thread. For production use, implement
the `Executor` trait with Tokio, smol, or your own runtime.

---

## License

MIT
