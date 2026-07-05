# ZeninDB

**Embedded, local-first, eventually consistent database with CRDT types,
offline mutation, and streaming reactive operators.**

---

## What It Is

ZeninDB is an embedded Rust database designed for applications that need:

- **Conflict-free collaborative data structures** — counters, sets, lists,
  registers, collaborative text, and nested records
- **Offline writes** with deterministic merge when devices reconnect
- **Reactive streaming operators** — attach computations to tables that process
  changes incrementally, maintain private state, and publish derived results
- **File-backed durability** with ordered and unordered storage backends
- **No external service** — link it into your application

## Crate Structure

| Crate | Purpose | Status |
|---|---|---|
| [`zendb-types`](zendb-types/) | CRDT data model, HLC clocks, cells, events, generated type dispatch | ✅ |
| [`zendb-storage`](zendb-storage/) | Reusable ordered and unordered KV backends (B+tree, Bitcask, skip list) | ✅ |
| [`zendb-engine`](zendb-engine/) | Tables, database catalog, streaming operator runtime with timers | ✅ |
| [`zendb-testing`](zendb-testing/) | Integration tests simulating a document search-engine pipeline | ✅ |

## Quick Example

```rust
use zendb_engine::{Database, DatabaseConfig, TableConfig, OperatorRuntimeConfig, Subscription};
use zendb_types::Cell;

// Create or open a database
let db = Database::create("/tmp/my_db", executor, DatabaseConfig::default())?;

// Create a table
let table = db.table("documents", Some(TableConfig::default()))?;

// Dispatch a streaming operator that indexes documents
db.dispatch_operator::<MyIndexer>(
    "indexer",
    MyIndexerConfig { /* ... */ },
    OperatorRuntimeConfig {
        subscriptions: vec![Subscription::pattern("documents")],
        poll_size: 128,
    },
)?;
```

## Architecture

```
zendb-types        — pure data model (cells, HLCs, CRDTs, no I/O)
    ↑
zendb-storage      — generic persisted KV backends (B+tree, KeyDir, SkipList)
    ↑
zendb-engine       — tables, database catalog, streaming operators, timers
```

## Building

```sh
cargo build
cargo test
```

## License

MIT
