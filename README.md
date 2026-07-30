# ZenDB

ZenDB is a small, synchronous embedded database foundation. The current phase
contains portable CRDT values, storage mechanics, and local workspace
orchestration. It intentionally has no transport, replication runtime,
snapshots, operator host, or networking implementation.

## Crates

| Crate | Responsibility |
|---|---|
| `zendb-types` | Peer and workspace identities, event stamps, cells, operations, CRDT values, and shared binary utilities |
| `zendb-storage` | B+ tree, KeyDir, SkipList, generic State, Topic, and the invariant-preserving Table facade |
| `zendb-workspace` | Catalog-owned Table and State lifecycle, peer metadata and roles, hybrid time, and duplicate detection |

## Event Model

Every Table mutation is a fully stamped `Event`:

```rust
pub struct EventId {
    pub peer_id: PeerId,
    pub sequence: u64,
}

pub struct EventStamp {
    pub id: EventId,
    pub time: EventTime,
}
```

`Path` is `Vec<Segment>`. `Op::Upsert` creates or replaces a value, while
`Op::Delete`, `Op::Merge`, and type-specific operations cover the other
cell-level mutations. Cells contain only an optional CRDT value and their
event stamp.

## Tables And States

A Table has a fixed `PrimaryKey -> Cell` shape backed by a materialized State,
a bounded in-memory cache, and a durable `Topic<Change>`. Its only mutation
entry point is `Table::insert(Event)`. Normal and ordered reads stream through
the storage traits; the Table lazily merges materialized and cached rows.

A State is caller-typed local storage, `State<K, V>`, with no Event or Topic.
Catalog persists its `StateConfig`, while callers choose concrete key and value
types when opening a handle.

## Workspace

The workspace lifecycle modules create and open storage under fixed physical
paths. `tables/_catalog` is a self-registering Table of table name to
`TableConfig`. `states/_catalog` is a State of state name to `StateConfig`.
All declared Tables are eagerly open; typed States can be opened and closed.

`tables/_devices` stores device display metadata and a progressive optional
`Role`. `states/_peers` stores receipt windows plus the local event clock.
`Devices` keeps a read-oriented registry cache and a separate write-back peer
cache, so minting does not search the registry or write storage.
`Workspace::flush()` and `Workspace::sync()` coordinate device writeback with
every open Table and State; dropping the workspace performs a best-effort
flush.

```rust
use zendb_storage::TableConfig;
use zendb_types::{Op, Path, PrimaryKey, Value};
use zendb_workspace::{Workspace, WorkspaceConfig};

let workspace = Workspace::create("./data", WorkspaceConfig::default())?;
let documents = workspace.create_table("documents", TableConfig::default())?;

documents.insert(
    PrimaryKey::String("doc-1".into()),
    Path::new(),
    Op::Upsert {
        value: Value::String("hello".into()),
    },
)?;
# Ok::<(), zendb_workspace::Error>(())
```

## Durable Layout

```text
workspace-root/
  _identity
  _lock
  tables/
    _catalog/
    _devices/
    <table-name>/
  states/
    _catalog/
    _peers/
    <state-name>/
```

The project is not migration-stable. Compile the workspace with:

```text
cargo check --workspace
```
