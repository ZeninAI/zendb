# 009: Catalog-Backed Table Lifecycle

Status: Implemented

## Catalog

`_catalog` is a replication-aware Table keyed by table name:

```text
_catalog[table_name]: Cell<Record {
  generation: Hlc,
  config: TableConfig Record,
}>
```

Every catalog row and configuration update is replicated. A live row declares
the table; its tombstone deletes that lifecycle. There is no local table
declaration and no table-level sync policy.

`TableConfig` recursively maps to CRDT Records through `CellCodec`. Backend
configuration structs own their codecs, so `state` and `topic` remain nested,
addressable values instead of binary configuration blobs. The catalog is the
only configuration authority; no `table.config` sidecar exists.

`generation` is assigned only on creation and survives configuration updates.
Application storage and handles are scoped to it. A tombstone invalidates old
handles; recreating the same name gets a new generation and cannot reopen the
old physical state.

An update changes the catalog immediately but does not migrate a materialized
table. It takes effect when the Workspace is initialized again, when the chosen
storage backend validates any existing files.

## Bootstrap And System Tables

`_catalog` has a fixed name, path, and initial physical format so it can open
before reading itself. It contains these initial rows:

```text
_catalog
_devices
_enrollment_tickets
```

System rows and application rows use ordinary Table storage and replication.
Concrete validators protect reserved schemas. There is no `_control` Cell or
separate `_tables` KeyDir.

## Lifecycle Rules

- Contributor creates, updates, or tombstones application catalog rows.
- Every live application catalog row is materialized by the Workspace.
- There is no parked/open split and no close operation.
- Catalog additions emit `TableLifecycleEvent::Added`.
- Catalog tombstones remove the materialization and emit
  `TableLifecycleEvent::Removed`.
- Data waits if its replicated catalog creation dependency is missing.
- Data older than the current table lifecycle epoch cannot resurrect a table.

## API

```rust
let table = workspace.create_table("documents", config)?;
let table = workspace.table("documents")?;
workspace.update_table_config("documents", updated_config)?;
workspace.delete_table("documents")?;
```

`list_tables()` returns every live application catalog row, all of which are
materialized. `observe_tables()` returns an RAII registration so optional local
subsystems can follow catalog additions and removals without being owned by or
parameterizing Workspace. `zendb-operator::OperatorHost` is the first consumer.
