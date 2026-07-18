# 009: Catalog-Backed Table Lifecycle

Status: Implemented

## Catalog

`_catalog` is a replication-aware Table keyed by table name:

```text
_catalog[table_name]: Cell<Record>
  config: Cell<Blob<TableConfig>>
```

The row's local SyncPolicy is the table-wide boundary. A live shared row means
the inherited table exists; its tombstone deletes that lifecycle. A local row
exists only for this device and ignores remote config/deletion until promoted.

`TableConfig` contains no sync flag. It is the replicated default recipe for a
new device. A device may choose an override only when materializing storage;
the effective config is persisted in `table.config`. A conflicting later
override fails and requires explicit migration.

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

- Contributor creates, promotes, or tombstones inherited application rows.
- Data waits if its catalog creation dependency is missing.
- Data older than the current table lifecycle epoch cannot resurrect a table.
- A local catalog boundary prevents remote lifecycle events from opening or
  closing that local table.
- Snapshot installation preserves same-named local catalog rows and skips the
  hidden remote table.

## API

```rust
workspace.table("documents").open()?;
workspace.table("drafts").config(config).local().create()?;
workspace.table("documents").config(config).shared().create()?;
workspace.table("documents").local().create()?;   // localize existing
workspace.table("documents").shared().create()?;  // promote and reconcile
workspace.table("documents").delete()?;
```

`list_tables()` returns application tables; `list_catalog_tables()` also shows
reserved system rows.

The concrete Workspace emits `TableLifecycleEvent::{Opened, Closed}` after its
cache changes. `observe_tables()` returns an RAII registration, so optional
local subsystems can follow table lifecycle without being owned by or
parameterizing Workspace. `zendb-operator::OperatorHost` is the first consumer.
