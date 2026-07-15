# 009: Shared Table Lifecycle

Status: Accepted

## Scope

This decision defines how a table enters and leaves the shared plane. Local and
nested sync boundaries are ADR 006. Event identity and anti-entropy are ADR
003. This note does not define schemas, indexes, or operator outputs.

## Context

A local physical table catalog cannot establish distributed table existence.
If each replica inferred shared tables from local files, an ordinary data event
could silently create a table, bypass the Contributor role, and produce
different lifecycle ordering on different devices.

The Workspace control value therefore needs a small replicated declaration for
each shared table. Physical `TableConfig` remains local because backend choice,
buffer sizes, and storage tuning are device concerns rather than shared data.

## Decision

The control record contains a recursively nested shared-table map:

```text
WorkspaceControl: Record
  shared_tables: Record
    <table_name>: Cell<Bool>
```

A live Cell means the shared table exists. A tombstoned Cell means it does not.
The Cell's structural HLC is its lifecycle epoch. There is no relational table,
`workspace_id` column, mutable status enum, or replicated storage config.

Only a Contributor may create or tombstone a shared-table declaration. Calling
create on an already-live declaration is idempotent and must not advance the
epoch. Recreating a tombstoned name creates a later epoch.

## Data Event Validation

A shared data event is accepted only when:

```text
the author is an admitted Device
the author has Contributor
the shared-table declaration is live
event.hlc >= the declaration's current lifecycle HLC
the event signature and origin sequence are valid
```

An event that arrives before the declaration is retained as pending and retried
after other origins advance. An event older than the current creation/deletion
epoch is rejected and cannot resurrect data from a previous incarnation.

The simplified policy intentionally uses HLC lifecycle ordering rather than a
causal-context object. Concurrent create/delete/data races resolve by the same
deterministic HLC ordering used by the CRDT cells. Applications that need a
strong administrative cutover must synchronize that policy change before
allowing further writers.

## Physical State

Creating a declaration opens or creates a local physical table with `sync =
true`. A receiver lazily creates that physical table when the declaration or
first valid data event arrives. Local backend configuration may differ while
the logical CRDT state remains the same.

Deleting a declaration closes the table but does not immediately erase its
files or private overlays. Retention permits crash recovery, historical event
validation, and later compaction. `list_shared_tables()` reports only live
declarations; the low-level physical catalog is not the distributed namespace.

Direct insertion into a physical shared `Table` is forbidden. All shared data
must pass through `Workspace::mutate()` so sequence allocation, authorization,
signature, durable journal append, and application happen as one serialized
path.

## Snapshots And Re-Creation

Snapshots contain only currently live shared tables. The control snapshot
retains the lifecycle Cells and their HLCs, so a receiver can reject delayed
events from a deleted incarnation even when its materialized table payload is
absent.

Recreating the same name starts a new lifecycle epoch. Retained older events
remain journal history but do not apply to the recreated table when their HLC
predates that epoch.
