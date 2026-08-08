# Iteration 0002: Workspace, Catalogs, and States

Status: implemented and reflected from the current source.

This iteration adds synchronous workspace orchestration around the storage
foundation. It defines how a workspace opens, how system tables describe the
workspace, and how application tables and typed states are exposed.

## Workspace lifecycle

`Workspace::create` and `Workspace::open` are the lifecycle entry points. A
workspace owns its root lock and durable layout:

```text
_identity
_lock
tables/
  _catalog/
  _installations/
  <application table>/
states/
  _catalog
  <named state>
```

Creation generates a workspace ID unless `WorkspaceConfig.workspace_id` is
provided, allocates the local installation ID, initializes durable storage,
and writes identity only after the initial storage barrier succeeds. Opening
loads and validates `_identity`, including an optional configured workspace ID
assertion. There is no separate join workflow in the current API.

## System tables and table catalog

`TableStore` creates/opens `_catalog` and `_installations` first. The catalog
contains table declarations and is itself a normal typed table. Application
tables declared in the catalog are eagerly opened during workspace assembly.

Catalog changes are projected through the table pipeline. An upsert opens or
creates the corresponding table directory; a delete removes the table from
the open map and its physical directory after the catalog event commits.
System tables cannot be deleted or changed through ordinary application-table
operations.

The public `Tables` facade supports contains/list/get/upsert/delete and checks
the appropriate workspace permission before mutations. `TableHandle` exposes
the table name, system-table status, insertion, reads, and change listeners.
Consumers are obtained through the read guard (`table.read().consumer(name)`)
and readers through the table read API; there is no separate consumer method
on `TableHandle`.

## Typed states

`States` manages workspace-local named states under `states/`. `_catalog` is a
protected typed system state. Other states are opened lazily and can be
queried, listed, closed, deleted, or upserted through the state facade.

State writes are local projections and do not become table events. The state
layer is separate from table CRDT data while sharing the storage backend and
durability conventions.

## Workspace core and local pipeline

`WorkspaceCore` owns the `TableStore`, `States`, membership snapshot, hybrid
clock, and optional replication notification sender. The local event pipeline
is:

```text
permission check -> mint event time -> table insert -> project change -> notify
```

The table allocates the per-table event sequence. The workspace projects
installation and catalog changes into their corresponding workspace views and
notifies registered listeners after the durable table operation succeeds.

## Design boundaries

- Workspace APIs are synchronous and embedded.
- Catalog state is represented by the `_catalog` table, not an independent
  hidden registry.
- Application table handles do not expose the underlying topic as a public
  field.
- Identity, authorization, causal timing, and replication are completed in
  later iterations, even though their owning fields are assembled by the final
  workspace constructor.
