# Iteration 0005: Lean Catalog APIs

Status: implemented.

Priority: this document supersedes earlier iterations where catalog handler
ownership, public method names, system-resource visibility, or per-mutation
durability differ.

## State Catalog

`States` owns the typed `_state_catalog` handle and an erased open-state map.
The typed field and map entry share one `Arc<StateHandle<...>>`, so the catalog
is never opened through a second handle or lock. Each handle stores its
`RwLock<State<...>>` directly, and `get` returns an Arc clone of the stored
handle.

States remain lazy after declaration. The public surface is:

- `contains`
- `list`
- `upsert`
- `get`
- `list_open`
- `close`
- `delete`

`list` and `list_open` return natural backend/map iteration order. System
states are visible to lookup and listing. Public `upsert` and
`StateHandle::write` reject system states. Workspace bootstrap uses
`upsert_internal`, and peer persistence uses `write_internal`. System states
cannot be deleted.

## Table Catalog

`Tables` owns the typed `_table_catalog` handle and an eager map containing
every declared table. The catalog field and map entry share one
`Arc<TableHandle>`; `_devices` similarly shares the handle owned by `Devices`.

The public surface is:

- `contains`
- `list`
- `upsert`
- `get`
- `delete`

`list` returns table names in natural map iteration order. There is no
`TableInfo`, config accessor, close operation, or open-only listing. System
tables are visible to lookup and listing. Public `upsert` and
`TableHandle::insert` reject system tables. System tables cannot be deleted.

Catalog mutations remain event driven. `CatalogSyncListener` is the sole owner
of eager map changes after construction. `write_entry` is shared by public
upsert and workspace bootstrap; no routing-only bootstrap method exists.

## Durability

Catalog lifecycle methods do not call `sync` or `flush` after intermediate
mutations. A later iteration will coordinate durability centrally.
