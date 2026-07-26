# Iteration 0006: Single Table Handle

Status: implemented.

Priority: this document supersedes earlier iterations where `TableEntry`,
`TableConsumer`, or workspace-owned table bootstrap are described.

## Runtime Ownership

`TableHandle` is the single in-memory representation of an open table. It owns
the guarded storage `Table`, listener collection, table name, system-table
flag, and a weak reference to `Devices`. `Tables` stores
`Arc<TableHandle>` values and `get` returns an Arc clone of the stored handle.
The `_table_catalog` field and `_table_catalog` map entry are the same Arc.
`Devices` and the `_devices` map entry similarly share one Arc.

There is no `TableEntry`/`TableHandle` split. System tables are created before
`Devices`, so their handles use an empty weak device reference. Public insert
checks the system flag before resolving the device reference. Application
handles use `Arc::downgrade(&devices)` and return `WorkspaceClosed` if retained
after their workspace has closed.

## Mutation Paths

`TableHandle::insert` is the public application path. It rejects system
tables, authorizes the local device, mints an event stamp, and delegates to
`insert_internal`.

`TableHandle::insert_internal` is crate-internal and accepts a complete
`Event`. It inserts into storage, releases the table write guard, and then
dispatches listeners. Catalog, device, bootstrap, and future replicated events
use this path.

Listener ownership stays in `zendb-workspace`. Moving listeners into the
storage `Table` would dispatch callbacks while the workspace write guard is
held and would couple storage mechanics to orchestration.

## Consumers

`TableHandle::consumer` returns `zendb_storage::TopicConsumer<Change>`
directly. The storage consumer owns its topic state and already implements
iteration and cursor management. The former workspace `TableConsumer` only
forwarded these methods and is removed.

`TableHandle::read` similarly returns `RwLockReadGuard<Table>` directly. The
former `TableReadGuard` added no behavior or restriction and is removed.

## Bootstrap

`Tables::create/open` owns the complete table/device dependency graph:

- create or open the `_devices` handle;
- create or open `Devices`;
- create or open the `_table_catalog` handle;
- eagerly create the application-table handles;
- construct the shared `Tables` registry;
- register receipt, catalog, and device listeners;
- on create, insert both self-referencing system catalog rows and bootstrap
  the local device.

Both constructors return only `Arc<Tables>`, matching the state catalog
constructors. `Tables` owns a crate-visible `Arc<Devices>` that
`Workspace::assemble` clones after table construction.

`Workspace::assemble` establishes the state subsystem and passes the peer-state
handle and peer identity to `Tables`. It does not construct storage tables,
register table listeners, or mint table bootstrap events.
