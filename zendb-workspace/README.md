# zendb-workspace

`zendb-workspace` owns synchronous workspace lifecycle, catalogs, membership,
authorization, and the private Zenin libp2p replication runtime.

## Lifecycle And Clock

`Workspace::create` creates system storage and the initial full-access local
installation. `Workspace::open` restores the durable identity and seeds the
process-wide hybrid clock from its checkpoint. Generated CRDT facades and
default value construction mint `EventTime` values through that shared
lock-free clock; the workspace no longer owns a separate clock instance.

## Core Ownership

Each open workspace owns a flat `WorkspaceCore` containing `TableStore`,
`States`, `Membership`, and an optional replication notification sender.
`TableStore` eagerly opens every catalog-declared table. Tables store direct
`Value` rows and `Change` records contain the previous and current values.

The local mutation pipeline is:

```text
typed operation facade or manual TypeOp -> PathOp with EventTime
    -> authorize -> table::insert batch -> project -> notify
```

`TableHandle::insert(primary_key, operations)` accepts a batch of manually
constructed `PathOp` values. There are no edit cursors or cell editors in the
API. Public generated methods are dispatch helpers only: they mint a clock and
call their matching `op_*` implementation.

Remote events enter through `table::observe`, which owns duplicate detection,
receipt updates, and operation application. Local commits notify replication
after durable insertion.

## Replication

Every ready `Active` installation is a direct replication peer. There is no
mesh rotation, neighbour set, join swarm, or topology election. Anti-entropy
uses `EventId` receipt ranges and network batches; `EventTime` is used only by
the CRDT types. Catalog, installation, and application topics are fetched in
catalog-first order so missing application tables can be opened before their
history is applied.

Unknown or pending installations are recorded as pending and rejected at the
protocol boundary. Active installations are dialed directly using their
durable route hints and maintain a connection to every other ready active
installation.
