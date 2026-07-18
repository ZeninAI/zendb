# zendb-replication

Concrete durable replication primitives without Workspace authorization or
network sockets.

## Table

`Table` owns one `State<PrimaryKey, Cell>`, one write cache, one durable local
`Topic<Change>`, and a local recovery consumer. Local and received mutations
produce the same topic format.

Table-wide `SyncPolicy` comes from the owning catalog row; nested policy lives
inside Cells. Remote events below local boundaries are durably classified by
the Workspace journal but are not materialized. Snapshot merge recurses into
shared siblings while preserving local children.

`Table` implements `ReadBackend` and `OrderedReadBackend`, including an owned
ordered fallback independent of the selected physical State backend. It does
not implement raw mutable `WriteBackend`.

`shared_merkle_root()` derives a canonical binary Merkle root directly from
authoritative shared rows. It returns no comparable root when the table
contains a local boundary. `shared_rows()` produces policy-free snapshot state.

The effective physical `TableConfig` is persisted beside the table. Reopening
with an incompatible config fails and requires explicit migration.

## SharedJournal

`SharedJournal` durably stores signed `ReplicatedEvent` values keyed by
`EventIdentity`, rejects conflicting payloads for one identity, retains
out-of-order events, and persists a gap-free `ContiguousFrontier`.

It does not verify signatures or roles. `zendb-engine::Workspace` owns those
policy decisions before application.

## Protocol

The `protocol` module owns transport-neutral summary, range, event-batch, and
snapshot records. It has no socket or session implementation. The engine runs
that protocol over `zendb_transport::SecureSession<L>`.
