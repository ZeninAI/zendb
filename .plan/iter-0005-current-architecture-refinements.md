# Iteration 0005: Current Architecture Refinements

Status: implemented and reflected from the current source.

This final iteration records the refinements that superseded earlier design
notes. It is the as-built consolidation point for ownership, state placement,
local metadata, replication configuration, and the public table boundary.

## Direct table state

The open table owns its projected state directly alongside its causal state,
topic, recovery consumer, and listeners. Workspace code does not maintain a
second table-state cache or route writes through a duplicate ownership layer.
`TableStore` owns open table handles and their lifecycle; the table remains the
authority for table reads, writes, recovery, and topic consumption.

## State placement

- Installation metadata is stored in the `_installations` table.
- Table declarations are stored in the `_catalog` table.
- Named workspace-local state is stored below `states/`.
- The workspace clock checkpoint is stored in `_identity`.
- Event causal receipt windows are stored per table in that table's causal
  state.

There is no separate global causal tracker, installation metadata journal, or
replication-only event log.

## Public ownership and notifications

`Workspace` owns the workspace core, table and state facades, membership, lock,
and optional replication controller. `TableHandle` exposes insertion, reads,
and listeners. The underlying topic is private; callers use
`table.read().reader()` or `table.read().consumer(name)` through the table read
guard. This keeps topic retention and mutation invariants inside storage.

Catalog and installation updates flow through normal table events and are
projected after commit. Local and remote changes use the same table insertion
and observation rules, while listeners and replication receive event-driven
notifications from the workspace core.

## Replication configuration

The final configuration is grouped by concern:

- `TransportConfig`: TCP/QUIC addresses, port reuse, and mDNS.
- `SyncConfig`: interval, maximum ranges, and recent-cache capacity.
- `BatchConfig`: byte threshold and optional linger.
- `MeshConfig`: maintenance interval and neighbor bounds.
- `ReplicationConfig`: enable/disable flag and the four groups above.

The runtime batches by table and applies remote events through the table API.
The configuration therefore tunes network work without changing storage
ownership or event semantics.

## Final invariants

- The code is synchronous at the public workspace API boundary.
- Storage is durable and recovery is topic-driven.
- A table is the unit of CRDT state, causal receipt tracking, and change-log
  retention.
- Installation permissions are checked before local mutations; admitted remote
  sessions use authenticated installation identity and trusted committed
  batches.
- Replication is private, optional, and workspace-owned.
- Catalog-first synchronization is required before installation and application
  table synchronization.
- There is no migration compatibility promise for the current pre-production
  on-disk formats.

## Superseded notes

Earlier plan files described intermediate or abandoned choices, including a
global causal state, a public topic field, a separate table consumer wrapper,
signed event envelopes, Gossipsub, a table-state cache, role-based policy
objects, and a public workspace join flow. Those are not part of the current
architecture and are intentionally omitted from this consolidated plan.
