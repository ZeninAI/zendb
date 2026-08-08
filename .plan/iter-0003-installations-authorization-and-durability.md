# Iteration 0003: Installations, Authorization, and Durability

Status: implemented and reflected from the current source.

This iteration establishes the local identity and policy boundary, the
workspace clock, per-table causal tracking, and the ordering of durable
operations.

## Installation identity

The application supplies a `PeerIdentity` containing the local key material,
display name, and addresses. ZenDB derives the workspace installation key
from the domain `zendb/v1`, the workspace ID, and the installation ID. The
derived public key is persisted in the local installation record and is
validated when opening the workspace.

`_identity` persists the workspace ID, local installation ID, and the latest
durable hybrid-clock checkpoint. The local installation must be present in
`_installations`, have the expected public key, and be active before the
workspace is usable.

## Membership and permissions

An installation is a CRDT value with display name, public key, addresses, and
state. Its lifecycle is `Pending`, `Rejected`, or `Active(Permissions)`;
rejection is the removal representation rather than a separate tombstone.

Permissions are explicit capabilities: read data, write data, manage tables,
and manage installations. The `Membership` view keeps the local installation
and the installation map in an immutable snapshot published through `ArcSwap`.
Permission checks therefore read one coherent membership snapshot while
catalog, installation, and application changes are being projected.

The `Installations` facade supports listing, lookup, local installation lookup,
and authorized upsert. The producing side enforces write and management
permissions; replication admission later relies on this same installation
record.

## Time and causality

`HybridClock` is workspace-owned and provides event timestamps. Local commits
mint a timestamp; observed remote stamps advance the clock. Its checkpoint is
persisted with workspace identity during durability barriers.

Event sequence state is not global. Each `Table` stores a causal receipt window
keyed by `InstallationId`, so the next local sequence and remote duplicate
detection are scoped to that table. The table's `insert` and `observe` methods
are the only places that allocate or accept event sequence numbers.

No-op behavior is intentional: a local no-op creates neither a change nor a
receipt, while a novel remote no-op records receipt state without adding a
change-log record. This preserves replication knowledge without inventing a
projected change.

## Durability ordering

Durability barriers cover the storage that can affect recovery:

1. Table projected state, causal receipts, and recovery-consumer offsets.
2. Table topic data.
3. Workspace-local named states.
4. Workspace clock checkpoint and local identity metadata.

Workspace creation writes identity only after the initial storage barrier.
Workspace drop shuts down replication before the final flush. Open recovery
replays topic changes into projected state and commits the recovery consumer
after synchronization.

The implementation has no production migration requirement. On-disk format
changes may therefore follow the current invariants directly.

## Boundaries

- Authorization is a workspace concern; storage backends remain policy-free.
- Causal receipts are per table; the workspace clock is for event time only.
- Installation records are data in `_installations`, not a second membership
  database.
- Network transport and peer admission are described in the next iteration.
