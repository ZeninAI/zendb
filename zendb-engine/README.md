# zendb-engine

Client-side workspace engine with local tables, authenticated CRDT
replication, device onboarding, and an existing local operator runtime.

## Responsibility

`zendb-engine` is the product integration boundary. `Workspace<D>` owns local
storage, replicated control state, the signed shared journal, snapshots,
anti-entropy sessions, presence observations, and the optional cluster loop.
There is no server process or central authority in this crate.

Generic CRDT values live in `zendb-types`, durable byte structures live in
`zendb-storage`, wire records live in `zendb-sync`, and authenticated framing
lives in `zendb-transport`. This crate enforces the invariants between them.

## Workspace Lifecycle

`Workspace<D>` is the lifecycle and authorization root. It serializes shared
sequence allocation, journal append, authorization, application, snapshot
capture, and snapshot installation.

```rust
let workspace = Workspace::<MyOps>::create(
    path,
    executor,
    WorkspaceConfig::default(),
)?;

let reopened = Workspace::<MyOps>::open(
    path,
    executor,
    WorkspaceConfig::default(),
)?;
```

The durable profile overrides caller-supplied device identity on reopen. The
workspace ID and random DeviceId are stable; a DeviceId is not derived from a
rotatable signing key.

Important files under a workspace root are:

```text
|-- _workspace_id
|-- _device_profile.a / _device_profile.b
|-- _control
|-- _shared_events
|-- _replication_frontier
|-- _snapshot
|-- _tables / _states / _operators / _timers
|-- tables/<name>/
`-- states/<name>/
```

The alternating device-profile slots preserve signing keys, HLC state,
presence sequence, and shared-origin sequence across crashes.

## Local And Shared Data

Local tables use `TableConfig::sync == false`. Their events have no
distributed identity, require no workspace role, and are never exported by a
snapshot or sync session.

```rust
let handle = workspace.table("drafts", Some(local_config))?;
handle.get()?.write().insert_event(event)?;
```

Shared-table existence is replicated control state. A Contributor creates and
deletes shared tables through the workspace and submits data mutations through
`mutate()`:

```rust
let handle = workspace.create_shared_table("users", TableConfig::default())?;
workspace.mutate("users", user_id, event)?;
```

Direct `Table::insert_event()` on a shared table is rejected because it would
bypass signing, authorization, and journal ordering. `list_shared_tables()` is
the canonical replicated list; the physical table catalog may retain deleted
shared-table files for recovery and local overlays.

Every admitted device receives all shared data. `Cell.sync == false` routes a
local table or nested subtree into a device-private overlay. The overlay
survives shared ancestor replacement and restart, is absent from snapshots,
and is discarded rather than published when sync is re-enabled.

Typed `state::<K, V>()` resources are local engine state. They are not part of
workspace replication and have no persisted schema registry.

## Devices And Roles

The replicated `DeviceRecord` is both membership record and authorization
subject. A live Device Cell is admitted; tombstoning it revokes the device.
Presence, application accounts, and OAuth identities are not database
authorities.

All admitted devices are implicit Readers. The only explicit workspace roles
are fixed and non-overlapping:

- `Contributor`: create/delete shared tables and mutate shared data.
- `Dispatcher`: manage operator specifications. Distributed operator execution
  is not implemented in this pass.
- `Manager`: admit/remove/rename devices, assign roles, and manage enrollment
  tickets.

A device may rename itself and update its capabilities, key ring, heartbeat,
and frontier under field-specific validation rules. Capabilities are
scheduler labels, not permissions or executable host functions.

## Onboarding

`create_joining()` creates a durable candidate identity with no roles. ZenDB
supports exactly two admission paths:

1. A Manager creates an enrollment presentation. A QR/link carries its private
   ticket credential. Any admitted peer can verify and relay the candidate's
   bound ticket proof.
2. A Manager calls `admit_device()` with an out-of-band DeviceId and public
   key. The candidate later bootstraps from any peer while pinning an expected
   peer public key.

The corresponding APIs are `create_enrollment_ticket()`,
`bootstrap_with_ticket()`, and `bootstrap_direct()`. Both paths prove
candidate-key possession, transfer a verified chunked snapshot, install
control/data state, and then use ordinary anti-entropy. Discovery and network
connectivity never grant membership.

## Replication

`sync_tcp()` performs one bilateral anti-entropy cycle over a mutually
authenticated encrypted `SecureTcpSession`. Peers exchange durable contiguous
frontiers, request exact missing origin ranges, transfer bounded event batches,
and fall back to a chunked snapshot when retained history cannot satisfy a
request.

Each shared event has `(origin_device_id, origin_seq)`, a strictly increasing
origin HLC, a signature, and optional ticket-admission evidence. A receiver
independently validates membership, key transition, role/field ownership, and
event signature before applying it. Out-of-order events are retained and only
advance the frontier once every gap is durably present and applicable.

`export_snapshot()` produces shared control and live shared-table state only.
Installation validates the workspace, manifest hash, local membership, and
retained journal tail before rebuilding resolved state with local overlays.

`checkpoint_local_frontier()` publishes durable progress. The stable frontier
is the minimum checkpoint across every admitted device. `compact_shared()`
requires a retained snapshot and compacts recursive tombstones only through the
HLC proven stable by that frontier. Shared journal pruning is intentionally
conservative and is not performed yet.

## Cluster Runtime

`start_cluster(ClusterConfig)` starts a concrete client-side runtime containing:

- a TCP listener for replication and bootstrap sessions;
- periodic synchronization and reconnect across multiple peers;
- signed UDP LAN announcements and dynamically learned endpoints;
- frontier checkpoints, signed heartbeats, and signed departure notices; and
- bounded diagnostics available through `ClusterRuntime::errors()`.

Presence is a local estimate based on the sender-advertised heartbeat interval
and a configured grace multiplier. It is a ranking signal only and never
changes membership or authorization.

## Key Rotation

`stage_local_key_rotation()` publishes a secondary key signed by the current
primary. `promote_local_key_rotation()` waits until the stable frontier proves
every admitted device has received the staged key, then publishes a promotion
signed by that staged key. The keys swap order; the old key remains secondary
for delayed in-flight events. A later rotation replaces it.

## Existing Local Operators

The repository already contains a native streaming operator runtime with
`Operator`, `DispatchOperator`, subscriptions, local state, facets, timers, and
worker lifecycle APIs. It remains usable as local functionality.

Distributed declarative reconciliation, placement, leases, fencing, Rhai
isolation, and Dispatcher enforcement are deliberately excluded from this
implementation pass. ADR 008 remains the design boundary for that work; the
current native `Operator` trait must not be mistaken for the future distributed
control-plane API.

## Concurrency

- Storage backends are single-threaded values protected by locks.
- The workspace owns strong table/state handles; application handles are weak.
- Shared mutation and snapshot operations are serialized under one workspace
  lock to preserve durable allocation and apply ordering.
- The engine does not require Tokio. Applications provide the `Executor` trait.
- Operator code must not hold a lock guard across `.await`.

## Modules

```text
src/workspace/
|-- mod.rs          lifecycle, handles, public control APIs
|-- control.rs      replicated schema and authorization validation
|-- journal.rs      durable signed shared journal and gap tracking
|-- replication.rs local/shared routing and event application
|-- snapshot.rs     snapshot, stable frontier, compaction
|-- network.rs      secure anti-entropy protocol
|-- onboarding.rs   ticket and direct bootstrap flows
|-- cluster.rs      listener, reconnect, LAN discovery, presence
|-- rotation.rs     staged signing-key rotation
|-- tables.rs       physical table catalog and handles
|-- states.rs       typed local state
|-- operators.rs    existing local operator workers
`-- timers.rs       existing processing-time timer store
```
