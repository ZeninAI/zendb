# zendb-engine

The concrete client-side Workspace runtime. It composes storage, replication,
transport, and portable types; it is not a server and has no central authority.

## Workspace Ownership

`Workspace` owns:

- the durable local `DeviceProfile` and stable WorkspaceId;
- table registry and catalog-backed lifecycle;
- `_catalog`, `_devices`, and `_enrollment_tickets` validators;
- shared-event authorization, signing, ingestion, and application;
- snapshot capture/install and bilateral TCP anti-entropy;
- onboarding, key rotation, presence, LAN discovery, and cluster reconnect;

It delegates Cell materialization, journal storage, and wire records to
`zendb-replication`, generic bytes to `zendb-storage`, and secure sessions to
`zendb-transport`. Optional local operators live in `zendb-operator` and
observe concrete table lifecycle events.

## Durable Layout

```text
_workspace_id
_device_profile.a / _device_profile.b
_shared_events
_replication_frontier
_state_reconciliation_required   present only while repair is owed
_snapshot
tables/
  _catalog/
  _devices/
  _enrollment_tickets/
  <application table>/
```

When `zendb-operator` is attached, its private catalogs, timers, and typed
State files live under `_operator/`; they are not core Workspace resources.

There is no `_control` Cell and no separate `_tables` logical catalog.

## Fluent Data API

```rust
let table = workspace.table("documents").open()?;

workspace
    .table("documents")
    .row(primary_key)
    .at(path)
    .apply(operation)?;

workspace.table("drafts").local().create()?;
workspace.table("drafts").shared().create()?; // promote and reconcile
workspace.table("drafts").delete()?;
```

Rows use the same API regardless of routing. A local mutation updates Table
state and its local topic. A shared mutation additionally requires
`Contributor`, allocates an origin sequence, signs an envelope, appends it
durably, and applies it through system/data validation.

`TableHandle::get()` yields the replication-aware Table. Reads use
`ReadBackend`/`OrderedReadBackend`; direct shared writes are rejected.

## System Policy

Every admitted device is an implicit Reader. Fixed roles are Contributor,
Dispatcher, and Manager. System validators enforce create-only admission,
Manager-owned role sets/tickets/removal, shared self-or-Manager naming, and
strictly self-owned capabilities, key rotation, and frontier checkpoints.

Device presence is ephemeral transport evidence. It neither writes `_devices`
nor changes authorization.

## Synchronization

`sync_tcp()` performs one encrypted bilateral cycle:

1. authenticate admitted device keys and exchange signed heartbeat summaries;
2. compare contiguous frontiers and request exact event ranges;
3. compare canonical roots for wholly shared open tables;
4. transfer signed event batches;
5. install a verified shared snapshot if history is absent, roots differ, or
   local-boundary reconciliation is durably requested.

Snapshot merge is policy-aware and keeps local catalog rows and nested local
Cells. Successful state installation clears the durable reconciliation marker.

`start_cluster()` adds a listener, peer reconnect, signed UDP LAN discovery,
periodic sync, frontier checkpoints, and departure notices.

## Onboarding And Rotation

`create_joining()` persists a candidate DeviceId and signing key. Ticket
bootstrap and direct pre-admission both prove candidate-key possession and
install a manifest-verified snapshot. The result has no explicit roles.

Key rotation stages a secondary key, waits for the stable receipt frontier,
then promotes with a staged-key signature and sequence boundary. DeviceId does
not change.

## Optional Operators

`zendb_operator::OperatorHost<D>` wraps a Workspace and separately owns local
operator catalogs, typed State handles, timers, executor tasks, and Rhai. This
keeps the core Workspace concrete and usable without an operator implementation.
Distributed specs, placement, leases, and fencing remain proposed in ADR 008.
