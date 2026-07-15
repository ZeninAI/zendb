# 001: Devices and Roles

Status: Accepted

## Scope

This decision defines Workspace membership, the Device record, Workspace-wide
roles, and field ownership. Key rotation, presence, replication progress,
onboarding, and operator leases have their own decisions.

## Decision

ZenDB authorizes devices. It does not model users, OAuth subjects, or services
as database principals.

A `DeviceId` is a stable, randomly generated 128-bit identifier persisted in a
local device profile. It is not derived from a public key or a machine ID.

The Workspace control value contains one nested Device Cell per admitted
device:

```text
WorkspaceControl: Record
  devices: Record
    <device_id>: Cell<Record(Device)>
```

A device belongs to the Workspace exactly while its Device Cell is live.
Deleting that Cell tombstones membership. There is no device `Pending`,
`Active`, `Online`, `Offline`, or `Revoked` field. Re-admission after deletion
is an explicit Manager action.

## Device Record

```text
Device: Record
  name: String
  key_ring: Record<DeviceKeyRing>
  roles: Set<WorkspaceRole>
  capabilities: OrSet<CapabilityId>
  replication_frontier: Record<ContiguousFrontier>
```

- `name` is a device-selected LWW alias.
- `key_ring` is the device's authenticated signing-key state; ADR 005 defines
  its contents and transition rules.
- `roles` holds the fixed Workspace roles below.
- `capabilities` is an advertised scheduling label set, not an authority grant.
- `replication_frontier` is a durable checkpoint of shared-journal progress;
  ADR 003 defines it.

All fields use the existing recursive Cell and CRDT primitives. The Device
record is not a relational table, and no separate device-permission table is
needed.

## Roles

Every admitted device can read and replicate shared Workspace data. `Reader` is
implicit and is not stored.

There are exactly three explicit, Workspace-wide role values:

```text
Contributor
  Create, alter, and delete shared tables; create, update, delete, and merge
  shared data.

Dispatcher
  Create, update, enable, disable, and delete declarative operator specs.

Manager
  Admit or delete devices; rename any device; issue or delete enrollment
  tickets; and add or remove roles from any Device record.
```

The roles have disjoint actions and are composable. `Owner` is only a UI label
for a device holding all three roles; it is not a protocol role or bypass. A
newly admitted device has no explicit role and is therefore a Reader.

`roles` uses the HLC-backed `Set`, not `OrSet`. Authorization removal needs the
Set's retained per-element delete clock; the additive-wins behavior of `OrSet`
is not suitable for role revocation.

## Field Ownership

An admitted device may update only its own:

```text
name
capabilities
key_ring, using ADR 005
replication_frontier, using ADR 003
```

A Manager may create or delete any Device Cell, change any device name, and
mutate any device's role Set. A Manager does not gain shared-data write access
without `Contributor`, and a Dispatcher does not gain shared-data write access
by editing an operator spec.

## Related Decisions

- ADR 002 defines whether a device is currently reachable.
- ADR 003 defines shared-journal progress and frontiers.
- ADR 004 defines admission and bootstrap.
- ADR 005 defines DeviceKeyRing rotation.
- ADR 008 defines how local operator reconcilers use roles, capabilities, and
  leases.
