# 001: Devices And Fixed Roles

Status: Implemented

## Decision

DeviceId is the only database authorization subject. It is generated, stable,
persisted in `DeviceProfile`, and independent of machine identity and signing
keys. OAuth users, principals, services, and operators are application/runtime
concepts rather than Workspace membership types.

Membership is one row:

```text
_devices[device_id]: Cell<DeviceRecord>
  name: String
  key_ring: DeviceKeyRing
  roles: Set<WorkspaceRole>
  capabilities: OR-Set<CapabilityId>
  replication_frontier: ContiguousFrontier
```

A live row admits the device. Tombstoning the row removes it. There is no
status field, workspace_id column, role-binding table, or workspace private
signing secret.

## Authorization

Every admitted device is an implicit Reader. Explicit roles are fixed and
workspace-wide:

- `Contributor`: mutate inherited data and catalog rows.
- `Dispatcher`: manage future distributed operator desired state.
- `Manager`: admit/remove devices, change role sets, rename devices, and
  manage enrollment tickets.

Roles are non-overlapping protocol values. A creator receives all three. A new
device receives none. "Owner" is only a UI description for all three roles.

## Field Ownership

- Admission is create-only and requires Manager, except ADR 004 ticket proof.
- Removal and role changes require Manager.
- Name may be changed by the device itself or a Manager.
- Capabilities, key ring, and frontier may be changed only by that device.
- A Manager cannot replace another device's cryptographic/runtime-owned state.

Capabilities are self-advertised scheduling labels, not permissions,
subscriptions, capability requests, or executable APIs.
