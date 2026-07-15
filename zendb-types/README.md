# zendb-types

Pure portable data for ZenDB: CRDT values, clocks, events, device membership,
fixed roles, enrollment records, presence messages, replication frontiers, and
future operator desired-state records. This crate has no filesystem, sockets,
policy lookup, hosted service, or execution runtime.

## CRDT Foundation

`Hlc` is a 24-byte hybrid logical clock ordered by physical milliseconds,
logical counter, and stable DeviceId. DeviceId is random and persisted; it is
not derived from a machine identifier or public key.

`Cell` is the universal recursive state unit:

```rust
pub struct Cell {
    pub value: Option<Value>,
    pub hlc: Hlc,
    pub sync: Option<bool>,
}
```

`None` value is a CRDT tombstone. `sync` is local routing metadata, not
replicated authorization. On a shared table, `Some(false)` starts a private
overlay boundary. `Some(true)` cannot escape a private ancestor. Overlay
storage and routing are implemented by `zendb-engine::Workspace`.

`Event` addresses a table, primary key, recursive Path, operation, and HLC.
Its `sync` field is transitional input to the local router; a remote sender
cannot use it to choose the replication plane. Shared identity and signatures
are carried by `ReplicatedEvent` and `SyncEnvelope`.

Built-in values include scalar Bool, Int, String, Timestamp, and Blob plus
Record, Set, OrSet, Counter, MvRegister, List, Text, and PriorityQueue CRDTs.
`Record` and `List` recursively contain Cells. The `register_types!` macro
generates `TypeTag`, `PrimaryKey`, `Value`, `TypeOp`, `Segment`, and dispatch
implementations.

`Type::compact(Hlc)` accepts an already proven watermark. Values do not infer
distributed safety from a wall clock. The engine derives a shared watermark
from durable frontiers before invoking recursive compaction.

## Device Membership

`DeviceRecord` is the complete database membership and authorization subject:

```text
DeviceRecord
  name
  key_ring
  roles: Set<WorkspaceRole>
  capabilities: Set<CapabilityId>
  replication_frontier: ContiguousFrontier
```

The Workspace stores each DeviceRecord in a nested Device Cell. A live Cell is
membership; a tombstone is removal. There is no UserId, PrincipalId,
DeviceMembership, WorkspaceMembership, OAuth record, status field, or shared
workspace private secret.

`DeviceKeyRing` is an atomically replaced two-key state machine:

```text
primary_key
secondary_key: optional
primary_from_seq
phase: Stable | Staged
```

The secondary is either a staged candidate or a historic verifier, depending
on phase. There is no key ID or expiry interval. DeviceId remains stable while
keys rotate.

Every admitted device is an implicit Reader. The only explicit role values are
Contributor, Dispatcher, and Manager. `WorkspaceAction` is the fixed action
vocabulary used by engine validation. Roles and actions are not customizable
policy records.

Capabilities are device-advertised string labels for future operator
placement. They are not requests, permissions, subscriptions, or callable
host functions.

## Enrollment And Presence

`EnrollmentTicket` stores only the public ticket verifier and expiry HLC. Its
private credential belongs in an `EnrollmentPresentation` from
`zendb-transport`, not replicated data.

`PresenceHeartbeat` and `DepartureNotice` are signed ephemeral protocol
messages. They are not DeviceRecord fields and do not affect membership or
roles. The heartbeat carries the sender's advertised idle period so receivers
can derive local liveness using an explicit grace policy.

## Replication Progress

`EventIdentity { origin_device_id, origin_seq }` identifies one shared-journal
event. `ContiguousFrontier` records the durable, gap-free applied prefix for
each origin. `VersionVector` only records maximum observed values and is not
safe for compaction.

`TicketAdmissionEvidence` is optional envelope evidence for the exceptional
ticket-created Device event. Keeping it outside DeviceRecord lets every replica
validate admission without expanding materialized membership state.

## Operator Records

The `control::operator` module contains portable desired-state and lease
vocabulary reserved for ADR 008. Their existence does not imply that
distributed reconciliation, scheduling, Rhai isolation, or fenced execution is
implemented. The existing native operator runtime is in `zendb-engine`.

## Module Map

```text
src/
|-- crdt/
|   |-- cell.rs, event.rs, hlc.rs, path.rs, replication.rs
|   `-- values/       built-in scalar and recursive CRDT values
|-- identity/
|   |-- ids.rs        DeviceId, WorkspaceId, ticket/operator IDs
|   |-- membership.rs DeviceRecord, key ring, ticket, signatures
|   `-- role.rs       fixed WorkspaceRole and WorkspaceAction
`-- control/
    |-- presence.rs   heartbeat and departure messages
    `-- operator.rs   future distributed operator records
```
