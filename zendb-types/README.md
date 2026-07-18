# zendb-types

Portable ZenDB data with no filesystem, sockets, hosted services, or runtime
policy lookup.

## CRDT Core

`Cell` is the recursive state unit:

```rust
pub struct Cell {
    pub value: Option<Value>,
    pub hlc: Hlc,
    pub sync: SyncPolicy,
}
```

`None` is a CRDT tombstone. `SyncPolicy::{Inherit, Local}` is durable
replica-local routing metadata and never changes the HLC.
`Cell::shared_clone` recursively projects shared state, strips policy, and
preserves structural list anchors when a shared item follows a local item.

`Event` contains only table ID, primary key, recursive path, operation, and
HLC. Shared identity, payload hash, signature, and optional ticket evidence
belong to `ReplicatedEvent` and `SyncEnvelope`.

Values include scalars plus Record, Set, OR-Set, Counter, MV-register, List,
Text, and PriorityQueue CRDTs. `Blob::encode` and `Blob::decode` provide
bincode helpers using the shared storage codec configuration.

`CrdtCodec` implementations live beside their CRDT value types. Derived
records use default codecs where unambiguous and can select an alternate codec
with `#[cell(codec = "path::to::Codec")]`.

## Device Authority

`DeviceRecord` contains:

```text
name
key_ring
roles: Set<Contributor | Dispatcher | Manager>
capabilities: Set<CapabilityId>
replication_frontier: ContiguousFrontier
```

There is no user/principal membership model, status field, key-derived
DeviceId, workspace signing secret, custom role definition, or row ACL.
`DeviceKeyRing` supports one primary and optional staged/historic secondary
without key IDs or expiries.

## Progress And Presence

`EventIdentity` is a per-device shared-journal coordinate.
`ContiguousFrontier` proves a gap-free durable prefix; `VersionVector` only
records maximum observation and is not a pruning proof.

Presence heartbeats and departure notices are signed ephemeral records. They
do not affect membership or authorization. Enrollment records store public
ticket verification material; private QR/link credentials live in transport
presentations.

## Identifiers

`DeviceId`, `WorkspaceId`, `OperatorId`, and `EnrollmentTicketId` share one
128-bit UUIDv7-compatible representation and generator. Their byte ordering is
time-sortable and the embedded millisecond timestamp is diagnostic only; it is
never authorization or causal evidence. Device identity remains independent of
signing keys. `CapabilityId` deliberately stays a human-readable string label
because capabilities are advertised scheduler vocabulary, not entity rows.
