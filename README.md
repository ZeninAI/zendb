# ZenDB

ZenDB is a synchronous embedded database foundation with portable CRDT values,
durable storage mechanics, workspace authorization, and workspace-owned
peer-to-peer replication.

## Crates

| Crate | Responsibility |
|---|---|
| `zendb-types` | Installation and workspace IDs, persisted libp2p public keys, event stamps, envelopes, cells, operations, CRDT values, and binary utilities |
| `zendb-storage` | B+ tree, KeyDir, SkipList, generic State, Topic, and the invariant-preserving Table facade |
| `zendb-workspace` | Catalog and device policy, local HLC and receipts, authenticated admission, and the private Tokio/libp2p replication runtime |

## Identity And Events

Applications provide an account-root libp2p keypair through `PeerIdentity`.
ZenDB derives a distinct Ed25519 transport key for every
`(WorkspaceId, InstallationId)` pair. Only that derived public key is stored in
the workspace device registry; private keys are never serialized.

Every table mutation is a fully stamped `Event`:

```rust
pub struct EventId {
    pub author: InstallationId,
    pub sequence: u64,
}

pub struct EventStamp {
    pub id: EventId,
    pub time: EventTime,
}
```

`InstallationId` and `WorkspaceId` are currently random eight-byte values with
thirteen-character Crockford Base32 display forms.

## Tables And States

A Table has a fixed `PrimaryKey -> Cell` shape backed by materialized state, a
bounded write cache, and a durable `Topic<Change>`. Local application writes
require Contributor access. System tables are publicly readable but writable
only through workspace-owned APIs.

A State is caller-typed local storage, `State<K, V>`, with no Event or Topic.
Catalog declarations are durable while typed handles are opened lazily.

## Replication

`Workspace` privately owns a Tokio worker and a libp2p Gossipsub swarm. The
runtime starts when `_devices` contains another installation and drains then
stops when the last remote installation is removed. Applications do not create
a replication link or supply an async runtime.

Gossipsub signs each bincode `Envelope` with the derived workspace key and uses
strict signature validation. `Workspace::admit_event` then binds the signed
libp2p source to the envelope's enrolled `InstallationId`, checks its role, and
applies events through the normal convergence path.

Publishing batches per table. `BatchConfig::max_bytes` is a flush threshold,
not an event-size admission limit; Gossipsub's transport limit defaults to
100 MiB.

## Enrollment

Only an Admin writes `_devices`. The Admin assigns an `InstallationId`, the
joining application derives its workspace public key with
`derive_workspace_public_key`, and the Admin stores that key in a
`DeviceRecord`.

`Workspace::join` currently persists the assigned local identity and creates
empty staged system storage. Initial registry/history synchronization is
deferred, so a staged join does not start replication or grant itself a role.

## Durable Layout

```text
workspace-root/
  _identity
  _lock
  tables/
    _catalog/
    _devices/
    <table-name>/
  states/
    _catalog/
    _peers/
    <state-name>/
```

The project is not migration-stable. Compile the workspace with:

```text
cargo check --workspace
```
