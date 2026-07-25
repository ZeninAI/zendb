# zendb-types

`zendb-types` owns ZenDB's portable data model and shared binary utility layer.
It contains no storage configuration, Catalog policy, networking runtime, or
replication protocol.

## Identity And Time

`PeerId` and `WorkspaceId` are distinct domain newtypes backed by
`libp2p-identity::PeerId`. Both use the same libp2p Ed25519-derived generator,
canonical PeerId bytes, ordering, and explicit bincode encoding.

```rust
pub struct EventId {
    pub peer_id: PeerId,
    pub sequence: u64,
}

pub struct EventTime {
    pub physical_ms: u64,
    pub logical: u32,
}
```

`EventStamp` orders by
`(physical_ms, logical, peer_id, sequence)`. `Roles` is inert persisted data
with `Contributor`, `Operator`, and `Dispatcher` variants; workspace owns all
authorization behavior.

### PeerIdentity

`PeerIdentity` is a trait abstracting a device's cryptographic identity:

```rust
pub trait PeerIdentity: Send + Sync {
    fn peer_id(&self) -> PeerId;
    fn sign(&self, message: &[u8]) -> Result<Signature, SigningError>;
}
```

The workspace uses `peer_id()` for minting `EventId`s and `sign()` for future
event signatures. It never sees private key material directly; the
implementation decides where the key lives (in-memory, OS keychain, HSM, KMS).

`LocalPeerIdentity` is a default in-memory implementation backed by an
Ed25519 `Keypair`, suitable for tests, examples, and local-only deployments.
Application-supplied impls may back the same trait by a persistent key store
without workspace changes.

`Signature` is an opaque owned byte container, `Encode`/`Decode` so future
event envelopes can carry it. `SigningError` is a distinct error type because
signing can occur outside the workspace (e.g. an account layer signing a join
request).

## Cells And Operations

A `Cell` contains `Option<Value>` and an `EventStamp`; `None` is a tombstone.
`Type::apply`, `Type::merge`, and `ContainerType::apply_walk` receive both
current and incoming stamps through `MergeStamps`.

An `Event` contains a `PrimaryKey`, `Path`, `Op`, and `EventStamp`. `Path` is
directly represented as `Vec<Segment>`. `Op::Upsert` creates or replaces a
value.

## Binary Utilities

`utils::serdes` is the single bincode layer used by types and storage. It uses
little-endian fixed-width integer encoding and provides direct buffer,
size-only, `serialize_to_vec`, and `deserialize_from` operations. Serialization
failures are returned as `io::Error`.

`utils::time::physical_ms()` returns the current wall-clock time as
milliseconds since the Unix epoch, centralizing the `SystemTime` → `u64`
conversion and overflow handling.

`Blob::encode` and `Blob::decode` use the serdes helpers directly and return
`io::Result`.

## Values

The registered value set includes scalar values, Counter, multi-value
register, OR-set, set, priority queue, collaborative text, Record, and List.
Record and List implement recursive Segment traversal.
