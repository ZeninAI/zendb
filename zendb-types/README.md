# zendb-types

`zendb-types` owns ZenDB's portable data model and shared binary utility layer.
It contains no storage configuration, Catalog policy, networking runtime, or
replication protocol.

## Identity And Time

`PeerId` is a ZenDB-owned wrapper around `libp2p-identity::PeerId`. It preserves
the canonical PeerId byte encoding while providing native bincode traits. Its
constructors and representation methods mirror the useful libp2p surface:
`from_public_key`, `from_bytes`, `random`, `to_bytes`, and `to_base58`.
`PrimaryKey::PeerId` is intentionally key-only; primary keys do not need to
implement the CRDT `Type` trait.

`WorkspaceId` is an independent random 128-bit identifier stored as a fixed
16-byte array. Its canonical display form is 26-character uppercase Crockford
Base32; parsing is case-insensitive and accepts the standard `O`/`I`/`L`
aliases. A workspace identifier identifies a workspace but does not authorize
joining it.

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
`(physical_ms, logical, peer_id, sequence)`. `Role` is persisted progressive
access data with `Contributor`, `Operator`, and `Admin` variants. No role means
Reader. `Role::has_at_least` expresses the hierarchy; workspace owns
authorization behavior.

### PeerIdentity

`PeerIdentity` is a trait abstracting a device's cryptographic identity:

```rust
pub trait PeerIdentity: Send + Sync {
    fn peer_id(&self) -> &PeerId;
    fn sign(&self, message: &[u8]) -> Result<Signature, SigningError>;
}
```

The workspace borrows `peer_id()` during initialization and keeps that stable
value for minting `EventId`s; `sign()` supports future event signatures. It
never sees private key material directly; the implementation decides where the
key lives (in-memory, OS keychain, HSM, KMS).

Applications supply the implementation and may back it with an in-memory key,
an OS keychain, HSM, KMS, or another persistent key store. Test-only
implementations belong in the consuming crate rather than the portable type
model.

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
