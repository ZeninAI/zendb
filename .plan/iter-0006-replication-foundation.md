# Iteration 0006: Replication Foundation - Trust, Identity, Direct Enrollment, And The Gossipsub Mesh

Status: implemented.

This iteration establishes the trust boundary and identity model used by
replication, the direct Admin enrollment model, and a Tokio plus Gossipsub
mesh. It does not implement anti-entropy, initial joiner synchronization,
snapshots, relay/NAT traversal, or operator dispatch.

The baseline names ZNIN-36 as the most important gap: `insert_internal` trusts
the claimed event author by role alone. This iteration authenticates the author
of every remote event before it reaches that internal convergence point.

---

## 1. Problems

### 1.1 Unauthenticated Remote Events

`EventStamp.peer_id` is currently a claim. A remote caller that can reach the
internal insertion path can forge another device identity. Replication needs
strict Gossipsub signed-message validation and a registry lookup of the author
before any event is applied.

### 1.2 No Public Admission Seam

There is no public operation for applying authenticated remote events.
Replication needs one top-level admission operation that verifies, authorizes,
and converges remote events without exposing `TableHandle::insert_internal`.

### 1.3 No Direct Enrollment Path

An Admin must be able to explicitly add a device to `_devices`, then provide the
new device with its assigned installation ID and connection bootstrap data. The
new device does not request an invitation, and no other device is permitted to
write the device registry on its behalf.

---

## 2. Identity Model

### 2.1 InstallationId Is The Event Author

`InstallationId` is the compact, random, stable installation identity. It is
the author in `EventId`, the key in per-device CRDT data, and the key of a
device registry row. It is not a transport peer ID and it is not a public key.

The identifier is eight bytes for this iteration. This is sufficient for the
small device populations expected in a workspace, while keeping every event
and CRDT actor reference compact.

```rust
pub struct EventId {
    pub author: InstallationId,
    pub sequence: u64,
}
```

### 2.2 Opaque ID Macro

`InstallationId` and `WorkspaceId` are distinct types with the same current
representation. A crate-private macro removes duplicated implementation while
preserving type safety:

```rust
// zendb-types/src/identity/opaque_id.rs
opaque_id!(InstallationId, InstallationIdParseError, 8);
opaque_id!(WorkspaceId, WorkspaceIdParseError, 8);
```

The macro takes the type name, its parse-error type, and its byte size. It
generates the opaque tuple type, `generate`, byte accessors, bincode traits,
and Crockford Base32 `Display` and `FromStr` implementations. Its codec works
on byte arrays rather than a fixed integer primitive. Increasing the workspace
ID to sixteen bytes is therefore a one-line macro invocation change.

The current eight-byte IDs display as thirteen Crockford Base32 characters.
Changing the byte count changes the display length automatically.

### 2.3 PublicKey Wraps Libp2p

ZenDB uses libp2p's generic `PublicKey`, not a ZenDB-owned fixed-size Ed25519
key. This keeps signing, verification, public-key encoding, and transport peer
identity consistent with libp2p.

`PublicKey` is a ZenDB newtype only because Rust does not permit implementing
bincode traits for an external type:

```rust
use libp2p_identity::PublicKey as Libp2pPublicKey;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PublicKey(Libp2pPublicKey);

impl PublicKey {
    pub fn from_libp2p(key: Libp2pPublicKey) -> Self {
        Self(key)
    }

    pub fn as_libp2p(&self) -> &Libp2pPublicKey {
        &self.0
    }
}

impl Encode for PublicKey {
    fn encode<E: Encoder>(&self, encoder: &mut E) -> Result<(), EncodeError> {
        self.0.encode_protobuf().encode(encoder)
    }
}

impl<Context> Decode<Context> for PublicKey {
    fn decode<D: Decoder<Context = Context>>(decoder: &mut D) -> Result<Self, DecodeError> {
        let bytes = Vec::<u8>::decode(decoder)?;
        Libp2pPublicKey::try_decode_protobuf(&bytes)
            .map(Self)
            .map_err(|error| DecodeError::OtherString(error.to_string()))
    }
}

bincode::impl_borrow_decode!(PublicKey);
```

The wrapper stores the canonical libp2p protobuf public-key representation,
including its key type. It is the only libp2p key type persisted by ZenDB.

### 2.4 PeerIdentity Supplies The Account Root Keypair

`PeerIdentity` stays in `zendb-types` and exposes precisely the application
owned libp2p keypair and device display name:

```rust
pub trait PeerIdentity: Send + Sync {
    fn keypair(&self) -> &libp2p_identity::Keypair;
    fn display_name(&self) -> &str;
}
```

There is no `sign` method and no public-key method. Private key material stays
inside the application implementation and is never serialized by ZenDB. This
keypair is an application account-root keypair; a workspace never places its
public key in `_devices` or uses it directly as its Gossipsub identity.

`zendb-types` depends on `libp2p-identity`. It still has no transport, async
runtime, swarm, or replication dependency.

### 2.5 WorkspaceIdentity Derives A Scoped Keypair

Every local workspace creates an in-memory `WorkspaceIdentity` from the
application root keypair, its `WorkspaceId`, and its local `InstallationId`:

```rust
struct WorkspaceIdentity {
    installation_id: InstallationId,
    keypair: libp2p_identity::Keypair,
}

fn derive_workspace_keypair(
    root: &libp2p_identity::Keypair,
    workspace_id: WorkspaceId,
    installation_id: InstallationId,
) -> libp2p_identity::Keypair {
    let mut domain = b"zendb/workspace-transport-key/v1\0".to_vec();
    domain.extend_from_slice(workspace_id.as_bytes());
    domain.extend_from_slice(installation_id.as_bytes());

    let seed = root.derive_secret(&domain)
        .expect("a root keypair that supports secret derivation");
    libp2p_identity::Keypair::ed25519_from_bytes(seed)
        .expect("derive_secret returns a valid Ed25519 seed")
}
```

`derive_secret` is libp2p-identity's deterministic KDF. ZenDB supplies the
fixed, versioned domain containing the workspace and installation identifiers;
it does not hash or mutate the root keypair itself. The root keypair must
support libp2p secret derivation; the resulting 32-byte secret always constructs
an Ed25519 workspace transport keypair.

The derived keypair is the only keypair used by the replication worker. Its
public key is stored in the local `DeviceRecord`, and Gossipsub signs with the
derived keypair. The derived keypair is deterministic and remains in memory;
only the local `(WorkspaceId, InstallationId)` pair is persisted in the local
workspace identity record so a later `Workspace::open` can derive it again.

This gives each installation a distinct network identity in each workspace,
even when several installations or workspaces use one account-root keypair.
It makes Gossipsub source authentication map one-to-one to an enrolled device,
so role checks, revocation, and per-installation event ordering remain sound.
`zendb-workspace` exposes `derive_workspace_public_key` so a joining
application can return the correct derived public key to the Admin without
duplicating the versioned derivation domain or exposing the derived private key.

### 2.6 PeerId Leaves The ZenDB Data Model

ZenDB's own `PeerId` wrapper is removed. `PeerId` is a libp2p transport address,
not a logical author or persisted table key. The transport derives it when
needed from a registry record:

```rust
fn peer_id(record: &DeviceRecord) -> libp2p_identity::PeerId {
    record.public_key.as_libp2p().to_peer_id()
}
```

`PrimaryKey::PeerId` is removed. `_devices` keys are
`PrimaryKey::Blob(installation_id.to_bytes().to_vec())`.

### 2.7 Data Model Changes

| Before | After |
|---|---|
| `EventId { peer_id: PeerId, sequence }` | `EventId { author: InstallationId, sequence }` |
| `PrimaryKey::PeerId(PeerId)` | `PrimaryKey::Blob` for `InstallationId` |
| CRDT maps keyed by `PeerId` | CRDT maps keyed by `InstallationId` |
| `Devices.local_peer_id` | `Devices.local_installation_id` |
| `DeviceRecord { display_name, role }` | `DeviceRecord { display_name, role, public_key }` |
| `PeerIdentity::peer_id() + sign()` | `PeerIdentity::keypair() + display_name()` |
| `WorkspaceId([u8; 16])` | `WorkspaceId([u8; 8])` |

`Signature` and ZenDB's `SigningError` are removed. Envelopes carry no
application signature; Gossipsub signs the complete encoded payload and owns
any signing failure.

### 2.8 Key Rotation

`InstallationId` is not tied to a root keypair, so replacing an application
root key does not rewrite event identities. It does change every derived
workspace public key for that application identity. A rotation protocol would
therefore update the relevant `DeviceRecord` values and retain prior keys for
historic verification. That is explicitly deferred.

---

## 3. Trust Model And Admission

There are two independent authentication layers:

| Layer | Proves | Owner |
|---|---|---|
| Noise transport authentication | The wire peer controls its libp2p keypair. | libp2p |
| Gossipsub signed-message authentication | The original Gossipsub source signed the payload. | libp2p |

Noise authenticates the direct wire hop. Gossipsub signed mode authenticates
the original source across mesh forwarding. ZenDB binds that authenticated
source peer to the logical `InstallationId` in the envelope. Strict Gossipsub
validation resolves the source public key carried or identified by the protocol,
checks that it produces the claimed source `PeerId`, and verifies the message
signature before the application receives the payload. ZenDB does not perform a
second signature verification.

`Workspace::admit_event` is the only public replication admission seam. It:

1. Looks up `Envelope.author` in `Devices`.
2. Derives the expected libp2p `PeerId` from `record.public_key` and requires
   it to equal the original Gossipsub message source.
3. Requires `Admin` for system-table events and `Contributor` for application
   table events.
4. Reconstructs events and calls `insert_internal` for each one.

```rust
pub fn admit_event(
    &self,
    envelope: Envelope,
    gossipsub_source: libp2p_identity::PeerId,
) -> Result<(), AdmitError> {
    let record = self.devices().get(&envelope.author)
        .ok_or(AdmitError::UnknownPeer)?;
    if record.public_key.as_libp2p().to_peer_id() != gossipsub_source {
        return Err(AdmitError::AuthorMismatch);
    }

    let required = if self.tables().is_system_table(&envelope.table) {
        Role::Admin
    } else {
        Role::Contributor
    };
    if !record.role.is_some_and(|role| role.has_at_least(required)) {
        return Err(AdmitError::Unauthorized);
    }

    let table = self.tables().get(&envelope.table)
        .map_err(AdmitError::Workspace)?;
    for compact in envelope.events {
        table.insert_internal(Event {
            stamp: EventStamp {
                id: EventId { author: envelope.author, sequence: compact.sequence },
                time: compact.time,
            },
            primary_key: compact.primary_key,
            path: compact.path,
            op: compact.op,
        }).map_err(AdmitError::Workspace)?;
    }
    Ok(())
}
```

`AdmitError` has `UnknownPeer`, `AuthorMismatch`, `Unauthorized`, and
`Workspace(Error)` variants. The workspace's internal replication worker calls
this method only for a Gossipsub message accepted under strict signed-message
validation. This keeps network trust at the workspace boundary and leaves
`insert_internal` crate-visible.

---

## 4. Envelope And Publishing

The network format batches events from one author to one table. It is never
written to the event log. `Envelope` and `CompactEvent` live in `zendb-types`:
they are pure, bincode-encoded value types needed by the workspace admission
API and have no Tokio or swarm dependency.

```rust
pub struct Envelope {
    pub author: InstallationId,
    pub table: String,
    pub events: Vec<CompactEvent>,
}

pub struct CompactEvent {
    pub sequence: u64,
    pub time: EventTime,
    pub primary_key: PrimaryKey,
    pub path: Path,
    pub op: Op,
}
```

The wire payload is bincode encoded `Envelope`; events stored on disk remain
ordinary full `Event` values. There is no envelope signature. Gossipsub signs
the complete encoded payload and verifies it before delivering a message to the
application.

`zendb_workspace::replication` owns the publisher, listener, Gossipsub swarm,
and worker. Its `ReplicationListener` observes successful local table changes
and submits only events authored by the local installation. It must not
republish admitted remote events. The worker's internal publisher logic groups
pending events per table and publishes completed batches through Gossipsub.

The outbound channel remains bounded and uses blocking send. A saturated network
therefore applies honest backpressure to the synchronous writer instead of
dropping locally committed events.

---

## 5. Direct Admin Enrollment And Joining

### 5.1 Lifecycle Paths

| Path | Initiator | Network required | Rule |
|---|---|---|---|
| Create | first device | no | local device becomes Admin |
| Open | enrolled device | no | local installation must be in `_devices` |
| Direct enrollment | Admin | no | only Admin writes the new `_devices` row |
| Join | enrolled device | deferred | stages assigned ID; initial sync starts future mesh participation |

There are no invitation links, bearer tokens, invite nonces, invitation state,
or join request-response protocol in this iteration.

### 5.2 Admin Enrollment

The Admin obtains the joiner's desired display name out of band, generates an
`InstallationId`, and gives the joiner the workspace ID and assigned
installation ID. The joiner derives its `WorkspaceIdentity` locally and
returns that derived public key through the same application-level setup flow.
The Admin then publishes the registry row through the normal Admin-authorized
device API:

```rust
let installation_id = InstallationId::generate();
let joiner_workspace_public_key = obtain_from_joiner();

workspace.devices().upsert(installation_id, DeviceRecord {
    display_name: "new-laptop".into(),
    role: Some(Role::Contributor),
    public_key: PublicKey::from_libp2p(joiner_workspace_public_key),
})?;
```

An Admin cannot derive this public key from the joiner's account-root public
key: derivation deliberately requires the joiner's root private key. The Admin
also gives the joining application bootstrap multiaddrs. This is configuration,
not a signed invitation capability.

### 5.3 Join Hints

```rust
pub struct JoinHints {
    pub bootstrap_peers: Vec<String>,
    pub installation_id: InstallationId,
}
```

`Workspace::join` persists the local `(WorkspaceId, InstallationId)` identity
pair, derives its workspace key, and creates empty staged system storage. It
does not grant the joiner a local role or start replication because it has no
trusted copy of the existing registry yet. The initial synchronization that
transfers registry and table history is a future replication feature. That
later synchronization starts mesh participation and advances the HLC through
normal receipt observation.

### 5.4 Joiner HLC

A joining device starts its local HLC at the current physical time. It does not
scan existing events at join time. Future initial joiner synchronization will
feed received event stamps through `Devices::observe`, advancing the clock
naturally before the joiner writes after synchronization.

---

## 6. Workspace-Owned Replication Runtime

Replication is an internal `zendb_workspace::replication` subsystem, not an
application-created link and not a separate crate. `zendb-workspace` directly
owns Tokio, libp2p, and Gossipsub dependencies. Its public API remains fully
synchronous: the subsystem owns a private Tokio network thread and a sequential
admission bridge thread.

The module contains the publisher, table listener, swarm construction, event
loop, and a private `ReplicationController` held by `Workspace`:

```rust
struct ReplicationController {
    remote_devices: Mutex<BTreeSet<InstallationId>>,
    runtime: Mutex<Option<RunningReplication>>,
}
```

`RunningReplication` owns the worker control channel and both thread handles.
It is never exposed through the public workspace API. The worker constructs
Gossipsub with `MessageAuthenticity::Signed(workspace_identity.keypair.clone())`
and strict validation. It publishes completed envelopes to the workspace topic;
for an incoming message it calls
`Workspace::admit_event(envelope, message.source)`. `message.source` is the
original signed Gossipsub author, not `propagation_source`, which is only the
immediate forwarding peer.

Incoming envelopes cross a bounded bridge to the sequential admission thread.
This preserves arrival order and keeps synchronous storage insertion off the
Tokio event loop.

### 6.1 Device Listener Controls Runtime Lifecycle

Replication runs exactly while the local installation remains enrolled with
its expected workspace public key and the registry contains at least one other
device. The local device itself never starts the runtime. Readers count as
remote devices: they participate in the mesh even though they cannot author
application-table writes.

On `Workspace::open`, after the registry has been loaded, the controller is
initialized from `Devices::list`. `Workspace::create` starts with only its
local Admin row, so its runtime remains stopped. The existing
`DeviceRegistryListener` owns all subsequent lifecycle changes:

- the first non-local device upsert starts the runtime;
- further non-local upserts leave it running;
- removing a non-local device removes and disconnects that peer;
- deleting the final non-local device drains the current outbound work and
  stops the runtime;
- deleting the local device, clearing its role, or replacing its workspace
  public key stops its runtime after the triggering registry event completes
  the listener chain.

Listener registration order is part of the invariant. For the first remote
device upsert, `DeviceRegistryListener` starts the runtime before
`ReplicationListener` submits that enrollment event. For a final remote-device
delete, the controller marks shutdown pending; the replication listener submits
the deletion, then the worker drains its outbound queue before it stops. This
is best-effort transport delivery, not a remote-delivery acknowledgement.

The workspace Drop path requests worker shutdown before performing its existing
best-effort flush. A future explicit close operation can expose a joinable
shutdown if applications need that lifecycle guarantee.

### 6.2 Device Revocation Removes The Peer From Gossip

Revocation is deletion of the device's `_devices` row, not a role change to an
empty value. `Devices::remove(installation_id)` requires Admin and publishes
that deletion. `DeviceRegistryListener` remains the owner of registry cache
mutation and forwards the removed record to `ReplicationController`.

The notification contains the deleted record's `PublicKey`, because the cache
no longer contains the record. The controller derives its `PeerId`, instructs
the worker to call Gossipsub `blacklist_peer`, removes it as an explicit peer if
applicable, and calls `Swarm::disconnect_peer_id`. Gossipsub neither sends to
nor accepts messages from the revoked peer after this action. Admission
independently continues to reject events from the absent installation ID.

This is forward-looking revocation. Existing events remain valid historical
data. Connection-level gating beyond the Gossipsub mesh is deferred.

### 6.3 Message Size And Batching

`BatchConfig::max_bytes` is a batch flush threshold, not an admission or event
size limit. The publisher accumulates events until it reaches `max_events`, the
accumulated batch reaches `max_bytes`, or `linger_ms` expires. A first event
larger than `max_bytes` is placed in its own envelope and published immediately;
it is never rejected or split merely because of the threshold.

```rust
pub struct BatchConfig {
    pub max_events: usize, // default: 16
    pub max_bytes: usize,  // default: 65_536; flush threshold only
    pub linger_ms: u64,    // default: 50
}
```

Gossipsub does impose a configurable transmit limit, so it cannot be removed.
`build_swarm` sets its maximum transmit size to `100 * 1024 * 1024` bytes. This
is a transport envelope limit, separate from `BatchConfig::max_bytes`; the
batch configuration must not be validated as a cap on admitted event size.

### 6.4 Configuration

```rust
pub struct ReplicationConfig {
    pub batch: BatchConfig,
    pub topology: TopologyConfig,
    pub outbound_capacity: usize,
    pub gossipsub_max_transmit_size: usize,
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            batch: BatchConfig::default(),
            topology: TopologyConfig::default(),
            outbound_capacity: 1024,
            gossipsub_max_transmit_size: 100 * 1024 * 1024,
        }
    }
}
```

`TopologyConfig` retains the LAN score bonus, latency weight, and target mesh
size. These are tuning controls; peer topology is discovered, not manually
declared by the application.

`WorkspaceConfig` contains this runtime configuration and is supplied to
`Workspace::create`, `Workspace::open`, and `Workspace::join`. It is not
persisted workspace data: replication tuning may change for each application
run. The device listener alone decides whether the configured runtime is
currently started.

---

## 7. Implementation Sequence

### Phase 0 - Identity Migration And Admission

1. Add `opaque_id!`, generate eight-byte `InstallationId` and `WorkspaceId`,
   and migrate all event and CRDT actors from `PeerId` to `InstallationId`.
2. Remove ZenDB's `PeerId`, `PrimaryKey::PeerId`, `Signature`, and
   `SigningError` types.
3. Add the bincode `PublicKey` wrapper over libp2p protobuf encoding. Keep
   `libp2p-identity` in `zendb-types`.
4. Change `PeerIdentity` to `keypair()` and `display_name()`. Persist the local
   `(WorkspaceId, InstallationId)` identity pair and derive an in-memory
   Ed25519 `WorkspaceIdentity` from the root keypair. Populate each initial
   `DeviceRecord.public_key` from the derived workspace keypair.
5. Migrate devices, peer state, receipt windows, and error values to
   `InstallationId`.
6. Add `Workspace::admit_event`, which binds an envelope author to the already
   authenticated Gossipsub source and performs authorization checks.
7. Run `cargo check --workspace`.

### Phase 1 - Replication Core

8. Add `Envelope` and compact events to `zendb-types`.
9. Add `zendb_workspace::replication`: its bounded publisher, table listener,
   private Tokio worker, and Gossipsub swarm over TCP, Noise, Yamux, mDNS,
   identify, and ping.
10. Add `ReplicationController` to `Workspace` and wire its remote-device set
    to `DeviceRegistryListener`, including first-remote start and final-remote
    drain then stop ordering.
11. Configure signed-message authenticity, strict validation, the 100 MiB
    transmit limit, and the workspace topic.
12. Connect device removal to Gossipsub peer blacklisting and swarm
    disconnection.
13. Add direct Admin device enrollment and `JoinHints`; do not add invitation
    types, invite state, or a direct-message protocol.
14. Run `cargo check --workspace`.

---

## 8. Completion Criteria

- `InstallationId` is the primary logical device identity in events, CRDTs,
  device lookup, and receipt tracking.
- `InstallationId` and `WorkspaceId` are generated by the byte-size parameter
  macro, currently at eight bytes each.
- `DeviceRecord` stores a bincode-capable wrapper around libp2p `PublicKey`.
- `PeerIdentity` exposes only `keypair()` and `display_name()`.
- The account-root keypair deterministically derives one in-memory Ed25519
  transport keypair for every local `(WorkspaceId, InstallationId)` pair.
- `_devices` stores those derived public keys; the root public key is never a
  workspace device identity.
- ZenDB does not serialize private key material, define a custom signing API,
  or define a custom persisted `PeerId`.
- Gossipsub signs every payload and applies strict validation before delivery.
- `Workspace::admit_event` binds the signed Gossipsub source to the public key
  stored for `Envelope.author`, authorizes it, and applies events through the
  normal convergence path.
- Envelopes carry no duplicate application signature.
- `Workspace` owns a private replication worker; applications neither create a
  replication link nor supply an async runtime.
- The existing device listener starts replication on the first remote device
  and stops it, after outbound drain, when the final remote device or the local
  device is removed.
- Only an Admin writes `_devices` for enrollment; invitation links and nonce
  tracking do not exist.
- Joiners start their HLC at now; initial historical synchronization remains
  future work.
- Deleting a device row removes the peer from Gossipsub and disconnects it.
- `max_bytes` is a flush threshold and Gossipsub is configured for 100 MiB.
- `cargo check --workspace` succeeds.
- Root and crate READMEs are updated when the implementation lands.

---

## 9. Deferred Work And Non-Goals

This iteration does not implement:

- anti-entropy or initial joiner synchronization;
- historical snapshot exchange;
- connection-level transport gating after a device revocation;
- relay or NAT traversal;
- multi-workspace links;
- Bluetooth transport;
- compression;
- key rotation history;
- operator dispatch;
- invitation links, invite tokens, nonce tracking, or request-response joining.

The absence of initial joiner synchronization is intentional. It is the future
feature that transfers historical registry and table state and advances a
joiner's HLC from those received event stamps.

---

## 10. Dependency Direction

```text
zendb-types              (InstallationId, WorkspaceId, PublicKey, PeerIdentity,
                           Event, Envelope, Role; libp2p-identity only)
  ^
zendb-storage            (Table, Topic, Backends)
  ^
zendb-workspace          (Devices, admission, catalogs, local HLC,
                           internal replication worker, Tokio, libp2p,
                           Gossipsub mesh lifecycle)
```

`zendb-types` knows the libp2p identity types because they define the persisted
public-key representation and the application identity contract. It does not
know networking transport or runtime concerns. Tokio and libp2p swarm code are
private implementation details of `zendb-workspace::replication`.
