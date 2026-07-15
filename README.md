# ZenDB

An embedded, local-first CRDT database for applications that need durable local
state and optional peer-to-peer workspace replication without a central
authority.

ZenDB provides:

- recursively nested CRDT values and deterministic merge;
- local tables and device-private nested overlays;
- signed shared journals with durable per-origin contiguous frontiers;
- device-only workspace membership and fixed workspace roles;
- QR/link ticket onboarding and direct Manager pre-admission;
- mutually authenticated encrypted TCP anti-entropy;
- signed LAN discovery, heartbeat presence, departure notices, and reconnect;
- snapshot bootstrap, stable-frontier derivation, and tombstone compaction; and
- an existing native local streaming operator runtime.

ZenDB is not SQL, distributed consensus, or a cloud control plane. Every
workspace replica runs inside the client application. Optional hosted adapters
can help peers find each other, but they do not become database authorities.

## Architecture

```text
Application
    |
    v
zendb-engine::Workspace<D>
    |-- local/shared mutation routing and authorization
    |-- device control state and signed shared journal
    |-- onboarding, anti-entropy, snapshots, compaction
    |-- TCP cluster runtime and LAN peer discovery
    `-- existing local operator workers and timers
         |
         +--> zendb-transport  authenticated encrypted sessions, profile
         +--> zendb-sync       portable replication wire records
         +--> zendb-storage    durable tables, states, logs, indexes
         `--> zendb-types      CRDTs, IDs, devices, roles, frontiers

zendb-external                optional outbound hosted discovery/rendezvous
```

The dependency direction keeps values portable and the product policy in one
concrete owner. `zendb-types` has no filesystem or socket behavior.
`zendb-storage` does not decide authorization. `zendb-transport` authenticates
and encrypts a byte session but does not admit devices. `Workspace` composes
those pieces and enforces the complete operation.

## Crates

| Crate | Responsibility | Important exports |
|---|---|---|
| [`zendb-types`](zendb-types/) | CRDT data and portable control records | `Cell`, `Event`, `Hlc`, `Value`, `DeviceRecord`, `WorkspaceRole`, `ContiguousFrontier` |
| [`zendb-storage`](zendb-storage/) | Generic durable data structures | `BPlusTree`, `KeyDir`, `SkipList`, `Topic`, `State`, `Table` |
| [`zendb-transport`](zendb-transport/) | Concrete client transport mechanics | `DeviceProfile`, `SecureTcpSession`, `EnrollmentPresentation`, `PresenceTracker` |
| [`zendb-sync`](zendb-sync/) | Engine-independent sync records | `SyncEnvelope`, `RangeRequest`, `WorkspaceSyncSummary`, `SnapshotManifest`, `SyncSnapshotChunk` |
| [`zendb-engine`](zendb-engine/) | Concrete workspace product runtime | `Workspace`, `ClusterConfig`, `ClusterRuntime`, `SyncReport`, `OnboardingResult` |
| [`zendb-external`](zendb-external/) | Optional outbound hosted adapters | `HostedRendezvousClient`, `HostedDiscoveryClient` |
| [`zendb-testing`](zendb-testing/) | Integration fixtures and local operators | document pipeline test types |

The former `zendb-identity` crate was removed. The database does not model
OAuth users or principals. Application identity may inform whether an app asks
a Manager to admit a device, but the replicated authority is always DeviceId.

## Workspace Model

A workspace is one distributed CRDT ledger with one stable `WorkspaceId`.
Every installation has a random, stable `DeviceId` and a durable signing
profile. DeviceId is independent of the signing key so keys can rotate.

Membership is represented by a live `devices.<device_id>` Cell in replicated
control state. Deleting that Cell revokes the device. There is no separate
membership table, status row, workspace secret, owner key, or principal-device
binding.

Every admitted device receives all shared data and is an implicit Reader. The
only explicit workspace-wide roles are:

- `Contributor`: create/delete shared tables and mutate shared data.
- `Dispatcher`: manage operator specifications.
- `Manager`: manage devices, role sets, and enrollment tickets.

The creator starts with all three roles. Newly onboarded devices start with no
explicit role and therefore read only. The roles are fixed protocol values,
not user-defined policy programs. Capabilities such as `vpn` or `gpu` are
self-advertised device labels for future placement, not permissions.

## Local And Shared Data

A local table is present only in the local catalog. It requires no workspace
role and consumes no shared origin sequence:

```rust
let table = workspace.table("drafts", Some(local_config))?;
table.get()?.write().insert_event(local_event)?;
```

A shared table is a replicated control declaration. Contributors create it and
route all data mutations through `Workspace`:

```rust
let table = workspace.create_shared_table("documents", TableConfig::default())?;
workspace.mutate("documents", document_id, shared_event)?;
```

Direct insertion on a physical shared table is rejected. The workspace path is
responsible for role validation, durable origin-sequence allocation, signing,
journal append, and application.

Within shared data, `Cell.sync = Some(false)` creates a device-private overlay
at that recursive path. Local writes under it never enter the shared journal.
Remote ancestor changes cannot erase the overlay. Re-enabling sync discards
the private overlay and reveals current shared state; it never silently
publishes local data.

## Onboarding

A joining installation first persists its DeviceId and signing key. Discovery
or a successful socket connection is not admission. ZenDB has two protocols:

1. **Enrollment presentation.** A Manager publishes a ticket verifier and puts
   the private ticket credential in a QR code or link. The candidate signs an
   exact admission proof. Any admitted reader can validate and relay it.
2. **Direct admission.** A Manager receives the candidate DeviceId and public
   key out of band and adds the Device record. The candidate may then bootstrap
   from any peer while pinning an expected peer public key.

Both flows prove possession of the candidate private key and transfer a
manifest-verified, independently hashed chunked snapshot. The resulting device
has no explicit roles. Ticket admission evidence accompanies the admission
event so every replica can validate the exceptional Manager-free write.

Relevant APIs are `Workspace::create_joining()`,
`create_enrollment_ticket()`, `bootstrap_with_ticket()`, `admit_device()`,
and `bootstrap_direct()`.

## Networking And Synchronization

`Workspace::sync_tcp()` performs a single bilateral anti-entropy session. The
transport uses signed ephemeral X25519 handshakes authenticated by accepted
Ed25519 device keys and ChaCha20-Poly1305 encrypted framing. Peers then:

1. exchange signed heartbeat presence and contiguous frontier summaries;
2. request exact missing `(origin, sequence range)` history;
3. send bounded batches of signed events;
4. retain out-of-order events until gaps and control dependencies arrive; and
5. use a chunked snapshot fallback when retained history is unavailable.

`Workspace::start_cluster(ClusterConfig)` runs a TCP listener plus periodic
multi-peer sync and reconnect. Optional signed UDP announcements discover LAN
peers. The runtime publishes frontier checkpoints and sends a signed departure
after stopping heartbeat producers.

Presence is deliberately local. A signed heartbeat advertises its intended
idle interval; each receiver derives `Direct`, `Indirect`, `Suspect`,
`Unreachable`, `Departed`, or `Unknown` using bounded arrival samples and a
local grace multiplier. A continuous suspicion score is available to consumers.
None of these observations revoke membership or grant authorization.

## Frontiers, Snapshots, And Compaction

Only shared events receive `EventIdentity { origin_device_id, origin_seq }`.
Each replica durably tracks the largest gap-free prefix per origin and
checkpoints it in its own Device record.

The stable frontier is the point observed by every admitted device. Offline
members continue to hold it back until a Manager removes them. This is a
membership and storage-retention consequence, not a liveness timeout.

Snapshots contain shared control and live shared tables only. They exclude
local tables and private overlays. `compact_shared()` first requires a
retained snapshot, derives a stable HLC from the stable frontier, and recursively
removes CRDT tombstones no newer than that proof. Shared journal pruning is not
implemented yet; retaining history is the conservative recovery behavior.

## Signing-Key Rotation

A device calls `stage_local_key_rotation()` to stage a secondary key in its
replicated key ring. It continues using
the primary until the stable frontier proves that every admitted device has
received the stage event. Promotion is signed by the staged key and swaps the
two keys. The prior primary remains as the secondary verifier for delayed old
events and is replaced by a later rotation. `promote_local_key_rotation()`
performs the stable-frontier-gated swap. DeviceId never changes.

## Operators

The existing native operator runtime supports compiled Rust operators,
subscriptions, local state, facets, timers, and worker lifecycle. Distributed
declarative operator reconciliation, capability placement, leases, fencing,
Rhai isolation, and Dispatcher enforcement are intentionally not implemented
in this pass. They are specified separately in ADR 008 and must not be inferred
from similarly named legacy operator types.

## Decisions

The normative distributed design is split into narrow records under
[`.plan/decisions`](.plan/decisions/README.md):

1. devices and roles;
2. device liveness;
3. shared journal and replication frontiers;
4. device onboarding;
5. device signing-key rotation;
6. local/shared sync boundaries;
7. tombstone compaction watermarks;
8. proposed distributed operator reconciliation and leases; and
9. shared-table lifecycle.

Older large documents under `.plan` are retained as historical exploration.
Their principal/OAuth abstractions and broad transport traits are not current
APIs.

## Building And Testing

```bash
cargo build --workspace
cargo test --workspace
```

The workspace uses Rust 2021. The engine has integration coverage for local and
shared routing, authenticated sync, ticket/direct onboarding, key rotation,
presence/departure, snapshots, and compaction.

## License

License terms have not yet been declared in this repository.
