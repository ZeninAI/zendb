# zendb-workspace

`zendb-workspace` is ZenDB's synchronous orchestration layer. It owns catalog
and installation policy, local clocks and receipts, authenticated remote admission,
and a private Tokio/libp2p replication runtime.

## Identity Boundary

Applications pass `Arc<dyn PeerIdentity>` to workspace construction. The
trait exposes only an account-root libp2p keypair and a display name. ZenDB
derives an in-memory Ed25519 key from the fixed versioned domain plus
`WorkspaceId` and `InstallationId`.

The local `_identity` file persists only those two IDs. On open, the workspace
derives the key again and verifies its public half against the local
`Installation`. The root public key is never a workspace installation identity.

The lifecycle entry points are:

- `Workspace::create(root, identity, config)` uses `config.workspace_id` when
  supplied, otherwise generates the workspace ID; it always generates the local
  installation ID and establishes it as Admin. It synchronizes the assembled
  system storage before committing `_identity` and removes partial storage if
  creation fails.
- `Workspace::open(root, identity, config)` reads both IDs and requires the
  derived local key to match an enrolled installation. When
  `config.workspace_id` is supplied, open also requires it to match the stored
  workspace ID.

There is no public join lifecycle until initial synchronization can create a
fully usable workspace. Direct enrollment still establishes the future
installation identity and connection data out of band.

The deterministic workspace keypair derivation helper is available through the
`test-support` feature for integration fixtures. Fixtures convert its public
key into the stored `PublicKey` value themselves. The production enrollment
API that will expose this operation to applications is still pending.

## Public Surface

`Workspace` exposes:

- `installations()` for registry and role operations.
- `tables()` for replicated table catalog operations.
- `states()` for local typed state lifecycle.
- `admit_event(envelope, source)` for authenticated remote admission.
- `flush()` and `sync()` as coordinated durability barriers.

The replication controller, publisher, worker, Swarm, and derived
`LocalInstallationIdentity` are crate-private.

## Authenticated Admission

Gossipsub signed mode authenticates the original message source across mesh
forwarding. `Workspace::admit_event` then:

1. Looks up `Envelope.author` in `_installations`.
2. Derives the expected libp2p PeerId from the stored `PublicKey`.
3. Requires it to match the signed Gossipsub source.
4. Requires Admin for system tables or Contributor for application tables.
5. Reconstructs full Events and sends them through the workspace's remote
   application path.

`AdmitError` distinguishes unknown authors, source/author mismatch,
authorization failure, and workspace insertion errors. Envelopes contain no
duplicate application signature.

## Workspace-Owned Replication

`WorkspaceConfig` contains `ReplicationConfig`; tuning is runtime-only and is
not persisted. The defaults are:

```rust
BatchConfig {
    max_events: 16,
    max_bytes: 65_536,
    linger: Duration::from_millis(50),
}
```

The replication channel capacity defaults to 1024 and Gossipsub's maximum
transmit size defaults to 100 MiB. `max_bytes` is only a batch flush threshold.
A first event larger than the threshold is sent immediately as a one-event
envelope.

The runtime listens on every `ReplicationConfig::listen_addresses` entry; the
default is `/ip4/0.0.0.0/tcp/0`. Known-peer dial retries begin at one second and
cap at sixty seconds through `DialConfig`.

The private single-thread Tokio network worker uses TCP, Noise, Yamux, DNS,
mDNS, ping, Identify, and strict signed Gossipsub. Incoming envelopes cross a bounded
bridge to a sequential admission thread, preserving arrival order without
blocking the Tokio event loop. mDNS peers receive a LAN score bonus and ping
latency adjusts their application score. Topics are scoped by `WorkspaceId`.

`Installation.addresses` contains durable Admin-managed `zendb-types`
`Multiaddr` values. The worker
derives the expected `PeerId` from the installation public key and uses known-peer
`DialOpts`, so an address selects an endpoint but cannot impersonate an installation.
Catalog, mDNS, and Identify addresses remain separate in memory. Unknown peers
are disconnected and their Gossipsub messages do not enter admission.

The synchronous workspace commit path uses blocking bounded submission when a
replication worker is running. It forwards only locally authored events, so
admitted remote events are never republished.

## Installation-Driven Lifecycle

Replication runs only while the local installation remains enrolled with its
expected workspace key and `_installations` contains another installation.
After an `_installations` event commits, `WorkspaceCore` updates `Membership`
and asks `ReplicationController` to reconcile. The controller reads the current
membership through its weak reference to the core and derives the peer routes:

- The first remote upsert starts the worker before the enrollment event is
  submitted.
- Additional remote installations reuse the worker.
- Address changes replace future dial candidates and reset reconnect backoff
  without disconnecting an authenticated connection.
- Removing an installation blacklists its derived PeerId and disconnects it.
- Removing the final remote marks shutdown pending; the commit path submits the
  deletion, then the worker drains and stops.
- Removing the local installation, clearing its role, or replacing its
  workspace key stops its worker after the registry event completes the
  listener chain.

Re-enrollment removes the peer from the blacklist. Replacing an installation key
revokes the previous transport PeerId before allowing the new one.
Workspace key derivation includes `InstallationId`, so correctly derived installation
keys are unique without enrollment-time or admission-time registry scans. The
runtime treats one public key per installation as a trusted enrollment
invariant. Admission still binds `Envelope.author` to Gossipsub's authenticated
original publisher by comparing the installation's key-derived PeerId.

The controller keeps the derived peer projection, pending shutdown, and worker
lifecycle in one mutex-protected state machine. Replication internals are split
by responsibility: `batcher` builds envelopes, `peers` owns routes and
retry state, `swarm` constructs libp2p, `worker` runs the async loop, and
`runtime` owns the worker and admission threads.

## Direct Enrollment

Only an Admin may mutate `_installations`:

The following fixture example requires the crate's `test-support` feature;
production enrollment does not yet expose this helper.

```rust
let installation_id = InstallationId::generate();
let public_key =
    PublicKey::from_libp2p(
        derive_workspace_public_key(joiner.as_ref(), workspace.workspace_id(), installation_id)?
            .public(),
    );

workspace.installations().upsert(
    installation_id,
    Installation {
        display_name: "new-laptop".to_owned(),
        role: Some(Role::Contributor),
        public_key,
        addresses: vec!["/dns4/laptop.example/tcp/7400".parse()?],
    },
)?;
```

The application communicates the workspace ID, assigned installation ID, and
bootstrap multiaddrs out of band. There are no invitation links, bearer tokens,
nonce state, or request-response enrollment protocol.

`Installations::delete` is revocation. Deleting the row makes later admission fail
independently of the network blacklist.

## Event Application

`ChangeListener` remains a public fire-and-forget table callback:

```rust
pub trait ChangeListener: Send + Sync {
    fn on_change(&self, change: &Change);
}
```

Listeners are application callbacks only. An `OpenTable` stores the table and
its application listener stack; `TableHandle` is a lightweight capability
containing that shared open table plus a weak reference to the owning workspace. Callbacks
are snapshotted and invoked after the table commit, causal observation, system
projection, and replication submission. They therefore run without holding
the listener lock.

`WorkspaceCore` owns the single explicit application sequence:

```text
authorize -> mint -> table apply -> observe -> project -> publish -> notify
```

Minting releases the causal lock before table I/O. A failed local table write
therefore consumes its sequence and leaves a valid gap for future anti-entropy
handling. Remote admission authenticates and authorizes the envelope once,
then applies its events without repeating those checks. System handles remain
publicly readable but reject public writes.

## Tables, States, And Durability

The private `TableStore` owns and eagerly opens replicated `OpenTable` values.
`Tables` is the public workspace-bound facade that returns authorized handles.
`_catalog` and `_installations` are created together by `TableStore` and are
maintained through authorized workspace APIs; their committed changes are
projected directly into table and membership state. Workspace assembly obtains
both system tables once through the ordinary store lookup: `Tables` retains the
catalog table and `Installations` retains the installations table for mutations.

`Membership` owns decoded installation and authorization state as an immutable
snapshot behind `ArcSwap`. The snapshot caches the local installation separately
from the complete registry, making the per-event local authorization path
lock-free and free of map lookup while keeping both views atomically consistent.
`CausalTracker` independently owns receipt windows and the local event clock in
`_causal`, keyed by `InstallationId`. `Installations` is the public
workspace-backed registry facade and owns no projection state. Future initial
synchronization will seed and advance a joining installation's clock by
observing received stamps.

`mint()` exclusively allocates and advances the local event sequence.
`observe()` updates receipt windows and merges event time into the HLC; it does
not modify the local sequence cursor.

`TableStore::create` writes the `_catalog` and `_installations` catalog events
as local sequences 1 and 2. Create assembly then writes the initial Admin
installation as sequence 3 and its matching local causal row with
`next_sequence = 4` before constructing the in-memory owners. Each event samples
physical time independently. The causal cache therefore starts clean and does
not defer its initial row to a later flush.
Open requires that persisted local row and its clock; it does not regenerate
causal state for a missing or malformed local installation ID.

The `_catalog` and `_causal` state handles remain open for the workspace
lifetime and cannot be closed or deleted through public state APIs. Application
states remain typed and lazily opened.

`Workspace::flush()` writes dirty causal state before flushing tables and local
states. `sync()` performs the same ordered writeback with durable backend
synchronization. Drop requests replication shutdown before the existing
best-effort flush.
