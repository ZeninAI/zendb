# zendb-workspace

`zendb-workspace` is ZenDB's synchronous orchestration layer. It owns catalog
and device policy, local clocks and receipts, authenticated remote admission,
and a private Tokio/libp2p replication runtime.

## Identity Boundary

Applications pass `Arc<dyn PeerIdentity>` to workspace construction. The
trait exposes only an account-root libp2p keypair and a display name. ZenDB
derives an in-memory Ed25519 key from the fixed versioned domain plus
`WorkspaceId` and `InstallationId`.

The local `_identity` file persists only those two IDs. On open, the workspace
derives the key again and verifies its public half against the local
`DeviceRecord`. The root public key is never a workspace device identity.

The lifecycle entry points are:

- `Workspace::create(root, identity, config)` generates both IDs and establishes
  the local installation as Admin.
- `Workspace::open(root, identity, config)` reads both IDs and requires the
  derived local key to match an enrolled device. An empty staged registry may
  be reopened but remains untrusted and offline.
- `Workspace::join(root, workspace_id, identity, hints, config)` persists an
  Admin-assigned installation ID and creates empty staged system storage.

Initial join synchronization is deferred. A staged join has no local role or
trusted remote registry and therefore does not start replication.

`derive_workspace_public_key(identity, workspace_id, installation_id)` exposes
the deterministic public half needed during direct enrollment without exposing
the derived private key.

## Public Surface

`Workspace` exposes:

- `devices()` for registry, role, and installation receipt operations.
- `tables()` for replicated table catalog operations.
- `states()` for local typed state lifecycle.
- `admit_event(envelope, source)` for authenticated remote admission.
- `flush()` and `sync()` as coordinated durability barriers.

The replication controller, publisher, worker, Swarm, and derived
`WorkspaceIdentity` are crate-private.

## Authenticated Admission

Gossipsub signed mode authenticates the original message source across mesh
forwarding. `Workspace::admit_event` then:

1. Looks up `Envelope.author` in `_devices`.
2. Derives the expected libp2p PeerId from the stored `PublicKey`.
3. Requires it to match the signed Gossipsub source.
4. Requires Admin for system tables or Contributor for application tables.
5. Reconstructs full Events and calls the crate-private insertion path.

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
    linger_ms: 50,
}
```

The outbound channel capacity defaults to 1024 and Gossipsub's maximum
transmit size defaults to 100 MiB. `max_bytes` is only a batch flush threshold.
A first event larger than the threshold is sent immediately as a one-event
envelope.

The private Tokio network worker uses TCP, Noise, Yamux, DNS, mDNS, ping,
identify, and strict signed Gossipsub. Incoming envelopes cross a bounded
bridge to a sequential admission thread, preserving arrival order without
blocking the Tokio event loop. mDNS peers receive a LAN score bonus and ping
latency adjusts their application score. Topics are scoped by `WorkspaceId`.

The synchronous replication listener uses blocking bounded submission. It
forwards only locally authored events, so admitted remote events are never
republished.

## Device-Driven Lifecycle

Replication runs only while the local installation remains enrolled with its
expected workspace key and `_devices` contains another installation.
`DeviceRegistryListener` updates authorization state before notifying the
private controller:

- The first remote upsert starts the worker before the enrollment event is
  submitted.
- Additional remote devices reuse the worker.
- Removing a device blacklists its derived PeerId, removes it from explicit
  Gossipsub peers, and disconnects it.
- Removing the final remote marks shutdown pending; the replication listener
  submits the deletion, then the worker drains and stops.
- Removing the local installation, clearing its role, or replacing its
  workspace key stops its worker after the registry event completes the
  listener chain.

Re-enrollment removes the peer from the blacklist. Replacing a device key
revokes the previous transport PeerId before allowing the new one.

## Direct Enrollment

Only an Admin may mutate `_devices`:

```rust
let installation_id = InstallationId::generate();
let public_key =
    derive_workspace_public_key(joiner.as_ref(), workspace.id(), installation_id)?;

workspace.devices().upsert(
    installation_id,
    DeviceRecord {
        display_name: "new-laptop".to_owned(),
        role: Some(Role::Contributor),
        public_key,
    },
)?;
```

The application communicates the workspace ID, assigned installation ID, and
bootstrap multiaddrs out of band. There are no invitation links, bearer tokens,
nonce state, or request-response enrollment protocol.

`Devices::remove` is revocation. Deleting the row makes later admission fail
independently of the network blacklist.

## Event-Driven Maintenance

`ChangeListener` remains a public fire-and-forget table callback:

```rust
pub trait ChangeListener: Send + Sync {
    fn on_change(&self, change: &Change);
}
```

Internal listener registration is private. A crate-private per-table factory
captures table names for replication without widening the public callback.
Catalog-opened tables inherit receipt and replication listeners.

`TableHandle::insert` is the local application mutation path.
`TableHandle::insert_internal` remains crate-private and independently checks
the event author's role. System handles are readable but reject public writes.

## Tables, States, And Durability

`Tables` eagerly opens declared replicated tables. `_catalog` and `_devices`
are system tables maintained through authorized workspace APIs.

`States` manages caller-typed local storage lazily. `_peers` stores receipt
windows plus the local event clock, keyed by `InstallationId`. A staged join's
clock begins at the current physical time; future initial synchronization will
advance it by observing received stamps.

`Workspace::flush()` writes device checkpoints before flushing tables and
states. `sync()` additionally requests durable backend synchronization. Drop
requests replication shutdown before the existing best-effort flush.
