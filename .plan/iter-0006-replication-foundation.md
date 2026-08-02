# Iteration 0006: Replication Foundation

Status: implemented.

This iteration defines the completed replication foundation. It combines the
trust and identity model, direct installation enrollment, workspace-owned
libp2p transport, durable installation routes, explicit workspace application,
and causal tracking. The implementation in `zendb-workspace` and
`zendb-types` is the source of truth for this document.

The design does not include initial join synchronization, anti-entropy,
snapshots, relays, NAT traversal, or a public join protocol. Direct enrollment
is currently an Admin operation that supplies the joining installation with
its identity and routes out of band.

The former iteration 0007 address/routing plan and iteration 0008 application
pipeline plan are consolidated here. Their superseded documents are removed.

---

## 1. Trust And Identity

### 1.1 Logical And Transport Identity

`InstallationId` is the logical author of an event, the key of an
`_installations` row, and the key used by causal state. It is not a libp2p
`PeerId` and it is not a public key. `WorkspaceId` scopes all replication for
one workspace.

An `Installation` stores:

```text
display_name
role: Option<Role>
public_key: PublicKey
addresses: Vec<Multiaddr>
```

The persisted `PublicKey` is a bincode-capable wrapper around libp2p's generic
public key. `Multiaddr` is the durable ZenDB wrapper around a libp2p address;
the wrapper rejects a trailing `/p2p` component because the peer identity is
stored separately in `Installation.public_key`.

`PeerIdentity` supplies an application-owned root libp2p keypair and a display
name. ZenDB never persists the root private key or uses the root public key as
the workspace transport identity.

### 1.2 Workspace-Scoped Transport Key

Each workspace installation derives an in-memory Ed25519 transport keypair
from the application root keypair, workspace ID, and installation ID:

```text
b"zendb/v1" || workspace_id.as_bytes() || installation_id.as_bytes()
```

The root keypair's `derive_secret` operation produces the Ed25519 seed. The
derived public key is stored in the local `Installation`, and the derived
keypair is used by libp2p Noise, Identify, and signed Gossipsub.

The local `_identity` file persists only the workspace ID and installation ID.
Opening the workspace derives the same keypair and validates that its public
key matches the enrolled local installation. The derivation helper is exposed
only under `test` or the `test-support` feature for integration fixtures; test
fixtures convert the returned keypair's public half into the stored
`zendb_types::PublicKey` themselves.

This gives every installation a distinct transport identity within a
workspace, even when several workspaces use the same application root keypair.
The current design treats one derived public key per installation as a trusted
enrollment invariant. Key rotation is deferred.

### 1.3 Authentication Layers

Replication has two transport-level checks and one workspace-level check:

| Layer | Responsibility |
|---|---|
| Noise | Authenticates the direct libp2p wire connection. |
| Strict signed Gossipsub | Authenticates the original publisher of the payload, even when it is forwarded through the mesh. |
| Workspace admission | Binds the authenticated source `PeerId` to the envelope's `InstallationId` and checks its role. |

ZenDB does not add a second application signature to an envelope. Gossipsub
signs the complete serialized payload and validates it before the worker sends
it to workspace admission.

---

## 2. Envelope And Admission

`Envelope` is a bincode-encoded wire value:

```rust
pub struct Envelope {
    pub author: InstallationId,
    pub table: String,
    pub events: Vec<CompactEvent>,
}
```

Each compact event contains the sequence, event time, primary key, path, and
operation. Events on disk remain ordinary full `Event` values. Envelopes are
not stored as a separate log format.

`Workspace::admit_event(envelope, gossipsub_source)` is the public admission
seam. It performs these checks once:

1. Find `Envelope.author` in the current membership projection.
2. Derive the expected `PeerId` from the stored public key and compare it with
   the authenticated original Gossipsub source.
3. Require `Admin` for `_catalog` and `_installations`, or `Contributor` for
   application tables.
4. Reconstruct each full event and apply it through the shared workspace
   commit path.

Admission errors distinguish an unknown installation, source/author mismatch,
authorization failure, and workspace insertion errors. The replication worker
uses the same crate-local admission implementation after receiving a message;
it does not have a second remote insertion path.

An accepted remote event is never republished. Only events authored by the
local installation enter the outbound replication channel.

---

## 3. Workspace Ownership And Application Order

`Workspace` is the synchronous public composition root. Its private
`Arc<WorkspaceCore>` owns the operational components:

```text
WorkspaceCore
|- TableStore               shared open tables and physical table lifecycle
|- States                   local typed state lifecycle
|- Membership               decoded installations and authorization snapshot
|- CausalTracker            local clock, sequence, and receipt state
`- ReplicationController    network lifecycle and worker commands
```

The replication controller holds only a weak reference to the core. A worker
cannot keep the workspace alive after the public workspace owner is dropped.

`TableStore` owns the shared `OpenTable` values, including `_catalog` and
`_installations`. `Tables` and `Installations` are workspace-bound public
facades that retain the relevant system table handles and use the core for
authorized writes. `TableHandle` is a lightweight capability containing an
open table and a weak workspace reference.

`ChangeListener` remains a public application callback only. The open table
snapshots listener references, releases its listener lock, and invokes the
callbacks after workspace-owned post-commit work. Internal membership,
catalog, causal, and replication behavior does not depend on listener
registration or callback ordering.

### 3.1 Local Application Pipeline

All local mutations converge on this order:

```text
authorize
-> mint causal stamp
-> apply to table
-> observe applied event
-> update system projections
-> submit local event to replication
-> complete pending replication stop
-> notify application listeners
```

CRDT operations that are ignored as duplicates do not enter the post-commit
pipeline: they do not advance receipts, publish, or notify listeners.

The causal lock is released before table I/O. A failed local table write
therefore consumes its allocated sequence and leaves a valid causal gap. The
future anti-entropy design, not this iteration, will decide how such gaps are
represented or repaired.

Remote admission authenticates and authorizes the envelope once, then uses the
same post-authorization commit path. Remote events update causal state and
projections but are not sent back to Gossipsub.

### 3.2 Membership Projection

`Membership` owns the decoded `_installations` projection in an immutable
`ArcSwap` snapshot. The snapshot contains both the complete installation map
and a separate local-installation cache. A registry change atomically replaces
both views, so local authorization can use the cached value without a map
lookup.

`Installations` owns no projection state. It performs Admin checks, exposes
registry reads, and writes installation events through `WorkspaceCore`. A
committed installation event updates membership before the replication
controller reconciles its peer projection.

Only an Admin may upsert or delete installation rows. Deletion is revocation;
later admission fails because the installation is no longer enrolled,
independently of the network peer blacklist.

### 3.3 Causal State

`CausalTracker` owns the `_causal` state, receipt windows, local sequence
allocation, and the local hybrid logical clock. State is keyed by
`InstallationId`.

- `mint` exclusively allocates the local sequence and advances the local HLC.
- `observe` records local or remote receipts and merges event time into the
  local HLC; it never allocates a local sequence.
- Dirty local and remote causal entries are written during `flush` or `sync`.
- Opening requires the persisted local causal row and a populated local clock.
  Missing or malformed local causal state is an error, not an invitation to
  silently seed new state.

Workspace creation bootstraps the local sequence explicitly:

1. `TableStore::create` writes the table catalog events at sequences 1 and 2.
2. Workspace assembly writes the initial local Admin installation at sequence
   3.
3. The matching local causal row is initialized with `next_sequence = 4`.

The causal row must be keyed by the installation ID stored in `_identity`.
Copying table history into a workspace with a different local installation ID
does not copy usable local causal state; test fixtures preserve the destination
workspace's states and copy only the trusted table history when constructing
replicated peers.

### 3.4 Workspace Lifecycle

Creation is a two-phase commit. Storage is assembled and synchronized first;
the serialized workspace and installation IDs are written to `_identity` only
after that synchronization succeeds. Failed creation removes partial tables,
states, and the temporary identity file while preserving the root and lock
file for retry.

Opening reads `_identity`, derives the local transport key, validates local
membership and causal state, constructs the core, and then allows the
replication controller to inspect the complete membership projection.

`WorkspaceConfig` contains an optional workspace ID and runtime-only
`ReplicationConfig`. During creation the optional ID selects the generated
workspace ID; during open it is only an assertion against the persisted ID.

`Workspace::drop` stops replication before the coordinated best-effort flush.

---

## 4. Replication Configuration

Replication configuration is runtime-only and is not persisted in the
workspace. The public configuration is split by responsibility:

```text
BatchConfig {
    max_events: usize,
    max_bytes: usize,
    linger: Duration,
}

TopologyConfig {
    lan_score_bonus: f64,
    latency_weight: f64,
    mesh_size: usize,
}

DialConfig {
    initial_backoff: Duration,
    max_backoff: Duration,
}

ReplicationConfig {
    batch: BatchConfig,
    topology: TopologyConfig,
    dial: DialConfig,
    listen_addresses: Vec<Multiaddr>,
    channel_capacity: usize,
    gossipsub_max_transmit_size: usize,
}
```

The defaults are 16 events, 65,536 approximate serialized bytes, a 50 ms
linger period, mesh size 6, a 10.0 LAN score bonus, latency weight 0.01,
one-second initial dial backoff, 60-second maximum backoff, a TCP listen
address of `/ip4/0.0.0.0/tcp/0`, channel capacity 1,024, and a 100 MiB
Gossipsub transmit limit.

`max_bytes` is a flush threshold, not an event-size admission limit. A first
event larger than the threshold is sent as a one-event envelope. The
Gossipsub transmit limit remains the network payload limit.

---

## 5. Installation Routes And Discovery

### 5.1 Durable Routes

`Installation.addresses` contains Admin-managed durable `Multiaddr` values.
For every enrolled installation, the controller derives:

```text
InstallationId -> Installation.public_key -> libp2p PeerId -> route candidates
```

The worker uses `DialOpts` with the derived `PeerId`; an address selects an
endpoint but cannot change which installation the connection claims to be.
Configured routes can be empty. An enrolled peer may still connect inbound or
be reached through mDNS or Identify.

Listen addresses are runtime configuration and are not automatically written
back into `Installation.addresses`.

### 5.2 Transient Discovery Sources

The worker keeps configured, mDNS, and Identify addresses in separate sets and
combines them only when constructing dial candidates. mDNS and Identify are
route discovery mechanisms, not membership mechanisms:

- mDNS addresses are accepted only when the discovered `PeerId` is already an
  enrolled peer.
- Identify addresses are accepted only for an enrolled peer.
- Unknown discovered peers are ignored.
- An unknown peer that establishes a connection is blacklisted and
  disconnected.
- Discovered addresses are never persisted automatically.

mDNS therefore finds libp2p peers on the local network, but workspace
membership still comes exclusively from the trusted `_installations`
projection. The LAN score bonus and ping latency affect the Gossipsub
application score; they do not grant access.

### 5.3 Dial Retry And Address Normalization

Each authorized peer has connection state and capped exponential retry state.
Successful connections reset the retry delay. Dial failures double the delay
up to the configured maximum. A scheduled attempt reserves its next attempt
time before calling the Swarm so the worker's short dial tick cannot repeatedly
schedule the same peer.

A matching trailing `/p2p/<PeerId>` component is removed before `DialOpts` is
built. An address containing a different trailing peer ID is rejected. This
keeps the stored endpoint and the independently derived transport identity
consistent.

---

## 6. Replication Controller And Runtime

### 6.1 Controller Projection

`ReplicationController` owns one mutex-protected state machine:

```text
Stopped | Starting | Running(ReplicationRuntime) | Stopping(JoinHandle)
        | Failed | Closed
```

Its installations projection contains:

```text
local_is_enrolled: bool
remote_routes: BTreeMap<InstallationId, PeerRoute>
pending_stop: Option<PendingStop>
```

Replication runs only while the local installation is enrolled with its
expected derived public key and at least one other installation exists. Every
other installation is a remote route, including one with no currently usable
address.

The controller derives a complete desired projection from membership and
diffs it against the previous projection. Worker commands use only `PeerId`:

```text
PublishEvent { table, event }
UpsertPeer { peer_id, addresses }
RemovePeer { peer_id }
Stop
```

The controller retains installation IDs only for deterministic route
projection and diffing. If an installation's public key changes, the old
derived peer is removed before the replacement is added.

### 6.2 Lifecycle Ordering

- The first remote installation starts the runtime with the complete initial
  route snapshot before the enrollment event is submitted for publication.
- Additional installations and route changes reuse the running worker.
- Removing an installation removes its route, blacklists its peer, removes it
  from Gossipsub's explicit peer set, and disconnects the Swarm connection.
- Re-upserting a peer removes it from the blacklist.
- Removing the final remote installation marks a pending stop. The triggering
  local event is submitted first, then the worker drains and stops.
- Removing the local installation, clearing its role, or changing its expected
  workspace key also stops the worker after the triggering registry event has
  completed the commit pipeline.

Pending-stop event IDs prevent an older registry change from stopping a worker
after a later membership change has made replication necessary again.

### 6.3 Private Runtime Threads

`ReplicationRuntime` owns two threads and a bounded command channel:

1. A current-thread Tokio thread owns the libp2p Swarm and async worker.
2. A blocking admission thread receives `(Envelope, PeerId)` values and applies
   them sequentially through workspace admission.

The inbound and command channels are bounded by `channel_capacity`. Storage
I/O is therefore kept off the Tokio event loop while network-to-storage
backpressure remains explicit.

Runtime startup uses a zero-capacity readiness handshake. `start` returns only
after the Tokio runtime and Swarm have initialized or reported an error. Stop
sends `Stop`; the worker publishes all already-batched events before exiting,
then the runtime joins the network thread and the admission thread.

### 6.4 Swarm And Worker

The Swarm uses:

- TCP with `nodelay` enabled;
- Noise and Yamux;
- DNS;
- Tokio mDNS;
- ping;
- Identify with protocol `/zendb/replication/1`;
- strict signed Gossipsub with configured mesh size and transmit limit.

The workspace topic is:

```text
zendb/{workspace_id}/events/v1
```

The worker is the sole owner of the Swarm, batcher, and peer directory. Its
single event loop serializes controller commands with network events. It:

- batches local events per table and publishes serialized envelopes;
- accepts Gossipsub messages only from active authorized peer IDs;
- sends accepted envelopes to the blocking admission bridge;
- records mDNS and Identify route changes;
- scores LAN and latency observations;
- dials due authorized peers;
- rejects unknown established connections;
- drains pending batches during shutdown.

The batcher does not perform admission or durability work. The peer directory
does not decide workspace authorization; it only manages routes and connection
state for peers already projected by the controller.

---

## 7. Direct Enrollment Boundary

The supported enrollment path is:

1. An Admin chooses an installation ID, role, display name, derived public key,
   and optional durable addresses.
2. The joining application receives the workspace ID, assigned installation
   ID, and connection data out of band.
3. The Admin writes the installation through `Installations::upsert`.
4. The joining workspace can open only after its own `_identity`, table data,
   membership row, and causal row are present and consistent.

There are no invitation links, bearer tokens, invitation nonces, request/
response enrollment messages, or `Workspace::join` API. A production public
operation for deriving a joining transport key is not part of this foundation;
the deterministic helper is available to tests through `test-support`.

Initial synchronization is deliberately deferred. It must transfer trusted
membership and table history and seed or advance the joining installation's
causal state before that installation authors new events.

---

## 8. Module Ownership

```text
zendb-types
|- Installation, Role, PublicKey, Multiaddr
|- InstallationId, WorkspaceId, Event and EventStamp
`- Envelope and CompactEvent

zendb-workspace
|- workspace       create/open, identity derivation, public facade
|- core            shared commit pipeline and component ownership
|- admission       source, author, role, and envelope admission
|- installations   public registry facade and Membership projection
|- causal          HLC, local sequence, receipts, and causal persistence
|- tables          public table facade, OpenTable, and TableStore
|- states          typed local state catalog and handles
`- replication
   |- config       runtime tuning
   |- command      controller/worker messages
   |- controller   installation projection and lifecycle state machine
   |- batcher      per-table outbound envelope batching
   |- peers        authorized routes, discovery, and retry state
   |- swarm        libp2p behaviour and transport construction
   |- worker       async command/network event loop
   `- runtime      Tokio and admission thread ownership
```

Tokio, libp2p, and transport policy remain private implementation details of
`zendb-workspace`. Applications use synchronous workspace APIs and do not
create the runtime, Swarm, or replication links themselves.

---

## 9. Non-Goals And Deferred Work

- Initial workspace join and historical synchronization.
- Anti-entropy, frontier exchange, gap repair, and no-op event generation.
- Snapshots, compaction, tombstone watermarks, and a separate replication log.
- Relay, NAT traversal, and operator-managed transport.
- Persisting mDNS, Identify, observed, or listen addresses automatically.
- Treating discovery or raw bootstrap routes as workspace membership.
- A shared Tokio runtime across workspaces.
- Application-level envelope signatures in addition to signed Gossipsub.
- Key rotation and public enrollment-key derivation for production callers.

---

## 10. Completion Criteria

- Every replicated envelope is accepted only when its author, signed source,
  and role agree.
- The derived workspace transport key is scoped by workspace and installation.
- Installation rows are Admin-owned and are the membership source of truth.
- Durable installation addresses are separated from transient discovery data.
- Unknown mDNS, Identify, and inbound peers cannot enter the workspace mesh.
- The public workspace API remains synchronous and owns its private runtime.
- The blocking admission bridge keeps storage I/O off the async worker.
- Local events follow `authorize -> mint -> apply -> observe -> project ->
  publish -> notify`; remote events are never republished.
- Replication starts and stops from committed membership changes with the
  first-remote and final-remote ordering described above.
- Causal state is persisted per installation and local open never silently
  recreates missing state.
- Internal correctness no longer depends on replication or registry listeners.
- `cargo check --workspace --all-targets` succeeds.
- The integration replication test exercises two live loopback workspaces,
  event convergence, replicated table creation, table deletion, and durable
  reopen behavior.
