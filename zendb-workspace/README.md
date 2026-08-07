# zendb-workspace

`zendb-workspace` is ZenDB's synchronous orchestration layer. It owns workspace
lifecycle, table and state catalogs, installation permissions, causal tracking,
network admission, and the private Zenin libp2p runtime.

## Lifecycle

Applications pass `Arc<dyn PeerIdentity>`. ZenDB derives an Ed25519 key from a
versioned domain, `WorkspaceId`, and `InstallationId`; `_identity` persists only
the two IDs.

- `Workspace::create` creates system storage and the initial full-access local
  installation, synchronizes it, then writes `_identity`.
- `Workspace::open` derives the same key and requires a matching active local
  installation and causal clock.

`WorkspaceConfig::workspace_id` is optional. Supplying the same ID to separate
`create` calls produces independent replicas that may later merge.

## Core Ownership

Each open workspace owns one flat `WorkspaceCore`:

- `TableStore` owns open system and application tables.
- `States` owns typed local states and the state catalog.
- `Membership` is an immutable `ArcSwap` projection of `_installations`.
- `CausalTracker` owns the local allocator, HLC, and per-author receipts.
- an MPSC sender delivers committed events to replication.

`Workspace` separately owns the always-running `ReplicationController`, which
contains the replication thread and its receiver. There is no intermediate
workspace-components bundle, assembly DTO, or separate runtime lifecycle
object.

The local mutation pipeline is:

```text
authorize -> mint -> table admission -> observe
          -> project -> notify
```

Remote events enter through the same table, causal, projection, and listener
path. Once an installation is admitted, committed events are trusted. The
receiver does not repeat per-event signature or permission checks.

Local commits notify the replication runtime immediately after durable
insertion through an unbounded Tokio channel. Notifications are granular:

- `Event { table, event }` — a locally committed event.
- `Admitted { installation_id }` — an installation became Active.
- `Rejected { installation_id }` — an installation was rejected.

Events are accumulated per-table using a configurable linger duration before
being broadcast as `Push` batches. The runtime is expected to remain alive for
the workspace lifetime, and a disconnected channel is surfaced as a replication
error.

## Network Admission

Each connection opens a long-lived `/zenin/1` session over TCP+Noise+Yamux or
QUIC. Noise authenticates the transport, and the handshake binds the connected
PeerId to the workspace ID, installation ID, display name, public key, and
route hints.

Unknown or Pending installations are rejected at the protocol level:
the replication runtime records the peer as `Pending` in `_installations` and
disconnects with `Error(NotAdmitted)`. Only peers whose installation is locally
Active are accepted for replication. This means both sides must independently
approve each other before event exchange begins.

`Installation` is a leaf CRDT whose merge gives `Active` higher precedence
than `Pending` and `Rejected`, so discovery races cannot demote an active
installation. Applications list pending installations and use
`Installations::upsert` to store `Active(permissions)` or `Rejected`. When an
installation becomes Active, the mesh automatically dials the peer using the
addresses from its handshake. There is no temporary join swarm, join claim, or
bootstrap protocol.

## Workspace Merge

Zenin uses one mesh and one global receipt stream because event sequences are
global per installation. Summary and fetch exchange missing author-sequence
ranges across every table.

Fetched batches are applied in this order:

1. `_catalog`
2. `_installations`
3. application tables

This lets catalog events materialize missing tables before their historical
events are applied. Installation histories then CRDT-merge the participating
clusters.

Permissions govern events produced after admission. They do not filter
committed history during replication. A newly admitted read-only installation
therefore still contributes events authored while its workspace was isolated.

## Lifecycle And Durability

Networking starts whenever a workspace is created or opened and stops when it
closes. Runtime shutdown uses a one-shot wakeup. Explicit barriers synchronize
table state and topics before causal receipts and remaining local states.
