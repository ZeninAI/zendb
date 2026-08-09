# zendb-workspace

`zendb-workspace` is ZenDB's synchronous orchestration layer. It owns workspace
lifecycle, table and state catalogs, installation permissions, workspace clock,
network admission, and the private Zenin libp2p runtime.

## Lifecycle

Applications pass `Arc<dyn PeerIdentity>`. ZenDB derives an Ed25519 key from a
versioned domain, `WorkspaceId`, and `InstallationId`; `_identity` persists the
two IDs and the latest durable hybrid-clock checkpoint.

- `Workspace::create` creates system storage and the initial full-access local
  installation from the peer identity's display name and route hints,
  synchronizes it, then writes `_identity`.
- `Workspace::open` derives the same key and requires a matching active local
  installation, then restores the persisted hybrid clock. Before replication
  starts, it updates only the
  local installation's display name and route hints when the peer identity has
  changed; this internal update does not require installation-management
  permission.

`WorkspaceConfig::workspace_id` is optional. Supplying the same ID to separate
`create` calls produces independent replicas that may later merge.

## Core Ownership

Each open workspace owns one flat `WorkspaceCore`:

- `TableStore` owns open system and application tables.
- `States` owns typed local states and the state catalog.
- `Membership` is an immutable `ArcSwap` projection of `_installations`.
- `HybridClock` owns only the workspace HLC. Each physical table owns its
  per-installation sequence allocator and receipt state through its configured
  causal `State`.
- an optional MPSC sender delivers committed events to replication when the
  runtime is enabled.

`Workspace` separately owns the always-running `ReplicationController`, which
contains the replication thread and its receiver. There is no intermediate
workspace-components bundle, assembly DTO, or separate runtime lifecycle
object.

The local mutation pipeline is:

```text
authorize -> mint time -> table::insert
          -> project -> notify
```

Remote events enter through `table::observe`, which owns duplicate detection,
receipt updates, CRDT application, and topic admission. Once an installation
is admitted, committed events are trusted. The receiver does not repeat
per-event signature or permission checks.

When enabled, local commits notify the replication runtime immediately after
durable insertion through an unbounded Tokio channel. Disabled replication
uses no controller and the core notification sender is a no-op. Notifications
are granular:

- `Event { table, event }` — a locally committed event.
- `InstallationChanged { installation_id }` — installation metadata or state
  changed.

Events are serialized once on admission and accumulated as length-delimited
bytes per table. A batch is completed when its soft `BatchConfig::batch_size`
threshold is crossed or its optional `BatchConfig::linger` duration expires,
whichever comes first. With no linger, size is the only completion trigger.

`ReplicationConfig` is grouped by responsibility: `transport` owns listener
bindings, `sync` owns anti-entropy cadence, ready-peer retry cadence, and
range/cache counts, and `batch` owns outbound accumulation. Every active
installation is a replication peer; there is no bounded neighbour set or
topology rotation. The default batch size is 1 MiB. Wire-frame and
sync-response byte ceilings are not public configuration settings.

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

`WorkspaceConfig`, `ReplicationConfig`, and their responsibility-specific
sub-configurations are defined together in the workspace-level `config.rs`.
Replication
can be disabled with `ReplicationConfig::enabled`; in that mode no replication
controller or notification channel is created. `TransportConfig::enable_port_reuse`
defaults to `true`; disabling it makes outbound dials allocate a fresh local
port, which is useful for isolated integration processes. TCP, QUIC, and mDNS
are independently controlled by `enable_tcp`, `enable_quic`, and `enable_mdns`,
all of which default to `true`.

`Installation` is a leaf CRDT whose merge gives `Active` higher precedence
than `Pending` and `Rejected`, so discovery races cannot demote an active
installation. Applications list pending installations and use
`Installations::upsert` to store `Active(permissions)` or `Rejected`. When an
installation is Active, the runtime automatically dials the peer using its
current durable route hints and maintains a direct connection to every other
active installation. The runtime binds only the configured
`ReplicationConfig::transport.listener_addresses`; listener bindings are not copied into
installation metadata. A local installation change also updates the handshake
used for future sessions. There is no temporary join swarm, join claim, or
bootstrap protocol.

If both peers dial each other at the same time, the runtime temporarily accepts
both physical connections. Once both directions are established, each peer
chooses the same connection using peer-ID ordering and endpoint direction, then
closes the other connection. A single connection is retained regardless of
which peer initiated it.

## Workspace Merge

Zenin exchanges receipt summaries and missing ranges per table and author.
Sequence numbers are allocated independently by each physical table, so the
same installation can have sequence 1 on multiple tables.

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
table state, table causal state, topics, local states, and the workspace clock
checkpoint.
