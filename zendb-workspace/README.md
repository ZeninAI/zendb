# zendb-workspace

`zendb-workspace` is ZenDB's synchronous orchestration layer. It owns the table
catalog, state catalog, and peer policy without introducing transport,
replication workers, snapshots, or an async service framework.

## Identity Boundary

The workspace owns its `WorkspaceId` but **does not own device identity**.
`PeerId` and the private key belong to the application; the workspace receives
a `PeerIdentity` trait object at construction and never sees private key
material directly. This separation supports the open modes the system must
support:

- **Create**: `Workspace::create(root, peer, config)` — generates and persists
  a fresh `WorkspaceId`.
- **Open**: `Workspace::open(root, peer)` — reads the existing `WorkspaceId`
  from disk.
- **Join**: `Workspace::join(root, workspace_id, peer, hints, config)` —
  persists a caller-provided `WorkspaceId` for the workspace being joined.
  `JoinHints` is a placeholder; networking is out of scope for this iteration.

`peer` is `Arc<dyn PeerIdentity>`. The default `LocalPeerIdentity` in
`zendb-types` wraps an in-memory Ed25519 keypair; production deployments supply
an impl backed by a persistent key store (OS keychain, HSM, KMS) without
workspace changes.

`Bootstrap` (pub(crate)) owns the workspace lock file and the `WorkspaceId`
blob. The identity file is write-once; there is no `flush`. The file lock is
inlined in `Bootstrap` via `std::fs::File::try_lock` (no separate utility).
Wall-clock reads go through `zendb-types::utils::time::physical_ms()`.

## Public Surface

`Workspace` exposes three handlers, each a cheaply-cloneable owning facade over
a shared `Arc`-backed core:

- `Workspace::devices()` — peer registry, hybrid clock, roles, and
  duplicate-event tracking.
- `Workspace::tables()` — table catalog management.
- `Workspace::states()` — state catalog management.
- `Workspace::peer_identity()` — the local device identity, for signing
  messages on behalf of the local device without the workspace holding the
  private key.

`Workspace` itself retains only identity, bootstrap, lock, root, and flush
responsibilities.

## Tables

`Tables` is the table catalog management surface. It owns `TablesCore`, the
only workspace component that opens storage Tables or chooses physical table
paths.

- `_table_catalog` is a normal self-registering Table. Blob cells contain
  `CatalogEntry { config: TableConfig }`.
- `_devices` is opened eagerly from the table catalog.
- Application Tables are eagerly opened from declarations.

`Tables` exposes `contains`, `list`, `create`, `update`, `delete`, and
`open(name) -> TableHandle`. Mutating methods wrap
`authorize(Roles::Operator)` + `mint` + core op + `observe`. `open` returns a
handle without stamping.

There is no format version, generic name validator, migration branch, or
runtime liveness flag. Exact system names are reserved structurally. The
`_table_catalog` Table is the source of truth for table declarations;
`TablesCore` caches only the opened `TableEntry` handles in memory, not the
declarations themselves. Deleting an application Table returns `ResourceBusy`
while a handle still owns it; otherwise `TablesCore` removes the declaration
and directory before the name can be recreated.

### Table operations

`Tables` does not expose table operations such as `insert`. Callers obtain a
`TableHandle` via `Tables::open` and perform operations directly on the handle:

    let table = workspace.tables().open("users")?;
    table.insert(primary_key, path, op)?;

`TableHandle::read` returns a guard that dereferences to the real storage
`Table`. Callers use `ReadBackend` and `OrderedReadBackend` methods directly,
and iterators stay lazy while the guard is held. There are no `get` or
materializing `entries` proxy methods on the handle.

`TableConsumer` owns a named Topic consumer. `next_change` decodes one Change,
`read` acquires a direct Table guard, and `commit` persists the consumer
cursor. It does not build an intermediate row snapshot.

## States

`States` is the state catalog management surface. It owns `StatesCore`, the
only workspace component that opens storage States or chooses physical state
paths.

- `_state_catalog` is `State<String, StateConfig>` and contains its own
  declaration.
- `_peer_state` is `State<PeerId, PeerRecord>` and is opened for `Devices`
  during bootstrap.
- Application States are typed and retain open/close semantics: the caller
  supplies `K` and `V` when requesting a `StateHandle`.

`States` exposes `contains`, `list`, `open::<K, V>(name, config)`,
`list_open`, `config`, `close`, and `delete`. State declarations are not
gated by a role in this iteration; reads and writes happen through the
returned `StateHandle`.

## Peers And Roles

`_devices` stores `PeerId -> DeviceRecord`, where each record has a display
name and a set of `Roles`.

- `Contributor` writes existing application Tables and may update its own
  metadata without changing roles.
- `Operator` includes Contributor behavior and may manage Tables and peer
  records.
- `Dispatcher` is persisted but has no behavior in this iteration.

`_peer_state` stores `PeerRecord { receipts, clock }`. Only the local peer has
a clock checkpoint. The module loads all records into an `ArcSwap` snapshot
for lock-free reads, serializes mutations, and tracks dirty peers for flush.
Minting persists the local sequence high-water mark before returning, so a
failed mutation burns a sequence rather than risking reuse.

Local mutations are linear: authorize, mint, insert, then observe. There is no
callback-based mutation API, reconciliation poison state, or event journal
outside each Table's Topic.
