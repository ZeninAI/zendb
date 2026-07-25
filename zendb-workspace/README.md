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

## Event-Driven Maintenance

Table and device maintenance is **reactive**: CRUD methods validate and publish
events; callbacks react. This makes local edits and (future) remote events go
through the exact same code path.

### Change listeners

`ChangeListener` is a `pub` trait on `TableEntry`:

```rust
pub trait ChangeListener: Send + Sync {
    fn on_change(&self, change: &Change);
}
```

Listeners are registered at open time and dispatched synchronously on every
`TableEntry::insert`. `TableHandle::add_listener` is `pub`, so applications can
register custom listeners on tables they hold handles to. Internal listeners
and application listeners share the same listener list.

### Internal listeners

| Listener | Registered on | Reaction |
| --- | --- | --- |
| `ReceiptListener` | every table | `Devices::observe(stamp)` — replaces the one-shot replay consumer |
| `CatalogSyncListener` | `_table_catalog` | open tables on `Upsert`, close + rmdir on `Delete` — single owner of the handle map |
| `DeviceSyncListener` | `_devices` | upsert/remove in the in-memory device records map — single owner after `reload` |

`CatalogSyncListener` holds a shared `Arc<dyn ChangeListener>` (the receipt
listener) to register on newly opened application tables, so every table has
the receipt listener.

### CRUD methods publish; callbacks react

`TablesCore` CRUD methods validate a precondition and publish a catalog event.
They do not open/close tables or mutate the handle map directly — the
`CatalogSync` callback does. `Devices::write_record` mints and publishes; the
`DeviceSync` callback updates the in-memory map and the `Receipt` callback
observes the stamp.

## Tables

`Tables` is the table catalog management surface. `TablesCore` owns the
`_table_catalog` Table and the in-memory map of opened `TableEntry` handles.
The `_table_catalog` Table is the source of truth for declarations; only opened
handles are cached in memory.

- `Tables::create(name, config) -> Result<()>` — declare a new table
  (void-returning). The `CatalogSync` callback opens it synchronously.
- `Tables::open(name) -> Result<TableHandle>` — obtain a handle to an existing
  table. Returns handles to system tables too; system handles refuse `insert`.
- `Tables::update(name, config) -> Result<bool>` — update a table's config.
  Works on system tables. Returns `true` if the config changed.
- `Tables::delete(name) -> Result<bool>` — delete a table. Refuses system
  tables with `ResourceBusy`.

### System tables

System tables (`_table_catalog`, `_devices`) are openable and updatable but
not deletable and not directly writable via `TableHandle`. `TableHandle`
stores `is_system: bool` set at construction; `insert` refuses if true.
Internal workspace code calls `TableEntry::insert` directly, bypassing the
check.

### Table operations

`TableHandle::insert` is the single local mutation path for application tables:
authorize, mint, `TableEntry::insert` (which dispatches callbacks). `read`
returns a guard dereferencing to the storage `Table`. `consumer` returns a
streaming `TableConsumer`.

## States

`States` is the state catalog management surface. States are **lazy**: a state
may be declared in `_state_catalog` but not currently open.

- `States::create(name, config) -> Result<()>` — declare a new state
  (void-returning). Writes the declaration only; the physical state is created
  on first `open`.
- `States::open::<K, V>(name) -> Result<StateHandle<K, V>>` — obtain a typed
  handle. Lazy-loads: if already open, downcast and return; otherwise read the
  declaration, physically create or open the state, and return the handle.
  Returns handles to system states too; system handles refuse `write()`.

### System states

`StateHandle` stores `is_system: bool`; `write()` returns `Result` and refuses
if true. `read()` is unaffected. Internal workspace code (`PeerStore` writing
`_peer_state`) holds the raw `Arc<RwLock<State>>` directly via
`StatesCore::raw_peer_state`, bypassing the `StateHandle` check.

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

Local mutations are linear: authorize, mint, insert, then observe (via
callback). There is no callback-based mutation API, reconciliation poison
state, or event journal outside each Table's Topic.
