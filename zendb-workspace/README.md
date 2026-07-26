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

`Workspace` owns the persisted `WorkspaceId` and keeps the workspace lock file
open for its lifetime. The narrow identity read/write and lock acquisition
logic lives directly in `workspace.rs`; there is no separate bootstrap type.
Persisted file, directory, and system-resource names are centralized in
`consts.rs`. Wall-clock reads go through
`zendb-types::utils::time::physical_ms()`.
System state and table bootstrap configurations are also centralized there.

## Public Surface

`Workspace` exposes three shared handlers:

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

`ChangeListener` is a `pub` trait owned by `TableHandle`:

```rust
pub trait ChangeListener: Send + Sync {
    fn on_change(&self, change: &Change);
}
```

Listeners are registered at open time and dispatched synchronously on every
`TableHandle::insert_internal`, after the storage-table write guard has been
released. `TableHandle::add_listener` is `pub`, so applications can register
custom listeners on tables they hold handles to. Internal listeners and
application listeners share the same listener list.

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

`Tables` CRUD methods validate a precondition and publish a catalog event.
They do not open/close tables or mutate the handle map directly — the
`CatalogSync` callback does. `Devices::write_record` mints and publishes; the
`DeviceSync` callback updates the in-memory map and the `Receipt` callback
observes the stamp.

## Tables

`Tables` owns the `_table_catalog` handle and the in-memory map of opened
`Arc<TableHandle>` values. The `_table_catalog` Table is the source of truth for
declarations, and every declared table is eagerly represented in the handle
map. The typed catalog field and its map entry share the same `Arc<TableHandle>`;
the devices registry follows the same ownership model.

- `Tables::upsert(name, config) -> Result<bool>` — declare a table or update
  its config. The `CatalogSync` callback opens new tables synchronously.
- `Tables::get(name) -> Result<Arc<TableHandle>>` — obtain the stored handle
  for an existing table. Returns handles to system tables too; system handles
  refuse `insert`.
- `Tables::contains(name)` and `Tables::list()` include system and application
  tables; `list()` returns the eager map's natural iteration order.
- `Tables::delete(name) -> Result<bool>` — delete a table. Refuses system
  tables with `ResourceBusy`.

### System tables

System tables (`_table_catalog`, `_devices`) are openable but cannot be
upserted, deleted, or directly written via `TableHandle`. `TableHandle` stores
`is_system: bool` set at construction; public mutation returns
`SystemTableReadOnly`. Internal workspace code calls
`TableHandle::insert_internal`.

### Table operations

`TableHandle::insert` is the single local mutation path for application tables:
authorize, mint, and delegate to `TableHandle::insert_internal`, which inserts
and dispatches callbacks. `read` returns the storage table's
`RwLockReadGuard` directly. `consumer` returns
`zendb_storage::TopicConsumer<Change>` directly; the storage consumer already
owns its topic state and cursor.

`Tables::create/open` owns table and device bootstrap: it creates or opens both
system table handles, constructs `Devices`, builds the eager handle map,
registers listeners, and writes the self-referencing system catalog rows on
creation. The constructors return `Arc<Tables>`; `Workspace::assemble` obtains
its device handle by cloning the crate-visible `Tables::devices` field.

## States

`States` is the state catalog management surface. States are **lazy** after
creation: a state may be declared in `_state_catalog` but not currently open.
Catalog lifecycle remains in `states/mod.rs`; the typed `StateHandle` runtime
API lives in `states/runtime.rs`.
The catalog itself is bootstrapped into the open-state registry; its typed
catalog field and erased registry entry share the same `Arc<StateHandle<_, _>>`.
Opening `_state_catalog` therefore returns the existing guarded state instead
of opening the same backend a second time.

- `States::upsert(name, config) -> Result<bool>` — declare and physically
  create a state, or update an existing declaration's config. System states
  are visible but cannot be upserted.
- `States::get::<K, V>(name) -> Result<Arc<StateHandle<K, V>>>` — obtain a
  shared typed handle. Lazy-loads by downcasting an open handle or opening the
  declared physical state.
- `contains`, `list`, `list_open`, and `close` treat system and
  application states uniformly.
- `delete` refuses `_state_catalog` and `_peer_state` with `ResourceBusy`.

### System states

`StateHandle` stores `is_system: bool`; public `write()` returns
`SystemStateReadOnly` for system states, while `read()` is unaffected.
`PeerStore` owns the `_peer_state` handle and uses its crate-internal
`write_internal()` method for workspace-managed updates. Workspace creation
uses `States::upsert_internal()` to bootstrap that system declaration without
exposing the bypass publicly.

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
