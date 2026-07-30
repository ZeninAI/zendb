# zendb-workspace

`zendb-workspace` is ZenDB's synchronous orchestration layer. It owns the table
catalog, state catalog, and peer policy without introducing transport,
replication workers, snapshots, or an async service framework.

## Identity Boundary

The workspace owns its random 128-bit `WorkspaceId` but **does not own device
identity**. Its canonical text form is 26-character Crockford Base32, suitable
for future join links. `PeerId` and the private key belong to the application;
the workspace receives a `PeerIdentity` trait object at construction and never
sees private key material directly. This separation supports the open modes
the system must support:

- **Create**: `Workspace::create(root, identity, config)` — generates and
  persists a fresh `WorkspaceId`.
- **Open**: `Workspace::open(root, identity)` — reads the existing `WorkspaceId`
  from disk.
- **Join**: `Workspace::join(root, workspace_id, identity, hints, config)` —
  persists a caller-provided `WorkspaceId` for the workspace being joined.
  `JoinHints` is a placeholder; networking is out of scope for this iteration.

`identity` is `Arc<dyn PeerIdentity>`. Applications supply an implementation
backed by their chosen key store, such as an OS keychain, HSM, KMS, or an
in-memory key for tests, without workspace changes.

`Workspace` owns the persisted `WorkspaceId` and keeps the workspace lock file
open for its lifetime. The narrow identity read/write and lock acquisition
logic lives directly in `workspace.rs`; there is no separate bootstrap type.
Persisted file, directory, and system-resource names are centralized in
`consts.rs`. Wall-clock reads go through
`zendb-types::utils::time::physical_ms()`.
System state and table bootstrap configurations are also centralized there.

## Public Surface

`Workspace` exposes three shared handlers plus the local identity:

- `Workspace::devices()` — peer registry, hybrid clock, roles, and
  duplicate-event tracking.
- `Workspace::tables()` — table catalog management.
- `Workspace::states()` — state catalog management.
- `Workspace::peer_identity()` — the local device identity, for signing
  messages on behalf of the local device without the workspace holding the
  private key.

`Workspace::flush()` and `Workspace::sync()` are the cross-module durability
barriers. `flush` drains runtime caches into their storage writeback paths;
`sync` additionally requests durable synchronization from every backend.

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

Listeners are registered at open time and dispatched synchronously after every
successful authorized handle insertion, once the storage-table write guard has
been released. `TableHandle::add_listener` is `pub`, so applications can
register custom listeners on tables they hold handles to. Internal listeners
and application listeners share the same listener list.

### Internal listeners

| Listener | Registered on | Reaction |
| --- | --- | --- |
| `ReceiptListener` | every table | `Devices::observe(stamp)` — replaces the one-shot replay consumer |
| `TableCatalogListener` | `_catalog` | open tables on `Upsert`, close + rmdir on `Delete` — single owner of the handle map |
| `DeviceRegistryListener` | `_devices` | update the registry cache after the initial open-time load |

`TableCatalogListener` holds a shared `Arc<dyn ChangeListener>` (the receipt
listener) to register on newly opened application tables, so every table has
the receipt listener.

### CRUD methods publish; callbacks react

`Tables` CRUD methods validate a precondition and publish a catalog event.
They do not open/close tables or mutate the handle map directly — the
`TableCatalogListener` does. `Devices::upsert` checks access, mints, and
publishes directly; `DeviceRegistryListener` updates the registry cache and
`ReceiptListener` observes the stamp.

## Tables

`Tables` owns the `_catalog` handle and the in-memory map of opened
`Arc<TableHandle>` values. The `_catalog` Table is the source of truth for
declarations, and every declared table is eagerly represented in the handle
map. The typed catalog field and its map entry share the same `Arc<TableHandle>`;
the devices registry follows the same ownership model.

- `Tables::upsert(name, config) -> Result<bool>` — declare a table or update
  its config. `TableCatalogListener` opens new tables synchronously.
- `Tables::get(name) -> Result<Arc<TableHandle>>` — obtain the stored handle
  for an existing table. Returns handles to system tables too; system handles
  refuse `insert`.
- `Tables::contains(name)` and `Tables::list()` include system and application
  tables; `list()` returns the eager map's natural iteration order.
- `Tables::delete(name) -> Result<bool>` — delete a table. Refuses system
  tables with `ResourceBusy`.
- `Tables::flush()` and `Tables::sync()` snapshot the eager handle map and
  invoke the matching durability operation on every table. The catalog and
  device registry need no special case because both are map entries.

### System tables

System tables (`_catalog`, `_devices`) are openable but cannot be
upserted, deleted, or directly written via `TableHandle`. `TableHandle` stores
`is_system: bool` set at construction; public mutation returns
`SystemTableReadOnly`. Idiomatic catalog and device methods require Admin and
call the authorized `TableHandle::insert_internal`.

### Table operations

`TableHandle::insert` is the single local mutation path for application tables:
reject system handles, require Contributor, mint, and delegate to
`TableHandle::insert_internal`. The internal path independently checks the
event actor, requiring Contributor for application tables and Admin for system
tables, before inserting and dispatching callbacks. `read` returns the storage
table's `RwLockReadGuard` directly. `consumer` returns
`zendb_storage::TopicConsumer<Change>` directly; the storage consumer already
owns its topic state and cursor.

`Tables::create/open` owns table runtime initialization: it creates or opens
both system tables, constructs `Devices`, builds the eager handle map,
registers listeners, and (on create) writes the self-referencing system catalog
rows. `Devices::create` inserts the initial Admin row directly into the raw
registry Table before wrapping it in `TableHandle`; subsequent catalog writes
use normal Admin-authorized insertion. `Devices::open` loads the durable
registry and rejects an unregistered local peer before returning. The
constructors return `Arc<Tables>`; `Workspace::assemble` only clones the device
handle from the crate-visible `Tables::devices` field.

## States

`States` is the state catalog management surface. States are **lazy** after
creation: a state may be declared in `_catalog` but not currently open.
Catalog lifecycle remains in `states/mod.rs`; the typed `StateHandle` runtime
API lives in `states/runtime.rs`.
The catalog itself is bootstrapped into the open-state registry; its typed
catalog field and erased registry entry share the same `Arc<StateHandle<_, _>>`.
Opening `_catalog` therefore returns the existing guarded state instead
of opening the same backend a second time.

- `States::upsert(name, config) -> Result<bool>` — declare and physically
  create a state, or update an existing declaration's config. System states
  are visible but cannot be upserted.
- `States::get::<K, V>(name) -> Result<Arc<StateHandle<K, V>>>` — obtain a
  shared typed handle. Lazy-loads by downcasting an open handle or opening the
  declared physical state.
- `contains`, `list`, `list_open`, and `close` treat system and
  application states uniformly.
- `delete` refuses `_catalog` and `_peers` with `ResourceBusy`.
- `flush` and `sync` visit every currently open state. Declared but unopened
  states have no runtime cache to drain.

### System states

`StateHandle` stores `is_system: bool`; public `write()` returns
`SystemStateReadOnly` for system states, while `read()` is unaffected.
`Devices` owns the `_peers` handle and uses its crate-internal
`write_internal()` method during durability barriers. `States::create`
declares and materializes the `_peers` system state before returning;
`Workspace::assemble` only obtains its handle with `States::get`.

## Peers And Roles

`_devices` stores `PeerId -> DeviceRecord`, where each record has
`display_name` and an optional progressive `Role`. No role means Reader.

The public `Devices` facade is intentionally small:

- `local_peer_id()` borrows the identity used for local mutations.
- `list()` returns the cached device records.
- `get(&peer_id)` reads one cached device record.
- `has_access(&peer_id, role)` checks any peer against the progressive
  hierarchy.
- `upsert(peer_id, record)` requires Admin and publishes a registry change,
  returning whether the record changed.

The registry's read-oriented cache is one
`RwLock<RegistryCache { entries, local_role }>` so a listener updates the
local record and its fast authorization value atomically. Event bookkeeping is
kept separately in `peer_cache`.

On create, `Devices` directly establishes the local Admin record as registry
event sequence `1`, seeds the registry cache, and records that receipt in the
initial peer state. Its clock starts with `next_sequence = 2`, so subsequent
events cannot reuse the bootstrap event ID. On open, `Devices` requires the
current peer to already have a registry entry; opening never silently promotes
an unknown peer. Only Admin may upsert device records or manage application
table declarations.

- Reader can read tables and is represented by no role.
- `Contributor` writes existing application tables.
- `Operator` includes Contributor behavior and is reserved for future
  dispatch operations.
- `Admin` includes Operator behavior and manages table declarations and device
  records through their idiomatic APIs.

States are local and do not perform role authorization. Their existing system
state handle guard remains unchanged. Direct system table insertion is refused
for every role, including Admin.

`_peers` stores `PeerState { receipts, clock }`; the optional clock is an
`EventClock { next_sequence, last_time }`. Only the local peer has a clock.
`Devices` keeps the local peer state separate from `others` in one
mutex-protected `PeerCache`. Minting updates only the local clock and marks it
dirty; it does not search a peer map or acquire the state write lock. Receipt
observation updates the same cache.

Local mutations are linear: require access, mint, insert, then observe (via
callback). There is no callback-based mutation API, reconciliation poison
state, or event journal outside each Table's Topic.

## Durability

`Devices`, `Tables`, and `States` each expose `flush` and `sync`. Device
durability first writes dirty peer states to `_peers`; Tables and States
then invoke the matching operation on the handles already stored in their
maps. `Workspace` coordinates them in that order so a device checkpoint is
written before table events are synchronized.

`Workspace` performs a coordinated best-effort `flush` from `Drop`, and
`Devices` flushes its separate peer cache before it is destroyed. Individual
storage `Table` and `State` values already flush when their final owners are
dropped, so `Tables` and `States` do not duplicate that behavior. Drop-time
errors cannot be reported; callers requiring error handling or an explicit
durability guarantee must call `Workspace::flush()` or `Workspace::sync()`.
Calling a submodule durability method only covers that submodule.
