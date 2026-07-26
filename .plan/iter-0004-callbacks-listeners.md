# Iteration 0004: Event-Driven Table And Device Maintenance

Status: architecture proposal for the callback/listener layer and the
unified local/remote event path.

Priority: this document supersedes iter-0002 and iter-0003 where the three
disagree. It is scoped to the callback mechanism, the event-driven
maintenance model, and the remote-event seam. Identity, bootstrap, and the
three-handler public surface are settled by iter-0003 and are not revisited
here except where the callback model changes their internals.

## 1. Problem

Iteration 0003 left table and device maintenance logic coupled to the CRUD
methods that publish the events:

- `Tables::create` both writes the catalog row AND opens the table
  + inserts the handle into the in-memory map.
- `Tables::delete` both writes the tombstone AND removes the handle
  + removes the physical directory.
- `Devices::write_record` both writes the device row AND updates the
  in-memory `records` map AND calls `observe`.
- `Tables::replay_receipts` drains a one-shot `TopicConsumer` per table at
  startup to feed `Devices::observe`, even though receipts are persisted in
  `_peer_state`.

This coupling has two costs:

1. **Local and remote mutations take different code paths.** A local
   `Tables::create` opens the table inline. A remote event that writes
   `_table_catalog` only writes the row; nothing opens the table locally.
   The workspace cannot today ingest a remote table-creation event.
2. **Redundant machinery.** The `TableEntry::receipt` consumer exists only for
   one-shot startup replay. The in-memory `records` map in `Devices` is
   updated inline in `write_record` rather than reactively, duplicating the
   authority of the `_devices` table.

The goal of this iteration is to make table and device maintenance **react to
catalog events**, so that local edits and remote events go through the exact
same code path. The CRUD method's job becomes: validate a precondition and
publish an event. The side effect (open/close a table, sync an in-memory map,
record a receipt) becomes a callback's job.

## 2. Target Model

### 2.1 CRUD methods publish; callbacks react

| Layer | Responsibility |
| --- | --- |
| `Tables` CRUD methods | Validate a precondition (read), publish an event to `_table_catalog`, return. No side effects. |
| `CatalogSync` callback (on `_table_catalog`) | React to catalog events: open tables on `Upsert`, close + rmdir on `Delete`. This is the single owner of table-handle-map mutation. |
| `Devices::upsert` / `bootstrap_local` | Validate authorization, mint, publish an event to `_devices`. No side effects on the in-memory map. |
| `DeviceSync` callback (on `_devices`) | React to device events: upsert/remove in the in-memory `records` map. This is the single owner of in-memory device-record mutation after initial load. |
| `Receipt` callback (on every table) | React to any insert: `Devices::observe(stamp)`. Replaces `replay_receipts` and the inline `observe` calls. |

The CRUD method and the callback are decoupled: the method writes the event,
the callback reacts. A remote event skips the method entirely and goes
through `Workspace::apply_event`, which inserts the event directly into the
target table — triggering the same callbacks.

### 2.2 Why this is uniform

A local `Tables::create("users", config)`:

    authorize → mint → publish Upsert("users", config) to _table_catalog
      → [Receipt callback: observe(stamp)]
      → [CatalogSync callback: open "users", insert handle into map]

A remote peer creating the same table:

    Workspace::apply_event("_table_catalog", Event::Upsert("users", config))
      → insert into _table_catalog
      → [Receipt callback: observe(stamp)]
      → [CatalogSync callback: open "users", insert handle into map]

The callback path is identical. The only difference is whether the event was
minted locally (after authorization) or arrived with a remote stamp. This is
the seam replication will use.

### 2.3 Precondition checks stay in the CRUD method

Some CRUD methods need precondition checks that read state but do not mutate:

- `create`: refuse if the name is already declared in `_table_catalog`.
- `update`: refuse if the name is not declared.
- `delete`: refuse if handles are outstanding (`Arc::strong_count > 1`).

These are read-only validations. They stay in the CRUD method. The mutation
(opening/closing the table, rmdir) moves to the callback. This honors the
spirit of the model: the method validates and publishes; the callback reacts.

A remote event bypasses these preconditions (the remote peer already
validated). The callback must therefore be idempotent and defensive: opening
an already-open table is a no-op; closing a table with outstanding handles
logs and leaves the handle in place (the directory is not removed until the
handle count drops). This is safe because the durable state (the catalog row)
is the source of truth; the in-memory handle map is a cache.

## 3. ChangeListener Trait And TableEntry

### 3.1 The trait

    pub trait ChangeListener: Send + Sync {
        fn on_change(&self, change: &Change);
    }

`on_change` returns `()` — fire-and-forget. The insert that triggered the
callback has already succeeded and is durable. A failing callback leaves the
in-memory state stale but never corrupts the durable state; on restart the
in-memory state is rebuilt from durable state (`reload`, `Tables::open`).
Callback errors are swallowed now; when logging is added they will be logged.

### 3.2 Listeners live on TableEntry

    pub(crate) struct TableEntry {
        pub(crate) table: Arc<RwLock<Table>>,
        listeners: RwLock<Vec<Arc<dyn ChangeListener>>>,
    }

Listeners are registered at open time and shared across all handles to the
same table (the `Arc<TableEntry>` is the shared inner). No central dispatcher
HashMap is consulted per insert — each table owns its listeners directly.

### 3.3 Table::insert returns the Change

The one storage API change:

    pub enum InsertOutcome {
        Applied(Box<Change>),
        Ignored,
    }

`Table::insert` already constructs the `Change` locally; returning it is a
move, zero extra cost. This lets `TableEntry::insert` dispatch the `Change` to
listeners without re-reading it from the topic.

### 3.4 TableEntry::insert encapsulates insert + dispatch

    impl TableEntry {
        pub(crate) fn insert(&self, event: Event) -> Result<InsertOutcome> {
            let outcome = self.table.write().insert(event)?;
            if let InsertOutcome::Applied(ref change) = outcome {
                for listener in self.listeners.read().iter() {
                    listener.on_change(change);
                }
            }
            Ok(outcome)
        }
    }

Both the local path (`TableHandle::insert`) and the remote path
(`Workspace::apply_event`) converge on `TableEntry::insert`. There is no
separate dispatch helper to forget to call.

### 3.5 Public listener registration on TableHandle

`TableHandle` exposes a method for applications to register their own
listeners on the underlying table:

    impl TableHandle {
        pub fn add_listener(&self, listener: Arc<dyn ChangeListener>) {
            self.entry.listeners.write().push(listener);
        }
    }

This lets an application observe changes on a table it holds a handle to —
for example, a reactive UI layer that updates when a table changes, or a
future replication hook that forwards application-table changes. The listener
is registered on the `TableEntry` and fires on every subsequent insert
through any handle to that table (local or, eventually, remote).

The `ChangeListener` trait is `pub` so applications can implement it. The
`Change` type it receives is already `pub` via `zendb_storage`.

Internal listeners (`ReceiptListener`, `CatalogSyncListener`,
`DeviceSyncListener`) are registered the same way, via `add_listener` or the
`pub(crate)` equivalent on `TableEntry`. There is no separate internal path;
application listeners and system listeners use the same `Vec`.

## 4. The Three Listeners

### 4.1 ReceiptListener (global)

Registered on every table, including system tables and application tables
opened by `CatalogSync`.

    struct ReceiptListener { devices: Weak<Devices> }

    impl ChangeListener for ReceiptListener {
        fn on_change(&self, change: &Change) {
            if let Some(devices) = self.devices.upgrade() {
                let _ = devices.observe(change.event.stamp);
            }
        }
    }

This replaces `Tables::replay_receipts` and the inline `observe` calls in
`Devices::insert_application` and `Devices::write_record`. Receipts are now
live-observed on every insert, local or remote.

### 4.2 CatalogSyncListener (on `_table_catalog` only)

    struct CatalogSyncListener {
        tables: Weak<Tables>,
        receipt_listener: Arc<dyn ChangeListener>,
    }

    impl ChangeListener for CatalogSyncListener {
        fn on_change(&self, change: &Change) {
            let Some(tables) = self.tables.upgrade() else { return; };
            match &change.event.op {
                Op::Upsert { value: Value::Blob(blob) } => {
                    let name = match &change.event.primary_key {
                        PrimaryKey::String(name) => name.clone(),
                        _ => return,
                    };
                    let config = match blob.decode() { Ok(c) => c, Err(_) => return };
                    if tables.tables.read().contains_key(&name) {
                        // Already open (update case or bootstrap no-op).
                        // Future: migrate on config change.
                        return;
                    }
                    match open_table(&tables, &name, config) {
                        Ok(entry) => {
                            entry.listeners.write().push(self.receipt_listener.clone());
                            core.tables.write().insert(name, entry);
                        }
                        Err(_) => { /* logged when logging lands */ }
                    }
                }
                Op::Delete => {
                    let name = match &change.event.primary_key {
                        PrimaryKey::String(name) => name.clone(),
                        _ => return,
                    };
                    let mut tables = core.tables.write();
                    if let Some(entry) = tables.remove(&name) {
                        drop(entry);
                        let path = core.root.join("tables").join(&name);
                        if path.exists() { let _ = fs::remove_dir_all(path); }
                    }
                }
                _ => {}
            }
        }
    }

`CatalogSync` holds a shared `Arc<dyn ChangeListener>` (the receipt listener)
so that when it opens a new application table, it registers the receipt
listener on that table. This guarantees every table — bootstrap-opened or
callback-opened — has the receipt listener.

The delete path does not check `Arc::strong_count` here. The precondition
check (refuse delete while handles are outstanding) stays in the CRUD method
for local deletes. For remote deletes, if handles are outstanding the
callback logs and leaves the handle in the map; the directory is not removed
until the handle count drops. This is safe: the durable catalog row is
already a tombstone, and the in-memory handle is a stale cache entry that
will be dropped on close or restart.

### 4.3 DeviceSyncListener (on `_devices` only)

    struct DeviceSyncListener { devices: Weak<Devices> }

    impl ChangeListener for DeviceSyncListener {
        fn on_change(&self, change: &Change) {
            let Some(devices) = self.devices.upgrade() else { return; };
            let PrimaryKey::PeerId(peer) = &change.event.primary_key else { return; };
            match &change.event.op {
                Op::Upsert { value: Value::Blob(blob) } => {
                    match blob.decode::<DeviceRecord>() {
                        Ok(record) => { devices.records.write().insert(*peer, record); }
                        Err(_) => {}
                    }
                }
                Op::Delete => { devices.records.write().remove(peer); }
                _ => {}
            }
        }
    }

This replaces the inline `self.inner.records.write().insert(peer, record)` in
`Devices::write_record`. The in-memory `records` map is now updated
reactively, whether the device record was written locally or by a remote
peer.

`Devices::reload` stays for the initial cold-start load (it reads the merged
state of the `_devices` table, which is faster than replaying the event log).
The callback handles subsequent deltas. `reload` and the callback share the
"apply a record to the map" operation.

## 5. Cycle-Breaking With Weak

`Tables` owns the `_table_catalog` `TableEntry`, whose listeners include
`CatalogSyncListener`, which holds `Weak<Tables>`. This is a cycle broken
by `Weak`:

    Tables
      → Arc<TableEntry> (_table_catalog)
        → Vec<ChangeListener>
          → CatalogSyncListener
            → Weak<Tables>

The `Weak` means `CatalogSyncListener::on_change` must `upgrade()` before
use; if `Tables` has been dropped (workspace shutting down), the callback
no-ops. The same pattern applies to `ReceiptListener` and
`DeviceSyncListener` holding `Weak<Devices>`.

No `Arc` cycles exist. The listeners are registered after their owners are
constructed and hold only `Weak` back-references.

## 6. Bootstrap Restructured

The current bootstrap writes the `_table_catalog` and `_devices` catalog rows
inside `Tables::create`, then constructs `Devices`, then calls
`replay_receipts`. With the callback model, bootstrap is staged so that every
catalog write goes through the callback path:

1. **`Tables::build`** opens the `_table_catalog` and `_devices` physical
   tables and inserts them into the handle map. It writes NO catalog rows.
2. **`States::build`** opens `_state_catalog` (unchanged).
3. **`Devices::create`** constructs `PeerStore` and calls `reload()` to load
   existing device records into the in-memory map.
4. **Register listeners.** The workspace assembly registers:
   - `ReceiptListener` on every table currently in the handle map (the system
     tables).
   - `CatalogSyncListener` on `_table_catalog`.
   - `DeviceSyncListener` on `_devices`.
5. **Write the catalog rows** for `_table_catalog` and `_devices` via the
   normal `TableEntry::insert` path. The `CatalogSync` callback fires, sees
   the tables are already in the handle map, and no-ops. The `Receipt`
   callback observes the bootstrap stamps.
6. **`bootstrap_local`** publishes the local device record to `_devices` via
   `TableEntry::insert`. The `DeviceSync` callback fires and updates the
   in-memory map. The `Receipt` callback observes the stamp.

Every bootstrap write now goes through the same callback path as runtime
writes. There is no special-case bootstrap code for receipts or handle-map
mutation. The only special case is that system tables are pre-opened before
their catalog rows are written (you cannot write a catalog row without the
catalog table existing).

### 6.1 Bootstrap stamps

The bootstrap catalog rows still need stamps. `Workspace::assemble` mints
them up front (sequences 1 and 2 for the local peer) and passes them to
`Tables::build`, which now only uses them to construct the bootstrap
`Event`s in step 5. The stamps are observed by the `Receipt` callback in step
5, so no separate `replay_receipts` is needed.

## 7. Removed And Moved

| Removed / moved | Reason |
| --- | --- |
| `TableEntry::receipt: Mutex<TopicConsumer>` | Receipts are live-observed via `ReceiptListener` |
| `Tables::replay_receipts()` | Redundant — receipts come from the callback + persisted `_peer_state` |
| `RECEIPT_CONSUMER` constant | Unused |
| `Devices::insert_application()` | Inlined into `TableHandle::insert` (authorize + mint + `TableEntry::insert`) |
| Inline `observe()` calls in `write_record` and `insert_application` | `ReceiptListener` does this reactively |
| Inline `records.insert()` in `write_record` | `DeviceSyncListener` does this reactively |
| Inline table-open in `Tables::create` | `CatalogSyncListener` does this reactively |
| Inline table-close + rmdir in `Tables::delete` | `CatalogSyncListener` does this reactively |

`Devices::reload()` stays (initial cold-start load). `Tables::build_open`
stays (reads catalog declarations and opens tables at workspace open — this is
the cold-start equivalent of what `CatalogSync` does at runtime).

## 8. Remote Event Ingestion (Deferred)

A `Workspace::apply_event` entry point for remote events is **not** added in
this iteration. The replication layer is not yet designed, and the exact shape
of the remote-event ingestion API (whether it takes a raw `Event`, a signed
envelope, or a batch) is premature to fix now.

The callback model is designed so that whatever the ingestion API turns out
to be, it will converge on `TableEntry::insert(event)` for the target table,
triggering the same listeners as local mutations. That convergence point is
the seam; the public entry point that feeds it is deferred to the replication
iteration.

## 9. Local Mutation Path

`TableHandle::insert` becomes the single local mutation path for application
tables:

    impl TableHandle {
        pub fn insert(&self, primary_key: PrimaryKey, path: Path, op: Op) -> Result<InsertOutcome> {
            self.devices.authorize(Roles::Contributor)?;
            let stamp = self.devices.mint()?;
            self.entry.insert(Event { primary_key, path, op, stamp })
        }
    }

`Devices::insert_application` is gone — its logic (authorize + mint + insert
+ observe) is split: authorize + mint stay in `TableHandle::insert`, insert +
dispatch move into `TableEntry::insert`, observe moves into `ReceiptListener`.

`Devices::upsert` and `bootstrap_local` publish to `_devices` via
`TableEntry::insert`:

    fn write_record(&self, peer: PeerId, record: DeviceRecord) -> Result<()> {
        let stamp = self.mint()?;
        let blob = Blob::encode(&record)?;
        self.inner.registry.insert(Event {
            primary_key: PrimaryKey::PeerId(peer),
            path: Path::new(),
            op: Op::Upsert { value: Value::Blob(blob) },
            stamp,
        })?;
        Ok(())
    }

The in-memory map update and the `observe` call are gone from `write_record` —
the `DeviceSync` and `Receipt` callbacks handle them.

## 10. Tables CRUD Methods

The CRUD methods become thin: validate, publish, return.

    fn create(&self, name: &str, config: TableConfig, stamp: EventStamp) -> Result<()> {
        if is_system_table(name) || self.catalog_entry(name).is_ok() {
            return Err(Error::AlreadyExists(name.to_owned()));
        }
        self.write_entry(name, &config, stamp)
    }

    fn update(&self, name: &str, config: TableConfig, stamp: EventStamp) -> Result<bool> {
        let current = self.catalog_entry(name)?;
        if current == config { return Ok(false); }
        self.write_entry(name, &config, stamp)?;
        Ok(true)
    }

    fn delete(&self, name: &str, stamp: EventStamp) -> Result<bool> {
        if is_system_table(name) { return Err(Error::ResourceBusy(name.to_owned())); }
        if let Some(entry) = self.tables.read().get(name) {
            if Arc::strong_count(entry) > 1 { return Err(Error::ResourceBusy(name.to_owned())); }
        } else {
            return Ok(false);
        }
        self.write_delete(name, stamp)
    }

`create` returns `()`. The callback opens the table synchronously
during `write_entry`'s dispatch, but the caller does not need the handle
immediately — `Tables::create` is now void-returning (see §10.1).

### 10.1 `Tables::create` is void-returning

`Tables::create` no longer returns a `TableHandle`:

    pub fn create(&self, name: &str, config: TableConfig) -> Result<()> {
        self.devices.authorize(Roles::Operator)?;
        let stamp = self.devices.mint()?;
        if is_system_table(name) || self.catalog_entry(name).is_ok() {
            return Err(Error::AlreadyExists(name.to_owned()));
        }
        self.write_entry(name, &config, stamp)
    }

The caller obtains a handle separately via `Tables::open(name)` when needed.
This removes the ordering dependency between the callback opening the table
and `create` returning the handle. The explicit `observe(stamp)` call is gone
— `ReceiptListener` on `_table_catalog` handles it.

`Tables::update` and `Tables::delete` similarly drop their explicit `observe`
calls; the `ReceiptListener` covers them. `Tables::update` returns
`Result<bool>` (true if the config changed, false if it was already the
same) — consistent with `Tables::delete`. The `UpdateOutcome` enum is removed.

### 10.2 System tables: openable and updatable, not deletable, not directly writable

System tables (`_table_catalog`, `_devices`) follow different rules than
application tables:

- **`Tables::open`** returns a handle to a system table. The workspace itself
  needs handles to system tables (e.g. `Devices` holds the `_devices` handle),
  and there is no reason to hide them from applications for reads.
- **`Tables::update`** works on system tables. A system table's `TableConfig`
  may change (e.g. `max_buffered_records`), and the catalog row is updated.
  The `CatalogSync` callback sees the `Upsert`, finds the table already open,
  and (in this iteration) no-ops. A future iteration may migrate the table on
  config change.
- **`Tables::delete`** refuses system tables with `ResourceBusy`. System
  tables cannot be removed; they are structural.
- **`TableHandle::insert`** refuses system tables. An application holding a
  handle to `_table_catalog` or `_devices` must not write to it directly —
  those tables are maintained by the workspace through `Tables` CRUD
  methods and `Devices` methods, which publish events that the callbacks
  react to. Direct writes would bypass authorization, minting, and the
  callback model.

The `is_system_table` check is **not** evaluated per insert. Instead, the
`TableHandle` stores an `is_system: bool` flag set at construction time:

    pub struct TableHandle {
        name: String,
        entry: Arc<TableEntry>,
        devices: Arc<Devices>,
        is_system: bool,
    }

    impl TableHandle {
        pub fn insert(&self, primary_key: PrimaryKey, path: Path, op: Op) -> Result<InsertOutcome> {
            if self.is_system {
                return Err(Error::PermissionDenied);
            }
            self.devices.authorize(Roles::Contributor)?;
            let stamp = self.devices.mint()?;
            self.entry.insert(Event { primary_key, path, op, stamp })
        }
    }

`Tables::open` sets `is_system` based on `is_system_table(name)`. The check
is a single branch per insert, not a string match or a hashmap lookup.

Internal workspace code (`Tables`, `Devices`, `PeerStore`) does not go
through `TableHandle` for system-table writes. It calls
`TableEntry::insert(event)` directly, bypassing the `is_system` check. This
is the seam between the workspace's internal authority and the application's
restricted handle.

### 10.3 System states: same model

`StateHandle` follows the same pattern. It stores `is_system: bool` set at
construction. `StateHandle::write()` returns `Result` and refuses if the
state is a system state:

    pub struct StateHandle<K: Ord, V> {
        name: String,
        state: Arc<RwLock<State<K, V>>>,
        is_system: bool,
    }

    impl<K: Ord, V> StateHandle<K, V> {
        pub fn write(&self) -> Result<RwLockWriteGuard<'_, State<K, V>>> {
            if self.is_system { return Err(Error::PermissionDenied); }
            Ok(self.state.write())
        }
        pub fn read(&self) -> RwLockReadGuard<'_, State<K, V>> { self.state.read() }
    }

Internal workspace code that needs to write to a system state (`PeerStore`
writing `_peer_state`) does not go through `StateHandle`. It holds the raw
`Arc<RwLock<State<K, V>>>` directly, bypassing the `is_system` check.
`States::open_state` returns the `StateHandle` with `is_system` set, but
`States` also exposes a `raw_peer_state` accessor for internal callers that
need the raw `Arc`.

`States::create` and `States::delete` refuse system states (as today).
`States::open_state` returns a handle to a system state with `is_system` set, so
reads work but writes are refused.

### 10.4 `States::create` and `States::open_state`

States have a different lifecycle than Tables. Tables are eagerly opened:
every declared table is in the in-memory handle map, so `Tables::open` is a
hashmap lookup. States are **lazy**: a state may be declared in
`_state_catalog` but not currently open. `States::open_state` must check the open
map, return if present, otherwise open from the catalog.

The two operations are split:

- **`States::create(name, config) -> Result<()>`** — declare a new state in
  `_state_catalog` and physically create it. Void-returning. Refuses if the
  state is already declared. This is the analog of `Tables::create`: it
  publishes the declaration.
- **`States::open_state::<K, V>(name) -> Result<StateHandle<K, V>>`** — obtain a
  typed handle to an existing state. If the state is already in the open
  map, downcast and return. Otherwise, read its declaration from
  `_state_catalog`, physically open it, insert into the open map, and return
  the typed handle. This is the lazy-load path.

    pub fn create(&self, name: &str, config: StateConfig) -> Result<()> {
        if is_system_state(name) { return Err(Error::NotFound(name.to_owned())); }
        // ...write declaration to _state_catalog, sync...
    }

    pub fn open_state<K, V>(&self, name: &str) -> Result<StateHandle<K, V>>
    where
        K: Encode + Decode<()> + Hash + Eq + Clone + Ord + Send + Sync + 'static,
        V: Encode + Decode<()> + Clone + Send + Sync + 'static,
    {
        // ...check open map, else read declaration + physically open...
    }

`States` is split accordingly:

- `create(name, config) -> Result<()>` — write the declaration to
  `_state_catalog`, physically create the state directory, insert into the
  open map as an erased `Arc`. Void-returning.
- `open_state::<K, V>(name) -> Result<StateHandle<K, V>>` — check the open map;
  if present, downcast and return. Otherwise read the declaration from
  `_state_catalog`, physically open the state, insert into the open map, and
  return the typed handle.

The typed `StateHandle` is obtained via `open_state`, not `create`. `create` only
establishes the state; `open_state` lazy-loads it on demand. This mirrors the
`Tables` pattern (create declares, open obtains a handle) while respecting
the lazy-load difference (tables are always open; states are opened on
first access).

`States::peer_state::<K, V>()` (used by `Devices` bootstrap) ensures
`_peer_state` is declared, then calls `open_state::<K, V>(PEER_STATE_NAME)`
after ensuring the state is declared — or the bootstrap path calls `create`
then `open_state` for `_peer_state`. Internal callers that need to write to
`_peer_state` use `States::raw_peer_state::<K, V>()`, which returns the raw
`Arc<RwLock<State<K, V>>>` bypassing the `is_system` check.

The existing `States::list_open`, `States::close`, `States::delete`, and
`States::config` methods remain. `States::contains` checks the catalog
(declared, whether or not open).

## 11. Implementation Sequence

Each phase should compile before the next one. No tests are added unless
explicitly requested.

### Phase 1: storage API change

- Change `InsertOutcome` to `Applied(Box<Change>)` / `Ignored`.
- Update `Table::insert` to move the constructed `Change` into the outcome.
- Update all call sites (`TableHandle::insert`, `Devices::insert_application`,
  `Tables::write_entry`, `Devices::write_record`, tests).

### Phase 2: ChangeListener trait and TableEntry

- Add `ChangeListener` trait to `tables/runtime.rs` (pub, so applications can
  implement it).
- Add `listeners: RwLock<Vec<Arc<dyn ChangeListener>>>` to `TableEntry`.
- Add `TableEntry::insert(event)` that does insert + dispatch.
- Add `TableEntry::add_listener` (pub(crate)) and `TableHandle::add_listener`
  (pub) for listener registration.

### Phase 3: ReceiptListener

- Implement `ReceiptListener` holding `Weak<Devices>`.
- Register it on every table during bootstrap.
- Remove `TableEntry::receipt`, `RECEIPT_CONSUMER`, `replay_receipts`.
- Remove inline `observe` calls from `Devices::insert_application` and
  `write_record`.
- Verify receipts are live-observed via the callback.

### Phase 4: CatalogSyncListener and system-handle protection

- Implement `CatalogSyncListener` holding `Weak<Tables>` + shared
  `Arc<dyn ChangeListener>` (the receipt listener).
- Register it on `_table_catalog`.
- Move table-open logic from `Tables::create` into the callback.
- Move table-close + rmdir logic from `Tables::delete` into the callback.
- `Tables::create` / `Tables::delete` become validate + publish.
- `Tables::create` becomes void-returning; callers use `Tables::open` to
  obtain a handle.
- Remove `UpdateOutcome`; `Tables::update` returns `Result<bool>`.
- `Tables::update` no longer refuses system tables (config updates allowed).
- Add `is_system: bool` to `TableHandle`; `insert` refuses if true.
- `Tables::open` sets `is_system` based on `is_system_table(name)` and no
  longer refuses system tables.
- Internal workspace code calls `TableEntry::insert` directly, bypassing the
  `is_system` check.

### Phase 5: DeviceSyncListener, States split, and system-state protection

- Implement `DeviceSyncListener` holding `Weak<Devices>`.
- Register it on `_devices`.
- Move in-memory `records` map update from `write_record` into the callback.
- `write_record` becomes mint + publish.
- `reload` stays for initial load.
- Split `States` into `create(name, config) -> Result<()>`
  (declare + physically create, void) and `open_state::<K, V>(name) ->
  Result<StateHandle<K, V>>` (lazy-load: check open map, else read declaration
  + physically open + return typed handle).
- `States::create` is void-returning; `States::open_state` returns the typed
  `StateHandle` (lazy-loading on first access, including for system states).
- Add `is_system: bool` to `StateHandle`; `write()` returns `Result` and
  refuses if true. `read()` is unaffected.
- `States::open_state` no longer refuses system states; it returns a handle with
  `is_system` set.
- Internal workspace code (`PeerStore`) holds the raw `Arc<RwLock<State>>`
  directly for system-state writes, bypassing `StateHandle`.
- Update `States::peer_state` to use `create` + `open_state` (or `raw_peer_state`
  accessor for internal writes).

### Phase 6: bootstrap restructure

- `Tables::build` opens system tables but writes no catalog rows.
- After `Devices` is constructed and listeners are registered, write the
  bootstrap catalog rows via `TableEntry::insert`.
- `bootstrap_local` publishes via `TableEntry::insert`.
- Remove the explicit `observe` calls from `Tables::create`/`update`/`delete`
  once the listener path covers them.

### Phase 7: documentation and static verification

- Update `zendb-workspace` README.
- Run `cargo fmt --all`, `cargo check --workspace`,
  `clippy --workspace --lib --no-deps -- -D warnings`.
- Do not add or run new tests unless requested.

## 12. Resolved Implementation Decisions

1. `ChangeListener::on_change` returns `()` (fire-and-forget). The insert is
   already durable; a failing callback leaves in-memory state stale but
   rebuildable on restart.
2. Listeners live on `TableEntry` as `Vec<Arc<dyn ChangeListener>>`, not in a
   central dispatcher HashMap. No per-insert lookup.
3. `CatalogSyncListener` holds a shared `Arc<dyn ChangeListener>` (the
   receipt listener) to register on newly opened application tables.
4. Precondition checks (already-exists, not-found, busy) stay in the CRUD
   methods. Side effects (open, close, rmdir, map sync) move to callbacks.
5. `Devices::reload` stays for cold-start; the `DeviceSync` callback handles
   deltas.
6. Remote deletes with outstanding handles log and leave the handle in the
   map; the directory is not removed until the handle count drops. The
   durable tombstone is the source of truth.
7. `Workspace::apply_event` is **not** added in this iteration. The
   replication layer is not yet designed; the remote-event ingestion API is
   premature. The callback model is designed so that whatever the ingestion
   API turns out to be, it will converge on `TableEntry::insert(event)`,
   triggering the same listeners as local mutations.
8. `Table::insert` returns `InsertOutcome::Applied(Box<Change>)` so callbacks
   receive the change without re-reading the topic.
9. Bootstrap writes go through the callback path. System tables are
   pre-opened before their catalog rows are written.
10. The explicit `observe` calls in the `Tables` handler are removed once
    `ReceiptListener` is verified to cover the `_table_catalog` path.
11. `Tables::create` is void-returning. Callers obtain a handle via
    `Tables::open(name)` separately. This removes the ordering dependency
    between the callback opening the table and `create` returning.
12. `States::create` is void-returning (declare a new state). `States::open_state`
    returns a typed `StateHandle` and lazy-loads: if the state is already in
    the open map, return it; otherwise read the declaration from
    `_state_catalog`, physically open the state, and return the handle.
    `States` is split into `create(name, config)` (void) and
    `open_state::<K, V>(name)` (returns `StateHandle<K, V>`). This mirrors the
    `Tables` pattern (create declares, open obtains a handle) while
    respecting the lazy-load difference (tables are always open; states are
    opened on first access).
13. `ChangeListener` is `pub` and `TableHandle::add_listener` is `pub`, so
    applications can register custom listeners on tables they hold handles
    to. Internal listeners and application listeners share the same `Vec`.
14. `UpdateOutcome` is removed. `Tables::update` returns `Result<bool>`
    (true if the config changed, false if unchanged), consistent with
    `Tables::delete`.
15. System tables are openable and updatable but not deletable and not
    directly writable via `TableHandle`. `TableHandle` stores `is_system: bool`
    set at construction; `insert` refuses if true. Internal workspace code
    calls `TableEntry::insert` directly, bypassing the check. The check is a
    single branch per insert, not a string match or hashmap lookup.
16. System states follow the same model: `StateHandle` stores `is_system: bool`;
    `write()` returns `Result` and refuses if true; `read()` is unaffected.
    Internal workspace code (`PeerStore`) holds the raw `Arc<RwLock<State>>`
    directly for system-state writes. `States::open_state` returns handles to system
    states with `is_system` set; `States::create` and `States::delete` refuse
    system states.

## 13. Completion Criteria

Iteration 0004 is complete when:

- `ChangeListener` is a trait on `TableEntry`, with listeners registered at
  open time and dispatched synchronously on every `TableEntry::insert`;
- `Table::insert` returns `InsertOutcome::Applied(Box<Change>)`;
- `TableEntry::receipt`, `RECEIPT_CONSUMER`, and `replay_receipts` are gone;
- `ReceiptListener` is registered on every table and replaces inline
  `observe` calls;
- `CatalogSyncListener` is registered on `_table_catalog` and is the single
  owner of table-handle-map mutation (open on `Upsert`, close + rmdir on
  `Delete`);
- `DeviceSyncListener` is registered on `_devices` and is the single owner
  of in-memory device-record mutation after `reload`;
- `Tables` CRUD methods validate and publish; they no longer open/close
  tables or mutate the handle map directly;
- `Devices::write_record` mints and publishes; it no longer updates the
  in-memory map or calls `observe` directly;
- `Devices::insert_application` is gone;
- `Tables::create` is void-returning; callers use `Tables::open` to obtain a
  handle;
- `States::create` is void-returning; `States::open_state` returns a typed
  `StateHandle` and lazy-loads on first access; `States` is split into
  `create` and `open_state::<K, V>`;
- `UpdateOutcome` is removed; `Tables::update` returns `Result<bool>`;
- `TableHandle` stores `is_system: bool` and refuses `insert` on system tables;
  `Tables::open` returns handles to system tables; `Tables::update` works on
  system tables; `Tables::delete` refuses system tables;
- `StateHandle` stores `is_system: bool` and refuses `write()` on system
  states; `States::open_state` returns handles to system states; `States::create`
  and `States::delete` refuse system states;
- `ChangeListener` is `pub` and `TableHandle::add_listener` is `pub`, so
  applications can register custom listeners;
- bootstrap writes catalog rows through the callback path;
- the workspace compiles, clippy is clean, and documentation reflects the
  event-driven maintenance model.
