# Iteration 0004: Event-Driven Catalog And Handle Maintenance

Status: implemented.

Priority: this document is the consolidated record for the catalog and handle
model. It supersedes the earlier `iter-0004-callbacks-listeners`,
`iter-0005-lean-catalog-apis`, and `iter-0006-single-table-handle` plans. Where
this document disagrees with those, this document wins — it describes the code
as it stands. The source of truth is the code; the iteration plans are
advisory and were consolidated here once the work landed.

This iteration covers three coupled concerns:

1. **Event-driven maintenance** — table and installation side effects react to
   catalog events through listeners, so local edits and (future) remote events
   share one code path.
2. **Lean catalog APIs** — both `Tables` and `States` expose a small, uniform
   surface (`contains` / `list` / `upsert` / `get` / `delete`) with no
   routing-only helpers and no per-mutation durability.
3. **Single table handle** — `TableHandle` is the one in-memory
   representation of an open table; there is no `TableEntry`/`TableHandle`
   split, and `Tables` owns the entire table/installation dependency graph.

## 1. Problem

Earlier iterations coupled maintenance logic to the CRUD methods that publish
the events:

- `Tables::create` both wrote the catalog row AND opened the table + inserted
  the handle into the in-memory map.
- `Installations::write_record` both wrote the installation row AND updated the in-memory
  `records` map AND called `observe`.
- Receipts were drained by a one-shot consumer at startup, duplicating the
  authority of the `_peer_state` table.

Two costs followed: local and remote mutations took different code paths (a
remote catalog event only wrote the row; nothing opened the table locally), and
the in-memory maps were updated inline rather than reactively.

The goal is to make table and installation maintenance **react to catalog events**,
so that local edits and remote events go through the exact same code path. The
CRUD method's job becomes: validate a precondition, publish an event. The side
effect (open/close a table, sync an in-memory map, record a receipt) becomes a
listener's job.

## 2. Target Model

| Layer | Responsibility |
| --- | --- |
| `Tables` / `States` lifecycle methods | Validate a precondition (read), publish an event to the catalog, return. No side effects on the handle map. |
| `CatalogSync` listener (on `_table_catalog`) | React to catalog events: open tables on `Upsert`, close + rmdir on `Delete`. Single owner of table-handle-map mutation after bootstrap. |
| `Installations::upsert` / `bootstrap_local` | Validate authorization, mint, publish an event to `_installations`. No side effects on the in-memory map. |
| `InstallationSync` listener (on `_installations`) | React to installation events: upsert/remove in the in-memory `records` map. Single owner of in-memory installation-record mutation after `reload`. |
| `Receipt` listener (on every table) | React to any insert: `Installations::observe(stamp)`. Replaces the one-shot startup replay and the inline `observe` calls. |

The CRUD method and the listener are decoupled: the method writes the event,
the listener reacts. A remote event skips the method entirely and goes through
`Workspace::apply_event` (deferred — see §8), which inserts the event directly
into the target table, triggering the same listeners.

### 2.1 Why this is uniform

A local `Tables::upsert("users", config)`:

    authorize → mint → publish Upsert("users", config) to _table_catalog
      → [Receipt listener: observe(stamp)]
      → [CatalogSync listener: open "users", insert handle into map]

A remote peer creating the same table:

    Workspace::apply_event("_table_catalog", Event::Upsert("users", config))
      → insert into _table_catalog
      → [Receipt listener: observe(stamp)]
      → [CatalogSync listener: open "users", insert handle into map]

The listener path is identical. The only difference is whether the event was
minted locally (after authorization) or arrived with a remote stamp. This is
the seam replication will use.

### 2.2 Precondition checks stay in the lifecycle method

Some lifecycle methods need precondition checks that read state but do not
mutate:

- `upsert`: refuse if the name is a system resource.
- `delete`: refuse if the name is a system resource; refuse if handles are
  outstanding (`Arc::strong_count > 1`).

These are read-only validations. They stay in the lifecycle method. The
mutation (opening/closing the table, rmdir, map sync) moves to the listener.
This honors the model: the method validates and publishes; the listener
reacts.

A remote event bypasses these preconditions (the remote peer already
validated). The listener must therefore be idempotent and defensive: opening
an already-open table is a no-op; closing a table with outstanding handles
logs and leaves the handle in place (the directory is not removed until the
handle count drops). This is safe because the durable state (the catalog row)
is the source of truth; the in-memory handle map is a cache.

## 3. Single Table Handle

`TableHandle` is the single in-memory representation of an open table. It owns
the guarded storage `Table`, the listener collection, the table name, a
system-table flag, and a weak reference to `Installations`. There is no
`TableEntry`/`TableHandle` split.

`Tables` stores `Arc<TableHandle>` values in an eager map and also keeps the
`_table_catalog` handle as a dedicated `catalog: Arc<TableHandle>` field. The
catalog field and the map entry are the same `Arc`. `Installations` owns the
`_installations` handle, which is the same `Arc` as the `_installations` map entry.

System tables are created before `Installations` exists, so their handles use an
empty weak installation reference (`Weak::new()`). The public `TableHandle::insert`
checks the system flag before resolving the installation reference; application
handles use `Arc::downgrade(&installations)` and return `WorkspaceClosed` if retained
after their workspace has closed.

### 3.1 The `ChangeListener` trait

    pub trait ChangeListener: Send + Sync {
        fn on_change(&self, change: &Change);
    }

`on_change` is fire-and-forget. The insert that triggered it has already
succeeded and is durable. A failing internal callback may leave derived
in-memory state stale but never corrupts durable state; on restart the
in-memory state is rebuilt from the stored `Table`. Callback errors are
swallowed now; when logging is added they will be logged.

The trait is `pub` so applications can implement custom listeners and register
them via `TableHandle::add_listener`.

### 3.2 Listeners live on the handle

    pub struct TableHandle {
        name: String,
        table: RwLock<Table>,
        listeners: RwLock<Vec<Arc<dyn ChangeListener>>>,
        installations: Weak<Installations>,
        is_system: bool,
    }

Listeners are registered at open time and shared across all handles to the same
table (the `Arc<TableHandle>` is the shared inner). No central dispatcher
HashMap is consulted per insert — each handle owns its listeners directly.

### 3.3 The two insert paths

`TableHandle::insert` is the public application path. It rejects system
tables, authorizes the local installation, mints an event stamp, and delegates to
`insert_internal`:

    impl TableHandle {
        pub fn insert(&self, primary_key: PrimaryKey, path: Path, op: Op)
            -> Result<InsertOutcome>
        {
            if self.is_system {
                return Err(Error::SystemTableReadOnly(self.name.clone()));
            }
            let installations = self.installations.upgrade().ok_or(Error::WorkspaceClosed)?;
            installations.authorize(zendb_types::Roles::Contributor)?;
            let stamp = installations.mint()?;
            self.insert_internal(Event { primary_key, path, op, stamp })
        }

        pub(crate) fn insert_internal(&self, event: Event) -> Result<InsertOutcome> {
            let outcome = { self.table.write().insert(event)? };
            if let InsertOutcome::Applied(ref change) = outcome {
                for listener in self.listeners.read().iter() {
                    listener.on_change(change);
                }
            }
            Ok(outcome)
        }
    }

`insert_internal` is crate-internal and accepts a complete `Event`. It inserts
into storage, releases the table write guard, and then dispatches listeners.
Catalog, installation, bootstrap, and future replicated events use this path. Both
the local path (`TableHandle::insert`) and the remote path
(`Workspace::apply_event`, deferred) converge on `insert_internal`. There is no
separate dispatch helper to forget to call.

Listener ownership stays in `zendb-workspace`. Moving listeners into the
storage `Table` would dispatch callbacks while the workspace write guard is
held and would couple storage mechanics to orchestration.

### 3.4 Public listener registration

`TableHandle::add_listener` is `pub`, so an application can observe changes on
a table it holds a handle to — for example, a reactive UI layer, or a future
replication hook. The listener is registered on the `TableHandle` and fires on
every subsequent insert through any handle to that table (local or, eventually,
remote). Application listeners and internal workspace listeners share the same
`Vec`.

## 4. The Three Listeners

### 4.1 ReceiptListener (global)

Registered on every table, including system tables and application tables
opened by `CatalogSync`.

    struct ReceiptListener { installations: Weak<Installations> }

    impl ChangeListener for ReceiptListener {
        fn on_change(&self, change: &Change) {
            let Some(installations) = self.installations.upgrade() else { return; };
            let _ = installations.observe(change.event.stamp);
        }
    }

This replaces the one-shot startup replay. Receipts are now live-observed on
every insert, local or remote.

### 4.2 CatalogSyncListener (on `_table_catalog` only)

    struct CatalogSyncListener {
        tables: Weak<Tables>,
        receipt_listener: Arc<dyn ChangeListener>,
    }

`CatalogSync` holds a shared `Arc<dyn ChangeListener>` (the receipt listener)
so that when it opens a new application table, it registers the receipt
listener on that table. This guarantees every table — bootstrap-opened or
callback-opened — has the receipt listener.

On `Upsert` it opens the table (creating the physical directory if absent,
otherwise opening it) and inserts the `Arc<TableHandle>` into the eager map,
registering the receipt listener. If the table is already open (update case or
bootstrap no-op) it returns early. On `Delete` it removes the handle from the
map and removes the physical directory if it still exists. This is the single
owner of table-handle-map mutation after bootstrap.

### 4.3 InstallationSyncListener (on `_installations` only)

    struct InstallationSyncListener { installations: Weak<Installations> }

On `Upsert` it decodes the `Installation` and inserts it into the in-memory
`records` map; on `Delete` it removes it. This replaces the inline
`records.insert()` in `Installations::write_record`. The in-memory map is now updated
reactively, whether the installation record was written locally or by a remote peer.

`Installations::reload` stays for the initial cold-start load (it reads the merged
state of the `_installations` table, which is faster than replaying the event log).
The listener handles subsequent deltas. `reload` and the listener share the
"apply a record to the map" operation.

## 5. Cycle-Breaking With Weak

`Tables` owns the `_table_catalog` `TableHandle`, whose listeners include
`CatalogSyncListener`, which holds `Weak<Tables>`. This is a cycle broken by
`Weak`:

    Tables
      → Arc<TableHandle> (_table_catalog)
        → Vec<ChangeListener>
          → CatalogSyncListener
            → Weak<Tables>

The `Weak` means `CatalogSyncListener::on_change` must `upgrade()` before use;
if `Tables` has been dropped (workspace shutting down), the callback no-ops.
The same pattern applies to `ReceiptListener` and `InstallationSyncListener` holding
`Weak<Installations>`.

No `Arc` cycles exist. The listeners are registered after their owners are
constructed and hold only `Weak` back-references.

## 6. Lean Catalog APIs

Both catalogs expose a small, uniform surface. There are no routing-only
helpers, no `TableInfo`, no config accessor, no `update` method, and no
open-only listing. System resources are visible to lookup and listing.

### 6.1 State catalog (`States`)

`States` owns the typed `_catalog` handle and an erased open-state map. The
typed catalog field and its erased map entry share one `Arc<StateHandle<_, _>>`,
so every catalog access is synchronized by the same lock. Each handle stores its
`RwLock<State<_, _>>` directly, and `get` returns an `Arc` clone of the stored
handle.

States remain lazy after declaration. The public surface is:

- `contains`
- `list`
- `upsert`
- `get`
- `list_open`
- `close`
- `delete`

`list` and `list_open` return natural backend/map iteration order. System
states are visible to lookup and listing. Public `upsert` and
`StateHandle::write` reject system states (`SystemStateReadOnly`). Workspace
bootstrap uses `upsert_internal`, and peer persistence uses `write_internal`.
System states cannot be deleted.

`States::get` never creates. It returns the open handle if present, or opens
the physical state declared in `_catalog` and returns it. If the state is not
declared (neither open nor in the catalog) it returns `NotFound`. Declaration
and physical creation are the job of `upsert`.

### 6.2 Table catalog (`Tables`)

`Tables` owns the typed `_catalog` handle and an eager map containing every
declared table. The catalog field and map entry share one `Arc<TableHandle>`;
`_installations` similarly shares the handle owned by `Installations`.

The public surface is:

- `contains`
- `list`
- `upsert`
- `get`
- `delete`

`list` returns table names in natural map iteration order. There is no
`TableInfo`, config accessor, close operation, or open-only listing. System
tables are visible to lookup and listing. Public `upsert` and
`TableHandle::insert` reject system tables (`SystemTableReadOnly`). System
tables cannot be deleted.

Catalog mutations remain event driven. `CatalogSyncListener` is the sole owner
of eager map changes after construction. `write_entry` is shared by public
`upsert` and workspace bootstrap; no routing-only bootstrap method exists.

### 6.3 `upsert` semantics

`upsert` merges create and update:

- If the name is a system resource, refuse.
- If already declared with the same config, return `false` (no-op).
- If declared with a different config, update and return `true`.
- If not declared, create the declaration (and materialize the physical state
  for `States`) and return `true`.

Return type is `Result<bool>` (true = a change was made). This removes the
separate `create`/`update` pair and the `UpdateOutcome` enum.

### 6.4 Durability

Catalog lifecycle methods do not call `sync` or `flush` after intermediate
mutations. A later iteration will coordinate durability centrally.

## 7. Bootstrap And The Dependency Graph

`Tables::create` / `Tables::open` own the complete table/installation dependency
graph:

- create or open the `_installations` handle (with an empty weak installation reference,
  since `Installations` does not yet exist);
- create or open `Installations` (passing the `_installations` handle and the peer-state
  handle);
- create or open the `_table_catalog` handle (with a weak reference to
  `Installations`);
- eagerly open every declared application-table handle by reading the catalog
  rows;
- construct the shared `Tables` registry (which holds `Arc<Installations>`);
- register the receipt, catalog, and installation listeners via
  `Tables::register_listeners`;
- on create, write both self-referencing system catalog rows (`_catalog`,
  `_installations`) and bootstrap the local installation.

Both constructors return only `Arc<Tables>`. `Workspace::assemble` clones
`tables.installations` after table construction.

`Workspace::assemble` itself establishes the state subsystem first: it creates
or opens `States`, in create mode calls `States::upsert_internal` for the
`_peers` system state, obtains the `_peers` `StateHandle` via `States::get`, and
passes that handle and the peer identity to `Tables::create` / `Tables::open`.
It does not construct storage tables, register table listeners, or mint table
bootstrap events — `Tables` does all of that.

Every bootstrap write goes through the same listener path as runtime writes.
There is no special-case bootstrap code for receipts or handle-map mutation.
The only special case is that system tables are pre-opened before their
catalog rows are written (you cannot write a catalog row without the catalog
table existing).

## 8. Remote Event Ingestion (Deferred)

A `Workspace::apply_event` entry point for remote events is **not** added in
this iteration. The replication layer is not yet designed, and the exact shape
of the remote-event ingestion API is premature to fix now.

The listener model is designed so that whatever the ingestion API turns out to
be, it will converge on `TableHandle::insert_internal(event)` for the target
table, triggering the same listeners as local mutations. That convergence
point is the seam; the public entry point that feeds it is deferred to the
replication iteration.

## 9. Resolved Implementation Decisions

1. `ChangeListener::on_change` returns `()` (fire-and-forget). The insert is
   already durable; a failing callback leaves in-memory state stale but
   rebuildable on restart.
2. Listeners live on `TableHandle` as `Vec<Arc<dyn ChangeListener>>`, not in a
   central dispatcher HashMap. No per-insert lookup.
3. `CatalogSyncListener` holds a shared `Arc<dyn ChangeListener>` (the receipt
   listener) to register on newly opened application tables.
4. Precondition checks (system resource, busy) stay in the lifecycle methods.
   Side effects (open, close, rmdir, map sync) move to listeners.
5. `Installations::reload` stays for cold-start; the `InstallationSync` listener handles
   deltas.
6. Remote deletes with outstanding handles log and leave the handle in the
   map; the directory is not removed until the handle count drops. The durable
   tombstone is the source of truth.
7. `Workspace::apply_event` is **not** added in this iteration. The callback
   model converges on `TableHandle::insert_internal(event)`.
8. `Table::insert` returns `InsertOutcome::Applied(Box<Change>)` so callbacks
   receive the change without re-reading the topic.
9. Bootstrap writes go through the listener path. System tables are pre-opened
   before their catalog rows are written.
10. `Tables` and `States` expose a lean, uniform surface: `contains` / `list`
    / `upsert` / `get` / `delete` (States also `list_open` / `close`). No
    `update`, no `TableInfo`, no config accessor, no routing-only bootstrap
    method.
11. `upsert` merges create and update and returns `Result<bool>`. The
    `UpdateOutcome` enum is gone.
12. `States::get` is lazy and never creates; declaration and physical creation
    are the job of `upsert`. `Tables::get` returns from the eager handle map and
    returns `NotFound` if absent.
13. `ChangeListener` is `pub` and `TableHandle::add_listener` is `pub`, so
    applications can register custom listeners. Internal and application
    listeners share the same `Vec`.
14. `TableHandle` is the single in-memory representation of an open table; there
    is no `TableEntry`/`TableHandle` split. `Tables` stores `Arc<TableHandle>`
    and owns the full table/installation dependency graph in `create`/`open`.
15. `TableHandle::insert` is the public application path (rejects system tables,
    authorizes, mints); `insert_internal` is the crate-internal path used by
    catalog, installation, bootstrap, and future replicated events.
16. `TableHandle::read` returns `RwLockReadGuard<Table>` directly and
    `TableHandle::consumer` returns `zendb_storage::TopicConsumer<Change>`
    directly; the former workspace `TableReadGuard` / `TableConsumer` wrappers
    added no behavior and are removed.
17. System resources are openable and consumable through public handles but not
    directly writable: `TableHandle::insert` and `StateHandle::write` refuse
    them. Internal code uses `insert_internal` / `write_internal` to bypass the
    check. The check is a single branch per call, not a string match or
    hashmap lookup.
18. `Tables` and `States` keep their data directory in a `root` field
    (`tables/` and `states/` respectively). The catalog is a `catalog` field;
    the open-handle map is `tables` / `states`. Directory and system-resource
    names live in a shared `consts` module (`is_system_table`,
    `is_system_state`, `TABLE_CATALOG_NAME`, `INSTALLATIONS_TABLE_NAME`,
    `STATE_CATALOG_NAME`, `PEER_STATE_NAME`, `TABLES_DIR`, `STATES_DIR`).

## 10. Completion Criteria

Iteration 0004 is complete when:

- `ChangeListener` is a trait on `TableHandle`, with listeners registered at
  open time and dispatched synchronously on every `insert_internal`;
- `Table::insert` returns `InsertOutcome::Applied(Box<Change>)`;
- `ReceiptListener` is registered on every table and replaces inline `observe`
  calls;
- `CatalogSyncListener` is registered on `_table_catalog` and is the single
  owner of table-handle-map mutation (open on `Upsert`, close + rmdir on
  `Delete`);
- `InstallationSyncListener` is registered on `_installations` and is the single owner of
  in-memory installation-record mutation after `reload`;
- `Tables` lifecycle methods validate and publish; they no longer open/close
  tables or mutate the handle map directly;
- `Installations::write_record` mints and publishes; it no longer updates the
  in-memory map or calls `observe` directly;
- `Tables` and `States` expose the lean surface (`contains` / `list` /
  `upsert` / `get` / `delete`, States also `list_open` / `close`); `upsert`
  merges create and update and returns `Result<bool>`;
- `States::get` is lazy and never creates; `Tables::get` returns from the eager
  map;
- `TableHandle` is the single in-memory representation of an open table; there
  is no `TableEntry`/`TableHandle` split;
- `Tables::create` / `Tables::open` own the full table/installation dependency graph
  and register listeners; `Workspace::assemble` only establishes the state
  subsystem and passes the peer-state handle to `Tables`;
- `TableHandle::insert` is the public application path and `insert_internal` is
  the crate-internal path;
- `TableHandle::read` and `TableHandle::consumer` return the storage guard /
  consumer directly (no workspace wrapper types);
- system resources are openable and consumable but not directly writable via
  `TableHandle::insert` / `StateHandle::write`; internal code uses
  `insert_internal` / `write_internal`;
- bootstrap writes catalog rows through the listener path;
- the workspace compiles, clippy is clean, and documentation reflects the
  event-driven, lean, single-handle catalog model.
