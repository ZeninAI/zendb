# Iteration 0002: Refine Workspace And Shared Types

Status: requirements capture and architecture proposal

Priority: this document supersedes iter-0001-table-installation.md where the two
documents disagree. It records the new direction; it does not implement any
of these changes yet.

The goal is to remove incidental framework code, reduce the number of workspace
state owners, and make the table/consumer boundary direct. The implementation
remains synchronous and local. Networking, replication, snapshots, operators,
and background workers remain out of scope.

## 1. Requirements Captured

| Area | Requirement |
| --- | --- |
| Paths | Replace PathStep with type Path = Vec<Segment> and update consumers. |
| Workspace metadata | Remove format versions, broad name validation, migration errors, and similar defensive surface from workspace. |
| Serialization | Move storage utils into zendb-types; storage consumes one shared serialization configuration. |
| Blob | Use shared serialization helpers and configuration; remove duplicated bincode setup. |
| Operations | Rename Op::Replace to Op::Upsert. |
| Roles | Move workspace permissions into identity data and rename them Roles: Contributor, Operator, Dispatcher. |
| Configuration | Do not reject State config changes with migration errors in this phase. |
| Identity | Use libp2p's identity generation/encoding primitive for both distinct PeerId and WorkspaceId types. |
| Installation state | Use one Catalog-owned system State<PeerId, PeerRecord>, physically backed by one unordered PeerId -> PeerRecord store. |
| Installation API | Replace callback-based perform and reconciliation poisoning with simple stamp minting and observation. |
| Table runtime | Remove live/invalidation and avoid materializing consumer iteration into intermediate collections. |
| Consumers | Expose direct Table access to a consumer through a read guard while preserving one-record-at-a-time Topic consumption. |
| Workspace boundaries | Catalog is the only workspace submodule allowed to open storage Tables/States or manipulate storage paths. Installations obtains state handles from Catalog. |
| System resources | Catalog owns the always-present _table_catalog Table and _state_catalog State. Tables are eagerly opened; typed application States retain open/close semantics. |

These requirements favor a small, understandable core over defensive validation
and crash-recovery machinery that has not yet earned its complexity.

## 2. Problems To Remove

The current implementation has several overlapping authorities:

    Workspace
      -> Catalog
           -> CatalogIndex
                -> TableRuntime { name, Table lock, receipt consumer, live flag }
      -> Installations
           -> LocalInstallation
                -> ClockStore KeyDir
                -> ReceiptIndex KeyDir
           -> cached Installation map
      -> StateCatalog
           -> StateConfig KeyDir

The resulting issues are:

1. Table identity, catalog ownership, liveness, and receipt replay are mixed in
   TableRuntime.
2. Clock and receipt persistence use separate stores and separate locks even
   though they are keyed by the same participant.
3. LocalInstallation::perform combines stamp minting, an arbitrary mutation callback,
   receipt recording, checkpointing, ambiguity detection, and a reconciliation
   state machine.
4. Workspace errors and metadata contain format/version/name policy that is not
   needed while the storage shape is still changing.
5. PathStep carries no information beyond its Segment field.
6. Blob encoding uses bincode's standard configuration while storage helpers use
   a different fixed-int little-endian configuration.
7. A runtime live flag invalidates handles after catalog deletion, while the
   actual filesystem lifetime problem is whether any handle still owns Table.

Iteration 0002 addresses these issues by making ownership and persistence
explicit rather than adding more proxy methods or defensive policy layers.

## 3. Target Dependency Shape

    workspace
      |  catalog, tables, roles, peer state
      v
    storage
      |  backends, State, Topic, Table
      v
    types
      |  PeerId, CRDTs, shared serialization
      v
    libp2p identity

zendb-types remains free of storage policy and workspace orchestration. It does
gain shared generic encoding helpers because both types and storage need the
same byte representation. Roles is inert serializable identity data in
zendb-types; authorization decisions remain in workspace. Roles must not gain
workspace-specific permission methods.

The libp2p dependency is an identity dependency only. No transport or network
runtime is introduced by this iteration.

## 4. Path Simplification

### 4.1 New representation

Remove PathStep and define:

    pub type Path = Vec<Segment>;

Path remains an ordered sequence from a row root to a nested Cell. An empty
path targets the row root. Segment already identifies the container and key
needed by CRDT values, so PathStep adds no information.

### 4.2 Required changes

This is a mechanical change:

- update ContainerType::cell_at_path, cell_at_path_mut, and apply_walk to
  consume &[Segment];
- update Cell and recursive Record/List implementations to pass a Segment
  directly;
- remove PathStep::new and its export;
- update event construction, workspace calls, examples, and documentation;
- preserve bincode derives on Event and Path.

No path validation or normalization is added. A path is data supplied to the
CRDT implementation; invalid traversal is handled by the existing operation
result.

## 5. One Serialization Utility Layer

### 5.1 Move the modules

Move zendb-storage/src/utils/{serdes.rs,reusables.rs,mod.rs} to
zendb-types/src/utils/. Storage imports shared functions through
zendb_types::utils and no longer owns a competing implementation.

The moved layer contains:

- one bincode configuration;
- size calculation;
- encoding into slices and writers;
- owned vector encoding;
- pooled scratch buffers;
- one owned decode helper;
- little-endian primitive page helpers;
- the small non-cryptographic random helper used by SkipList.

The utility module is generic and does not refer to B+ tree pages, Topics, or
workspace types. If a helper is truly storage-specific after the move, it stays
in the backend file instead of making zendb-types aware of storage.

### 5.2 Canonical bincode configuration

There is one canonical configuration: the current fixed-int little-endian
configuration used by storage.

    pub fn cfg() -> Configuration<LittleEndian, Fixint, NoLimit>;

Blob, catalog metadata, installation records, State backends, and Topic records all use
this configuration. The format is intentionally allowed to change while the
project is pre-production.

The shared owned-value API is:

    serialize_to_vec<T: Encode>(&T) -> io::Result<Vec<u8>>
    deserialize_from<T: Decode<()>>(&[u8]) -> io::Result<T>

Both helpers map bincode failures to `io::Error`. `deserialize_from` retains
permissive trailing-byte behavior. There is no parallel `encode_to_vec`,
exact-decoding helper, or helper-specific error type.

### 5.3 Blob integration

`Blob::encode` and `Blob::decode` delegate directly to `serialize_to_vec` and
`deserialize_from`. Both return `io::Result`; Blob has no `BlobCodecError` or
trailing-byte policy of its own.

## 6. Operation Naming

Rename the Op variant Replace { value } to Upsert { value }.

Semantics remain last-writer-wins replacement at the operation stamp. The new
name communicates that the operation creates a missing Cell as well as updating
an existing one. Update all CRDT matches, workspace catalog/installation writes,
examples, and documentation. No compatibility alias is required.

## 7. Identity And Roles

### 7.1 Shared identity primitive, distinct domain identities

PeerId and WorkspaceId remain different concepts. PeerId identifies a peer that
can participate in future replication; WorkspaceId identifies the workspace
itself. They must not be collapsed into one field or one type alias.

What is unified is the identity primitive: both IDs use the same libp2p-derived
generation, canonical byte representation, byte length rules, ordering, and
binary encoding. The current custom fixed-size identity macro is removed.

The desired public shape is:

    pub use libp2p::PeerId;

    pub struct EventId {
        pub peer_id: PeerId,
        pub sequence: u64,
    }

The identity module owns the shared codec boundary. Use libp2p PeerId as the
source primitive and provide explicit bincode encoding/decoding for PeerId,
WorkspaceId, EventId, and KeyDir keys through canonical PeerId byte
conversions. WorkspaceId should use the same canonical identity-byte format,
but remain a distinct newtype so replication and workspace ownership cannot be
confused by the type system.

If the selected PeerId cannot implement required bincode traits directly,
implement Encode and Decode for the local identity newtypes manually. A local
wrapper is acceptable as a serialization adapter and domain-distinguishing
newtype; it must not introduce a second identity generator or a different byte
format.

Workspace continues to retain its WorkspaceId. The local peer ID and workspace
ID may be generated from the same key/identity primitive, but they are stored
and passed as separate values.

### 7.2 Roles value

Move the serializable role value into zendb-types::identity and name it Roles:

    pub enum Roles {
        Contributor,
        Operator,
        Dispatcher,
    }

Installation metadata stores a set of Roles, not WorkspacePermission. Role semantics
are workspace policy, but the enum is inert data so it can be persisted and
later replicated without coupling types to Catalog implementation.

Capability table:

| Role | Current capability |
| --- | --- |
| Contributor | Write existing application Tables; cannot create/delete Tables or create installations; may update fields of its own peer/installation record but cannot change the PeerId key or grant roles. |
| Operator | Includes Contributor capabilities; may create/update/delete application Tables and create peer/installation records. System Tables remain internal. |
| Dispatcher | Defined and persisted only; no behavior is attached in this iteration. |

The phrase “modify the primary key in the installations catalog belonging to its own
installation” is interpreted conservatively as modifying the row belonging to the
current PeerId. Changing that row's PeerId key is identity transfer and is not
permitted. This interpretation must be confirmed before implementation.

Role checks belong to one workspace authorization function. Do not scatter role
checks across Table, Catalog, and Installations.

## 8. Lean Workspace Metadata

### 8.1 Remove version and name policy

Remove from zendb-workspace:

- WorkspaceFormat.version and WORKSPACE_FORMAT_VERSION;
- CatalogEntry.format_version and CATALOG_ENTRY_FORMAT_VERSION;
- UnsupportedFormat;
- MigrationRequired;
- generic filesystem/name validation and InvalidName;
- corruption branches whose only purpose is rejecting a version or name policy.

CatalogEntry becomes:

    pub struct CatalogEntry {
        pub config: TableConfig,
    }

A catalog row that cannot decode as CatalogEntry remains an ordinary storage
failure. It does not need a format-version decision tree.

There is still one structural reservation: Catalog must keep _catalog and
_installations from colliding with application rows. This is not general name
validation. The public application API should reject those two exact reserved
names, or expose system Tables only through internal methods.

No cross-platform punctuation, length, reserved-installation-name, or trailing-space
checks are performed. The caller owns those requirements.

### 8.2 Bootstrap without a versioned manifest

Preferred lean layout:

    root/
      _lock
      _identity                 unversioned WorkspaceId/local-peer metadata
      tables/
        _table_catalog/         Catalog's self-registering Table
        _installations/               installation metadata Table
        <table-name>/           application Tables
      states/
        _state_catalog/         Catalog's String -> StateConfig State
        _peer_state/             Installations' PeerId -> PeerRecord State
        <state-name>/            application States

Remove the versioned _format manifest. The logical system names and physical
paths are separate: _table_catalog, _installations, _state_catalog, and _peer_state
are Catalog-owned resources, not arbitrary directories created by Installations or
other workspace submodules. The Catalog opens _table_catalog and
_state_catalog with fixed bootstrap configurations, then stores their
declarations in themselves. The unified peer-state State keeps the local
PeerId record; _identity stores WorkspaceId and the minimal local bootstrap
identity using the shared identity codec.

If custom catalog configuration is still required, the fallback is an
unversioned bootstrap record containing only that config. It must not grow a
format-version protocol before core behavior is stable. The fixed default is
preferred for iteration speed.

### 8.3 Configuration changes

State Catalog behavior becomes simple:

- if a State is open, return its typed handle and ignore supplied config;
- if a State is closed and declared, always use persisted config;
- if a State is new, use supplied config or default;
- never return a migration error for a caller-supplied mismatch.

The same principle applies to Table declarations: Catalog owns persisted config,
and an already-open Table is not reconfigured in place. A later open uses the
current catalog declaration. No migration framework is introduced.

## 9. Catalog As The Workspace Storage Boundary

Catalog is not merely an index around miscellaneous files. It owns the two
system resources that describe workspace data:

    _table_catalog  Table<String, CatalogEntry>
    _state_catalog  State<String, StateConfig>

The table catalog is itself a normal Table and must contain its own declaration
under the reserved key _table_catalog. The row maps table names to their
TableConfig. Its physical path is tables/_table_catalog, while its logical
catalog name is _table_catalog and it is visible to Catalog's internal table
registry.

The state catalog is a State, not a Table. It maps state names to their
StateConfig. Catalog opens it internally with the fixed types String and
StateConfig so it can manage declarations. Application States remain generic:
the caller supplies K and V when requesting a StateHandle, and the caller can
close that typed State before reopening it with another concrete type.

The state catalog itself must exist under the reserved name _state_catalog.
Self-registration in the state catalog is recommended for symmetry, but its
bootstrap row must not be required before the fixed catalog state can be
created.

### 9.1 Catalog owns storage lifecycle

Catalog is the only workspace component that may:

- call storage DurableStorage create/open for Tables or States;
- choose physical paths under the workspace root;
- construct or hold raw storage backend values;
- open the catalog Table and catalog State;
- translate catalog declarations into TableRuntime objects.

Installations and any future workspace submodule must not import or construct
KeyDir, BPlusTree, SkipList, State, Topic, or backend configuration directly.
They obtain storage through Catalog methods such as:

    catalog.table(name) -> TableHandle
    catalog.state::<K, V>(name, config) -> StateHandle<K, V>

The Catalog API is therefore the workspace-internal storage boundary. This
prevents every submodule from inventing its own directory layout and ensures
that state declarations, paths, and lifecycle rules remain in one place.

Catalog may use storage traits internally, but Installations should depend only on
Catalog-provided handles and workspace-level operations. A direct storage
backend import in Installations is an architectural violation for this iteration.

### 9.2 Bootstrap order

The bootstrap sequence is deliberately non-recursive:

1. Workspace obtains its WorkspaceId and local PeerId using the shared identity
   primitive.
2. Catalog creates/opens the fixed _table_catalog Table with bootstrap config.
3. Catalog creates/opens the fixed _state_catalog State with
   String -> StateConfig types.
4. Catalog records _table_catalog in the table catalog and _state_catalog in
   the state catalog using their fixed bootstrap declarations.
5. Catalog reconciles all remaining table declarations and eagerly opens every
   Table runtime.
6. Installations asks Catalog for its declared State and Table handles; it performs
   no path or backend initialization itself.

Tables have no open/close API at the workspace level. Catalog opens every
declared Table during Workspace open and keeps its runtime entry available.
Only typed application States have open/close behavior because their concrete
K and V types are selected by the caller.

## 10. Unified Peer State

### 10.1 Persistent record

Replace separate clock and receipt stores with one Catalog-owned system State,
logically named _peer_state and configured as an unordered State (the Catalog
may select KeyDir internally):

    pub struct PeerRecord {
        pub receipts: ReceiptWindow,
        pub clock: Option<ClockCheckpoint>,
    }

    State<PeerId, PeerRecord>

clock is Some only for the local PeerId. Remote peers retain receipt windows
without a local minting checkpoint. The _installations Table remains the place for
display name and Roles; this State is operational peer state, not a second
installation catalog.

Installations never constructs this State or its underlying KeyDir. It asks Catalog
for a typed StateHandle<PeerId, PeerRecord> for _peer_state. The physical
backend choice is a Catalog concern and can change without changing Installations.

### 10.2 In-memory representation

On open, Catalog opens _peer_state and the Installations module loads its rows into
an immutable in-memory snapshot. Use
ArcSwap<PeerMap> for lock-free reads of receipt membership, missing ranges, and
peer checkpoints. Mutations are serialized through one small writer gate, which
replaces the snapshot and marks changed PeerIds dirty.

    PeerStore
      state: Catalog-provided StateHandle<PeerId, PeerRecord>
      snapshot: ArcSwap<PeerMap>
      writer: Mutex<PeerMutationState>
      dirty: set<PeerId>

flush() writes dirty PeerRecords through the Catalog-provided State handle and
calls the Catalog/state durability boundary.
Drop performs best-effort flush using the same shared implementation pattern as
storage backends. There is no per-peer ClockStore or ReceiptIndex exposed to
workspace code.

### 10.3 Clock and receipt authority

Installations becomes the single public facade over PeerStore and the _installations Table.
Internal files can still separate role-table code from peer-state code, but
there is one authority for local identity, clock, receipts, and persistence.

Reduced API:

    local_peer_id()
    mint() -> EventStamp
    observe(EventStamp) -> ObserveOutcome
    has_received(EventId) -> bool
    missing(PeerId) -> ranges
    flush() -> io::Result<()>
    list / record / upsert peer metadata

There is no LocalInstallation public concept, no callback-taking perform, and no
Mutation enum.

### 10.4 Sequence durability trade-off

Removing reconciliation and callback coupling creates an unavoidable choice:

    mint in memory + batch flush
      -> fastest, but a crash can reuse a sequence

    persist the local sequence reservation before mint returns
      -> crash-safe uniqueness, but one durable write per mint or reserved block

Because EventId uniqueness is fundamental, the recommended first implementation
persists the local clock high-water mark before mint returns. Receipt
observations and remote PeerRecords can still batch through flush(). A later
iteration may add sequence-block reservation, but it must represent abandoned
ranges so they are not mistaken for missing remote events.

mint() only mints and advances the local clock. It does not call application
code, inspect Table outcomes, or attempt rollback. A failed Table write may burn
a sequence; it must not reuse one.

## 11. Simple Installation Mutation Flow

Workspace mutation code becomes linear:

    authorize role
      -> stamp = installations.mint()
      -> build Event
      -> table.insert(Event)
      -> if accepted, installations.observe(stamp)
      -> sync affected Table when caller requests durability
      -> installations.flush() at the workspace durability boundary

The exact receipt timing is not coupled to a callback. An ignored CRDT operation
is still an accepted event and is recorded as received; it simply produces no
Change. A failed Table insertion may consume a sequence but is not marked
received.

There is no ReconciliationRequired poison state. If a storage operation returns
an error, the error is returned directly. Recovery policy can be added once
actual ingestion and replication callers exist.

## 12. Table Runtime Redesign

### 12.1 Remove liveness invalidation

Delete TableRuntime.live, invalidate(), ensure_live(), and StaleHandle. A Table
handle is an ownership reference, not a revocable capability.

The Catalog registry stores an Arc<TableEntry>:

    struct TableEntry {
        table: Arc<RwLock<Table>>,
        receipt: Mutex<TopicConsumer<Change>>,
    }

The application handle stores a clone of this Arc and the catalog name for
display only. Storage Table still knows no name or identity.

The runtime must not become a second Table API made of one-method proxies. Its
only read bridge is a lock guard:

    TableHandle::read() -> TableReadGuard

TableReadGuard dereferences to Table. With the storage read traits in scope,
users call the real Table implementations through the guard:

    let read = handle.read()?;
    let cell = ReadBackend::get(&*read, &key);
    let rows = OrderedReadBackend::range(&*read, &start, &end);

There is no runtime-level get/keys/values/entries forwarding layer. The guard
holds the read lock for the lifetime of the borrowed iterator, so the iterator
reads directly from Table's lazy cache/State merge and never constructs an
intermediate collection.

### 12.2 Delete and recreate lifecycle

Deletion is based on ownership:

1. Check whether the registry Arc has application handles in addition to the
   registry reference.
2. If handles remain, return ResourceBusy and leave the catalog unchanged.
3. If no handles remain, write the catalog tombstone, remove the registry entry,
   and remove the physical directory.
4. A later create of the same name starts with a clean directory.

This directly addresses the deferred ad-hoc requirement. There is no invalid
handle state to explain and no unsafe same-name reuse while a Table owns maps,
files, or Topic readers.

### 12.3 Consumer access without intermediate collections

Topic consumers already decode one record per next() call. The workspace
consumer should preserve that behavior and add direct Table access:

    pub struct TableConsumer {
        table: Arc<RwLock<Table>>,
        changes: TopicConsumer<Change>,
    }

    impl TableConsumer {
        pub fn next_change(&mut self) -> io::Result<Option<Change>>;
        pub fn read(&self) -> TableReadGuard<'_>;
    }

read() acquires the Table read lock and exposes the existing Table API directly.
It does not collect entries or create a proxy snapshot. The caller controls the
guard lifetime:

    consumer.next_change()        one durable Change
    consumer.read().get(key)      one borrowed Table read
    consumer.read().entries()     lazy iterator while guard is held

The exact Rust lifetime can use the existing borrowed TableReadGuard when the
consumer borrows a TableHandle. If an owning consumer is required, use an
Arc-owned read guard or reacquire the read lock per operation; do not introduce
a Vec-backed consumer snapshot merely to avoid a lifetime design problem.

Receipt replay loops process one Change at a time and call Installations::observe
directly. No replay helper materializes a collection.

### 12.4 Iteration performance target

The Table MergedEntries two-way merge remains the correct lazy design for
ordered State plus pending cache. This iteration should additionally audit:

- default OrderedReadBackend reverse and reverse-range implementations that
  currently collect and reverse;
- unordered State ordered fallbacks that need temporary sorting;
- duplicate key handling where cache must win;
- Table::size while pending cache rows shadow existing State rows;
- Topic consumer decoding and segment positioning for one-record streaming.

The target is no intermediate full-table map in Table or Consumer. The
workspace-level handle should not expose a convenience entries() method that
silently returns Vec. If an owned snapshot is needed later, name it explicitly
snapshot_entries() so materialization is visible at the call site; the normal
read API returns the lazy iterator through TableReadGuard.

## 13. Catalog Simplification

Catalog remains a Table-backed catalog because it is the future replication
boundary, but its implementation becomes smaller:

    _table_catalog Table
      key: table name string
      value: Blob(CatalogEntry { config })

    _state_catalog State<String, StateConfig>
      key: state name string
      value: StateConfig

    CatalogIndex
      entries: map<table name, TableConfig>
      tables: map<table name, Arc<TableEntry>>

No version field, name validator, migration branch, or liveness flag is kept.
The two reserved system names are structural constants, not general name
policy. CatalogEntry decoding uses the shared types serialization helper.

Catalog still owns table create/list/update/delete and State declaration/open/
close/delete. It passes stored TableConfig into storage create/open and is the
only workspace module that does so. It never passes a TableId or table name to
storage.

## 14. Error Surface After Simplification

Remove error variants that only support removed policy:

    UnsupportedFormat
    InvalidName
    MigrationRequired
    ReconciliationRequired
    StaleHandle

Retain only errors needed for current behavior:

    Io
    AlreadyExists
    NotFound
    PermissionDenied
    ResourceBusy
    CorruptCatalog
    CorruptInstallationRegistry
    CorruptLocalState
    InvalidEventSequence
    ClockExhausted

The final enum can be reduced further after call sites change. Do not add
defensive categories for hypothetical network or migration paths.

## 15. Public Handler Surface

The workspace public API is split into three handlers, each a cheaply
cloneable owning facade over a shared `Arc`-backed core. `Workspace` exposes
`installations()`, `tables()`, and `states()` accessors and retains only identity,
bootstrap, lock, root, and flush responsibilities. It no longer hosts table or
state management methods directly.

### 15.1 Three handlers

- `Installations` — peer registry, hybrid clock, roles, and duplicate-event tracking.
  Unchanged from the unified peer state design in section 10.
- `Tables` — table catalog management. Owns the renamed `TablesCore` (the
  former `Catalog` minus state methods) and exposes `contains`, `list`,
  `create`, `update`, `delete`, and `open(name) -> TableHandle`. Mutating
  methods wrap `authorize(Roles::Operator)` + `mint` + core op + `observe`.
  `open` returns a handle without stamping.
- `States` — state catalog management. Owns the extracted `StatesCore` (the
  former `StateCatalog`) and exposes `contains`, `list`,
  `open::<K, V>(name, config)`, `list_open`, `config`, `close`, and `delete`.
  No mint/observe and no authorization: the state catalog is a `State`, not a
  stamped Table, and state declarations are not gated by a role in this
  iteration.

### 15.2 Table operations live on the handle

`Tables` is a catalog management surface. It does not expose table operations
such as `insert` or `write_entry`. Callers obtain a `TableHandle` via
`Tables::open` and perform operations directly on the handle:

    let table = workspace.tables().open("users")?;
    table.insert(primary_key, path, op)?;
    let guard = table.read();
    // ReadBackend / OrderedReadBackend methods on guard

This mirrors how `States::open` returns a `StateHandle` whose `read`/`write`
methods are the operation surface, while `States` itself only manages
lifecycle.

### 15.3 Module layout

The `catalog/` module is removed. Its responsibilities are split between two
new top-level modules:

- `tables/` — `Tables` (public handler), `TablesCore` (internal storage
  boundary for tables), `TableEntry`, `TableHandle`, `TableConsumer`,
  `TableReadGuard`, `CatalogEntry`, `TableInfo`, `UpdateOutcome`, and the
  table system-name constants (`_table_catalog`, `_installations`).
- `states/` — `States` (public handler), `StatesCore` (internal storage
  boundary for states), `StateHandle`, and the state system-name constants
  (`_state_catalog`, `_peer_state`).

`TablesCore` remains the only workspace component that opens storage Tables or
chooses physical table paths. `StatesCore` is the only component that opens
storage States or chooses physical state paths. `Installations` depends on
`TablesCore`-provided table entries and `StatesCore`-provided state handles;
it never imports storage backends directly.

### 15.4 Two-phase construction

`TablesCore` and `Installations` have a construction dependency: `Installations` needs the
`_installations` table entry and the `_peer_state` state handle, both of which come
from the cores. The bootstrap sequence is therefore staged:

1. `TablesCore::create(...)` returns `Arc<TablesCore>` with `_table_catalog`
   and `_installations` opened and self-registered.
2. `StatesCore::create(...)` returns `Arc<StatesCore>` with `_state_catalog`
   self-registered.
3. `Installations::create(core.table_entry(INSTALLATIONS_NAME)?, states.peer_state()?,
   ...)` returns `Arc<Installations>`.
4. Assemble `Tables { core, installations }` and `States { core }`.
5. `core.replay_receipts(&installations)`.
6. `installations.bootstrap_local(...)`.

This mirrors how `Installations` already receives Catalog-provided handles at
construction time. No new pattern is introduced.

### 15.5 System-name reservation

System-name filtering (`is_system_table`, `is_system_state`) moves into the
respective handler modules and is applied at the public API boundary. The
cores do not filter; they serve all declared names so that internal callers
such as `Installations` can reach `_installations` and `_peer_state`.

## 16. Implementation Sequence

Each phase should compile before the next one. No tests are added unless
explicitly requested.

### Phase 0: record the boundary

- Treat this file as the active plan.
- Keep ad-hoc.md as a separate deferred-work record.
- Update READMEs only after implementation phases settle.

### Phase 1: mechanical type cleanup

- Replace PathStep with Vec<Segment>.
- Rename Op::Replace to Op::Upsert.
- Update all CRDT, storage, workspace, example, and documentation call sites.

### Phase 2: shared utilities

- Move storage utilities into zendb-types/src/utils.
- Make storage consume the moved helpers.
- Make Blob use the shared `io::Result` serialization helpers.
- Remove duplicate serializer configuration and imports.

### Phase 3: identity and roles

- Add the libp2p identity dependency.
- Replace InstallationId with PeerId while retaining WorkspaceId as a distinct type.
- Make PeerId and WorkspaceId use the same libp2p-derived generator and codec.
- Implement explicit PeerId byte encoding/decoding and deterministic ordering.
- Move inert Roles into identity and update Installation persistence.
- Centralize role capability checks in workspace.

### Phase 4: lean workspace metadata

- Remove format/version manifest logic.
- Remove general name validation and migration errors.
- Simplify CatalogEntry to { config }.
- Make State config mismatch use persisted config without an error.
- Keep only exact system-name reservations.

### Phase 5: unified peer state

- Introduce PeerRecord and the Catalog-owned _peer_state State.
- Load it into an ArcSwap read snapshot.
- Implement serialized writes, dirty tracking, and shared flush/Drop.
- Replace LocalInstallation, ClockStore, ReceiptIndex, Mutation, and callback perform
  paths with Installations mint/observe/flush methods.
- Persist the local clock high-water mark before returning mint().

### Phase 6: direct Table handles and consumers

- Remove TableRuntime liveness and stale-handle invalidation.
- Use Arc ownership and ResourceBusy deletion semantics.
- Add direct TableConsumer with one-record Topic iteration and read guards.
- Move receipt replay to a streaming consumer loop.
- Audit backend reverse/range iteration for avoidable materialization.

### Phase 7: three-handler public surface

- Rename `catalog/` to `tables/` and split state management into a new
  `states/` module.
- Introduce `Tables` and `States` public handlers wrapping `Arc<TablesCore>`
  and `Arc<StatesCore>`.
- Move table/state management methods off `Workspace` and onto the handlers.
- Keep `TableHandle` as the table operation surface; `Tables` exposes only
  catalog management.
- Update `Workspace` to expose `installations()`, `tables()`, and `states()`.
- Update integration tests and READMEs to the new API.

### Phase 8: documentation and static verification

- Update root, types, storage, and workspace READMEs.
- Run cargo fmt --all.
- Run cargo check --workspace.
- Use clippy --workspace --lib --no-deps -- -D warnings if supported.
- Do not add or run new tests unless requested.

## 17. Resolved Implementation Decisions

1. Identity uses the narrow `libp2p-identity` crate. Local `PeerId` and
   `WorkspaceId` newtypes wrap its `PeerId` and encode canonical PeerId bytes.
2. `_table_catalog`, `_installations`, and `_state_catalog` use fixed default
   bootstrap configurations. No bootstrap configuration manifest is stored.
3. Contributor self-edit means editing the current peer's metadata row without
   changing its key or Roles.
4. Installation metadata stores `BTreeSet<Roles>`.
5. `mint()` persists the local clock high-water mark before returning.
6. `TableConsumer` owns the Table `Arc` and reacquires a borrowed read guard;
   it never creates a row snapshot.
7. The public workspace API is split into `Installations`, `Tables`, and `States`
   handlers. `Tables` exposes only catalog management; table operations are
   performed through `TableHandle` obtained from `Tables::open`.
8. `TablesCore` (the renamed `Catalog` minus state methods) is the only
   workspace component that opens storage Tables; `StatesCore` is the only one
   that opens storage States. `Installations` depends on handles from both.
9. `Workspace` exposes `installations()`, `tables()`, and `states()` accessors and
   hosts no table or state management methods itself.

## 18. Completion Criteria

Iteration 0002 is complete when:

- Path is Vec<Segment> and no PathStep remains;
- all operation creation/update code uses Op::Upsert;
- storage and types share one utility/serialization implementation;
- Blob uses `serialize_to_vec` and `deserialize_from` without a custom codec
  error;
- PeerId and WorkspaceId remain distinct and use the same libp2p-derived
  identity generation and durable encoding;
- Roles are inert identity data and workspace authorization implements the
  Contributor/Operator/Dispatcher rules;
- workspace has no format-version, generic name-validation, migration, or
  reconciliation machinery;
- one Catalog-owned _peer_state State owns receipts and optional local clock state;
- TablesCore is the only workspace module that opens storage Tables and
  StatesCore is the only module that opens storage States; Installations uses
  core-provided handles;
- the public API is split into `Installations`, `Tables`, and `States` handlers;
  `Tables` exposes only catalog management and table operations are performed
  through `TableHandle` obtained from `Tables::open`;
- read paths use lock-free snapshots while writes are flushed through one peer
  state authority;
- minting is a simple function with a documented sequence durability policy;
- Table handles use ownership rather than liveness invalidation;
- TableReadGuard exposes the real Table ReadBackend and OrderedReadBackend
  methods through a read lock without per-method runtime proxies;
- deletion returns ResourceBusy while handles remain and cleans a released
  directory before same-name recreation;
- consumers stream changes one at a time and can acquire a direct Table read
  guard without intermediate collection;
- backend/table iteration has no hidden full-table materialization in the
  streaming path;
- workspace compiles and documentation reflects the resulting API.
