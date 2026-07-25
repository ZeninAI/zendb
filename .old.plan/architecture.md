# Current Architecture

## Dependency Direction

```text
zendb-types
  CRDTs, IDs, device records, fixed roles, frontiers, protocol data

zendb-storage
  generic State, Topic, BPlusTree, KeyDir, SkipList

zendb-replication
  one-tree Table materialization, local topics, sparse Merkle index,
  per-origin journal,
  range/summary/snapshot protocol records

zendb-transport
  device profile, key possession, secure TCP, enrollment, discovery, presence

zendb-engine
  concrete Workspace lifecycle, catalog/system policy, onboarding, network
  orchestration

zendb-operator
  optional local operator host, typed State, timers, executor, native/Rhai
  operators
```

Dependencies point upward from portable mechanics to product coordination.
`Workspace` is concrete glue, not a trait or a container of one-implementation
service traits.

## Workspace

`Workspace` owns one local replica:

```text
WorkspaceId + DeviceProfile
SystemTables
Table registry
SharedJournal
Cluster/presence runtime
```

`OperatorHost<D>` optionally wraps `Arc<Workspace>` and owns local operator,
State, timer, and executor resources. The engine does not depend on it.

Ordinary application mutation ordering is:

```text
TableHandle validates its captured catalog generation
resolve Cell SyncPolicy and cached Contributor capability
allocate HLC
append the local Table Change topic
update one Table Cell tree
```

The built-in maintenance projector independently drains table topics in
batches. Publication intent and shared projections are captured in each Change,
so later policy edits cannot reinterpret history. A batch signs and appends to
the local origin Topic, syncs once, checkpoints its sequence range once, and
advances the already-materialized frontier. Received and local-only changes are
never republished.

## Table And Reads

`zendb-replication::Table` owns:

```text
one State<PrimaryKey, Cell>
one write-back cache
one durable Topic<Change>
one recovery consumer
one in-memory physical TableConfig supplied by Workspace
```

Table implements read-only storage contracts, including range and reverse
reads. It does not implement raw mutable `WriteBackend`. Mutations enter through
generation-bound `TableHandle` event methods. Ordered state
and cache streams are merged without collecting; unordered storage allocates
only when an ordered operation requires sorting.

The catalog is the sole physical configuration authority. Configuration is
decoded when Workspace materializes a table. Catalog updates leave an existing
materialization unchanged and take effect at the next Workspace initialization;
the storage backend then validates the existing files.

## Catalog And System Tables

`_catalog[table_name]` is a replicated `Cell<CatalogEntry>` containing a stable
creation generation and nested `TableConfig`:

```text
generation: Cell<Hlc>
config: Cell<Record {
  state: Record { kind, config: Record<backend fields> }
  max_buffered_records: Int
  topic: Record { max_segment_bytes, offsets: Record<KeyDirConfig> }
}>
```

Every live application catalog row is materialized. There is no parked/open
split and no table-level sync policy. Catalog rows and their configuration are
always replicated; local policy applies only to application data Cells.
Physical application tables are scoped by generation, so recreating a name
cannot reopen old data while stale handles remain alive.
`_catalog` contains itself, `_devices`, `_enrollment_tickets`, and application
tables. Only `_catalog` has a fixed bootstrap path/config convention.

System tables use ordinary Table storage, topics, snapshot projection, and
replication. Engine validators add field ownership and exceptional ticket
admission rules. There is no monolithic `_control` value or `_tables` KeyDir.

## Local Boundaries

`SyncPolicy::Inherit` follows the parent policy. A row root inherits from a
shared table boundary, so new rows are shared by default. `SyncPolicy::Local`
protects the subtree. There is no child override that escapes a local ancestor.

Policy is persisted with local Cells but removed by shared-state projection.
Remote path events below a local boundary are durably received and skipped.
Policy-aware snapshot merge updates shared siblings without touching local
children. List projections retain zero-clock structural anchors when needed.

`Local -> Inherit` publishes projected CRDT state with original clocks and
sets a durable workspace repair marker. A successful verified state install
clears it. This first implementation repairs using a full shared snapshot;
targeted Merkle node exchange is a future bandwidth optimization.

## Anti-Entropy

Peers exchange:

1. authenticated presence and contiguous frontiers;
2. exact missing per-origin event ranges;
3. fresh materialized Merkle roots of every catalog table's shared projection;
4. a verified snapshot when history is missing, comparable roots differ, or a
   promoted local branch requests current state.

Merkle projectors consume Table Changes with durable cursors and update sparse
per-key, leaf, and node state idempotently. A stale root is omitted rather than
advertised. Local subtrees are omitted from the canonical projection. The durable repair
marker guarantees a state pull after a path returns to inherited sharing.

## Retention

Stable receipt is the element-wise minimum frontier checkpoint across all
admitted devices. It gates key rotation and informs retention. It is not enough
to erase delete meaning when a previously local branch can return.

Snapshots and physical-file compaction are allowed. Per-origin journal segments
are pruned only behind both a durable snapshot and the stable frontier, with an
HLC checkpoint retained. CRDT tombstone pruning remains disabled pending ADR 007.

## Operators

Current operators are local streaming workers over Table topics. Proposed
distributed operators will add cataloged desired state, concrete local
reconcilers, capability placement, soft leases, and fenced output. They do not
receive a separate principal; they act with the hosting Device's roles.
