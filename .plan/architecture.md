# Current Architecture

## Dependency Direction

```text
zendb-types
  CRDTs, IDs, device records, fixed roles, frontiers, protocol data

zendb-storage
  generic State, Topic, BPlusTree, KeyDir, SkipList

zendb-replication
  one-tree Table materialization, local topics, Merkle roots, shared journal,
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

Shared mutation ordering is:

```text
resolve catalog and Cell SyncPolicy
authorize the author Device
allocate HLC and per-device origin sequence
hash and sign the envelope
durably append SharedJournal
apply through system/data validation
update one Table Cell tree
append the local Table Change topic
advance the contiguous frontier
```

Local mutations use the same Table materialization/topic path but skip role
validation, envelope creation, shared journal append, and origin sequence.

## Table And Reads

`zendb-replication::Table` owns:

```text
one State<PrimaryKey, Cell>
one write-back cache
one durable Topic<Change>
one recovery consumer
one persisted physical TableConfig
```

Table implements read-only storage contracts, including range and reverse
reads. It does not implement raw mutable `WriteBackend`. Mutations enter through the
Workspace fluent row/path API or narrowly classified Table methods.

The catalog config is the default recipe sent to a new device. The local
physical config is persisted beside the table and cannot silently change on
reopen.

## Catalog And System Tables

`_catalog[table_name]` is a `Cell<Record>` containing:

```text
config: Cell<Blob<TableConfig>>
```

The catalog row's local SyncPolicy is the table-wide boundary. `_catalog`
contains itself, `_devices`, `_enrollment_tickets`, and application tables.
Only `_catalog` has a fixed bootstrap path/config convention.

System tables use ordinary Table storage, topics, snapshot projection, and
replication. Engine validators add field ownership and exceptional ticket
admission rules. There is no monolithic `_control` value or `_tables` KeyDir.

## Local Boundaries

`SyncPolicy::Inherit` follows the parent/table policy. `SyncPolicy::Local`
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
3. canonical Merkle roots for wholly shared open tables;
4. a verified snapshot when history is missing, comparable roots differ, or a
   promoted local branch requests current state.

A table with any local boundary advertises no comparable root. This avoids
false perpetual mismatches. The durable repair marker still guarantees a
one-time state pull after promotion.

## Retention

Stable receipt is the element-wise minimum frontier checkpoint across all
admitted devices. It gates key rotation and informs retention. It is not enough
to erase delete meaning when a previously local branch can return.

Snapshots and physical-file compaction are allowed. CRDT tombstone and shared
journal pruning remain disabled pending ADR 007.

## Operators

Current operators are local streaming workers over Table topics. Proposed
distributed operators will add cataloged desired state, concrete local
reconcilers, capability placement, soft leases, and fenced output. They do not
receive a separate principal; they act with the hosting Device's roles.
