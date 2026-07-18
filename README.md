# ZenDB

ZenDB is an embedded, local-first CRDT workspace for client applications. A
workspace may stay entirely local or replicate directly among admitted devices
without a central database authority.

## Current Model

- `Workspace` is the concrete lifecycle and authorization root.
- A generated, persisted `DeviceId` is the only database identity.
- Every admitted device receives all shared data and is an implicit Reader.
- `Contributor`, `Dispatcher`, and `Manager` are fixed, workspace-wide roles.
- Tables and nested Cells can be local on one device or inherit replication.
- Local and shared values occupy one materialized Cell tree, not an overlay.
- Shared events are signed and identified by per-device origin sequences.
- Exact event ranges are the normal anti-entropy path; canonical table Merkle
  roots and a durable repair marker trigger verified snapshot state repair.
- OAuth, accounts, subscriptions, and hosted authorization stay in the
  application layer.

ZenDB is not SQL, consensus, a server control plane, or a source of
linearizable locks.

## Crates

| Crate | Owns |
|---|---|
| `zendb-types` | CRDT values, IDs, device records, roles, frontiers, and wire-safe records |
| `zendb-storage` | Generic B+ tree, KeyDir, SkipList, State, and Topic storage |
| `zendb-replication` | Replication-aware Table materialization, shared journal, and sync protocol records |
| `zendb-transport` | Durable device keys, carrier-neutral secure sessions, TCP/LAN mechanics, enrollment, and presence |
| `zendb-engine` | Concrete Workspace policy, system tables, onboarding, sync orchestration, and cluster runtime |
| `zendb-operator` | Optional local operator host, state, timers, Rhai, and native operators |
| `zendb-testing` | Integration fixtures and example operators |

The former `zendb-identity` crate is gone. Application users and OAuth
principals are not replicated authorization subjects.

## Devices And Roles

`_devices[device_id]` is the membership record. A live row admits the device;
its Cell tombstone removes it. The record contains its display name, two-key
rotation ring, fixed role set, advertised capability labels, and replicated
frontier checkpoint.

- `Contributor` mutates shared data and table declarations.
- `Dispatcher` is reserved for distributed operator specifications.
- `Manager` admits/removes devices, changes role sets, renames devices, and
  manages enrollment tickets.

Roles are non-overlapping. The creator receives all three; a new device starts
with none and is read-only. A device updates its own name, capabilities, key
ring, and frontier. A Manager cannot forge another device's cryptographic or
runtime-owned fields.

Capabilities such as `gpu` or `vpn` are scheduling labels, not permissions or
callable functions.

## Table API

`_catalog` is itself a Table. Each row stores a bincode `Blob<TableConfig>`;
the catalog row's local `SyncPolicy` is the table-wide replication boundary.
`_devices` and `_enrollment_tickets` are cataloged system tables using the same
Table storage and replication path with stricter engine validators.

```rust
use zendb_engine::{TableConfig, Workspace};
use zendb_types::{PrimaryKey, Value};

let drafts = workspace
    .table("drafts")
    .config(TableConfig::default())
    .local()
    .create()?;

let documents = workspace
    .table("documents")
    .config(TableConfig::default())
    .shared()
    .create()?;

workspace
    .table("documents")
    .row(PrimaryKey::String("doc-1".into()))
    .replace(Value::String("hello".into()))?;
```

The same row/path API routes local mutations without a shared identity and
shared mutations through authorization, signing, durable journal append, and
CRDT application. `Table` implements `ReadBackend` and `OrderedReadBackend`, so
callers can use lookup, iteration, endpoint, reverse, and range reads directly
through a table guard. It intentionally does not expose raw backend mutation.

The catalog config is the cross-device construction default. The effective
physical config chosen on one device is persisted in `table.config`; changing
it later requires explicit migration.

## Local Boundaries

Every Cell carries replica-local metadata:

```rust
pub enum SyncPolicy {
    Inherit,
    Local,
}
```

`Local` prevents local publication and remote materialization at that subtree.
It is durable but excluded from replicated projections, signatures, snapshots,
and Merkle roots. A remote ancestor replacement cannot erase a local child.

Returning a path or table to `Inherit` publishes its current projected CRDT
state with the original data clocks and records durable reconciliation debt.
The next successful peer sync installs current shared state and clears the
debt. LWW and type-specific merge rules, not the toggle time, select winners.
Publishing local state requires `Contributor`.

## Onboarding

There are two database protocols:

1. A Manager creates an `_enrollment_tickets` row and a QR/link presentation
   containing the private ticket credential. The candidate binds its DeviceId,
   public key, name, and capabilities into a possession proof. Any admitted
   peer can validate and relay the narrowly authorized admission.
2. A Manager directly creates a known candidate's `_devices` row. The
   candidate later bootstraps from any peer while pinning the expected peer
   public key.

Both paths transfer a manifest-verified chunked snapshot and then continue
normal anti-entropy. Discovery and connectivity never imply admission.

## Networking And Progress

`sync_tcp()` creates a mutually authenticated encrypted session using signed
ephemeral X25519 handshakes and ChaCha20-Poly1305 framing. Peers exchange
signed presence, contiguous frontiers, exact missing event ranges, table
Merkle roots, and snapshot state when history or current materialization needs
repair.

`start_cluster()` adds a TCP listener, reconnect loop, signed UDP LAN
announcements, frontier checkpoints, heartbeats, and best-effort departure
notices. Presence is local soft state and never changes membership.

Only shared events consume `EventIdentity { origin_device_id, origin_seq }`.
The stable frontier is the per-origin minimum checkpoint across every admitted
device. Offline members hold retention and key-rotation barriers until a
Manager removes them.

## Key Rotation And Compaction

DeviceId is independent of signing keys. Rotation stages a secondary key,
waits for the stable frontier to cover the stage event, then promotes it and
retains the old key only for earlier event sequences.

Snapshots and physical storage compaction are implemented. Irreversible CRDT
tombstone and shared-journal pruning are deliberately disabled: a scalar HLC
watermark cannot safely distinguish a never-existing value from a compacted
deletion when old local branches may return. ADR 007 tracks the required
pruning-context design.

## Operators

The native local operator runtime is available through
`zendb_operator::OperatorHost<D>`, which wraps a concrete `Arc<Workspace>`.
Core replication does not depend on an executor, Rhai, or an operator set.
Distributed
declarative placement, leases, fencing, and capability scheduling are proposed
in ADR 008 and are not represented as completed functionality.

## Documentation

The concise source of truth is [`.plan/README.md`](.plan/README.md). Narrow
decisions live in [`.plan/decisions`](.plan/decisions/README.md), and only open
work appears in [`.plan/roadmap.md`](.plan/roadmap.md).

## Verification

```bash
cargo check --workspace
cargo test --workspace
```
