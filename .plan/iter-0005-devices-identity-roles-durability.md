# Iteration 0005: Devices, Identity, Roles, And Durability

Status: implemented.

This document consolidates the original iterations 0005 (Devices Lifecycle
And API), 0006 (Workspace Identity And Progressive Roles), and 0007 (Device
Cache And Workspace Durability). Later iterations supersede earlier decisions
where they conflict:

- iteration 0006 supersedes the PeerId-backed `WorkspaceId` decision from
  iteration 0003 and the initial local `Operator` assignment from iteration
  0005;
- iteration 0007 supersedes iteration 0005's decision to keep durability
  coordination outside the workspace and iteration 0006's `flush`/`sync`
  non-goal.

The public `Tables`, `TableHandle`, `States`, and `StateHandle` APIs stay
lean throughout.

## 1. Device Ownership And Lifecycle

`Devices` owns the device registry, local hybrid clock, receipt windows,
authorization cache, and the `_peers` handle. The separate `PeerStore`
layer was removed because it shared the lifetime and ownership boundary of
`Devices`. `devices/receipts.rs` stays separate because receipt-window
maintenance is an independent algorithm; `devices/mod.rs` only declares
modules and exports the public device types plus the crate-internal
`PeerState`.

`Devices::create/open` construct the runtime directly. Create receives the raw
device registry Table, inserts the initial local Admin event before wrapping
the Table in a handle, and initializes both caches. Open loads durable records
and validates local membership before returning. `Tables::create/open` owns
the surrounding table-runtime initialization, while `Workspace::assemble`
only composes the state and table handlers.

The create path inserts the creator as an `Admin` exactly once with event
sequence `1`. The initial `PeerState` records receipt `1` and owns an
`EventClock` whose `next_sequence` is `2`. The open path validates the local
registry entry inline alongside the local peer-state checks; opening a
workspace never silently promotes an unknown peer. A future join/enrollment
flow must explicitly establish both the peer's `_devices` record and its
`_peers` clock record before that peer can open the workspace.

The public `Devices` surface is the registry facade: `local_peer_id`, `list`,
`get`, `upsert`. Clock, receipt, authorization, durability, and
initialization operations are crate-internal. `Devices::upsert` returns
`Result<bool>` like the state and table catalogs; unchanged records do not
mint or publish an event.

Single-use routing helpers are removed; event construction, wall-clock error
mapping, record loading, and HLC observation remain inline at their only call
sites.

## 2. Workspace And Peer Identity

`zendb-types::identity` keeps network peer definitions in `peer.rs` and the
independent workspace identifier in `workspace.rs`. The portable types crate
exports the `PeerIdentity` abstraction but no concrete local key owner;
consumers provide implementations appropriate to their key storage.

### 2.1 WorkspaceId

`WorkspaceId` identifies one workspace. It is not a network peer, does not own
a signing key, and must not use the libp2p PeerId representation. It is a
fixed-size domain type:

```rust
pub struct WorkspaceId([u8; 16]);
```

`WorkspaceId::generate` fills all 128 bits from the operating system's
cryptographic random source. Its binary encoding is the fixed 16-byte array,
without a string or libp2p serialization envelope. The canonical text
representation is 26-character Crockford Base32 (uppercase; parsing is
case-insensitive, accepts `O` as `0` and `I`/`L` as `1`, and rejects
malformed length and non-canonical overflow bits). A workspace identifier is
an identifier, not a secret or authorization token.

`WorkspaceId` provides `generate`, `from_bytes`, `as_bytes`, `to_bytes`, and
implements `Copy`, `Clone`, `Eq`, `Ord`, `Hash`, `Display`, `Debug`,
`FromStr`, `Encode`, and `Decode`. It does not implement `Default`.

The `_identity` file continues to contain one encoded `WorkspaceId`.
`Workspace::create`, `open`, `join`, and `id` retain their current method
shapes. No on-disk migration is required.

### 2.2 PeerId

The ZenDB `PeerId` wrapper around `libp2p_identity::PeerId` remains, owning
the native bincode `Encode`/`Decode` implementation and stable canonical byte
encoding. Network code converts through the existing `From`, `Into`, and
`as_libp2p` APIs. It mirrors the useful public construction and representation
methods (`from_public_key`, `from_bytes`, `random`, `to_bytes`,
`to_base58`). The former `PeerId::generate` is removed in favor of
`libp2p_identity::PeerId::random`. `from_multihash` is intentionally not
mirrored.

`PrimaryKey::PeerId` remains a key-only scalar; it does not implement the CRDT
`Type` trait and is not converted to `PrimaryKey::Blob`. One device registry
entry remains one-to-one with one PeerId.

## 3. Progressive Role Model And Authorization

The plural role set becomes one optional progressive role:

```rust
pub enum Role { Contributor, Operator, Admin }

pub struct DeviceRecord {
    pub display_name: String,
    pub role: Option<Role>,
}
```

`None` means Reader. `Role` owns the hierarchy explicitly via
`has_at_least`, rather than inferring it from enum discriminants. `Dispatcher`
is removed; `Operator` is reserved for future dispatch/operation access while
currently inheriting Contributor behavior.

### 3.1 Access Matrix

| Operation | Reader | Contributor | Operator | Admin |
| --- | --- | --- | --- | --- |
| Read tables | yes | yes | yes | yes |
| Write application tables | no | yes | yes | yes |
| Write application states | yes | yes | yes | yes |
| Directly write system table handles | no | no | no | no |
| Create, update, or delete application table declarations | no | no | no | yes |
| Add or update device records and roles | no | no | no | yes |
| Future dispatch operations | no | no | yes | yes |

States are local and do not perform role authorization. The existing
`StateHandle` system-state write guard remains unchanged.

### 3.2 Devices Authorization

`Devices` owns one read-oriented
`RwLock<RegistryCache { entries, local_role }>` for device metadata and
authorization. Keeping the local role beside the registry entries lets
`DeviceRegistryListener` update both atomically while avoiding an entry lookup
on every local table insert. The public
`has_access(&self, peer_id, required)` checks any peer. The crate-internal
`require_access(peer_id, required)` returns `Result<()>`; `PermissionDenied`
remains the public failure and discloses no role details.

`Devices::upsert` requires the current peer to be Admin before checking or
publishing a device-record mutation; the Contributor self-metadata exception
is removed. An unchanged upsert returns `false` without minting an event; a
changed upsert authorizes before minting.

### 3.3 Table Authorization

`TableHandle::insert` retains its signature and checks in order: refuse every
system table (`SystemTableReadOnly`), upgrade the weak `Devices` reference,
require at least Contributor access, mint the event stamp, then delegate to
`insert_internal`. The pre-mint check ensures a denied write does not consume
a local sequence number.

`TableHandle::insert_internal` independently checks the actor identified by
`event.stamp.id.peer_id`: application tables require Contributor, system
tables require Admin. Both checks are intentionally present — the public check
protects the local sequence before minting; the internal check is the final
guard for a future admitted replicated event. Shared storage insertion and
listener dispatch remain private implementation details of authorized handle
paths.

`Tables::upsert` and `Tables::delete` retain their signatures and require
Admin before minting catalog events. System tables are returned by `get` and
`list`, system handles reject direct `insert`, `_catalog` and `_devices`
cannot be upserted or deleted as declarations, and outstanding application
handles protect deletion. Reads, `contains`, `list`, and `get` remain
unguarded.

### 3.4 Create And Open Ordering

`Tables::create` uses this order:

1. Receive the peer-state handle from `States` and create the raw device Table.
2. Let `Devices::create` insert the creator's Admin record directly as event
   sequence `1`, seed the registry cache, initialize the local receipt at `1`,
   set `next_sequence` to `2`, and wrap the registry in `TableHandle`.
3. Create the catalog handle and the `Tables` ownership graph.
4. Register receipt, catalog, and device listeners.
5. Write the `_catalog` and `_devices` self-referencing catalog entries through
   the normal Admin-authorized internal insertion path.
6. Return the assembled `Tables`.

There is no bootstrap method on `TableHandle`: only `Devices::create` can reach
the raw registry Table. `Devices::open` loads device records and the cached
local role and validates that the current peer exists before `Tables::open`
continues; it does not create, promote, or repair roles.

### 3.5 Replication Boundary

The internal actor-role check is necessary but not a complete remote-event
admission protocol. `EventStamp.peer_id` identifies a claimed actor but does
not authenticate it, and role checks against arrival-time registry state can
become order-dependent. Future replication must authenticate the event or
envelope signature, establish deterministic authorization semantics for the
relevant role version or causal state, validate system-table-specific
invariants, and call `insert_internal` only after admission. `insert_internal`
remains the final table-level authorization check, not the network trust
boundary.

## 4. Cached Peer State And Durability

`Devices` keeps registry data and event bookkeeping in deliberately separate
caches. `registry_cache` is read-oriented and contains device entries plus the
local authorization value. The mutex-protected `peer_cache` is write-oriented
and contains the local `PeerState`, `others: BTreeMap<PeerId, PeerState>`, a
local dirty flag, and `dirty_others`. `PeerState` contains a receipt window and
an optional `EventClock { next_sequence, last_time }`. Keeping the local state
separate from `others` means minting does not clone or search the full peer
set.

`mint` serializes only the small cache update, advances the sequence and HLC
together, marks the local record dirty, and returns. It does not acquire the
state write lock or perform storage I/O. Independent atomics are not used for
the sequence and HLC because a stamp is one coherent checkpoint; separate
atomics would require an additional snapshot protocol to persist those fields
consistently. Receipt observation updates the same cache and marks the
affected non-local state and local clock state dirty. Dirty peer states are
written back to `_peers` only at a durability barrier.

`Devices`, `Tables`, `States`, and `Workspace` expose:

```rust
pub fn flush(&self) -> Result<()>;
pub fn sync(&self) -> Result<()>;
```

`flush` moves pending in-memory changes into the underlying storage writeback
path and flushes buffered OS writes; `sync` performs the same writeback and
requests the underlying durable sync operation. `Devices` writes dirty peer
states to `_peers` then flushes or syncs that state; `Tables` snapshots
its handle map and flushes or syncs each table (the catalog and device
registry are already entries); `States` snapshots its open handle map and
flushes or syncs each state (the catalog and opened peer state are already
entries; declared but unopened states have no runtime cache). `Workspace`
calls `Devices` first, then `Tables`, then `States`. `Workspace` performs that
coordinated flush from `Drop`, while `Devices` also flushes its separate peer
cache from its own `Drop`. `Tables` and `States` need no custom `Drop` because
their underlying storage `Table` and `State` values already flush when their
final owners are dropped. Drop-time errors are ignored; callers needing error
reporting or a durability guarantee must call `flush` or `sync` explicitly.

The open-state map needs both runtime type recovery and durability dispatch, so
its erased value becomes a private trait object implemented by every typed
`StateHandle`, providing type erasure plus `flush` and `sync`; the public
`StateHandle` API is unchanged.

## 5. Non-Goals

These iterations do not:

- replace the ZenDB PeerId wrapper with the external libp2p type;
- expose multihash construction through the ZenDB PeerId wrapper;
- convert PeerId primary keys to Blob or implement the CRDT `Type` trait for
  PeerId;
- add connection hints, peer discovery, transport, or replication;
- add dispatch operations;
- add role checks to States or change State APIs;
- allow direct system-table writes through `TableHandle`;
- add on-disk migration compatibility.

## 6. Completion

The consolidated iteration is complete when:

- `PeerStore` and `devices/peer.rs` are removed;
- `WorkspaceId` is independent of libp2p and round-trips through fixed binary
  and canonical Crockford Base32 forms;
- `PeerId` mirrors the selected libp2p constructors and representation methods,
  uses `random` instead of `generate`, and does not expose `from_multihash`;
- `Role` replaces the role set and exposes `has_at_least`;
- the creator is bootstrapped as Admin;
- `Devices::has_access` checks any peer and uses the local cache when
  applicable;
- application table writes require Contributor before minting and internally;
- table and device catalog mutations require Admin before minting and
  internally;
- minting performs no peer-map clone or state write;
- all four owners expose `flush` and `sync`; `Workspace` and `Devices` flush
  their orchestration caches from `Drop`, while storage values handle their own
  final-owner flush;
- system table and state handle guards remain unchanged;
- no public Tables, TableHandle, States, or StateHandle method signature is
  expanded or wrapped;
- `cargo check --workspace --all-targets` succeeds;
- workspace and crate documentation describe the identity, role, cache, and
  durability model.
