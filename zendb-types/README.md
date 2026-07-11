# zendb-types

**Pure shared data model for ZeninDB — CRDT types, HLC clocks, identity,
authorization vocabulary, and declarative operator objects. No storage,
network sessions, or hosted-service implementations.**

---

## Crate Role

`zendb-types` is the foundation. It defines every data structure needed for
collaborative conflict resolution and shared control state and sits at the
bottom of the dependency graph. It contains no policy lookup, storage, network
session, OIDC, or operator execution implementation.

---

## Core Types

### `Hlc` — Hybrid Logical Clock

24-byte lexicographically ordered timestamp:

```text
Bytes 0–5:  physical_ms  (48-bit big-endian, ms since epoch)
Bytes 6–7:  logical      (16-bit big-endian, monotonic counter)
Bytes 8–23: device_id    (128-bit, persisted installation identity)
```

`Hlc::ZERO` is the all-zero sentinel — any real clock beats it. Clock ordering
is: physical → logical → device ID.

`DeviceId` is generated from the operating system CSPRNG, persisted by the
replica, and bound to its public key. It is not derived from a machine UID.

### `Cell`

The universal addressable state unit:

```rust
pub struct Cell {
    pub value: Option<Value>,   // None = tombstone
    pub hlc: Hlc,               // structural clock
    pub sync: Option<bool>,     // per-cell sync policy override
}
```

- `value = Some(v)` — cell is live
- `value = None` — cell is a tombstone (preserved for CRDT correctness)
- `sync = None` — inherit from ancestor or database default
- `sync = Some(true/false)` — explicit sync policy for this subtree

Key methods: `apply_event`, `merge`, `compact`, `max_hlc`, `ensure_type`,
`is_dummy`, `is_tombstone`.

### `Event`

The unit of mutation — self-contained and replication-ready:

```rust
pub struct Event {
    pub table_id: TableId,
    pub primary_key: PrimaryKey,
    pub path: Path,
    pub op: Op,
    pub hlc: Hlc,
    pub sync: bool,
    pub signature: Signature,
}
```

### Shared control objects

The `control` module contains pure, serializable vocabulary used by the
client-side database control plane:

- authorization contexts, decisions, rules, resources, and obligations;
- operator desired state (`OperatorSpec`) and local observations;
- placement, capability requests, output policy, retry policy, and approvals;
- fenced leases, checkpoints, jobs, and job results.

These objects are database truth when replicated. They do not imply that a
server scheduler exists; the embedded engine reconciles them on each device.

### `Path` / `PathStep`

```rust
pub struct PathStep {
    pub container_tag: TypeTag,  // expected container type at this depth
    pub segment: Segment,        // how to descend (field name, list id, etc.)
}

pub type Path = Vec<PathStep>;
```

An empty path addresses the row root. Each step carries type information so
the apply walk can self-heal: if an intermediate container is missing, it can
be created from the expected type.

### `Change`

Before/after snapshot produced by applying an event:

```rust
pub struct Change {
    pub event: Event,
    pub previous: Option<Cell>,
    pub current: Option<Cell>,
}
```

---

## CRDT Type System

### The `Type` Trait

```rust
pub trait Type: Sized + Encode + Decode<()> {
    type Op: Encode + Decode<()>;
    type Error: std::error::Error;

    fn apply(&mut self, op: &Self::Op, op_hlc: Hlc) -> Result<bool, Self::Error>;
    fn merge(&mut self, remote: &Self, clocks: MergeClocks) -> Result<bool, Self::Error>;
    fn is_synced(&self, inherited: bool, path: &[PathStep]) -> bool;
    fn compact(&mut self, watermark: Hlc) -> Result<bool, Self::Error>;
    fn max_hlc(&self) -> Hlc;
}
```

`ContainerType` extends `Type` with `apply_walk` for recursive path traversal.

### Built-in Types

| Type | Kind | Semantics |
|---|---|---|
| `Bool` | Scalar | LWW merge |
| `Int` | Scalar | LWW merge (`i64`) |
| `String` | Scalar | LWW merge |
| `Timestamp` | Scalar | LWW merge (`u64`) |
| `Blob` | Scalar | LWW merge (`Vec<u8>`) |
| `Record` | Container | Named-field map, recursive field-wise merge |
| `Set` | CRDT | LWW per-element add/remove clocks |
| `OrSet` | CRDT | Observed-remove set (additive-wins) |
| `Counter` | CRDT | PN-Counter with per-device accumulators |
| `MvRegister` | CRDT | Multi-value register |
| `List` | Container CRDT | RGA ordered list with stable element IDs |
| `Text` | CRDT | RGA collaborative text with per-character formatting |
| `PriorityQueue` | CRDT | Replicated min-heap |

---

## `register_types!` Macro

A `macro_rules!` invocation that generates the type dispatch layer:

```rust
register_types! {
    leaf Bool => crate::crdt::values::bool::Bool,
    leaf Int => crate::crdt::values::int::Int,
    leaf String => crate::crdt::values::string::String,
    leaf Timestamp => crate::crdt::values::timestamp::Timestamp,
    leaf Blob => crate::crdt::values::blob::Blob,
    leaf Counter => crate::crdt::values::counter::Counter,
    leaf MvRegister => crate::crdt::values::mv_register::MvRegister,
    leaf OrSet => crate::crdt::values::or_set::OrSet,
    leaf Set => crate::crdt::values::set::Set,
    leaf PriorityQueue => crate::crdt::values::priority_queue::PriorityQueue,
    leaf Text => crate::crdt::values::text::Text,
    container Record(RecordSegment) => crate::crdt::values::record::Record,
    container List(ListSegment) => crate::crdt::values::list::List,
}
```

This generates: `TypeTag`, `PrimaryKey`, `Value`, `TypeOp`, `Segment`,
`TypeError`, and all dispatch implementations. Adding a new type requires
one module, 2–3 trait impls, and one line in the macro invocation.

---

## Cell-Level Operations (`Op`)

```rust
pub enum Op {
    Type(TypeOp),                     // type-specific operation
    SetSync { sync: Option<bool> },   // set/clear sync override
    Delete,                           // tombstone the cell
    Replace { value: Value },         // replace entire cell value
    Merge { cell: Cell },             // merge remote cell state
}
```

---

## Merge Semantics

- **Same type:** delegate to `Type::merge` (type-specific CRDT logic)
- **Different type:** LWW by Cell HLC (newer clock wins)
- **Live vs tombstone:** LWW by Cell HLC
- **Sync metadata:** local `sync` is preserved; `SetSync` is always local-only

---

## Module Structure

```
src/
├── lib.rs              # register_types! macro, invocation, re-exports
├── crdt/
│   ├── cell.rs         # Cell struct + Type/ContainerType impls
│   ├── change.rs       # Change struct
│   ├── event.rs        # Event, TableId, Signature
│   ├── hlc.rs          # Hlc and canonical DeviceId integration
│   ├── op.rs           # Op enum (cell-level operations)
│   ├── path.rs         # PathStep, Path
│   ├── replication.rs  # Event identity, envelopes, version vectors
│   └── values/
    ├── mod.rs
    ├── blob.rs         # Blob (scalar)
    ├── bool.rs         # Bool (scalar)
    ├── counter.rs      # Counter (PN-Counter)
    ├── int.rs          # Int (scalar)
    ├── list.rs         # List (RGA container)
    ├── mv_register.rs  # MvRegister (multi-value register)
    ├── or_set.rs       # OrSet (observed-remove set)
    ├── priority_queue.rs # PriorityQueue (replicated min-heap)
    ├── record.rs       # Record (named-field container)
    ├── set.rs          # Set (LWW set)
    ├── string.rs       # String (scalar)
    ├── text.rs         # Text (RGA collaborative text)
    └── timestamp.rs    # Timestamp (scalar)
├── identity/
│   ├── ids.rs          # DeviceId and other shared identifiers
│   ├── principal.rs    # User/device/guest/service/operator principals
│   └── membership.rs   # Workspace and device membership records
└── control/
    ├── authorization.rs # Policy vocabulary and evaluator interface
    ├── capability.rs    # Capability invocation/result records
    └── operator.rs      # Desired operators, leases, jobs, checkpoints
```
