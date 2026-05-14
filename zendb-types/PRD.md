# zendb-types — Product Requirement Document

This document is the canonical specification for the `zendb-types` crate. It defines every type, trait, algorithm, and encoding rule that the crate must implement. No other crate in the ZeninDB workspace should define these primitives.

---

## 1. Crate Purpose & Scope

### 1.1 What this crate owns

`zendb-types` is the foundation of ZeninDB. It defines:

| Component            | Description                                              |
| -------------------- | -------------------------------------------------------- |
| HLC                  | Hybrid Logical Clock — 12-byte causal timestamp          |
| Cell                 | Universal addressable value wrapper                      |
| TypeTag              | Discriminant for runtime type dispatch                   |
| Type trait           | Contract every data type must implement                  |
| ContainerType trait  | Extension for types that have child cells                |
| Value                | Generated enum — one variant per registered type         |
| Op                   | Generated enum — one variant per type's operation set    |
| Segment              | Generated enum — one variant per container type's selector |
| Path / PathStep      | Recursive addressing into the cell tree                  |
| Delta                | The unit of mutation — what a write produces             |
| Apply walk           | Recursive algorithm that applies an Op through a Path    |
| Merge                | Type-directed algorithm that converges two Cells         |
| Encoding / Decoding  | Binary wire format for every type above                  |
| Error types          | Unified error enum for all type-system operations        |

### 1.2 What this crate does NOT own

- **Networking** — no sockets, no peer management, no transport
- **Storage I/O** — no filesystem access, no WAL, no compaction
- **Replication protocols** — no live sync, no anti-entropy negotiation (though Delta and StateChunk *structures* live here)
- **Signature verification** — Delta carries a raw signature byte array; verification belongs to `zendb-replication`
- **User-facing database API** — that is `zendb-engine`
- **The `register_types!` macro** — that is `zendb-types-macros` (initially we hand-write the generated code in `zendb-types`)

### 1.3 Design principles

1. **Zero external I/O.** Every function is pure — takes data, returns data.
2. **No allocation where avoidable.** HLC is inline 12 bytes. PathSteps borrow Segments where possible. But Value/Op/Segment enums necessarily allocate for heap-sized variants.
3. **Exhaustive everywhere.** Match on Value/Op/Segment must be exhaustive so the compiler rejects any unhandled type.
4. **Deterministic.** Encoding, apply, and merge always produce the same output for the same input.
5. **Composable.** Types are registered once and dispatched through generated code. Adding a type touches one registration point plus its module.

---

## 2. HLC — Hybrid Logical Clock

### 2.1 Purpose

The HLC provides deterministic, causal ordering for every mutation in ZeninDB. It is used for:

- Last-writer-wins (LWW) comparisons
- Operation ordering in the apply walk
- Merge decisions
- Tombstone comparisons

An HLC is **not** a wall clock, a WAL cursor, or a sync policy bitfield.

### 2.2 Binary layout

12 bytes, big-endian:

```text
┌──────────────────┬──────────────┬──────────────────┐
│ Bytes 0–5        │ Bytes 6–7    │ Bytes 8–11       │
│ physical_ms      │ logical      │ node_id          │
│ 48 bits          │ 16 bits      │ 32 bits          │
│ ms since epoch   │ monotonic    │ unique node id   │
└──────────────────┴──────────────┴──────────────────┘
```

Rationale:

- Big-endian so a raw `memcmp` / `[u8]::cmp` yields correct ordering.
- 48-bit physical_ms: ~8.9 million years range. Overflow is not a practical concern.
- 16-bit logical: 65,536 events per millisecond before artificial physical bump.
- 32-bit node_id: 4.3 billion nodes. Uniqueness is the operator's responsibility.

### 2.3 Ordering

Comparison order (descending = "beats"):

```text
1. physical_ms  (higher wins)
2. logical      (higher wins)
3. node_id      (higher wins)
```

In Rust, `Hlc` implements `Ord` such that "greater" means "later / beats". The byte layout guarantees that a big-endian `[u8; 12]` memcmp matches this ordering.

### 2.4 Hlc::ZERO

The all-zero HLC (`[0u8; 12]`) is the sentinel value `Hlc::ZERO`.

Properties:

- Any real HLC beats `ZERO`.
- `ZERO` is used for dummy cells created during the apply walk.
- The generator must never produce `ZERO` as a real timestamp (physical_ms=0 is year 1970, which is valid; but `Hlc::ZERO` as all-zero means logical=0 AND node_id=0, which is invalid for a real node).

### 2.5 HLC type definition

```rust
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Hlc([u8; 12]);

impl Hlc {
    pub const ZERO: Hlc = Hlc([0u8; 12]);

    pub fn physical_ms(&self) -> u64;
    pub fn logical(&self) -> u16;
    pub fn node_id(&self) -> u32;

    /// True if self compares greater than other (self is later / beats other).
    pub fn beats(&self, other: Hlc) -> bool;

    /// Construct from components. Returns None if physical_ms exceeds 48 bits.
    pub fn new(physical_ms: u64, logical: u16, node_id: u32) -> Option<Hlc>;

    /// Raw byte access for encoding.
    pub fn as_bytes(&self) -> &[u8; 12];
    pub fn from_bytes(bytes: [u8; 12]) -> Hlc;
}
```

### 2.6 Generator

The HLC generator is a stateful component that lives in `zendb-engine` (or a shared crate), but the algorithm is specified here because it defines HLC semantics.

**State:**

```rust
struct HlcGenerator {
    node_id: u32,
    last_physical_ms: u64,
    last_logical: u16,
}
```

**Local event (new local write):**

```text
wall = current_wall_clock_ms()
physical = max(last_physical_ms, wall)

if physical == last_physical_ms:
    logical = last_logical + 1
    if logical overflowed ( > 65535 ):
        physical += 1
        logical = 0
else:
    logical = 0

last_physical_ms = physical
last_logical = logical

return Hlc::new(physical, logical, node_id)
```

**Observing a remote HLC (on receiving a delta or state chunk):**

```text
wall = current_wall_clock_ms()
physical = max(last_physical_ms, remote.physical_ms, wall)

if physical == remote.physical_ms and physical == last_physical_ms:
    logical = max(last_logical, remote.logical) + 1
    if logical overflowed:
        physical += 1
        logical = 0
elif physical == remote.physical_ms:
    logical = remote.logical + 1
    if logical overflowed:
        physical += 1
        logical = 0
else:
    logical = 0

last_physical_ms = physical
last_logical = logical
```

**Invariant:** The generator must never emit an HLC that is ≤ any previously emitted or observed HLC.

---

## 3. Cell

### 3.1 Purpose

A `Cell` is the universal wrapper around every addressable value in ZeninDB. Every field of a record, every element of a list, every value at any path — it is a Cell.

### 3.2 Structure

```rust
pub struct Cell {
    pub value: Value,
    pub hlc: Hlc,
    /// Explicit sync policy for this cell. Three states:
    /// - `Some(true)`:  sync this cell and (by default) its descendants
    /// - `Some(false)`: keep this cell local and (by default) its descendants
    /// - `None`:         inherit from the nearest ancestor that has an opinion
    pub sync: Option<bool>,
}
```

| Field   | Meaning                                                                 |
| ------- | ----------------------------------------------------------------------- |
| `value` | The typed runtime value (Atom, Record, or future types)                 |
| `hlc`   | HLC of the last operation that **directly** changed this Cell           |
| `sync`  | Explicit replication policy for this subtree, or `None` to inherit      |

### 3.3 Key semantics

**HLC does not cascade.** When a child cell is modified, the parent's HLC does not change. The parent's HLC only changes when the parent itself is the direct target of a write. For example, a `RecordOp::SetField` walks *through* the Record cell to the child field Cell — only the child Cell's HLC is bumped. The Record Cell's HLC is only bumped by `Replace` operations that target it directly.

This keeps HLC semantics simple:
```text
cell.hlc = timestamp of the last operation that targeted this exact cell as the leaf
```

**sync flag has three states.** `Cell.sync` is `Option<bool>`, not a plain `bool`. This enables selective override of the sync policy at any depth in the tree:

- `None` — "I don't set a policy here; walk up to my parent and use whatever it says."
- `Some(true)` — "I am synced. Future writes at or below me default to synced, unless a descendant explicitly overrides with `Some(false)`."
- `Some(false)` — "I am local-only. Future writes at or below me default to local-only, unless a descendant explicitly overrides with `Some(true)`."

**sync inheritance.** When a write is created without an explicit `.synced()` or `.local()` override, the engine walks **up** from the target cell toward the root, looking for the first `Some(value)`:

```text
Write targets:  row → record → field "cursor_pos"
                                  ↑ leaf

Inheritance walk (leaf to root):
  field "cursor_pos".sync → Some(false)  (found it! stop walking)
  → Delta.sync = false
```

If the walk reaches the root without finding a `Some(...)`, a database-level default is used.

```text
Write targets:  row → record → field "name"
                                  ↑ leaf

Inheritance walk:
  field "name".sync → None       (no opinion, keep walking)
  record.sync       → Some(true)  (found it!)
  → Delta.sync = true
```

**Selective override example.** A record synced with one local-only field:

```text
Record "document"    sync = Some(true)
├── field "title"    sync = None          → inherits Some(true)  → synced
├── field "body"     sync = None          → inherits Some(true)  → synced
└── field "cursor"   sync = Some(false)   → explicitly local-only
```

Or the inverse — a local record with one synced field:

```text
Record "draft"       sync = Some(false)
├── field "title"    sync = None          → inherits Some(false) → local-only
├── field "body"     sync = None          → inherits Some(false) → local-only
└── field "shared"   sync = Some(true)    → explicitly synced
```

**Delta.sync is always a plain `bool`.** The inheritance resolution from `Option<bool>` to `bool` happens at Delta creation time. Every Delta must have a definitive answer: replicate or not.

### 3.4 Dummy Cells

A dummy cell is one with `hlc = Hlc::ZERO`. Dummy cells are created during the apply walk when an operation descends through a path where an intermediate container does not yet exist. Any real write later targeting that cell will beat `ZERO` and replace the dummy.

```rust
impl Cell {
    pub fn dummy(value: Value) -> Cell {
        Cell {
            value,
            hlc: Hlc::ZERO,
            sync: None,  // inherit from parent when a real write arrives
        }
    }

    pub fn is_dummy(&self) -> bool {
        self.hlc == Hlc::ZERO
    }
}
```

### 3.5 API surface

```rust
impl Cell {
    pub fn new(value: Value, hlc: Hlc, sync: Option<bool>) -> Cell;
    pub fn dummy(value: Value) -> Cell;
    pub fn is_dummy(&self) -> bool;
    pub fn type_tag(&self) -> TypeTag;  // delegates to Value::type_tag()
}
```

### 3.6 Sync Model

#### 3.6.1 Three states, not two

`Cell.sync` is `Option<bool>`. This is the foundation of the entire sync model:

| Value | Meaning | Effect on descendants |
|-------|---------|----------------------|
| `None` | No opinion — inherit from parent | Walk upward until a `Some(...)` is found |
| `Some(true)` | This subtree is synced | Descendants default to synced, but can override with `Some(false)` |
| `Some(false)` | This subtree is local-only | Descendants default to local-only, but can override with `Some(true)` |

This is different from a plain `bool` because it allows **selective opt-out** (or opt-in) at any depth. A parent can say `Some(true)` while one specific child says `Some(false)`.

#### 3.6.2 Two sync concepts

| Concept | Type | Where | Lifetime | Meaning |
|---------|------|-------|----------|---------|
| `Cell.sync` | `Option<bool>` | Every Cell | Persistent | Policy for this subtree — `None` means inherit |
| `Delta.sync` | `bool` | Every Delta | Ephemeral | Whether this specific write replicates |

`Delta.sync` is always a plain `bool` — at Delta creation time, the `Option<bool>` from the cell tree is resolved through inheritance into a definitive yes/no.

#### 3.6.3 Inheritance algorithm

When a write is created without an explicit `.synced()` or `.local()` override:

```text
fn resolve_sync(path_to_target: &[Cell]) -> bool:
    for cell in path_to_target.reverse():  # leaf to root
        if let Some(sync) = cell.sync:
            return sync   # first explicit opinion wins
    return database_default_sync
```

This means `Some(false)` on a child **overrides** `Some(true)` on its parent for writes targeting that child. The first `Some(...)` encountered during the leaf-to-root walk is authoritative.

#### 3.6.4 Delta.sync — per-write replication gate

`Delta.sync` is always a plain `bool`. At Delta creation time, the `Option<bool>` from the cell tree is resolved through inheritance into a definitive yes/no. It answers a single question: **should this write leave the local device?**

```text
Delta.sync = true:
  → append to WAL (local durability)
  → broadcast to peers via live sync
  → included in state-hash index for anti-entropy

Delta.sync = false:
  → append to WAL (local durability only)
  → NEVER broadcast
  → excluded from state-hash index
  → invisible to anti-entropy and cold sync
```

The WAL accepts everything regardless of sync — it is the local durability primitive.

#### 3.6.4 Sync boundary as a bidirectional isolation gate

When the **effective** sync for a subtree resolves to `false` (either because the cell itself has `Some(false)` or it inherits `false` from an ancestor with no override), the subtree is in local-only mode:

```text
Effective sync = false on a subtree:

  Outbound (local writes):
    → all deltas under this subtree get Delta.sync = false
    → nothing under this subtree is ever broadcast

  Inbound (remote writes):
    → incoming deltas targeting this subtree are dropped on the floor
    → they are NOT applied, NOT buffered, NOT acknowledged

  State-hash index:
    → the subtree is excluded entirely from replicated state hashes
    → anti-entropy does not detect or resolve divergence here
    → cold sync does not stream this subtree
```

No buffering — reconciliation after the gate reopens uses compacted current state via `merge_cells`.

#### 3.6.5 The two views of the database

```text
Local view       = synced state  ∪  local-only state    (what the user sees)
Replicated view  = synced state only                     (what peers see)
```

State-hash indexes only hash the replicated view. Anti-entropy only compares the replicated view. Cold sync only streams the replicated view.

#### 3.6.6 The SetSync operation

Toggling sync policy is an operation like any other:

```rust
enum Op {
    Atom(AtomOp),
    Record(RecordOp),
    SetSync { sync: Option<bool> },   // Some(true), Some(false), or None
}
```

`SetSync` is handled by the engine directly (bypasses the apply walk — see Section 11.5). It carries its own LWW gate via HLC.

**SetSync is always local.** All `SetSync` deltas carry `Delta.sync = false`. The sync flag is a local policy decision — toggling it on one device should not affect peers. Broadcasting a "I'm making this private" message would itself be a privacy leak. Broadcasting "I'm making this public" is unnecessary — anti-entropy will discover the divergence and converge naturally.

**Effective sync transitions:**

The engine tracks the **effective sync** for each cell — the resolved boolean after walking up through `None` ancestors. Transitions trigger state-hash index changes:

```text
Effective sync transitions from false to true:
  1. Delta { op: SetSync { sync: Some(true) }, sync: false } is created
  2. Applied locally: cursor.sync = Some(true); cursor.hlc = op_hlc
  3. Subtree is re-admitted to the state-hash index
  4. State-hash recomputed
  5. Next anti-entropy round naturally detects divergence and merges
  6. Normal bidirectional sync resumes from that point

Effective sync transitions from true to false:
  1. Delta { op: SetSync { sync: Some(false) }, sync: false } is created
  2. Applied locally: cursor.sync = Some(false); cursor.hlc = op_hlc
  3. Subtree removed from state-hash index
  4. Future local writes under this subtree get Delta.sync = false
  5. Incoming remote writes are dropped at the sync gate
  6. Peers are unaware — they keep syncing their version among themselves

Set to None (inherit from parent):
  1. Delta { op: SetSync { sync: None }, sync: false } is created
  2. Applied locally: cursor.sync = None
  3. Engine re-evaluates effective sync by walking up to parent
  4. If effective sync changed, adjust state-hash index accordingly
```

#### 3.6.7 Merge of sync flags

When `merge_cells` combines two Cells:

```text
Merge rule for Option<bool>:
  Some(true)  + anything    = Some(true)    # once synced, always synced
  None        + Some(false) = Some(false)   # explicit local wins over no-opinion
  None        + Some(true)  = Some(true)
  None        + None        = None          # neither had an opinion
  Some(false) + Some(false) = Some(false)
```

`Some(true)` dominates everything. This is the only merge that is commutative, idempotent, and associative while respecting that synced data cannot be un-synced by a merge.

#### 3.6.8 Why SetSync is local-only

Sync policy is per-device. When you mark a subtree as local-only, that decision itself should not leave the device. Peers don't need to know — writes they send to that subtree will be dropped at the incoming sync gate. When you mark a subtree as synced, anti-entropy naturally detects the divergence on the next round and `merge_cells` converges it. No explicit notification is needed.

Peers that continue sending writes to a subtree that is local-only on your device are wasting bandwidth, but this is an acceptable tradeoff for the privacy guarantee that sync policy decisions are never leaked.

#### 3.6.9 Why there is no buffering

State-based anti-entropy converges **compacted current state**, not historical operations. When sync is re-enabled, `merge_cells(local_cell, remote_cell)` converges both sides in one operation regardless of how many intermediate writes occurred.

Storing every intermediate delta that arrived during the offline period would only add complexity with no benefit — the merge result is identical either way.

---

## 4. TypeTag

`TypeTag` is a generated enum (see Section 7.3.4). It is a compact discriminant that identifies which registered type a `Value`, `Op`, or `Segment` belongs to. Each variant corresponds to one line in `register_types!`.

`TypeTag` is used in:

- `PathStep.container_tag` — tells the apply walk what type to expect at each depth
- `Value::type_tag()`, `Op::type_tag()`, `Segment::type_tag()` — runtime type identification
- `descend_or_create` — receives `child_tag` to know what type of dummy `Cell` to create
- Dispatch functions — match on `TypeTag` to route to the correct `Type` impl

---

## 5. Type Trait

### 5.1 Purpose

Every data type in ZeninDB implements the `Type` trait. This trait is the contract that enables the apply walk, merge, and encoding subsystems to treat all types uniformly.

### 5.2 Full definition

```rust
pub trait Type {
    /// Unique discriminant for this type.
    const TAG: TypeTag;

    /// Human-readable name, e.g. "Atom", "Record".
    const NAME: &'static str;

    /// Whether values of this type can be used as primary keys.
    /// True for Atom (scalars can be keys), false for Record.
    const KEYABLE: bool;

    /// Whether this type can contain child Cells.
    /// True for Record, false for Atom.
    const IS_CONTAINER: bool;

    /// The concrete value representation for this type.
    type Value;

    /// The concrete operation representation for this type.
    type Op;

    /// The error type for operations on this type.
    type Error: std::error::Error;

    /// Produce an empty / default value of this type.
    fn empty() -> Self::Value;

    /// Apply an operation to a value, producing a new value.
    /// The `hlc` parameter is the operation's HLC — it is available
    /// for types that need timestamp context (e.g., Record uses it
    /// for tombstone HLCs in RemoveField).
    fn apply_op(state: Self::Value, op: Self::Op, hlc: Hlc) -> Result<Self::Value, Self::Error>;

    /// Merge two values of this type.
    /// `local` / `remote` are the two values.
    /// `local_hlc` / `remote_hlc` are their respective Cell HLCs.
    /// Returns the merged value.
    fn merge(
        local: Self::Value,
        local_hlc: Hlc,
        remote: Self::Value,
        remote_hlc: Hlc,
    ) -> Result<Self::Value, Self::Error>;

    /// Whether the given operation replaces the target Cell wholesale
    /// (as opposed to modifying internal structure).
    /// Replacement ops are subject to LWW checking against the target
    /// Cell's direct HLC before apply_op is called.
    fn is_replacement(op: &Self::Op) -> bool;

    /// Encode a value to bytes. Appends to `out`.
    fn encode_value(value: &Self::Value, out: &mut Vec<u8>) -> Result<(), Self::Error>;

    /// Decode a value from bytes. Returns the value and the number of bytes consumed.
    fn decode_value(bytes: &[u8]) -> Result<(Self::Value, usize), Self::Error>;

    /// Encode an operation to bytes. Appends to `out`.
    fn encode_op(op: &Self::Op, out: &mut Vec<u8>) -> Result<(), Self::Error>;

    /// Decode an operation from bytes. Returns the op and the number of bytes consumed.
    fn decode_op(bytes: &[u8]) -> Result<(Self::Op, usize), Self::Error>;
}
```

### 5.3 Design rationale for each item

| Item               | Rationale                                                                                               |
| ------------------ | ------------------------------------------------------------------------------------------------------- |
| `TAG`              | Links the trait impl to the generated enum dispatch.                                                    |
| `NAME`             | Debug logging, error messages.                                                                           |
| `KEYABLE`          | The engine needs to know which types can serve as primary keys.                                          |
| `IS_CONTAINER`     | The apply walk checks this at each path step to ensure the cursor has children.                          |
| `Value` / `Op`     | Associated types allow each type to define its own representation without boxing.                        |
| `Error`            | Each type can have its own error variants. The generated outer Error enum wraps them.                    |
| `empty()`          | Used to create dummy cells during apply walk. Also the default for new containers.                       |
| `apply_op()`       | Consumes the old value, applies the op, returns the new value. The hlc parameter provides context.       |
| `merge()`          | Receives HLCs for both sides so types can make timestamp-informed decisions beyond simple LWW.           |
| `is_replacement()` | Lets the apply walk know whether to perform the LWW gate before dispatching to `apply_op`.              |
| `encode_*`         | Append-style encoding minimizes allocations. Returns bytes-consumed on decode for streaming.             |
| `decode_*`         | Returns bytes-consumed so the caller can advance the buffer.                                             |

### 5.4 Contract invariants

1. `apply_op(empty(), op, hlc)` must not panic for any valid `op`.
2. `merge(a, _, b, _)` must be commutative: `merge(a, ha, b, hb) == merge(b, hb, a, ha)`.
3. `merge(a, _, a, _)` must be idempotent: `merge(a, ha, a, ha) == a`.
4. `decode_value(encode_value(v))` must round-trip.
5. `decode_op(encode_op(op))` must round-trip.

---

## 6. ContainerType Trait

### 6.1 Purpose

`ContainerType` extends `Type` for types that contain child Cells. It provides:

- A `Segment` type for selecting a specific child
- Navigation methods (`descend`, `descend_or_create`)
- Segment encoding/decoding

### 6.2 Full definition

```rust
pub trait ContainerType: Type {
    /// The type-specific selector for descending one level.
    type Segment;

    /// Read-only descent. Returns `None` if the segment does not address
    /// an existing child, or `Some(None)` if the child slot exists but is empty
    /// (this distinction matters for tombstones in Record).
    fn descend(value: &Self::Value, segment: &Self::Segment) -> Result<Option<&Cell>, Self::Error>;

    /// Mutable descent with auto-creation. If the segment does not address
    /// an existing child, creates a dummy Cell of type `child_tag` and inserts it.
    /// Returns a mutable reference to the child Cell.
    fn descend_or_create(
        value: &mut Self::Value,
        segment: &Self::Segment,
        child_tag: TypeTag,
    ) -> Result<&mut Cell, Self::Error>;

    /// Encode a segment to bytes. Appends to `out`.
    fn encode_segment(segment: &Self::Segment, out: &mut Vec<u8>) -> Result<(), Self::Error>;

    /// Decode a segment from bytes. Returns the segment and bytes consumed.
    fn decode_segment(bytes: &[u8]) -> Result<(Self::Segment, usize), Self::Error>;
}
```

### 6.3 descend vs descend_or_create

- `descend` is used during **merge** and **read** operations. It never mutates.
- `descend_or_create` is used during the **apply walk**. It materializes missing intermediate nodes as dummy cells so the walk can continue.

### 6.4 child_tag parameter

`descend_or_create` takes a `child_tag: TypeTag` so it knows what type of dummy `Cell` to create. The `child_tag` comes from the next `PathStep`'s `container_tag` (or from the leaf operation's target type tag).

---

## 7. Type Registration & Code Generation

### 7.1 Philosophy

ZeninDB's type system is trait-first, macro-generated. To add a new data type:

1. **Create one module** containing your type's value struct, op enum, error enum, and (if a container) segment struct.
2. **Implement `Type`** on a unit struct (e.g., `pub struct MapType;`).
3. **Implement `ContainerType`** on the same unit struct if the type has children.
4. **Add one line** to the `register_types!` macro invocation.

The macro generates every cross-cutting enum (`Value`, `Op`, `Segment`) and every dispatch function. You never write a match arm over all types by hand.

### 7.2 The `register_types!` macro

`register_types!` is a `macro_rules!` macro defined inside `zendb-types` (no separate proc-macro crate needed). It takes an explicit list of registered types:

```rust
register_types! {
    // leaf types (no children, no Segment):
    leaf Atom => atom::AtomType,

    // container types (have children, implement ContainerType):
    container Record => record::RecordType,

    // future:
    // leaf    Counter => counter::CounterType,
    // container Map     => map::MapType,
    // container List    => list::ListType,
    // container Text    => text::TextType,
}
```

`leaf` types implement `Type` only. `container` types implement both `Type` and `ContainerType`.

The `SetSync` operation is NOT type-specific — it is added to the `Op` enum directly by the macro regardless of registered types.

### 7.3 What the macro generates

#### 7.3.1 Value enum

```rust
#[derive(Clone)]
pub enum Value {
    Atom(<atom::AtomType as Type>::Value),
    Record(<record::RecordType as Type>::Value),
}

impl Value {
    pub fn type_tag(&self) -> TypeTag {
        match self {
            Value::Atom(_) => TypeTag::Atom,
            Value::Record(_) => TypeTag::Record,
        }
    }
}
```

#### 7.3.2 Op enum

```rust
#[derive(Clone)]
pub enum Op {
    Atom(<atom::AtomType as Type>::Op),
    Record(<record::RecordType as Type>::Op),
    /// Modify Cell.sync. Not type-specific — handled by the engine,
    /// not by the apply walk or type dispatch.
    SetSync { sync: Option<bool> },
}

impl Op {
    pub fn type_tag(&self) -> TypeTag {
        match self {
            Op::Atom(_) => TypeTag::Atom,
            Op::Record(_) => TypeTag::Record,
            Op::SetSync { .. } => panic!("SetSync has no TypeTag — handle before type dispatch"),
        }
    }

    pub fn is_replacement(&self) -> bool {
        match self {
            Op::Atom(op) => <atom::AtomType as Type>::is_replacement(op),
            Op::Record(op) => <record::RecordType as Type>::is_replacement(op),
            Op::SetSync { .. } => true,
        }
    }
}
```

#### 7.3.3 Segment enum

```rust
#[derive(Clone)]
pub enum Segment {
    Record(<record::RecordType as ContainerType>::Segment),
}

impl Segment {
    pub fn type_tag(&self) -> TypeTag {
        match self {
            Segment::Record(_) => TypeTag::Record,
        }
    }
}
```

Only `container` types contribute variants.

#### 7.3.4 TypeTag enum

```rust
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum TypeTag {
    Atom   = 0,
    Record = 1,
}
```

Tags are assigned in registration order starting from 0.

#### 7.3.5 Dispatch functions

```rust
pub(crate) fn apply_op_dispatch(value: Value, op: Op, hlc: Hlc) -> Result<Value, TypeError> {
    match (value, op) {
        (Value::Atom(v), Op::Atom(o)) =>
            <atom::AtomType as Type>::apply_op(v, o, hlc)
                .map(Value::Atom)
                .map_err(TypeError::from),
        (Value::Record(v), Op::Record(o)) =>
            <record::RecordType as Type>::apply_op(v, o, hlc)
                .map(Value::Record)
                .map_err(TypeError::from),
        _ => Err(TypeError::TypeMismatch { /* ... */ }),
    }
}

pub(crate) fn merge_dispatch(
    local: Value, local_hlc: Hlc,
    remote: Value, remote_hlc: Hlc,
) -> Result<Value, TypeError> {
    match (local, remote) {
        (Value::Atom(l), Value::Atom(r)) =>
            <atom::AtomType as Type>::merge(l, local_hlc, r, remote_hlc)
                .map(Value::Atom)
                .map_err(TypeError::from),
        (Value::Record(l), Value::Record(r)) =>
            <record::RecordType as Type>::merge(l, local_hlc, r, remote_hlc)
                .map(Value::Record)
                .map_err(TypeError::from),
        _ => Err(TypeError::MergeConflict(/* ... */)),
    }
}

pub(crate) fn descend_dispatch<'a>(
    value: &'a Value, segment: &Segment,
) -> Result<Option<&'a Cell>, TypeError> {
    match (value, segment) {
        (Value::Record(v), Segment::Record(s)) =>
            <record::RecordType as ContainerType>::descend(v, s)
                .map_err(TypeError::from),
        _ => Err(TypeError::TypeMismatch { /* ... */ }),
    }
}

pub(crate) fn descend_or_create_dispatch<'a>(
    value: &'a mut Value, segment: &Segment, child_tag: TypeTag,
) -> Result<&'a mut Cell, TypeError> {
    match (value, segment) {
        (Value::Record(v), Segment::Record(s)) =>
            <record::RecordType as ContainerType>::descend_or_create(v, s, child_tag)
                .map_err(TypeError::from),
        _ => Err(TypeError::TypeMismatch { /* ... */ }),
    }
}

pub(crate) fn empty_for_tag(tag: TypeTag) -> Value {
    match tag {
        TypeTag::Atom => Value::Atom(<atom::AtomType as Type>::empty()),
        TypeTag::Record => Value::Record(<record::RecordType as Type>::empty()),
    }
}
```

#### 7.3.6 Encoding/decoding dispatch

The macro also generates `encode_value`, `decode_value`, `encode_op`, `decode_op`, `encode_segment`, and `decode_segment` dispatch functions following the same pattern.

### 7.4 Adding a type: step-by-step

To add a hypothetical `Map<K, V>` type:

**Step 1 — Create `src/map.rs`:**

```rust
pub struct MapType;

pub struct MapValue { /* ... */ }
pub enum MapOp { /* ... */ }
pub struct MapSegment { /* ... */ }
pub enum MapError { /* ... */ }

impl Type for MapType {
    type Value = MapValue;
    type Op = MapOp;
    type Error = MapError;
    const TAG = TypeTag::Map;  // generated by register_types!
    const NAME = "Map";
    const KEYABLE = false;
    const IS_CONTAINER = true;
    fn empty() -> MapValue { /* ... */ }
    fn apply_op(state: MapValue, op: MapOp, hlc: Hlc) -> Result<MapValue, MapError> { /* ... */ }
    fn merge(local: MapValue, local_hlc: Hlc, remote: MapValue, remote_hlc: Hlc) -> Result<MapValue, MapError> { /* ... */ }
    fn is_replacement(op: &MapOp) -> bool { /* ... */ }
    fn encode_value(value: &MapValue, out: &mut Vec<u8>) -> Result<(), MapError> { /* ... */ }
    fn decode_value(bytes: &[u8]) -> Result<(MapValue, usize), MapError> { /* ... */ }
    fn encode_op(op: &MapOp, out: &mut Vec<u8>) -> Result<(), MapError> { /* ... */ }
    fn decode_op(bytes: &[u8]) -> Result<(MapOp, usize), MapError> { /* ... */ }
}

impl ContainerType for MapType {
    type Segment = MapSegment;
    fn descend(value: &MapValue, segment: &MapSegment) -> Result<Option<&Cell>, MapError> { /* ... */ }
    fn descend_or_create(value: &mut MapValue, segment: &MapSegment, child_tag: TypeTag) -> Result<&mut Cell, MapError> { /* ... */ }
    fn encode_segment(segment: &MapSegment, out: &mut Vec<u8>) -> Result<(), MapError> { /* ... */ }
    fn decode_segment(bytes: &[u8]) -> Result<(MapSegment, usize), MapError> { /* ... */ }
}
```

**Step 2 — Add one line to `register_types!`:**

```rust
register_types! {
    leaf Atom => atom::AtomType,
    container Record => record::RecordType,
    container Map => map::MapType,       // <-- NEW
}
```

**That's it.** The macro generates:
- `Value::Map(MapValue)`
- `Op::Map(MapOp)`
- `Segment::Map(MapSegment)`
- `TypeTag::Map`
- All dispatch match arms
- All encode/decode match arms

No other file needs to be touched. No hand-written enum. No manual dispatch.

---

## 8. Path System

### 8.1 Purpose

A `Path` addresses any Cell within a row by describing the sequence of container descents from the row root.

### 8.2 Structures

```rust
/// One step in a Path.
pub struct PathStep {
    /// The expected type of the container at this depth.
    pub container_tag: TypeTag,
    /// How to descend from that container.
    pub segment: Segment,
}

/// A Path is a sequence of steps from the row root.
/// An empty path refers to the row root Cell itself.
pub struct Path {
    pub steps: Vec<PathStep>,
}
```

### 8.3 Empty path

An empty `Path` (`steps.is_empty()`) refers to the **root Cell of the row**. The apply walk with an empty path immediately hits the leaf case and applies the operation directly to the row root Cell.

### 8.4 Self-healing

Every `PathStep` carries `container_tag`. This is what enables self-healing: if a peer receives a delta targeting a path where intermediate containers do not yet exist locally, the apply walk sees the `container_tag` and creates dummy cells of the correct type.

### 8.5 Path manipulation

```rust
impl Path {
    pub fn new() -> Path;
    pub fn is_empty(&self) -> bool;
    pub fn len(&self) -> usize;

    /// Append a step. Returns the new Path (builder pattern).
    pub fn step(mut self, container_tag: TypeTag, segment: Segment) -> Path;

    /// Return a new Path referring to the parent (pop last step).
    pub fn parent(&self) -> Option<Path>;

    /// Return a new Path with an additional step.
    pub fn child(&self, container_tag: TypeTag, segment: Segment) -> Path;
}

impl PathStep {
    pub fn new(container_tag: TypeTag, segment: Segment) -> PathStep;
}
```

### 5.6 Path encoding

See Section 13 (Encoding Specification) for the binary format.

---

## 9. Built-in Type: Atom

### 9.1 Purpose

Atom is the scalar type. It represents all leaf values that have no children.

### 9.2 AtomValue

```rust
pub enum AtomValue {
    Null,
    Bool(bool),
    Int(i64),
    UInt(u64),
    Float(f64),        // Note: f64 is not Eq/Ord. See 9.8.
    String(String),
    Bytes(Vec<u8>),
    Timestamp(i64),     // microseconds since UNIX epoch
    Uuid([u8; 16]),
    Ulid([u8; 16]),     // 26-char ULID stored as 16 bytes
}
```

### 9.3 AtomOp

```rust
pub enum AtomOp {
    /// Replace the entire atom value.
    Set(AtomValue),
}
```

`Set` is a **replacement** operation. `is_replacement` returns `true`.

### 9.4 Type impl constants

| Constant        | Value    |
| --------------- | -------- |
| `TAG`           | `TypeTag::Atom` |
| `NAME`          | `"Atom"` |
| `KEYABLE`       | `true`   |
| `IS_CONTAINER`  | `false`  |

### 9.5 apply_op

```rust
fn apply_op(_state: AtomValue, op: AtomOp, _hlc: Hlc) -> Result<AtomValue, AtomError> {
    match op {
        AtomOp::Set(v) => Ok(v),
    }
}
```

The old value is discarded. HLC is ignored (Atom does not need timestamp context).

### 9.6 merge

**LWW by HLC:** the value with the higher HLC wins. If HLCs are equal (should not happen with correct generators), keep local.

```rust
fn merge(
    local: AtomValue,
    local_hlc: Hlc,
    remote: AtomValue,
    remote_hlc: Hlc,
) -> Result<AtomValue, AtomError> {
    if remote_hlc.beats(local_hlc) {
        Ok(remote)
    } else {
        Ok(local)
    }
}
```

### 9.7 AtomError

```rust
pub enum AtomError {
    /// Float values cannot be compared for merge if NaN is involved.
    /// NaN values are encoded but merging two NaN values is undefined.
    /// In practice, NaN AtomValues will be treated as unequal and LWW-applied.
    NaNMerge,
}
```

### 9.8 Float handling

`f64` does not implement `Eq` or `Ord`. This is a known challenge.

Design decisions:

- **Encoding:** NaN values are encoded/decoded faithfully using `f64::to_be_bytes()` / `f64::from_be_bytes()`.
- **Merge:** If both local and remote are `Float(f)` and `f.is_nan()`, the merge result is also NaN (LWW on HLC still applies to the *Cell*, not the float value itself). The float payload is carried opaquely.
- **Comparison for Eq on AtomValue:** We derive `PartialEq` but NOT `Eq` because of `f64`. Two `AtomValue::Float(f64::NAN)` values are NOT equal to themselves (IEEE 754 semantics). This is acceptable because AtomValue equality is not used in merge paths — merge always compares HLCs, not values.
- **Hash:** We do NOT derive `Hash` on `AtomValue` because `f64` is not `Hash`. If hashing is needed (e.g., for state-hash indexes), we hash the byte representation of the float.

Better alternative: Use `ordered_float::OrderedFloat<f64>` or our own wrapper. But that adds a dependency. For now, document the limitation and use byte-level hashing for state hashes.

Decision: Wrap f64 in a newtype `AtomFloat(f64)` that implements `Eq`, `Ord`, and `Hash` by treating the raw bits as `u64`. This makes NaN comparable (all NaN bit patterns are equal to each other) and gives a total order. This is acceptable for database use where exact bit preservation matters.

```rust
#[derive(Clone, Copy)]
pub struct AtomFloat(pub f64);

impl PartialEq for AtomFloat {
    fn eq(&self, other: &Self) -> bool {
        self.0.to_bits() == other.0.to_bits()
    }
}

impl Eq for AtomFloat {}

impl PartialOrd for AtomFloat {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for AtomFloat {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Compare raw bits. This gives a total order but the ordering
        // of NaN vs non-NaN is arbitrary. This is fine because merge
        // uses HLC ordering, not value ordering.
        self.0.to_bits().cmp(&other.0.to_bits())
    }
}

impl Hash for AtomFloat {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.to_bits().hash(state);
    }
}
```

With this, `AtomValue` can derive `PartialEq`, `Eq`, `PartialOrd`, `Ord`, and `Hash`.

### 9.9 Updated AtomValue

```rust
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum AtomValue {
    Null,
    Bool(bool),
    Int(i64),
    UInt(u64),
    Float(AtomFloat),
    String(String),
    Bytes(Vec<u8>),
    Timestamp(i64),
    Uuid([u8; 16]),
    Ulid([u8; 16]),
}
```

---

## 10. Built-in Type: Record

### 10.1 Purpose

Record is the named-field container type. It maps field names (`String`) to child `Cell` values. It is the primary mechanism for structuring data in ZeninDB.

### 10.2 RecordValue

```rust
use indexmap::IndexMap;

pub struct RecordValue {
    /// Named child cells. Insertion order is preserved.
    pub fields: IndexMap<String, Cell>,

    /// Deletion tombstones. Maps field name to the HLC at which it was deleted.
    pub tombstones: IndexMap<String, Hlc>,
}
```

### 10.3 Field visibility

A field named `name` is **visible** if and only if:

```text
fields.contains_key(name) AND fields[name].hlc > tombstones.get(name)
```

If `fields[name].hlc <= tombstones[name]`, the field is considered deleted. If the field is not in `fields` at all, it does not exist.

The equality case (`hlc == tombstone`) means tombstone wins (the delete operation had an equal-or-later timestamp; in practice this shouldn't happen with correct HLC generation but we define it defensively).

### 10.4 RecordOp

```rust
pub enum RecordOp {
    /// Set or update a single field.
    SetField {
        name: String,
        value: Cell,
    },

    /// Replace the entire record contents.
    Replace {
        value: RecordValue,
    },

    /// Delete a field by recording a tombstone.
    RemoveField {
        name: String,
    },
}
```

### 10.5 is_replacement

| Op            | is_replacement |
| ------------- | -------------- |
| `SetField`    | `false`        |
| `Replace`     | `true`         |
| `RemoveField` | `false`        |

### 10.6 Type impl constants

| Constant        | Value            |
| --------------- | ---------------- |
| `TAG`           | `TypeTag::Record` |
| `NAME`          | `"Record"`       |
| `KEYABLE`       | `false`          |
| `IS_CONTAINER`  | `true`           |

### 10.7 empty()

```rust
fn empty() -> RecordValue {
    RecordValue {
        fields: IndexMap::new(),
        tombstones: IndexMap::new(),
    }
}
```

### 10.8 apply_op — SetField

```rust
fn apply_op(mut state: RecordValue, op: RecordOp, hlc: Hlc) -> Result<RecordValue, RecordError> {
    match op {
        RecordOp::SetField { name, value } => {
            // If a tombstone exists and the new value's HLC doesn't beat the tombstone,
            // the set is ignored (the delete happened later).
            if let Some(&tombstone_hlc) = state.tombstones.get(&name) {
                if !value.hlc.beats(tombstone_hlc) {
                    return Ok(state); // drop the set, tombstone wins
                }
                // New value beats the tombstone. Remove the tombstone — the field is alive again.
                state.tombstones.swap_remove(&name);
            }
            state.fields.insert(name, value);
            Ok(state)
        }
        RecordOp::Replace { value } => {
            // HLC check happens at the Cell level (in apply_at_leaf, via is_replacement),
            // not here. If we reach here, the Replace has won the LWW check.
            Ok(value)
        }
        RecordOp::RemoveField { name } => {
            // Record a tombstone. Keep the field data in `fields` so we can
            // correctly resolve concurrent operations.
            let existing = state.tombstones.get(&name).copied().unwrap_or(Hlc::ZERO);
            // Use the max of existing tombstone and this op's HLC.
            // Actually: the tombstone should be set to the op's HLC, which should
            // always beat any previous tombstone since HLCs are monotonically increasing.
            // But in out-of-order delivery, an older delete might arrive after a newer one.
            // So we take max.
            state.tombstones.insert(name, if hlc.beats(existing) { hlc } else { existing });
            Ok(state)
        }
    }
}
```

### 10.9 RecordSegment

```rust
pub struct RecordSegment {
    pub field_name: String,
}
```

### 10.10 ContainerType impl — descend

```rust
fn descend(value: &RecordValue, segment: &RecordSegment) -> Result<Option<&Cell>, RecordError> {
    // Check tombstone first.
    if let Some(&tombstone_hlc) = value.tombstones.get(&segment.field_name) {
        if let Some(cell) = value.fields.get(&segment.field_name) {
            if !cell.hlc.beats(tombstone_hlc) {
                return Ok(None); // field exists but is tombstoned — treated as absent
            }
        } else {
            return Ok(None); // tombstone exists but no field — treated as absent
        }
    }
    Ok(value.fields.get(&segment.field_name))
}
```

### 10.11 ContainerType impl — descend_or_create

```rust
fn descend_or_create(
    value: &mut RecordValue,
    segment: &RecordSegment,
    child_tag: TypeTag,
) -> Result<&mut Cell, RecordError> {
    // Check tombstone.
    if let Some(&tombstone_hlc) = value.tombstones.get(&segment.field_name) {
        if let Some(cell) = value.fields.get(&segment.field_name) {
            if !cell.hlc.beats(tombstone_hlc) {
                // Field is tombstoned. The incoming op will create a new cell.
                // Remove the tombstone if the incoming op's HLC beats it (checked in apply_op).
                value.fields.swap_remove(&segment.field_name);
            }
        }
    }

    if !value.fields.contains_key(&segment.field_name) {
        // Create a dummy cell of the expected child type.
        let dummy_value = match child_tag {
            TypeTag::Atom => Value::Atom(AtomValue::Null),
            TypeTag::Record => Value::Record(RecordValue::empty()),
            // Future types handled here.
            _ => return Err(RecordError::UnknownChildTag(child_tag)),
        };
        value.fields.insert(
            segment.field_name.clone(),
            Cell::dummy(dummy_value),
        );
    }

    Ok(value.fields.get_mut(&segment.field_name).unwrap())
}
```

### 10.12 merge

Record merge is recursive:

1. For each field present in either local or remote:
   - If field is in both and both are visible: recursively `merge_cells(local_cell, remote_cell)`.
   - If field is visible only in one and that one's cell HLC beats the other's tombstone (if any): keep it.
   - If field is tombstoned in both: keep the max tombstone HLC, remove field data.
2. Merge tombstones: for each field, max(local_tombstone, remote_tombstone).

```rust
fn merge(
    local: RecordValue,
    local_hlc: Hlc,
    remote: RecordValue,
    remote_hlc: Hlc,
) -> Result<RecordValue, RecordError> {
    let mut merged = RecordValue::empty();

    // Collect all field names from both sides
    let all_fields: IndexSet<String> = local.fields.keys()
        .chain(remote.fields.keys())
        .cloned()
        .collect();

    for field_name in all_fields {
        let local_cell = local.fields.get(&field_name);
        let remote_cell = remote.fields.get(&field_name);
        let local_tomb = local.tombstones.get(&field_name);
        let remote_tomb = remote.tombstones.get(&field_name);

        let local_visible = local_cell.is_some_and(|c| {
            local_tomb.map_or(true, |t| c.hlc.beats(*t))
        });
        let remote_visible = remote_cell.is_some_and(|c| {
            remote_tomb.map_or(true, |t| c.hlc.beats(*t))
        });

        match (local_visible, remote_visible) {
            (true, true) => {
                let merged_cell = merge_cells(
                    local_cell.unwrap().clone(),
                    remote_cell.unwrap().clone(),
                );
                merged.fields.insert(field_name, merged_cell);
            }
            (true, false) => {
                // Local is visible, remote is not. Keep local.
                merged.fields.insert(field_name, local_cell.unwrap().clone());
            }
            (false, true) => {
                // Remote is visible, local is not. Keep remote.
                merged.fields.insert(field_name, remote_cell.unwrap().clone());
            }
            (false, false) => {
                // Neither visible. Don't add to fields.
            }
        }

        // Merge tombstones: take the max HLC
        let max_tomb = match (local_tomb, remote_tomb) {
            (Some(&lt), Some(&rt)) => if rt.beats(lt) { rt } else { lt },
            (Some(&lt), None) => lt,
            (None, Some(&rt)) => rt,
            (None, None) => continue,
        };

        // Only store tombstone if the field is not visible AND the tombstone
        // beats any existing cell HLC (defensive).
        if !merged.fields.get(&field_name).is_some_and(|c| c.hlc.beats(max_tomb)) {
            merged.tombstones.insert(field_name, max_tomb);
        }
    }

    Ok(merged)
}
```

### 10.13 RecordError

```rust
pub enum RecordError {
    UnknownChildTag(TypeTag),
    /// A field operation referenced a field name that is not valid.
    InvalidFieldName(String),
}
```

---

## 11. Apply Walk

### 11.1 Purpose

The apply walk is the algorithm that applies a type-specific `Op` to a row by walking down the `Path` and dispatching at the leaf. It handles only operations that modify `Cell.value` — `SetSync` (which modifies `Cell.sync`) is intercepted by the engine before the apply walk and handled directly.

### 11.2 Algorithm overview

```
fn apply_recursive(cursor: &mut Cell, steps: &[PathStep], op: Op, op_hlc: Hlc, op_tag: TypeTag) -> bool
```

1. If `steps` is not empty:
   - Pop the head `PathStep`.
   - Call `ensure_type(cursor, head.container_tag, op_hlc)`. If it returns false, STOP (operation dropped).
   - Determine `child_tag`: if `tail` is non-empty, use `tail[0].container_tag`; otherwise use `op_tag`.
   - Call `descend_or_create_dispatch` on the cursor's value to get `&mut Cell` for the child.
   - Recurse: `apply_recursive(child, tail, op, op_hlc, op_tag)`.
2. If `steps` is empty (leaf):
   - Call `apply_at_leaf(cursor, op, op_hlc, op_tag)`.

### 11.3 ensure_type

```rust
/// Ensure the cursor Cell has the expected type.
/// Returns false if the operation should be dropped.
fn ensure_type(cursor: &mut Cell, expected_tag: TypeTag, op_hlc: Hlc) -> bool {
    if cursor.type_tag() == expected_tag {
        return true; // types match, proceed
    }

    // Type mismatch. If cursor is dummy (Hlc::ZERO), allow replacement.
    // If cursor is real, use LWW: incoming op_hlc must beat cursor's hlc.
    if cursor.is_dummy() || op_hlc.beats(cursor.hlc) {
        // Replace cursor with an empty value of the expected type.
        let new_value = empty_for_tag(expected_tag);
        *cursor = Cell::new(new_value, cursor.hlc, cursor.sync);
        return true;
    }

    // Incoming op is older than the existing cell. Drop the op.
    false
}
```

### 11.4 apply_at_leaf

```rust
fn apply_at_leaf(cursor: &mut Cell, op: Op, op_hlc: Hlc, op_tag: TypeTag) -> bool {
    // Ensure the cursor has the expected type for the operation.
    if !ensure_type(cursor, op_tag, op_hlc) {
        return false;
    }

    // For replacement operations, LWW check against the cursor's current HLC.
    if op.is_replacement() && cursor.hlc.beats(op_hlc) {
        return false; // current cell is newer, drop the op
    }

    // Dispatch to the type-specific apply_op.
    match apply_op_dispatch(cursor.value.clone(), op, op_hlc) {
        Ok(new_value) => {
            cursor.value = new_value;
            cursor.hlc = op_hlc; // update the cell's HLC to the operation's HLC
            true
        }
        Err(_) => false, // apply_op failed, drop
    }
}
```

### 11.5 SetSync — engine-level, local-only

`SetSync` modifies `Cell.sync` (metadata), not `Cell.value`. It does not go through the apply walk. The engine handles it before `apply_recursive` is called. `SetSync` deltas always carry `Delta.sync = false` — sync policy is per-device and never replicated.

```text
Delta arrives with op = SetSync { sync }:
  → engine resolves the path to a &mut Cell (using descend, not descend_or_create)
  → if the cell does not exist: drop the delta (SetSync on a non-existent cell is a no-op)
  → if cursor.hlc.beats(delta.hlc): drop (LWW — a newer write already changed this cell)
  → cursor.sync = sync  // sync is Option<bool>: Some(true), Some(false), or None
  → cursor.hlc = delta.hlc
  → engine re-evaluates the effective sync for this subtree:
        if effective sync transitioned from false to true:
            trigger targeted anti-entropy reconciliation
        if effective sync transitioned from true to false:
            remove subtree from state-hash index
```

This keeps the apply walk pure: it only deals with `Cell.value`.

### 11.6 Incoming sync gate

The apply walk itself is a pure function. It does not check `Cell.sync` to decide whether to accept a delta. That check lives in the engine's delta ingestion path, before `apply_recursive` is called:

```text
Incoming delta arrives:
  → engine resolves the target cell (or nearest existing ancestor) from the path
  → if the effective sync for that cell resolves to false:
        drop the delta (the subtree is in local-only mode)
  → else if op is SetSync:
        handle directly (see 11.5)
  → else:
        call apply_recursive(cursor, path.steps, delta.op, delta.hlc, delta.op.type_tag())
```

### 11.7 Key invariants

1. **No ancestor HLCs are updated.** Only the leaf Cell's HLC changes.
2. **Missing containers are materialized as dummies.** This is self-healing.
3. **Type conflicts are resolved by LWW on the Cell HLC.** If an older op tries to replace a newer cell of a different type, it's dropped.
4. **Replacement ops get an LWW gate at the leaf.** Non-replacement ops (SetField, RemoveField) skip this gate and go directly to apply_op.
5. **The operation's HLC is always used for the leaf Cell's new HLC**, never a fresh generated HLC. This ensures all peers that apply the same delta set the same HLC.
6. **SetSync bypasses the apply walk entirely.** It is intercepted by the engine and handled as a direct Cell metadata mutation.
7. **Sync gate is above the apply walk.** Incoming deltas are dropped before `apply_recursive` if the effective sync for the target subtree resolves to `false`.

---

## 12. Merge

### 12.1 Purpose

Merge combines two `Cell` values at the same path — one local, one remote — into a single converged `Cell`. Merge is used during state-based anti-entropy and during compaction.

### 12.2 Cell-level merge

```rust
pub fn merge_cells(local: Cell, remote: Cell) -> Cell {
    // If types differ, LWW on the Cell's HLC decides the winner.
    if local.type_tag() != remote.type_tag() {
        if remote.hlc.beats(local.hlc) {
            return remote;
        } else {
            return local;
        }
    }

    // Types match. Merge values with type-specific merge.
    // Use the max HLC.
    let new_hlc = if remote.hlc.beats(local.hlc) {
        remote.hlc
    } else {
        local.hlc
    };
    // Merge sync flags. Some(true) dominates everything.
    // This is the only commutative, idempotent, associative merge
    // for Option<bool> where true is the dominant value:
    //   Some(true) + anything    = Some(true)
    //   None       + Some(false) = Some(false)
    //   None       + None        = None
    let new_sync = match (local.sync, remote.sync) {
        (Some(true), _) | (_, Some(true)) => Some(true),
        (Some(false), _) | (_, Some(false)) => Some(false),
        (None, None) => None,
    };

    // Dispatch to type-specific merge.
    let new_value = merge_dispatch(local.value, local.hlc, remote.value, remote.hlc)
        .unwrap_or_else(|_| {
            // On merge failure, fall back to LWW on the Cell HLC.
            if remote.hlc.beats(local.hlc) {
                remote.value
            } else {
                local.value
            }
        });

    Cell {
        value: new_value,
        hlc: new_hlc,
        sync: new_sync,
    }
}
```

### 12.3 Merge contracts

1. **Commutative:** `merge_cells(a, b) == merge_cells(b, a)`.
2. **Idempotent:** `merge_cells(a, a) == a`.
3. **Associative:** `merge_cells(a, merge_cells(b, c)) == merge_cells(merge_cells(a, b), c)`.

These properties guarantee that all peers converge to the same state regardless of delivery order.

### 12.4 Merge failure fallback

If a type-specific merge returns an error, the fallback is LWW on the Cell HLC. This ensures the system never loses data due to a merge bug — at worst, it falls back to LWW semantics.

---

## 13. Delta

### 13.1 Purpose

A `Delta` is the unit produced by every write. It contains everything needed to apply the write locally and replicate it to peers.

### 13.2 Structure

```rust
pub struct Delta {
    /// Which table this write targets.
    pub table_id: TableId,

    /// Primary key of the row.
    pub primary_key: PrimaryKey,

    /// Path from the row root to the target Cell.
    pub path: Path,

    /// The operation to apply.
    pub op: Op,

    /// HLC timestamp of this write.
    pub hlc: Hlc,

    /// Whether this write should replicate (always a plain bool).
    /// Resolved at creation time from Cell.sync (Option<bool>) via inheritance,
    /// or set explicitly via .synced() / .local().
    pub sync: bool,

    /// Cryptographic signature over the delta payload.
    /// Raw bytes; verification is handled by zendb-replication.
    pub signature: Signature,
}
```

### 13.3 TableId and PrimaryKey

```rust
/// A table identifier. Opaque; defined here so Delta can reference it.
pub struct TableId(pub String);  // or u64, or UUID — TBD by storage layer

/// A primary key value. Must be a KEYABLE type.
pub struct PrimaryKey(pub AtomValue);
```

### 13.4 Signature

```rust
/// A cryptographic signature. Raw bytes.
/// For Ed25519, this is 64 bytes.
/// Verification is handled by zendb-replication.
pub struct Signature(pub Vec<u8>);
```

### 13.5 Delta creation flow

1. User calls a write API on `zendb-engine`.
2. Engine determines `table_id`, `primary_key`, constructs `Path`.
3. Engine calls `HlcGenerator::local_event()` to get `hlc`.
4. Engine resolves `sync`:
   - If user explicitly set `.synced()` → `sync = true`
   - If user explicitly set `.local()` → `sync = false`
   - Otherwise → walk ancestors from target cell upward, use first found `cell.sync`, or database default.
5. Engine signs the delta (in `zendb-replication` or `zendb-engine`).
6. `Delta` enters the WAL and is applied locally via `apply_recursive`.

### 13.6 Delta encoding

See Section 13 for binary format.

---

## 14. Encoding Specification

### 14.1 Design principles

- **Big-endian** for all multi-byte integers (consistency with HLC ordering).
- **Variable-length integers** for lengths, counts, and TypeTags (compact for small values, expandable).
- **Length-prefixed** for all variable-length payloads.
- **Type-tag-prefixed** for Value, Op, and Segment.
- **Append-style:** `encode_*` methods append to `&mut Vec<u8>` to minimize allocations.

### 14.2 Varint encoding

Unsigned varint (LEB128-like, but big-endian doesn't matter for varint):

```text
While value > 0x7F:
    emit (value & 0x7F) | 0x80
    value >>= 7
emit value & 0x7F
```

Max 9 bytes for u64. Most values (TypeTags, short lengths) fit in 1 byte.

### 14.3 TypeTag encoding

Encoded as a single byte (the `repr(u8)` discriminant). If we exceed 256 types, we'll use varint with a sentinel. For now: 1 byte.

### 14.4 HLC encoding

Fixed 12 bytes, big-endian:

```text
[physical_ms[0..6], logical[0..2], node_id[0..4]]
```

No length prefix (it's always 12 bytes).

### 14.5 Cell encoding

```text
Cell = Value HLC[12] Sync(1 byte)
```

`Sync` encoding: `0x00` = `None`, `0x01` = `Some(false)`, `0x02` = `Some(true)`.

### 14.6 Value encoding

```text
Value = TypeTag TypeSpecificPayload
```

Where `TypeSpecificPayload` is defined by each type:

**AtomValue:**

```text
AtomValue = VariantTag Payload

VariantTag (1 byte):
    0x00 = Null
    0x01 = Bool(false)
    0x02 = Bool(true)   // note: separate tags for true/false, or 0x01 + 1 byte
    0x03 = Int          (i64, 8 bytes big-endian)
    0x04 = UInt         (u64, 8 bytes big-endian)
    0x05 = Float        (f64, 8 bytes big-endian IEEE 754)
    0x06 = String       (varint length + UTF-8 bytes)
    0x07 = Bytes        (varint length + raw bytes)
    0x08 = Timestamp    (i64, 8 bytes big-endian)
    0x09 = Uuid         (16 bytes)
    0x0A = Ulid         (16 bytes)
```

**RecordValue:**

```text
RecordValue = FieldCount(varint) Field* TombstoneCount(varint) Tombstone*

Field  = FieldName(String) Cell
Tombstone = FieldName(String) HLC[12]
```

Note: RecordValue has no `replace_hlc` — the LWW check for `RecordOp::Replace` uses the Cell's HLC, not a field inside the value.

### 14.7 Op encoding

```text
Op = TypeTag TypeSpecificOpPayload
```

**AtomOp:**

```text
AtomOp = 0x00 AtomValue   // Set
```

**RecordOp:**

```text
RecordOp = VariantTag Payload

VariantTag:
    0x00 = SetField { FieldName(String) Cell }
    0x01 = Replace  { RecordValue }
    0x02 = RemoveField { FieldName(String) }
```

**SetSync:**

```text
SetSync = 0xFF Sync(1 byte)
```

`Sync` encoding: `0x00` = `None`, `0x01` = `Some(false)`, `0x02` = `Some(true)`.

`SetSync` uses a special variant tag `0xFF` that does not correspond to any `TypeTag`. The engine intercepts this before type dispatch.

### 14.8 Segment encoding

```text
Segment = TypeTag TypeSpecificSegmentPayload
```

**RecordSegment:**

```text
RecordSegment = FieldName(String)
```

### 14.9 Path encoding

```text
Path = StepCount(varint) PathStep*

PathStep = TypeTag Segment
```

An empty path is encoded as `0x00` (zero steps).

### 14.10 Delta encoding

```text
Delta = TableId(String) PrimaryKey(AtomValue) Path Op HLC[12] Sync(1 byte) SignatureLength(varint) Signature[SignatureLength]
```

`Sync` is a plain `bool` (`0x00` or `0x01`). Unlike `Cell.sync` and `SetSync`, `Delta.sync` is always resolved to a definitive yes/no at creation time.

### 14.11 String encoding

```text
String = ByteLength(varint) UTF8Bytes[ByteLength]
```

---

## 15. Error Handling

### 15.1 Unified error type

```rust
#[derive(Debug, thiserror::Error)]
pub enum TypeError {
    #[error("atom error: {0}")]
    Atom(#[from] AtomError),

    #[error("record error: {0}")]
    Record(#[from] RecordError),

    #[error("unknown type tag: {0:?}")]
    UnknownTypeTag(TypeTag),

    #[error("type mismatch: expected {expected:?}, got {got:?}")]
    TypeMismatch {
        expected: TypeTag,
        got: TypeTag,
    },

    #[error("encoding error: {0}")]
    EncodeError(String),

    #[error("decoding error: {0}")]
    DecodeError(String),

    #[error("merge conflict: types {0:?} and {1:?} cannot be merged")]
    MergeConflict(TypeTag, TypeTag),
}
```

### 16.2 When errors occur

| Operation             | Error conditions                                      |
| --------------------- | ----------------------------------------------------- |
| `apply_op` (Atom)     | None (infallible for now)                             |
| `apply_op` (Record)   | Unknown child tag in descend_or_create                |
| `merge` (Atom)        | None (LWW is infallible)                              |
| `merge` (Record)      | None (recursive merge is infallible)                  |
| `encode_*`            | I/O errors if writing to a failing sink (not applicable with `Vec<u8>`) |
| `decode_*`            | Truncated input, invalid variant tags, invalid UTF-8  |
| `apply_recursive`     | Type mismatch that can't be healed, decode errors in path |

---

## 17. Module Structure

```
zendb-types/
├── Cargo.toml
├── PRD.md                          # This document
└── src/
    ├── lib.rs                      # Crate root, module declarations, register_types! invocation
    ├── hlc.rs                      # Hlc struct
    ├── cell.rs                     # Cell struct
    ├── traits.rs                   # Type trait + ContainerType trait definitions
    ├── atom.rs                     # AtomType impl + AtomValue, AtomOp, AtomFloat, AtomError
    ├── record.rs                   # RecordType impl + RecordValue, RecordOp, RecordSegment, RecordError
    ├── path.rs                     # Path, PathStep
    ├── delta.rs                    # Delta, TableId, PrimaryKey, Signature
    ├── apply.rs                    # apply_recursive, apply_at_leaf, ensure_type
    ├── merge.rs                    # merge_cells
    ├── codec.rs                    # Varint encoding and other shared encode/decode helpers
    └── error.rs                    # TypeError
```

Key points:

- **No `type_tag.rs`** — `TypeTag` is generated by `register_types!` in `lib.rs`.
- **No `value.rs`, `op.rs`, `segment.rs`** — `Value`, `Op`, and `Segment` enums plus all dispatch functions are generated by `register_types!` in `lib.rs`.
- **No `traits/` directory** — both traits live in a single `traits.rs` file.
- **No `types/` directory** — each type is one flat file (`atom.rs`, `record.rs`).
- **No `registry.rs`** — `register_types!` generates `empty_for_tag` and all dispatch directly.

---

## 18. Dependencies

| Crate       | Version | Rationale                                              |
| ----------- | ------- | ------------------------------------------------------ |
| `indexmap`  | 2.x     | `IndexMap` for Record fields and tombstones. Preserves insertion order for deterministic encoding and hashing. |
| `thiserror` | 2.x     | Derive `Error` for error enums. Lightweight.           |

**No other dependencies.** Specifically:

- No `serde` — encoding is custom binary, not serde-based. Users who want serde can enable a feature later.
- No `bytes` — `Vec<u8>` is sufficient for encoding output.
- No `ed25519-dalek` — signatures are opaque bytes in this crate.
- No `rand` — HLC generation is in the engine crate, not here.

---

## 19. Public API Surface

The following items are `pub` and constitute the stable API of `zendb-types`:

```
pub use hlc::Hlc;
pub use cell::Cell;
pub use traits::{Type, ContainerType};
pub use atom::{AtomValue, AtomOp, AtomError, AtomFloat, AtomType};
pub use record::{RecordValue, RecordOp, RecordSegment, RecordError, RecordType};
pub use path::{Path, PathStep};
pub use delta::{Delta, TableId, PrimaryKey, Signature};
pub use apply::apply_recursive;
pub use merge::merge_cells;
pub use error::TypeError;
// Value, Op, Segment, TypeTag are generated by register_types! in lib.rs
```

Everything else is `pub(crate)` or private.

---

## 20. Future Types

The type system is designed for extension. Planned types and their characteristics:

| Type      | IS_CONTAINER | KEYABLE | Segment           | Notes                                    |
| --------- | ------------ | ------- | ----------------- | ---------------------------------------- |
| Map<K,V>  | true         | false   | key: K            | CRDT map with LWW per key                |
| List<T>   | true         | false   | index: u64        | CRDT list (RGA or similar)               |
| Text      | true         | false   | position: u64     | Collaborative text (CRDT, e.g. Peritext) |
| Counter   | false        | false   | N/A               | PN-Counter or similar                    |
| Set<T>    | true         | false   | element: T        | CRDT set (OR-Set)                        |

Each will:
1. Implement `Type` (and `ContainerType` if applicable).
2. Define its `Value`, `Op`, `Segment` (if container), and `Error` types.
3. Be added as one line to `register_types!` in `lib.rs`.
4. The macro generates all `Value`, `Op`, `Segment` variants and dispatch arms automatically.

---

## 21. Design Review Checklist

Before implementation begins, verify:

- [x] Every type has a defined encoding format (including SetSync in Op encoding).
- [x] The apply walk handles type mismatch, missing containers, and LWW gating.
- [x] SetSync bypasses the apply walk and is handled by the engine directly.
- [x] Merge is commutative, idempotent, and associative.
- [x] Merge of sync flags uses logical OR (true dominates).
- [x] Dummy cells (Hlc::ZERO) are always replaced by real writes.
- [x] Tombstone semantics are clear: field is visible iff `cell.hlc > tombstone`.
- [x] RecordOp::Replace LWW is gated by the Cell's HLC, not a separate replace_hlc.
- [x] RecordValue has no replace_hlc — it's just a value like any other.
- [x] Float (f64) is wrapped in AtomFloat for Eq/Ord/Hash.
- [x] No external I/O, no networking, no storage in this crate.
- [x] Delta structure includes everything needed for replication (signature is opaque bytes).
- [x] Path's container_tag enables self-healing.
- [x] register_types! is a macro_rules! macro in zendb-types (no separate crate).
- [x] Adding a type = one module + two trait impls + one line in register_types!.

---

*End of PRD for `zendb-types`.*
