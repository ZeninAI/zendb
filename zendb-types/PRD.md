# zendb-types — Architecture & Implementation PRD

## 1. Crate Overview

`zendb-types` is the foundation of ZeninDB. It defines all core data types, traits, encoding, and the `register_types!` macro. **No I/O, no networking, no storage.**

### Module structure

```
src/
├── lib.rs              # register_types! macro, invocation, re-exports
├── core/
│   ├── mod.rs
│   ├── cell.rs         # Cell + apply_walk + merge
│   ├── hlc.rs          # 10-byte Hybrid Logical Clock
│   ├── traits.rs       # Type, MergeClocks
│   ├── path.rs         # Path + PathStep
│   └── event.rs        # Event + TableId + PrimaryKey + Signature
├── types/
│   ├── mod.rs
│   ├── atom.rs         # AtomValue, AtomOp, AtomFloat, AtomError, AtomType
│   └── record.rs       # RecordValue, RecordOp, RecordSegment, RecordError, RecordType
```

---

## 2. HLC — Hybrid Logical Clock

10 bytes, big-endian:

```text
Bytes 0–5:  physical_ms   (48-bit, ms since epoch)
Bytes 6–7:  logical       (16-bit monotonic counter)
Bytes 8–9:  node_id       (16-bit node identifier)
```

`Hlc::ZERO` is the all-zero sentinel. Dummy cells use it. Any real HLC beats `ZERO`. Ordering: physical desc → logical desc → node_id desc.

```rust
pub struct Hlc([u8; 10]);
impl Hlc {
    pub const ZERO: Hlc;
    pub fn new(physical_ms: u64, logical: u16, node_id: u16) -> Option<Hlc>;
    pub fn beats(&self, other: Hlc) -> bool;  // self > other
    pub fn as_bytes(&self) -> &[u8; 10];
    pub fn from_bytes(bytes: [u8; 10]) -> Hlc;
    pub fn physical_ms(&self) -> u64;
    pub fn logical(&self) -> u16;
    pub fn node_id(&self) -> u16;
}
```

---

## 3. Cell

The universal addressable value wrapper.

```rust
pub struct Cell {
    pub value: Option<Value>,
    pub hlc: Hlc,
    pub sync: Option<bool>,   // None = inherit, Some(true) = sync, Some(false) = local
    sync_hlc: Hlc,            // does not participate in value conflict resolution
    generation: Hlc,          // whole-value replacement generation
}

impl Cell {
    pub fn new(value: Value, hlc: Hlc, sync: Option<bool>) -> Cell;
    pub fn dummy(value: Value) -> Cell;          // HLC = ZERO, sync = None
    pub fn is_dummy(&self) -> bool;
    pub fn type_tag(&self) -> TypeTag;

    // Apply a event to this cell (root of the path).
    pub fn apply_event(&mut self, event: &Event) -> bool;

    // Merge a remote cell into this one.
    pub fn merge(&mut self, remote: &Cell);
}
```

### Apply walk

`apply_event` delegates recursive path routing to `ContainerType::apply_walk`, materializing missing intermediate containers as dummy cells. At the leaf, it dispatches to the type-specific `Type::apply`. Returns `true` if the state was modified.

`ensure_type` handles type conflicts: if the cursor has the wrong type, LWW decides whether to replace with a dummy of the expected type. Whole-value replacements establish a new generation. Same-type recursive state merges occur only within the same generation, preventing older descendants from being resurrected after replacement.

`SetSync` uses `sync_hlc`; sync metadata never advances the value HLC or affects value conflict resolution.

### Merge

`merge_cells(local, remote)` dispatches to type-specific `Type::merge` when types match, falls back to LWW on the Cell HLC when they differ. Sync flags merge with `Some(true)` dominating (OR semantics).

---

## 4. Sync Model

Three states on `Cell.sync`:

| Value | Meaning |
|-------|---------|
| `None` | No opinion — inherit from nearest ancestor with `Some(...)` |
| `Some(true)` | Subtree is synced (descendants default to synced) |
| `Some(false)` | Subtree is local-only (descendants default to local-only) |

A child can override its parent: `Some(false)` on a field under a `Some(true)` record keeps just that field local.

`Event.sync` is always a plain `bool`, resolved at creation time from the `Option<bool>` chain.

`SetSync` is **local-only** (`Event.sync = false`). Sync policy is per-device. Turning sync ON re-admits the subtree to the state-hash index; the next anti-entropy round detects divergence and converges. Turning sync OFF removes the subtree from the state-hash; peers keep syncing their version.

---

## 5. Trait Hierarchy

```
Type          ── Self: Encode + Decode, Op: Encode + Decode,
                 Error: std::error::Error,
                 apply(), merge(), is_synced(), compact(), max_hlc()
ContainerType ── Type + apply_walk()
```

### Type

```rust
pub trait Type: Sized + Encode + Decode<()> {
    type Op: Encode + Decode<()>;
    type Error: std::error::Error;

    fn apply(&mut self, op: &Self::Op, op_hlc: Hlc)
        -> Result<bool, Self::Error>;
    fn merge(&mut self, remote: &Self, clocks: MergeClocks)
        -> Result<bool, Self::Error>;
    fn is_synced(&self, inherited: bool, path: &[PathStep]) -> bool;
    fn compact(&mut self, watermark: Hlc) -> Result<bool, Self::Error>;
    fn max_hlc(&self) -> Hlc;
}
```

Container types implement `ContainerType::apply_walk`. They consume their own path segment and delegate to the selected child `Cell`.

Complex CRDT representations are structs with private backing state. Atomic primitives remain aliases. Consumer reads use semantic methods such as `Counter::value`, `Set::contains`, `Text::string`, and `List::cell_at`.

Receiver-state-dependent mutations are resolved when constructing events:

- `OrSet::remove` captures the observed add tags in `OrSetOp::Remove`.
- `PriorityQueue::pop` captures the selected element ID in `PqOp::Pop`.
- `Text::format` captures explicit character IDs in `TextOp::Format`.
- `MvRegister::assign` captures the assignments causally replaced by the new assignment.

`compact(watermark)` removes tombstones and obsolete metadata known to be stable at or before the watermark. Containers recursively compact live children. Sequence tombstones that remain anchors are retained until no entry references them.

---

## 6. Built-in Types

### Atom

Scalar leaf type. `AtomValue` covers Null, Bool, Int, UInt, Float (via `AtomFloat`), String, Bytes, Timestamp, Uuid, Ulid.

`AtomOp::Set(v)` does LWW: if `local_hlc.beats(op_hlc)`, returns `Ok(false)` (no change); otherwise replaces the value and returns `Ok(true)`.

`AtomType::merge` is LWW by HLC — returns `Ok(true)` only if the remote value beats the local.

### Record

Named-field container. `RecordValue` has `fields: BTreeMap<String, Cell>` and `tombstones: BTreeMap<String, Hlc>`. No `replace_hlc` — the Cell's HLC is the sole authority.

Operations:
- `SetField { name, value }` — inserts the cell. Checks tombstone: if `value.hlc` doesn't beat the tombstone, returns `Ok(false)`. Otherwise inserts and returns `Ok(true)`.
- `Replace { value }` — LWW gate: if `local_hlc.beats(op_hlc)`, returns `Ok(false)`. Otherwise replaces the entire record and returns `Ok(true)`.
- `RemoveField { name }` — records a tombstone at `op_hlc`. Always returns `Ok(true)` (the tombstone write is new).

`RecordType::merge` — recursive field-by-field merge with tombstone resolution. Mutates `local` in place.

`RecordType::descend_or_create` — navigates into a named field. Creates a dummy of `child_tag` if absent, using `child_tag.empty_value()`. Respects tombstones.

---

## 7. The `register_types!` Macro

A `macro_rules!` macro in `lib.rs`. Invocation:

```rust
register_types! {
    leaf Atom => crate::types::atom::AtomType,
    container Record => crate::types::record::RecordType,
}
```

**Generates:**
- `TypeTag` enum (with `name()`, `from_u8()`, `empty_value()`)
- `TypeError` enum (per-type variants + cross-cutting: `UnknownTypeTag`, `TypeMismatch`, `EncodeError`, `DecodeError`, `MergeConflict`). Implements `Display` and `Error::source()`.
- `Value` enum (with `type_tag()`, `encode()`, `decode()`)
- `Op` enum (with `type_tag()`, `encode()`, `decode()`, `SetSync` variant)
- `Segment` enum (with `type_tag()`, `encode()`, `decode()`)
- local `Type::apply` dispatch for registered `TypeOp` variants
- recursive `ContainerType::apply_walk` dispatch for registered containers
- `merge_dispatch(&mut Value, &Value, local_hlc, remote_hlc) -> Result<bool, TypeError>`
- `descend_or_create_dispatch(&mut Value, &Segment, child_tag) -> Result<&mut Cell, TypeError>`

**Adding a type:** 1 module + 2-3 trait impls + 1 line in `register_types!`. Zero hand-written dispatch.

---

## 8. Encoding

All persisted types derive `Encode` and `Decode` from bincode. Serialization is handled by storage and other typed callers through bincode directly; `zendb-types` does not expose hand-written or compatibility encode/decode methods.

The `register_types!` macro derives bincode for generated `TypeTag`, `Value`, `Op`, and `Segment`, so adding a type does not require hand-written serialization dispatch.

---

## 9. Dependencies

- `bincode` — canonical serialization/deserialization for all persisted types
- No serde, no crypto, no I/O
