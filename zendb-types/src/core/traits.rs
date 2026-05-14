//! Core traits for the ZeninDB type system.
//!
//! Every data type must implement `Type`. Container types (types with child
//! cells) additionally implement `ContainerType`.
//!
//! These traits are the ONLY contracts needed to register a new type.
//! The `register_types!` macro consumes trait impls and generates all
//! cross-cutting enums and dispatch functions.

use crate::{Cell, Hlc, TypeTag};

/// The base trait every data type must implement.
///
/// # Associated types
///
/// - `Value`: The concrete value representation (e.g., `AtomValue`, `RecordValue`).
/// - `Op`: The operation type for mutations (e.g., `AtomOp`, `RecordOp`).
/// - `Error`: Type-specific error type.
///
/// # Contract invariants
///
/// 1. `apply_op(empty(), op, hlc)` must not panic for any valid `op`.
/// 2. `merge(a, _, b, _)` must be commutative, idempotent, and associative.
/// 3. `encode_value`/`decode_value` must round-trip.
/// 4. `encode_op`/`decode_op` must round-trip.
pub trait Type {
    /// Unique discriminant for this type.
    const TAG: TypeTag;

    /// Human-readable name.
    const NAME: &'static str;

    /// Whether values of this type can serve as primary keys.
    const KEYABLE: bool;

    /// Whether this type can contain child cells.
    const IS_CONTAINER: bool;

    /// The concrete value type.
    type Value;

    /// The concrete operation type.
    type Op;

    /// Type-specific error type.
    type Error: std::fmt::Display;

    /// Produce an empty value of this type.
    fn empty() -> Self::Value;

    /// Apply an operation, producing a new value.
    ///
    /// The `hlc` is the operation's HLC — available for types that need
    /// timestamp context (e.g., Record uses it for tombstone HLCs).
    fn apply_op(state: Self::Value, op: Self::Op, hlc: Hlc) -> Result<Self::Value, Self::Error>;

    /// Merge two values. `local_hlc` and `remote_hlc` are the HLCs of the
    /// cells wrapping these values — available for timestamp-aware merge logic.
    fn merge(
        local: Self::Value,
        local_hlc: Hlc,
        remote: Self::Value,
        remote_hlc: Hlc,
    ) -> Result<Self::Value, Self::Error>;

    /// Whether this operation replaces the cell wholesale.
    ///
    /// Replacement ops get an LWW check against the cell's HLC in
    /// `apply_at_leaf` before dispatch. Non-replacement ops skip this gate.
    fn is_replacement(op: &Self::Op) -> bool;

    /// Encode a value, appending bytes to `out`.
    fn encode_value(value: &Self::Value, out: &mut Vec<u8>) -> Result<(), Self::Error>;

    /// Decode a value from `bytes`. Returns the value and bytes consumed.
    fn decode_value(bytes: &[u8]) -> Result<(Self::Value, usize), Self::Error>;

    /// Encode an operation, appending bytes to `out`.
    fn encode_op(op: &Self::Op, out: &mut Vec<u8>) -> Result<(), Self::Error>;

    /// Decode an operation from `bytes`. Returns the op and bytes consumed.
    fn decode_op(bytes: &[u8]) -> Result<(Self::Op, usize), Self::Error>;
}

/// Extension trait for types that contain child cells.
///
/// Provides segment-based navigation (`descend`, `descend_or_create`) and
/// segment encoding.
pub trait ContainerType: Type {
    /// A selector for descending one level into the container.
    type Segment;

    /// Read-only descent. Returns `None` if the segment does not address
    /// an existing child.
    fn descend<'a>(
        value: &'a Self::Value,
        segment: &Self::Segment,
    ) -> Result<Option<&'a Cell>, Self::Error>;

    /// Mutable descent with auto-creation.
    ///
    /// If the segment does not address an existing child, creates a dummy
    /// cell of type `child_tag` and inserts it.
    fn descend_or_create<'a>(
        value: &'a mut Self::Value,
        segment: &Self::Segment,
        child_tag: TypeTag,
    ) -> Result<&'a mut Cell, Self::Error>;

    /// Encode a segment, appending bytes to `out`.
    fn encode_segment(segment: &Self::Segment, out: &mut Vec<u8>) -> Result<(), Self::Error>;

    /// Decode a segment from `bytes`. Returns the segment and bytes consumed.
    fn decode_segment(bytes: &[u8]) -> Result<(Self::Segment, usize), Self::Error>;
}
