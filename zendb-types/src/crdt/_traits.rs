//! Core traits for the ZenDB type system.

use bincode::{Decode, Encode};

use crate::crdt::{Cell, Hlc, Op, PathStep, SyncPolicy, SyncScope};
use crate::{PrimaryKey, Segment, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MergeClocks {
    pub local: Hlc,
    pub remote: Hlc,
}

impl MergeClocks {
    pub const ZERO: Self = Self {
        local: Hlc::ZERO,
        remote: Hlc::ZERO,
    };

    pub const fn new(local: Hlc, remote: Hlc) -> Self {
        Self { local, remote }
    }
}

/// Errors produced while materializing a Rust value from CRDT state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CellCodecError {
    MissingField(&'static str),
    InvalidField {
        field: &'static str,
        expected: &'static str,
    },
    InvalidValue(&'static str),
}

impl CellCodecError {
    pub fn expected(expected: &'static str) -> Self {
        Self::InvalidValue(expected)
    }

    pub fn with_field(self, field: &'static str) -> Self {
        match self {
            Self::MissingField(_) => Self::MissingField(field),
            _ => Self::InvalidField {
                field,
                expected: "valid Cell value",
            },
        }
    }
}

impl std::fmt::Display for CellCodecError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingField(field) => write!(formatter, "missing Cell field {field}"),
            Self::InvalidField { field, expected } => {
                write!(formatter, "invalid Cell field {field}: expected {expected}")
            }
            Self::InvalidValue(expected) => {
                write!(formatter, "invalid Cell value: expected {expected}")
            }
        }
    }
}

impl std::error::Error for CellCodecError {}

/// Converts a Rust representation to and from one CRDT `Value` variant.
///
/// Implementations live beside the CRDT that owns the representation. The
/// codec type, rather than the Rust type alone, selects CRDT semantics.
pub trait CrdtCodec {
    type Rust;

    fn encode(value: &Self::Rust, hlc: Hlc) -> Value;

    fn decode(value: &Value) -> Result<Self::Rust, CellCodecError>;
}

/// Selects the unambiguous default CRDT codec for a Rust type.
pub trait DefaultCrdtCodec: Sized {
    type Codec: CrdtCodec<Rust = Self>;
}

/// Converts a resolved domain type to and from its canonical CRDT Cell view.
///
/// `#[derive(CellCodec)]` supports named record structs, single-field tuple
/// newtypes, and unit enums. Field codecs default from their Rust types;
/// `#[cell(crdt = "or_set")]` opts a `BTreeSet` into OR-Set semantics, and
/// `#[cell(codec = "path::to::Codec")]` selects an alternate representation.
///
/// The derive rejects shapes whose CRDT semantics cannot be selected
/// unambiguously. Decoding remains fallible because a remotely supplied Cell
/// can be malformed even when its Rust type was valid at compile time.
pub trait CellCodec: Sized {
    fn to_cell(&self, hlc: Hlc) -> Cell;

    fn from_cell(cell: &Cell) -> Result<Self, CellCodecError>;
}

impl<T: DefaultCrdtCodec> CellCodec for T {
    fn to_cell(&self, hlc: Hlc) -> Cell {
        Cell {
            value: Some(<T::Codec as CrdtCodec>::encode(self, hlc)),
            hlc,
            sync: SyncPolicy::Inherit,
        }
    }

    fn from_cell(cell: &Cell) -> Result<Self, CellCodecError> {
        let value = cell
            .value
            .as_ref()
            .ok_or_else(|| CellCodecError::expected("live Cell"))?;
        <T::Codec as CrdtCodec>::decode(value)
    }
}

/// Converts values used by keyed CRDTs into primary keys.
pub trait CellCodecKey: Sized {
    fn to_primary_key(&self) -> PrimaryKey;

    fn from_primary_key(key: &PrimaryKey) -> Result<Self, CellCodecError>;
}

pub trait Type: Sized + Encode + Decode<()> {
    type Op: Encode + Decode<()>;
    type Error: std::error::Error;

    /// Apply this type's local operation.
    fn apply(&mut self, op: &Self::Op, op_hlc: Hlc) -> Result<bool, Self::Error>;

    /// Merge same-type state. Containers recursively merge their child cells.
    fn merge(&mut self, remote: &Self, clocks: MergeClocks) -> Result<bool, Self::Error>;

    /// Compact type-owned state below a trusted watermark.
    fn compact(&mut self, _watermark: Hlc) -> Result<bool, Self::Error> {
        Ok(false)
    }

    fn max_hlc(&self) -> Hlc {
        Hlc::ZERO
    }
}

pub trait ContainerType: Type {
    /// Select one existing immediate child by this container's segment type.
    fn child(&self, segment: &Segment) -> Option<&Cell>;

    /// Mutable immediate-child lookup. This never creates state.
    fn child_mut(&mut self, segment: &Segment) -> Option<&mut Cell>;

    /// Resolve an existing descendant Cell without creating state.
    fn cell_at_path(&self, path: &[PathStep]) -> Option<&Cell> {
        let (step, remaining) = path.split_first()?;
        self.child(&step.segment)?.cell_at_path(remaining)
    }

    /// Mutable counterpart of [`Self::cell_at_path`].
    fn cell_at_path_mut(&mut self, path: &[PathStep]) -> Option<&mut Cell> {
        let (step, remaining) = path.split_first()?;
        self.child_mut(&step.segment)?.cell_at_path_mut(remaining)
    }

    /// Resolve the effective replication scope at a descendant path.
    fn effective_scope_at(&self, parent_scope: SyncScope, path: &[PathStep]) -> SyncScope {
        let Some((step, remaining)) = path.split_first() else {
            return parent_scope;
        };
        self.child(&step.segment)
            .map(|child| child.effective_scope_at(parent_scope, remaining))
            .unwrap_or(parent_scope)
    }

    /// Apply a path-targeted operation recursively.
    ///
    /// Creation semantics are container-specific: Records create named fields,
    /// while Lists create stable-ID placeholders.
    fn apply_walk(&mut self, op: &Op, op_hlc: Hlc, path: &[PathStep]) -> Result<bool, Self::Error>;

    /// Merge shared state into this container while preserving receiver-local
    /// child Cells. Lists and Records align children differently.
    fn merge_shared(
        &mut self,
        remote: &Self,
        clocks: MergeClocks,
        parent_scope: SyncScope,
    ) -> Result<bool, Self::Error>;

    /// Clone this container's shared projection.
    ///
    /// `None` omits a local boundary from its parent's projection.
    fn shared_clone(&self, parent_scope: SyncScope) -> Option<Self>;
}
