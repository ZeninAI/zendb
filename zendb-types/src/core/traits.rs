//! Core traits for the ZenDB type system.

use bincode::{Decode, Encode};

use crate::{Hlc, Op, PathStep};

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

pub trait Type: Sized + Encode + Decode<()> {
    type Op: Encode + Decode<()>;
    type Error: std::error::Error;

    /// Apply this type's local operation.
    fn apply(&mut self, op: &Self::Op, op_hlc: Hlc) -> Result<bool, Self::Error>;

    /// Merge same-type state. Containers recursively merge their child cells.
    fn merge(&mut self, remote: &Self, clocks: MergeClocks) -> Result<bool, Self::Error>;

    /// Resolve sync policy recursively for a path.
    fn is_synced(&self, inherited: bool, _path: &[PathStep]) -> bool {
        inherited
    }

    /// Compact type-owned state below a trusted watermark.
    fn compact(&mut self, _watermark: Hlc) -> Result<bool, Self::Error> {
        Ok(false)
    }

    fn max_hlc(&self) -> Hlc {
        Hlc::ZERO
    }
}

pub trait ContainerType: Type {
    /// Apply a global operation recursively.
    ///
    /// Implementations consume the path segment they own and delegate the
    /// remaining path to the selected child container.
    fn apply_walk(&mut self, op: &Op, op_hlc: Hlc, path: &[PathStep]) -> Result<bool, Self::Error>;
}
