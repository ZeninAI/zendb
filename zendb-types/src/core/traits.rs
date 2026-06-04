//! Core traits for the ZeninDB type system.
//!
//! ## Trait hierarchy
//!
//! ```text
//! Type          ── Self: Encode + Decode,
//!                  Op: Encode + Decode, Error, apply(), merge(), max_hlc()
//! ContainerType ── Type + Segment: Encode + Decode, child_or_insert()
//! ```

use bincode::{Decode, Encode};

use crate::{Cell, Hlc, TypeTag};

pub trait Type: Sized + Encode + Decode<()> {
    type Op: Encode + Decode<()>;
    type Error: std::error::Error;

    /// Apply an operation, mutating this value in place.
    /// Returns `Ok(true)` if state was modified, `Ok(false)` if the op was
    /// rejected (e.g. LWW loss — local HLC beats op HLC).
    fn apply(&mut self, op: &Self::Op, local_hlc: Hlc, op_hlc: Hlc) -> Result<bool, Self::Error>;

    /// Merge `remote` into this value.
    /// Returns `Ok(true)` if this value was modified.
    fn merge(
        &mut self,
        remote: &Self,
        local_hlc: Hlc,
        remote_hlc: Hlc,
    ) -> Result<bool, Self::Error>;

    fn max_hlc(&self) -> Hlc {
        Hlc::ZERO
    }
}

pub trait ContainerType: Type {
    type Segment: Encode + Decode<()>;

    /// Navigate into a child, creating a placeholder if absent.
    ///
    /// `child_tag = Some(tag)` creates a live empty child of that type.
    /// `child_tag = None` creates a tombstone placeholder, useful for path
    /// targeted cell operations such as delete where no value type is known.
    fn child_or_insert<'a>(
        &'a mut self,
        segment: &Self::Segment,
        child_tag: Option<TypeTag>,
    ) -> Result<&'a mut Cell, Self::Error>;
}
