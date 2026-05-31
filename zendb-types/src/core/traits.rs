//! Core traits for the ZeninDB type system.
//!
//! ## Trait hierarchy
//!
//! ```text
//! Type        ── TAG, NAME, KEYABLE, IS_CONTAINER,
//!                Value: Encode + Decode, Op: Encode + Decode, Error,
//!                empty(), apply_op(), merge()
//! ContainerType ── Type + Segment: Encode + Decode,
//!                   descend_or_create()
//! ```

use bincode::{Decode, Encode};

use crate::{Cell, Hlc, TypeTag};

pub trait Type {
    const TAG: TypeTag;
    const NAME: &'static str;
    const KEYABLE: bool;
    const IS_CONTAINER: bool;
    type Value: Encode + Decode<()>;
    type Op: Encode + Decode<()>;
    type Error: std::error::Error;

    fn empty() -> Self::Value;

    /// Apply an operation, mutating `value` in place.
    /// Returns `Ok(true)` if state was modified, `Ok(false)` if the op was
    /// rejected (e.g. LWW loss — local HLC beats op HLC).
    fn apply_op(
        value: &mut Self::Value,
        op: &Self::Op,
        local_hlc: Hlc,
        op_hlc: Hlc,
    ) -> Result<bool, Self::Error>;

    /// Merge `remote` into `local`, mutating `local` in place.
    /// Returns `Ok(true)` if local was modified.
    fn merge(
        local: &mut Self::Value,
        local_hlc: Hlc,
        remote: &Self::Value,
        remote_hlc: Hlc,
    ) -> Result<bool, Self::Error>;
}

pub trait ContainerType: Type {
    type Segment: Encode + Decode<()>;

    /// Navigate into a child, creating a dummy if absent.
    fn descend_or_create<'a>(
        value: &'a mut Self::Value,
        segment: &Self::Segment,
        child_tag: TypeTag,
    ) -> Result<&'a mut Cell, Self::Error>;
}
