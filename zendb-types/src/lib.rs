//! # zendb-types
//!
//! Core type system for ZeninDB — an embedded, local-first, eventually
//! consistent database with first-class collaborative editing.
//!
//! ## Adding a type
//!
//! 1. Create a module (e.g., `src/map.rs`) with a unit struct implementing
//!    `Type` (and `ContainerType` if the type has children)
//! 2. Add one line to `register_types!` below

// --- hand-written modules ---
pub mod apply;
pub mod codec;
pub mod core;
pub mod delta;
pub mod encode;
pub mod error;
pub mod merge;
pub mod path;
pub mod types;

// --- generated enums and dispatch ---
// Everything below this point is produced by register_types!

macro_rules! register_types {
    (
        $( leaf $leaf_var:ident => $leaf_ty:ty, )*
        $( container $cont_var:ident => $cont_ty:ty, )*
    ) => {
        // =================================================================
        // TypeTag
        // =================================================================
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        #[repr(u8)]
        pub enum TypeTag {
            $($leaf_var,)*
            $($cont_var,)*
        }

        impl TypeTag {
            pub const fn name(self) -> &'static str {
                match self {
                    $(TypeTag::$leaf_var => <$leaf_ty as $crate::Type>::NAME,)*
                    $(TypeTag::$cont_var => <$cont_ty as $crate::Type>::NAME,)*
                }
            }

            pub fn from_u8(v: u8) -> Result<TypeTag, $crate::TypeError> {
                match v {
                    $(v if v == TypeTag::$leaf_var as u8 => Ok(TypeTag::$leaf_var),)*
                    $(v if v == TypeTag::$cont_var as u8 => Ok(TypeTag::$cont_var),)*
                    other => Err($crate::TypeError::UnknownTypeTag(other)),
                }
            }
        }

        impl std::fmt::Display for TypeTag {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(self.name())
            }
        }

        // =================================================================
        // Value
        // =================================================================
        #[derive(Debug, Clone, PartialEq)]
        pub enum Value {
            $($leaf_var(<$leaf_ty as $crate::Type>::Value),)*
            $($cont_var(<$cont_ty as $crate::Type>::Value),)*
        }

        impl Value {
            pub fn type_tag(&self) -> TypeTag {
                match self {
                    $(Value::$leaf_var(_) => TypeTag::$leaf_var,)*
                    $(Value::$cont_var(_) => TypeTag::$cont_var,)*
                }
            }
        }

        // =================================================================
        // Op
        // =================================================================
        #[derive(Debug, Clone)]
        pub enum Op {
            $($leaf_var(<$leaf_ty as $crate::Type>::Op),)*
            $($cont_var(<$cont_ty as $crate::Type>::Op),)*
            /// Modify Cell.sync. Handled by the engine, not the apply walk.
            SetSync { sync: Option<bool> },
        }

        impl Op {
            pub fn type_tag(&self) -> TypeTag {
                match self {
                    $(Op::$leaf_var(_) => TypeTag::$leaf_var,)*
                    $(Op::$cont_var(_) => TypeTag::$cont_var,)*
                    Op::SetSync { .. } => panic!("SetSync has no TypeTag"),
                }
            }

            pub fn is_replacement(&self) -> bool {
                match self {
                    $(Op::$leaf_var(op) => <$leaf_ty as $crate::Type>::is_replacement(op),)*
                    $(Op::$cont_var(op) => <$cont_ty as $crate::Type>::is_replacement(op),)*
                    Op::SetSync { .. } => true,
                }
            }
        }

        // =================================================================
        // Segment
        // =================================================================
        #[derive(Debug, Clone)]
        pub enum Segment {
            $($cont_var(<$cont_ty as $crate::ContainerType>::Segment),)*
        }

        impl Segment {
            pub fn type_tag(&self) -> TypeTag {
                match self {
                    $(Segment::$cont_var(_) => TypeTag::$cont_var,)*
                }
            }
        }

        // =================================================================
        // Dispatch: empty_for_tag
        // =================================================================
        pub(crate) fn empty_for_tag(tag: TypeTag) -> Value {
            match tag {
                $(TypeTag::$leaf_var => Value::$leaf_var(<$leaf_ty as $crate::Type>::empty()),)*
                $(TypeTag::$cont_var => Value::$cont_var(<$cont_ty as $crate::Type>::empty()),)*
            }
        }

        // =================================================================
        // Dispatch: apply_op
        // =================================================================
        pub(crate) fn apply_op_dispatch(
            value: Value,
            op: Op,
            hlc: $crate::Hlc,
        ) -> Result<Value, $crate::TypeError> {
            match (value, op) {
                $(
                    (Value::$leaf_var(v), Op::$leaf_var(o)) => {
                        <$leaf_ty as $crate::Type>::apply_op(v, o, hlc)
                            .map(Value::$leaf_var)
                            .map_err(|e| $crate::TypeError::ApplyError {
                                tag: <$leaf_ty as $crate::Type>::TAG,
                                message: e.to_string(),
                            })
                    }
                )*
                $(
                    (Value::$cont_var(v), Op::$cont_var(o)) => {
                        <$cont_ty as $crate::Type>::apply_op(v, o, hlc)
                            .map(Value::$cont_var)
                            .map_err(|e| $crate::TypeError::ApplyError {
                                tag: <$cont_ty as $crate::Type>::TAG,
                                message: e.to_string(),
                            })
                    }
                )*
                (v, o) => Err($crate::TypeError::TypeMismatch {
                    expected: v.type_tag(),
                    actual: o.type_tag(),
                }),
            }
        }

        // =================================================================
        // Dispatch: merge
        // =================================================================
        pub(crate) fn merge_dispatch(
            local: Value,
            local_hlc: $crate::Hlc,
            remote: Value,
            remote_hlc: $crate::Hlc,
        ) -> Result<Value, $crate::TypeError> {
            match (local, remote) {
                $(
                    (Value::$leaf_var(l), Value::$leaf_var(r)) => {
                        <$leaf_ty as $crate::Type>::merge(l, local_hlc, r, remote_hlc)
                            .map(Value::$leaf_var)
                            .map_err(|e| $crate::TypeError::MergeError {
                                tag: <$leaf_ty as $crate::Type>::TAG,
                                message: e.to_string(),
                            })
                    }
                )*
                $(
                    (Value::$cont_var(l), Value::$cont_var(r)) => {
                        <$cont_ty as $crate::Type>::merge(l, local_hlc, r, remote_hlc)
                            .map(Value::$cont_var)
                            .map_err(|e| $crate::TypeError::MergeError {
                                tag: <$cont_ty as $crate::Type>::TAG,
                                message: e.to_string(),
                            })
                    }
                )*
                (l, r) => Err($crate::TypeError::MergeConflict {
                    local: l.type_tag(),
                    remote: r.type_tag(),
                }),
            }
        }

        // =================================================================
        // Dispatch: descend
        // =================================================================
        #[allow(dead_code)]
        pub(crate) fn descend_dispatch<'a>(
            value: &'a Value,
            segment: &Segment,
        ) -> Result<Option<&'a $crate::Cell>, $crate::TypeError> {
            match (value, segment) {
                $(
                    (Value::$cont_var(v), Segment::$cont_var(s)) => {
                        <$cont_ty as $crate::ContainerType>::descend(v, s)
                            .map_err(|e| $crate::TypeError::DescendError {
                                tag: <$cont_ty as $crate::Type>::TAG,
                                message: e.to_string(),
                            })
                    }
                )*
                _ => Err($crate::TypeError::TypeMismatch {
                    expected: segment.type_tag(),
                    actual: value.type_tag(),
                }),
            }
        }

        // =================================================================
        // Dispatch: descend_or_create
        // =================================================================
        pub(crate) fn descend_or_create_dispatch<'a>(
            value: &'a mut Value,
            segment: &Segment,
            child_tag: TypeTag,
        ) -> Result<&'a mut $crate::Cell, $crate::TypeError> {
            let tag = value.type_tag();
            match (value, segment) {
                $(
                    (Value::$cont_var(v), Segment::$cont_var(s)) => {
                        <$cont_ty as $crate::ContainerType>::descend_or_create(v, s, child_tag)
                            .map_err(|e| $crate::TypeError::DescendError {
                                tag: <$cont_ty as $crate::Type>::TAG,
                                message: e.to_string(),
                            })
                    }
                )*
                _ => Err($crate::TypeError::TypeMismatch {
                    expected: segment.type_tag(),
                    actual: tag,
                }),
            }
        }

        // =================================================================
        // Dispatch: encode_value
        // =================================================================
        pub fn encode_value(value: &Value, out: &mut Vec<u8>) -> Result<(), $crate::TypeError> {
            match value {
                $(
                    Value::$leaf_var(v) => {
                        out.push(TypeTag::$leaf_var as u8);
                        <$leaf_ty as $crate::Type>::encode_value(v, out)
                            .map_err(|e| $crate::TypeError::EncodeError(e.to_string()))
                    }
                )*
                $(
                    Value::$cont_var(v) => {
                        out.push(TypeTag::$cont_var as u8);
                        <$cont_ty as $crate::Type>::encode_value(v, out)
                            .map_err(|e| $crate::TypeError::EncodeError(e.to_string()))
                    }
                )*
            }
        }

        /// Decode a Value from bytes. Returns the value and bytes consumed.
        pub fn decode_value(bytes: &[u8]) -> Result<(Value, usize), $crate::TypeError> {
            if bytes.is_empty() {
                return Err($crate::TypeError::DecodeError("empty input".into()));
            }
            let tag = TypeTag::from_u8(bytes[0])?;
            match tag {
                $(
                    TypeTag::$leaf_var => {
                        let (v, n) = <$leaf_ty as $crate::Type>::decode_value(&bytes[1..])
                            .map_err(|e| $crate::TypeError::DecodeError(e.to_string()))?;
                        Ok((Value::$leaf_var(v), 1 + n))
                    }
                )*
                $(
                    TypeTag::$cont_var => {
                        let (v, n) = <$cont_ty as $crate::Type>::decode_value(&bytes[1..])
                            .map_err(|e| $crate::TypeError::DecodeError(e.to_string()))?;
                        Ok((Value::$cont_var(v), 1 + n))
                    }
                )*
            }
        }

        // =================================================================
        // Dispatch: encode_op
        // =================================================================
        pub fn encode_op(op: &Op, out: &mut Vec<u8>) -> Result<(), $crate::TypeError> {
            match op {
                $(
                    Op::$leaf_var(o) => {
                        out.push(TypeTag::$leaf_var as u8);
                        <$leaf_ty as $crate::Type>::encode_op(o, out)
                            .map_err(|e| $crate::TypeError::EncodeError(e.to_string()))
                    }
                )*
                $(
                    Op::$cont_var(o) => {
                        out.push(TypeTag::$cont_var as u8);
                        <$cont_ty as $crate::Type>::encode_op(o, out)
                            .map_err(|e| $crate::TypeError::EncodeError(e.to_string()))
                    }
                )*
                Op::SetSync { sync } => {
                    out.push(0xFF);
                    out.push(match sync {
                        None => 0x00,
                        Some(false) => 0x01,
                        Some(true) => 0x02,
                    });
                    Ok(())
                }
            }
        }

        /// Decode an Op from bytes. Returns the op and bytes consumed.
        pub fn decode_op(bytes: &[u8]) -> Result<(Op, usize), $crate::TypeError> {
            if bytes.is_empty() {
                return Err($crate::TypeError::DecodeError("empty input".into()));
            }
            // 0xFF is the special SetSync tag.
            if bytes[0] == 0xFF {
                if bytes.len() < 2 {
                    return Err($crate::TypeError::DecodeError("truncated SetSync".into()));
                }
                let sync = match bytes[1] {
                    0x00 => None,
                    0x01 => Some(false),
                    0x02 => Some(true),
                    b => return Err($crate::TypeError::DecodeError(
                        format!("invalid SetSync byte: {}", b)
                    )),
                };
                return Ok((Op::SetSync { sync }, 2));
            }
            let tag = TypeTag::from_u8(bytes[0])?;
            match tag {
                $(
                    TypeTag::$leaf_var => {
                        let (o, n) = <$leaf_ty as $crate::Type>::decode_op(&bytes[1..])
                            .map_err(|e| $crate::TypeError::DecodeError(e.to_string()))?;
                        Ok((Op::$leaf_var(o), 1 + n))
                    }
                )*
                $(
                    TypeTag::$cont_var => {
                        let (o, n) = <$cont_ty as $crate::Type>::decode_op(&bytes[1..])
                            .map_err(|e| $crate::TypeError::DecodeError(e.to_string()))?;
                        Ok((Op::$cont_var(o), 1 + n))
                    }
                )*
            }
        }

        // =================================================================
        // Dispatch: encode_segment
        // =================================================================
        pub fn encode_segment(segment: &Segment, out: &mut Vec<u8>) -> Result<(), $crate::TypeError> {
            match segment {
                $(
                    Segment::$cont_var(s) => {
                        out.push(TypeTag::$cont_var as u8);
                        <$cont_ty as $crate::ContainerType>::encode_segment(s, out)
                            .map_err(|e| $crate::TypeError::EncodeError(e.to_string()))
                    }
                )*
            }
        }

        /// Decode a Segment from bytes. Returns the segment and bytes consumed.
        pub fn decode_segment(bytes: &[u8]) -> Result<(Segment, usize), $crate::TypeError> {
            if bytes.is_empty() {
                return Err($crate::TypeError::DecodeError("empty input".into()));
            }
            let tag = TypeTag::from_u8(bytes[0])?;
            match tag {
                $(
                    TypeTag::$cont_var => {
                        let (s, n) = <$cont_ty as $crate::ContainerType>::decode_segment(&bytes[1..])
                            .map_err(|e| $crate::TypeError::DecodeError(e.to_string()))?;
                        Ok((Segment::$cont_var(s), 1 + n))
                    }
                )*
                other => Err($crate::TypeError::DecodeError(
                    format!("type {:?} has no Segment variant", other)
                )),
            }
        }
    };
}

// --- invoke the macro ---
register_types! {
    leaf Atom => crate::types::atom::AtomType,
    container Record => crate::types::record::RecordType,
}

// --- re-exports ---
pub use apply::apply_recursive;
pub use core::cell::Cell;
pub use core::hlc::Hlc;
pub use core::traits::{ContainerType, Type};
pub use delta::{Delta, PrimaryKey, Signature, TableId};
pub use error::TypeError;
pub use merge::merge_cells;
pub use path::{Path, PathStep};
pub use types::atom::{AtomFloat, AtomOp, AtomType, AtomValue};
pub use types::record::{RecordError, RecordOp, RecordSegment, RecordType, RecordValue};
// Value, Op, Segment, TypeTag are generated above by register_types!
