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
pub mod codec;
pub mod core;
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

            pub fn from_u8(v: u8) -> Result<TypeTag, TypeError> {
                match v {
                    $(v if v == TypeTag::$leaf_var as u8 => Ok(TypeTag::$leaf_var),)*
                    $(v if v == TypeTag::$cont_var as u8 => Ok(TypeTag::$cont_var),)*
                    other => Err(TypeError::UnknownTypeTag(other)),
                }
            }

            /// Produce an empty value for this type tag.
            pub fn empty_value(self) -> Value {
                match self {
                    $(TypeTag::$leaf_var => Value::$leaf_var(<$leaf_ty as $crate::Type>::empty()),)*
                    $(TypeTag::$cont_var => Value::$cont_var(<$cont_ty as $crate::Type>::empty()),)*
                }
            }
        }

        impl std::fmt::Display for TypeTag {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(self.name())
            }
        }

        // =================================================================
        // TypeError — per-type variants + cross-cutting errors
        // =================================================================
        #[derive(Debug)]
        pub enum TypeError {
            $($leaf_var(<$leaf_ty as $crate::Type>::Error),)*
            $($cont_var(<$cont_ty as $crate::Type>::Error),)*
            UnknownTypeTag(u8),
            TypeMismatch { expected: TypeTag, actual: TypeTag },
            EncodeError(String),
            DecodeError(String),
            MergeConflict { local: TypeTag, remote: TypeTag },
        }

        impl std::fmt::Display for TypeError {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                match self {
                    $(TypeError::$leaf_var(e) => write!(f, "{}({})", TypeTag::$leaf_var.name(), e),)*
                    $(TypeError::$cont_var(e) => write!(f, "{}({})", TypeTag::$cont_var.name(), e),)*
                    TypeError::UnknownTypeTag(tag) => write!(f, "unknown type tag: {}", tag),
                    TypeError::TypeMismatch { expected, actual } => {
                        write!(f, "type mismatch: expected {:?}, got {:?}", expected, actual)
                    }
                    TypeError::EncodeError(msg) => write!(f, "encode error: {}", msg),
                    TypeError::DecodeError(msg) => write!(f, "decode error: {}", msg),
                    TypeError::MergeConflict { local, remote } => {
                        write!(f, "merge conflict: {:?} vs {:?}", local, remote)
                    }
                }
            }
        }

        impl std::error::Error for TypeError {
            fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
                match self {
                    $(TypeError::$leaf_var(e) => Some(e),)*
                    $(TypeError::$cont_var(e) => Some(e),)*
                    _ => None,
                }
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

            /// Encode as `TypeTag TypeSpecificPayload`.
            pub fn encode(&self, out: &mut Vec<u8>) -> Result<(), TypeError> {
                match self {
                    $(Value::$leaf_var(v) => {
                        out.push(TypeTag::$leaf_var as u8);
                        $crate::TypedValue::encode(v, out)
                            .map_err(|e| TypeError::$leaf_var(e))
                    })*
                    $(Value::$cont_var(v) => {
                        out.push(TypeTag::$cont_var as u8);
                        $crate::TypedValue::encode(v, out)
                            .map_err(|e| TypeError::$cont_var(e))
                    })*
                }
            }

            /// Decode from bytes. Returns value and bytes consumed.
            pub fn decode(bytes: &[u8]) -> Result<(Value, usize), TypeError> {
                if bytes.is_empty() {
                    return Err(TypeError::DecodeError("empty input".into()));
                }
                let tag = TypeTag::from_u8(bytes[0])?;
                match tag {
                    $(TypeTag::$leaf_var => {
                        let (v, n) = < <$leaf_ty as $crate::Type>::Value as $crate::TypedValue >::decode(&bytes[1..])
                            .map_err(|e| TypeError::$leaf_var(e))?;
                        Ok((Value::$leaf_var(v), 1 + n))
                    })*
                    $(TypeTag::$cont_var => {
                        let (v, n) = < <$cont_ty as $crate::Type>::Value as $crate::TypedValue >::decode(&bytes[1..])
                            .map_err(|e| TypeError::$cont_var(e))?;
                        Ok((Value::$cont_var(v), 1 + n))
                    })*
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

            /// Encode as `TypeTag TypeSpecificOpPayload` (or `0xFF` for SetSync).
            pub fn encode(&self, out: &mut Vec<u8>) -> Result<(), TypeError> {
                match self {
                    $(Op::$leaf_var(o) => {
                        out.push(TypeTag::$leaf_var as u8);
                        $crate::TypedOp::encode(o, out)
                            .map_err(|e| TypeError::$leaf_var(e))
                    })*
                    $(Op::$cont_var(o) => {
                        out.push(TypeTag::$cont_var as u8);
                        $crate::TypedOp::encode(o, out)
                            .map_err(|e| TypeError::$cont_var(e))
                    })*
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

            /// Decode from bytes. Returns op and bytes consumed.
            pub fn decode(bytes: &[u8]) -> Result<(Op, usize), TypeError> {
                if bytes.is_empty() {
                    return Err(TypeError::DecodeError("empty input".into()));
                }
                if bytes[0] == 0xFF {
                    if bytes.len() < 2 {
                        return Err(TypeError::DecodeError("truncated SetSync".into()));
                    }
                    let sync = match bytes[1] {
                        0x00 => None,
                        0x01 => Some(false),
                        0x02 => Some(true),
                        b => return Err(TypeError::DecodeError(format!("invalid SetSync byte: {}", b))),
                    };
                    return Ok((Op::SetSync { sync }, 2));
                }
                let tag = TypeTag::from_u8(bytes[0])?;
                match tag {
                    $(TypeTag::$leaf_var => {
                        let (o, n) = < <$leaf_ty as $crate::Type>::Op as $crate::TypedOp >::decode(&bytes[1..])
                            .map_err(|e| TypeError::$leaf_var(e))?;
                        Ok((Op::$leaf_var(o), 1 + n))
                    })*
                    $(TypeTag::$cont_var => {
                        let (o, n) = < <$cont_ty as $crate::Type>::Op as $crate::TypedOp >::decode(&bytes[1..])
                            .map_err(|e| TypeError::$cont_var(e))?;
                        Ok((Op::$cont_var(o), 1 + n))
                    })*
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

            /// Encode as `TypeTag TypeSpecificSegmentPayload`.
            pub fn encode(&self, out: &mut Vec<u8>) -> Result<(), TypeError> {
                match self {
                    $(Segment::$cont_var(s) => {
                        out.push(TypeTag::$cont_var as u8);
                        $crate::TypedSegment::encode(s, out)
                            .map_err(|e| TypeError::$cont_var(e))
                    })*
                }
            }

            /// Decode from bytes. Returns segment and bytes consumed.
            pub fn decode(bytes: &[u8]) -> Result<(Segment, usize), TypeError> {
                if bytes.is_empty() {
                    return Err(TypeError::DecodeError("empty input".into()));
                }
                let tag = TypeTag::from_u8(bytes[0])?;
                match tag {
                    $(TypeTag::$cont_var => {
                        let (s, n) = < <$cont_ty as $crate::ContainerType>::Segment as $crate::TypedSegment >::decode(&bytes[1..])
                            .map_err(|e| TypeError::$cont_var(e))?;
                        Ok((Segment::$cont_var(s), 1 + n))
                    })*
                    other => Err(TypeError::DecodeError(
                        format!("type {:?} has no Segment variant", other)
                    )),
                }
            }
        }

        // =================================================================
        // Dispatch: apply_op
        // =================================================================
        pub(crate) fn apply_op_dispatch(
            value: &mut Value,
            op: &Op,
            local_hlc: $crate::Hlc,
            op_hlc: $crate::Hlc,
        ) -> Result<bool, TypeError> {
            match (value, op) {
                $(
                    (Value::$leaf_var(v), Op::$leaf_var(o)) => {
                        <$leaf_ty as $crate::Type>::apply_op(v, o, local_hlc, op_hlc)
                            .map_err(TypeError::$leaf_var)
                    }
                )*
                $(
                    (Value::$cont_var(v), Op::$cont_var(o)) => {
                        <$cont_ty as $crate::Type>::apply_op(v, o, local_hlc, op_hlc)
                            .map_err(TypeError::$cont_var)
                    }
                )*
                (v, o) => Err(TypeError::TypeMismatch {
                    expected: v.type_tag(),
                    actual: o.type_tag(),
                }),
            }
        }

        // =================================================================
        // Dispatch: merge
        // =================================================================
        pub(crate) fn merge_dispatch(
            local: &mut Value,
            local_hlc: $crate::Hlc,
            remote: &Value,
            remote_hlc: $crate::Hlc,
        ) -> Result<bool, TypeError> {
            match (local, remote) {
                $(
                    (Value::$leaf_var(l), Value::$leaf_var(r)) => {
                        <$leaf_ty as $crate::Type>::merge(l, local_hlc, r, remote_hlc)
                            .map_err(TypeError::$leaf_var)
                    }
                )*
                $(
                    (Value::$cont_var(l), Value::$cont_var(r)) => {
                        <$cont_ty as $crate::Type>::merge(l, local_hlc, r, remote_hlc)
                            .map_err(TypeError::$cont_var)
                    }
                )*
                (l, r) => Err(TypeError::MergeConflict {
                    local: l.type_tag(),
                    remote: r.type_tag(),
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
        ) -> Result<&'a mut $crate::Cell, TypeError> {
            let tag = value.type_tag();
            match (value, segment) {
                $(
                    (Value::$cont_var(v), Segment::$cont_var(s)) => {
                        <$cont_ty as $crate::ContainerType>::descend_or_create(v, s, child_tag)
                            .map_err(TypeError::$cont_var)
                    }
                )*
                _ => Err(TypeError::TypeMismatch {
                    expected: segment.type_tag(),
                    actual: tag,
                }),
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
pub use core::cell::Cell;
pub use core::delta::{Delta, PrimaryKey, Signature, TableId};
pub use core::hlc::Hlc;
pub use core::path::{Path, PathStep};
pub use core::traits::{ContainerType, Type, TypedOp, TypedSegment, TypedValue};
pub use types::atom::{AtomFloat, AtomOp, AtomType, AtomValue};
pub use types::record::{RecordError, RecordOp, RecordSegment, RecordType, RecordValue};
// TypeTag, Value, Op, Segment, TypeError are generated above by register_types!
