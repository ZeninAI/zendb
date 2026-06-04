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
pub mod core;
pub mod types;

use bincode::{Decode, Encode};

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
        #[derive(
            Debug,
            Clone,
            Copy,
            PartialEq,
            Eq,
            PartialOrd,
            Ord,
            Hash,
            Encode,
            Decode,
        )]
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
        #[derive(Debug, Clone, PartialEq, Encode, Decode)]
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
        // TypeOp
        // =================================================================
        #[derive(Debug, Clone, Encode, Decode)]
        pub enum TypeOp {
            $($leaf_var(<$leaf_ty as $crate::Type>::Op),)*
            $($cont_var(<$cont_ty as $crate::Type>::Op),)*
        }

        impl TypeOp {
            pub fn type_tag(&self) -> TypeTag {
                match self {
                    $(TypeOp::$leaf_var(_) => TypeTag::$leaf_var,)*
                    $(TypeOp::$cont_var(_) => TypeTag::$cont_var,)*
                }
            }
        }

        // =================================================================
        // Segment
        // =================================================================
        #[derive(Debug, Clone, Encode, Decode)]
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
        // Dispatch: apply_op
        // =================================================================
        pub(crate) fn apply_op_dispatch(
            value: &mut Value,
            op: &TypeOp,
            local_hlc: $crate::Hlc,
            op_hlc: $crate::Hlc,
        ) -> Result<bool, TypeError> {
            match (value, op) {
                $(
                    (Value::$leaf_var(v), TypeOp::$leaf_var(o)) => {
                        <$leaf_ty as $crate::Type>::apply_op(v, o, local_hlc, op_hlc)
                            .map_err(TypeError::$leaf_var)
                    }
                )*
                $(
                    (Value::$cont_var(v), TypeOp::$cont_var(o)) => {
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
            child_tag: Option<TypeTag>,
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
pub use core::hlc::{device_id, init_device_id, DeviceId, Hlc};
pub use core::op::Op;
pub use core::path::{Path, PathStep};
pub use core::traits::{ContainerType, Type};
pub use types::atom::{AtomFloat, AtomOp, AtomType, AtomValue};
pub use types::record::{RecordError, RecordOp, RecordSegment, RecordType, RecordValue};
// TypeTag, Value, TypeOp, Segment, TypeError are generated above by register_types!
