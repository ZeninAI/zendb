//! # zendb-types
//!
//! Core type system for ZeninDB — an embedded, local-first, eventually
//! consistent database with first-class collaborative editing.
//!
//! ## Adding a type
//!
//! 1. Create a module (e.g., `src/map.rs`) with a value struct implementing
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
        $( key $key_var:ident => $key_ty:ty, )*
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
                    $(TypeTag::$leaf_var => stringify!($leaf_var),)*
                    $(TypeTag::$cont_var => stringify!($cont_var),)*
                }
            }

            /// Produce an empty value for this type tag.
            pub fn empty_value(self) -> Value {
                match self {
                    $(TypeTag::$leaf_var => Value::$leaf_var(<$leaf_ty as Default>::default()),)*
                    $(TypeTag::$cont_var => Value::$cont_var(<$cont_ty as Default>::default()),)*
                }
            }
        }

        impl std::fmt::Display for TypeTag {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(self.name())
            }
        }

        // =================================================================
        // PrimaryKey
        // =================================================================
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Encode, Decode)]
        pub enum PrimaryKey {
            $($key_var($key_ty),)*
        }

        impl PrimaryKey {
            pub fn type_tag(&self) -> TypeTag {
                match self {
                    $(PrimaryKey::$key_var(_) => TypeTag::$key_var,)*
                }
            }
        }

        // =================================================================
        // TypeError — per-type variants + cross-cutting errors
        // =================================================================
        #[derive(Debug)]
        pub enum TypeError {
            $($leaf_var(<$leaf_ty as $crate::Type>::Error),)*
            $($cont_var(<$cont_ty as $crate::Type>::Error),)*
            TypeMismatch { expected: TypeTag, actual: TypeTag },
            MergeConflict { local: TypeTag, remote: TypeTag },
        }

        impl std::fmt::Display for TypeError {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                match self {
                    $(TypeError::$leaf_var(e) => write!(f, "{}({})", TypeTag::$leaf_var.name(), e),)*
                    $(TypeError::$cont_var(e) => write!(f, "{}({})", TypeTag::$cont_var.name(), e),)*
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
            $($leaf_var($leaf_ty),)*
            $($cont_var($cont_ty),)*
        }

        impl Value {
            pub fn type_tag(&self) -> TypeTag {
                match self {
                    $(Value::$leaf_var(_) => TypeTag::$leaf_var,)*
                    $(Value::$cont_var(_) => TypeTag::$cont_var,)*
                }
            }
        }

        impl $crate::Type for Value {
            type Op = TypeOp;
            type Error = TypeError;

            fn apply(
                &mut self,
                op: &TypeOp,
                local_hlc: $crate::Hlc,
                op_hlc: $crate::Hlc,
            ) -> Result<bool, TypeError> {
                match (self, op) {
                    $(
                        (Value::$leaf_var(v), TypeOp::$leaf_var(o)) => {
                            v.apply(o, local_hlc, op_hlc)
                                .map_err(TypeError::$leaf_var)
                        }
                    )*
                    $(
                        (Value::$cont_var(v), TypeOp::$cont_var(o)) => {
                            v.apply(o, local_hlc, op_hlc)
                                .map_err(TypeError::$cont_var)
                        }
                    )*
                    (v, o) => Err(TypeError::TypeMismatch {
                        expected: v.type_tag(),
                        actual: o.type_tag(),
                    }),
                }
            }

            fn merge(
                &mut self,
                remote: &Value,
                local_hlc: $crate::Hlc,
                remote_hlc: $crate::Hlc,
            ) -> Result<bool, TypeError> {
                match (self, remote) {
                    $(
                        (Value::$leaf_var(l), Value::$leaf_var(r)) => {
                            $crate::Type::merge(l, r, local_hlc, remote_hlc)
                                .map_err(TypeError::$leaf_var)
                        }
                    )*
                    $(
                        (Value::$cont_var(l), Value::$cont_var(r)) => {
                            $crate::Type::merge(l, r, local_hlc, remote_hlc)
                                .map_err(TypeError::$cont_var)
                        }
                    )*
                    (l, r) => Err(TypeError::MergeConflict {
                        local: l.type_tag(),
                        remote: r.type_tag(),
                    }),
                }
            }

            fn max_hlc(&self) -> $crate::Hlc {
                match self {
                    $(Value::$leaf_var(v) => v.max_hlc(),)*
                    $(Value::$cont_var(v) => v.max_hlc(),)*
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

        impl $crate::ContainerType for Value {
            type Segment = Segment;

            fn child_or_default<'a>(
                &'a mut self,
                segment: &Segment,
                child_tag: Option<TypeTag>,
            ) -> Result<&'a mut $crate::Cell, TypeError> {
                let tag = self.type_tag();
                match (self, segment) {
                    $(
                        (Value::$cont_var(v), Segment::$cont_var(s)) => {
                            v.child_or_default(s, child_tag)
                                .map_err(TypeError::$cont_var)
                        }
                    )*
                    _ => Err(TypeError::TypeMismatch {
                        expected: segment.type_tag(),
                        actual: tag,
                    }),
                }
            }
        }
    };
}

// --- invoke the macro ---
register_types! {
    key Bool => crate::types::bool::Bool,
    key Int => crate::types::int::Int,
    key String => crate::types::string::String,
    key Timestamp => crate::types::timestamp::Timestamp,
    key Blob => crate::types::blob::Blob,
    leaf Bool => crate::types::bool::Bool,
    leaf Int => crate::types::int::Int,
    leaf String => crate::types::string::String,
    leaf Timestamp => crate::types::timestamp::Timestamp,
    leaf Blob => crate::types::blob::Blob,
    leaf Counter => crate::types::counter::Counter,
    leaf MvRegister => crate::types::mv_register::MvRegister,
    leaf OrSet => crate::types::or_set::OrSet,
    leaf PriorityQueue => crate::types::priority_queue::PriorityQueue,
    leaf Set => crate::types::set::Set,
    leaf Text => crate::types::text::Text,
    container Record => crate::types::record::Record,
    container List => crate::types::list::List,
}

// --- re-exports ---
pub use core::cell::Cell;
pub use core::delta::{Delta, Signature, TableId};
pub use core::hlc::{device_id, init_device_id, DeviceId, Hlc};
pub use core::op::Op;
pub use core::path::{Path, PathStep};
pub use core::traits::{ContainerType, Type};
pub use types::blob::{Blob, BlobError, BlobOp};
pub use types::bool::{Bool, BoolError, BoolOp};
pub use types::counter::{counter_value, Counter, CounterError, CounterOp};
pub use types::int::{Int, IntError, IntOp};
pub use types::list::{
    list_cell_at, list_id_at, list_visible_ids, List, ListEntry, ListError, ListId, ListOp,
    ListSegment,
};
pub use types::mv_register::{mv_register_values, MvRegister, MvRegisterError, MvRegisterOp};
pub use types::or_set::{or_set_contains_key, or_set_keys, OrSet, OrSetEntry, OrSetError, OrSetOp};
pub use types::priority_queue::{pq_live, PqEntry, PqError, PqOp, PriorityQueue};
pub use types::record::{Record, RecordError, RecordOp, RecordSegment};
pub use types::set::{set_contains_key, set_keys, Meta, Set, SetError, SetOp};
pub use types::string::{String, StringError, StringOp};
pub use types::text::{
    text_format_at, text_id_at, text_string, text_visible_ids, Text, TextEntry, TextError, TextId,
    TextOp,
};
pub use types::timestamp::{Timestamp, TimestampError, TimestampOp};
// TypeTag, Value, TypeOp, Segment, TypeError are generated above by register_types!
