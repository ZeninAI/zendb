//! Macro definitions for CRDT type registration and dispatch.
//!
//! The `register_types!` macro generates the complete type dispatch layer:
//! - `TypeTag` enum for runtime type identification
//! - `PrimaryKey` enum for table keys
//! - `Value` enum for runtime value dispatch
//! - `TypeOp` enum for operation dispatch
//! - `Segment` enum for container path segments
//! - `TypeError` enum for unified error handling
//!
//! This allows adding new CRDT types with minimal boilerplate - just implement
//! the `Type` trait and add one line to the macro invocation.

/// Generate the complete CRDT type dispatch layer.
///
/// # Syntax
///
/// ```ignore
/// register_types! {
///     key TypeName => path::to::Type,    // Types that can be primary keys
///     leaf TypeName => path::to::Type,   // Scalar CRDT types
///     container TypeName(SegmentType) => path::to::Type,  // Container types
/// }
/// ```
///
/// # Generated Types
///
/// - `TypeTag`: Enum identifying each registered type
/// - `PrimaryKey`: Enum for table primary keys
/// - `Value`: Enum for runtime value dispatch
/// - `TypeOp`: Enum for operation dispatch
/// - `Segment`: Enum for container path segments
/// - `TypeError`: Enum for unified error handling
#[macro_export]
macro_rules! register_types {
    (
        $( key $key_var:ident => $key_ty:ty, )*
        $( leaf $leaf_var:ident => $leaf_ty:ty, )*
        $( container $cont_var:ident ($seg_ty:ty) => $cont_ty:ty, )*
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
            ::bincode::Encode,
            ::bincode::Decode,
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
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, ::bincode::Encode, ::bincode::Decode)]
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
        #[derive(Debug, Clone, PartialEq, ::bincode::Encode, ::bincode::Decode)]
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
                op_hlc: $crate::Hlc,
            ) -> Result<bool, TypeError> {
                match (self, op) {
                    $(
                        (Value::$leaf_var(v), TypeOp::$leaf_var(o)) => {
                            v.apply(o, op_hlc)
                                .map_err(TypeError::$leaf_var)
                        }
                    )*
                    $(
                        (Value::$cont_var(v), TypeOp::$cont_var(o)) => {
                            v.apply(o, op_hlc)
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
                clocks: $crate::MergeClocks,
            ) -> Result<bool, TypeError> {
                match (self, remote) {
                    $(
                        (Value::$leaf_var(l), Value::$leaf_var(r)) => {
                            $crate::Type::merge(l, r, clocks)
                                .map_err(TypeError::$leaf_var)
                        }
                    )*
                    $(
                        (Value::$cont_var(l), Value::$cont_var(r)) => {
                            $crate::Type::merge(l, r, clocks)
                                .map_err(TypeError::$cont_var)
                        }
                    )*
                    (l, r) => Err(TypeError::MergeConflict {
                        local: l.type_tag(),
                        remote: r.type_tag(),
                    }),
                }
            }

            fn is_synced(&self, inherited: bool, path: &[$crate::PathStep]) -> bool {
                match self {
                    $(Value::$leaf_var(v) => v.is_synced(inherited, path),)*
                    $(Value::$cont_var(v) => v.is_synced(inherited, path),)*
                }
            }

            fn compact(
                &mut self,
                watermark: $crate::Hlc,
            ) -> Result<bool, TypeError> {
                match self {
                    $(Value::$leaf_var(v) => v.compact(watermark).map_err(TypeError::$leaf_var),)*
                    $(Value::$cont_var(v) => v.compact(watermark).map_err(TypeError::$cont_var),)*
                }
            }

            fn max_hlc(&self) -> $crate::Hlc {
                match self {
                    $(Value::$leaf_var(v) => v.max_hlc(),)*
                    $(Value::$cont_var(v) => v.max_hlc(),)*
                }
            }
        }

        impl $crate::ContainerType for Value {
            fn apply_walk(
                &mut self,
                op: &$crate::Op,
                op_hlc: $crate::Hlc,
                path: &[$crate::PathStep],
            ) -> Result<bool, TypeError> {
                match self {
                    $(Value::$cont_var(v) => {
                        $crate::ContainerType::apply_walk(v, op, op_hlc, path)
                            .map_err(TypeError::$cont_var)
                    },)*
                    _ => Ok(false),
                }
            }
        }

        // =================================================================
        // TypeOp
        // =================================================================
        #[derive(Debug, Clone, ::bincode::Encode, ::bincode::Decode)]
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
        #[derive(Debug, Clone, ::bincode::Encode, ::bincode::Decode)]
        pub enum Segment {
            $($cont_var($seg_ty),)*
        }

        impl Segment {
            pub fn type_tag(&self) -> TypeTag {
                match self {
                    $(Segment::$cont_var(_) => TypeTag::$cont_var,)*
                }
            }
        }

    };
}
