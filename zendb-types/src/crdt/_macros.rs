//! Registration and runtime dispatch for CRDT values.

macro_rules! register_types {
    (
        $( key $key_var:ident => $key_ty:ty, )*
        $( leaf $leaf_var:ident => $leaf_ty:ty, )*
        $( container $cont_var:ident ($seg_ty:ty) => $cont_ty:ty, )*
    ) => {
        #[derive(
            Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash,
            ::bincode::Encode, ::bincode::Decode,
        )]
        pub enum TypeTag {
            $($leaf_var,)*
            $($cont_var,)*
        }

        impl TypeTag {
            pub const fn name(self) -> &'static str {
                match self {
                    $(Self::$leaf_var => stringify!($leaf_var),)*
                    $(Self::$cont_var => stringify!($cont_var),)*
                }
            }

            pub fn empty_value(self) -> Value {
                match self {
                    $(Self::$leaf_var => Value::$leaf_var(<$leaf_ty as Default>::default()),)*
                    $(Self::$cont_var => Value::$cont_var(<$cont_ty as Default>::default()),)*
                }
            }
        }

        impl std::fmt::Display for TypeTag {
            fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str(self.name())
            }
        }

        #[derive(
            Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash,
            ::bincode::Encode, ::bincode::Decode,
        )]
        pub enum PrimaryKey {
            $($key_var($key_ty),)*
        }

        #[derive(Debug)]
        pub enum TypeError {
            $($leaf_var(<$leaf_ty as $crate::Type>::Error),)*
            $($cont_var(<$cont_ty as $crate::Type>::Error),)*
            TypeMismatch { expected: TypeTag, actual: TypeTag },
            MergeConflict { current: TypeTag, incoming: TypeTag },
        }

        impl std::fmt::Display for TypeError {
            fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                match self {
                    $(Self::$leaf_var(error) => write!(formatter, "{}({error})", TypeTag::$leaf_var),)*
                    $(Self::$cont_var(error) => write!(formatter, "{}({error})", TypeTag::$cont_var),)*
                    Self::TypeMismatch { expected, actual } => {
                        write!(formatter, "type mismatch: expected {expected}, got {actual}")
                    }
                    Self::MergeConflict { current, incoming } => {
                        write!(formatter, "merge conflict: {current} vs {incoming}")
                    }
                }
            }
        }

        impl std::error::Error for TypeError {
            fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
                match self {
                    $(Self::$leaf_var(error) => Some(error),)*
                    $(Self::$cont_var(error) => Some(error),)*
                    _ => None,
                }
            }
        }

        #[derive(Debug, Clone, PartialEq, ::bincode::Encode, ::bincode::Decode)]
        pub enum Value {
            $($leaf_var($leaf_ty),)*
            $($cont_var($cont_ty),)*
        }

        impl Value {
            pub fn type_tag(&self) -> TypeTag {
                match self {
                    $(Self::$leaf_var(_) => TypeTag::$leaf_var,)*
                    $(Self::$cont_var(_) => TypeTag::$cont_var,)*
                }
            }
        }

        impl $crate::Type for Value {
            type Op = TypeOp;
            type Error = TypeError;

            fn apply(
                &mut self,
                op: &TypeOp,
                stamps: $crate::MergeStamps,
            ) -> Result<bool, TypeError> {
                match (self, op) {
                    $((Value::$leaf_var(value), TypeOp::$leaf_var(op)) =>
                        value.apply(op, stamps).map_err(TypeError::$leaf_var),)*
                    $((Value::$cont_var(value), TypeOp::$cont_var(op)) =>
                        value.apply(op, stamps).map_err(TypeError::$cont_var),)*
                    (value, op) => Err(TypeError::TypeMismatch {
                        expected: value.type_tag(),
                        actual: op.type_tag(),
                    }),
                }
            }

            fn merge(
                &mut self,
                incoming: &Value,
                stamps: $crate::MergeStamps,
            ) -> Result<bool, TypeError> {
                match (self, incoming) {
                    $((Value::$leaf_var(current), Value::$leaf_var(incoming)) =>
                        current.merge(incoming, stamps).map_err(TypeError::$leaf_var),)*
                    $((Value::$cont_var(current), Value::$cont_var(incoming)) =>
                        current.merge(incoming, stamps).map_err(TypeError::$cont_var),)*
                    (current, incoming) => Err(TypeError::MergeConflict {
                        current: current.type_tag(),
                        incoming: incoming.type_tag(),
                    }),
                }
            }

            fn apply_stamp(
                &self,
                stamps: $crate::MergeStamps,
                changed: bool,
            ) -> Option<$crate::EventStamp> {
                match self {
                    $(Value::$leaf_var(value) => value.apply_stamp(stamps, changed),)*
                    $(Value::$cont_var(value) => value.apply_stamp(stamps, changed),)*
                }
            }

            fn merge_stamp(
                &self,
                stamps: $crate::MergeStamps,
                changed: bool,
            ) -> Option<$crate::EventStamp> {
                match self {
                    $(Value::$leaf_var(value) => value.merge_stamp(stamps, changed),)*
                    $(Value::$cont_var(value) => value.merge_stamp(stamps, changed),)*
                }
            }

            fn max_stamp(&self) -> $crate::EventStamp {
                match self {
                    $(Value::$leaf_var(value) => value.max_stamp(),)*
                    $(Value::$cont_var(value) => value.max_stamp(),)*
                }
            }
        }

        impl $crate::ContainerType for Value {
            fn child(&self, segment: &$crate::Segment) -> Option<&$crate::Cell> {
                match self {
                    $(Value::$cont_var(value) => value.child(segment),)*
                    _ => None,
                }
            }

            fn child_mut(&mut self, segment: &$crate::Segment) -> Option<&mut $crate::Cell> {
                match self {
                    $(Value::$cont_var(value) => value.child_mut(segment),)*
                    _ => None,
                }
            }

            fn apply_walk(
                &mut self,
                op: &$crate::Op,
                stamps: $crate::MergeStamps,
                path: &[$crate::Segment],
            ) -> Result<bool, TypeError> {
                match self {
                    $(Value::$cont_var(value) =>
                        value.apply_walk(op, stamps, path).map_err(TypeError::$cont_var),)*
                    _ => Ok(false),
                }
            }
        }

        #[derive(Debug, Clone, ::bincode::Encode, ::bincode::Decode)]
        pub enum TypeOp {
            $($leaf_var(<$leaf_ty as $crate::Type>::Op),)*
            $($cont_var(<$cont_ty as $crate::Type>::Op),)*
        }

        impl TypeOp {
            pub fn type_tag(&self) -> TypeTag {
                match self {
                    $(Self::$leaf_var(_) => TypeTag::$leaf_var,)*
                    $(Self::$cont_var(_) => TypeTag::$cont_var,)*
                }
            }
        }

        #[derive(Debug, Clone, PartialEq, Eq, ::bincode::Encode, ::bincode::Decode)]
        pub enum Segment {
            $($cont_var($seg_ty),)*
        }

        impl Segment {
            pub fn type_tag(&self) -> TypeTag {
                match self {
                    $(Self::$cont_var(_) => TypeTag::$cont_var,)*
                }
            }
        }
    };
}
