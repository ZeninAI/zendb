//! Closed registry that connects generated type operations to portable enums.

macro_rules! register_types {
    (
        $( key $key_var:ident => $key_ty:ty, )*
        $( leaf $leaf_var:ident => $leaf_ty:ty, )*
        $( container $cont_var:ident => $cont_ty:ty, )*
    ) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, ::bincode::Encode, ::bincode::Decode)]
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
                let mut value = match self {
                    $(Self::$leaf_var => Value::$leaf_var(Default::default()),)*
                    $(Self::$cont_var => Value::$cont_var(Default::default()),)*
                };
                <$crate::Value as $crate::TypeMetadata>::set_event_time(
                    &mut value,
                    $crate::EventTime::ZERO,
                );
                value
            }
        }

        impl std::fmt::Display for TypeTag {
            fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str(self.name())
            }
        }

        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, ::bincode::Encode, ::bincode::Decode)]
        pub enum PrimaryKey {
            $($key_var($key_ty),)*
        }

        #[derive(Debug)]
        pub enum TypeError {
            $($leaf_var(<$leaf_ty as $crate::Type>::Error),)*
            $($cont_var(<$cont_ty as $crate::Type>::Error),)*
            TypeMismatch($crate::TypeMismatch),
        }

        impl std::fmt::Display for TypeError {
            fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                match self {
                    $(Self::$leaf_var(error) => write!(formatter, "{}({error})", TypeTag::$leaf_var),)*
                    $(Self::$cont_var(error) => write!(formatter, "{}({error})", TypeTag::$cont_var),)*
                    Self::TypeMismatch(error) => error.fmt(formatter),
                }
            }
        }

        impl std::error::Error for TypeError {
            fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
                match self {
                    $(Self::$leaf_var(error) => Some(error),)*
                    $(Self::$cont_var(error) => Some(error),)*
                    Self::TypeMismatch(error) => Some(error),
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

        impl $crate::OpDispatcher for Value {
            fn dispatch(
                &mut self,
                remote: $crate::EventTime,
                path: &[$crate::Segment],
                op: &TypeOp,
            ) -> Result<bool, TypeError> {
                let Some((segment, remaining)) = path.split_first() else {
                    return <$crate::Value as $crate::Type>::apply(self, remote, op);
                };
                let expected = remaining
                    .first()
                    .map($crate::Segment::type_tag)
                    .unwrap_or_else(|| op.type_tag());
                {
                    let Some(child) = <$crate::Value as $crate::ContainerType>::ensure_child(
                        self,
                        remote,
                        segment,
                        expected,
                    )? else {
                        return Ok(false);
                    };
                    <$crate::Value as $crate::OpDispatcher>::dispatch(
                        child,
                        remote,
                        remaining,
                        op,
                    )
                }
            }
        }

        impl $crate::OpDispatcher for Option<Value> {
            fn dispatch(
                &mut self,
                remote: $crate::EventTime,
                path: &[$crate::Segment],
                op: &TypeOp,
            ) -> Result<bool, TypeError> {
                let expected = path
                    .first()
                    .map($crate::Segment::type_tag)
                    .unwrap_or_else(|| op.type_tag());
                let value = self.get_or_insert_with(|| expected.empty_value());
                <$crate::Value as $crate::OpDispatcher>::dispatch(value, remote, path, op)
            }
        }

        $(impl From<$leaf_ty> for Value { fn from(value: $leaf_ty) -> Self { Self::$leaf_var(value) } })*
        $(impl From<$cont_ty> for Value { fn from(value: $cont_ty) -> Self { Self::$cont_var(value) } })*

        $(impl ::std::convert::TryFrom<&Value> for $leaf_ty {
            type Error = $crate::TypeMismatch;
            fn try_from(value: &Value) -> Result<Self, Self::Error> {
                match value {
                    Value::$leaf_var(value) => Ok(value.clone()),
                    value => Err($crate::TypeMismatch::new(TypeTag::$leaf_var, value.type_tag())),
                }
            }
        })*
        $(impl ::std::convert::TryFrom<&Value> for $cont_ty {
            type Error = $crate::TypeMismatch;
            fn try_from(value: &Value) -> Result<Self, Self::Error> {
                match value {
                    Value::$cont_var(value) => Ok(value.clone()),
                    value => Err($crate::TypeMismatch::new(TypeTag::$cont_var, value.type_tag())),
                }
            }
        })*

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
            $($cont_var(<$cont_ty as $crate::ContainerType>::Segment),)*
        }

        impl Segment {
            pub fn type_tag(&self) -> TypeTag {
                match self {
                    $(Self::$cont_var(_) => TypeTag::$cont_var,)*
                }
            }
        }

        impl $crate::Type for Value {
            type Op = TypeOp;
            type Error = TypeError;

            fn apply(
                &mut self,
                remote: $crate::EventTime,
                op: &Self::Op,
            ) -> Result<bool, Self::Error> {
                let expected = op.type_tag();
                if self.type_tag() != expected {
                    if remote <= <$crate::Value as $crate::Type>::event_time(self) {
                        return Ok(false);
                    }
                    *self = expected.empty_value();
                }
                match (self, op) {
                    $((Self::$leaf_var(value), TypeOp::$leaf_var(op)) =>
                        <$leaf_ty as $crate::Type>::apply(value, remote, op)
                            .map_err(TypeError::$leaf_var),)*
                    $((Self::$cont_var(value), TypeOp::$cont_var(op)) =>
                        <$cont_ty as $crate::Type>::apply(value, remote, op)
                            .map_err(TypeError::$cont_var),)*
                    (value, op) => Err(TypeError::TypeMismatch($crate::TypeMismatch::new(
                        value.type_tag(),
                        op.type_tag(),
                    ))),
                }
            }

            fn event_time(&self) -> $crate::EventTime {
                match self {
                    $(Self::$leaf_var(value) => <$leaf_ty as $crate::Type>::event_time(value),)*
                    $(Self::$cont_var(value) => <$cont_ty as $crate::Type>::event_time(value),)*
                }
            }

            fn is_tombstone(&self) -> bool {
                match self {
                    $(Self::$leaf_var(value) => <$leaf_ty as $crate::Type>::is_tombstone(value),)*
                    $(Self::$cont_var(value) => <$cont_ty as $crate::Type>::is_tombstone(value),)*
                }
            }

        }

        impl $crate::TypeMetadata for Value {
            fn set_event_time(&mut self, time: $crate::EventTime) {
                match self {
                    $(Self::$leaf_var(value) => <$leaf_ty as $crate::TypeMetadata>::set_event_time(value, time),)*
                    $(Self::$cont_var(value) => <$cont_ty as $crate::TypeMetadata>::set_event_time(value, time),)*
                }
            }

            fn set_tombstone(&mut self, tombstone: bool) {
                match self {
                    $(Self::$leaf_var(value) => <$leaf_ty as $crate::TypeMetadata>::set_tombstone(value, tombstone),)*
                    $(Self::$cont_var(value) => <$cont_ty as $crate::TypeMetadata>::set_tombstone(value, tombstone),)*
                }
            }
        }

        impl $crate::ContainerType for Value {
            type Segment = Segment;

            fn ensure_child(
                &mut self,
                remote: $crate::EventTime,
                segment: &Self::Segment,
                expected: TypeTag,
            ) -> Result<Option<&mut $crate::Value>, Self::Error> {
                let container_tag = segment.type_tag();
                if self.type_tag() != container_tag {
                    if remote <= <$crate::Value as $crate::Type>::event_time(self) {
                        return Ok(None);
                    }
                    *self = container_tag.empty_value();
                }
                match (self, segment) {
                    $((Self::$cont_var(value), Segment::$cont_var(segment)) =>
                        <$cont_ty as $crate::ContainerType>::ensure_child(
                            value,
                            remote,
                            segment,
                            expected,
                        )
                        .map_err(TypeError::$cont_var),)*
                    _ => Ok(None),
                }
            }

        }
    };
}
