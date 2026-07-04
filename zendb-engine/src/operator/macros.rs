/// Generate a type-safe dispatch enum for a set of operators.
///
/// Produces `OperatorConfig`, `OperatorConfigVariant`, and `OperatorInstance`
/// (the generated enum that implements [`DispatchOperator`]).
///
/// ## Example
///
/// ```ignore
/// define_operator_set! {
///     pub mod ops {
///         FullTextIndex(FullTextIndexOperator),
///         MerkleTree(MerkleTreeOperator),
///         MyCustom(MyCustomOperator),
///     }
/// }
/// ```
///
/// This creates `ops::OperatorInstance`, `ops::OperatorConfig`, and
/// `ops::OperatorConfigVariant`. Use it as the `D` type parameter when creating
/// a [`Database`].
#[macro_export]
macro_rules! define_operator_set {
    (
        $vis:vis mod $module:ident {
            $( $variant:ident ( $operator:ty ) ),+ $(,)?
        }
    ) => {
        $crate::__zendb_with_prelude_operators! {
            $crate::__zendb_define_operator_set,
            $vis mod $module {
                $( $variant($operator), )+
            }
        }
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __zendb_define_operator_set {
    (
        $vis:vis mod $module:ident {
            $first_variant:ident ( $first_operator:ty )
            $(, $variant:ident ( $operator:ty ) )* $(,)?
        }
    ) => {
        $vis mod $module {
            #[allow(unused_imports)]
            use super::*;

            #[derive(Debug, Clone, PartialEq, ::bincode::Encode, ::bincode::Decode)]
            pub enum OperatorConfigVariant {
                $first_variant(<$first_operator as $crate::Operator>::Config),
                $( $variant(<$operator as $crate::Operator>::Config), )*
            }

            #[derive(Debug, Clone, PartialEq, ::bincode::Encode, ::bincode::Decode)]
            pub struct OperatorConfig {
                pub operator: OperatorConfigVariant,
                pub runtime: $crate::OperatorRuntimeConfig,
            }

            pub enum OperatorInstance {
                $first_variant(
                    $first_operator,
                    $crate::OperatorContext<$first_operator, OperatorInstance>,
                ),
                $(
                    $variant(
                        $operator,
                        $crate::OperatorContext<$operator, OperatorInstance>,
                    ),
                )*
            }

            impl $crate::DispatchConfig for OperatorConfig {
                fn runtime_config(&self) -> &$crate::OperatorRuntimeConfig {
                    &self.runtime
                }

                fn new<O>(
                    config: <O as $crate::Operator>::Config,
                    runtime: $crate::OperatorRuntimeConfig,
                ) -> ::std::io::Result<Self>
                where
                    O: $crate::Operator,
                {
                    let config = Box::new(config) as Box<dyn ::std::any::Any>;
                    let operator = if ::std::any::TypeId::of::<O>()
                        == ::std::any::TypeId::of::<$first_operator>()
                    {
                        OperatorConfigVariant::$first_variant(
                            *config
                                .downcast::<<$first_operator as $crate::Operator>::Config>()
                                .expect("operator config type checked by TypeId"),
                        )
                    }
                    $(
                        else if ::std::any::TypeId::of::<O>()
                            == ::std::any::TypeId::of::<$operator>()
                        {
                            OperatorConfigVariant::$variant(
                                *config
                                    .downcast::<<$operator as $crate::Operator>::Config>()
                                    .expect("operator config type checked by TypeId"),
                            )
                        }
                    )*
                    else {
                        return Err(::std::io::Error::new(
                            ::std::io::ErrorKind::InvalidInput,
                            format!(
                                "operator type {:?} is not registered in this dispatch set",
                                ::std::any::type_name::<O>(),
                            ),
                        ));
                    };

                    Ok(Self {
                        operator,
                        runtime,
                    })
                }
            }

            impl $crate::DispatchOperator for OperatorInstance {
                type Config = OperatorConfig;

                fn create<'a>(
                    db: ::std::sync::Weak<$crate::Database<Self>>,
                    name: &'a str,
                    config: &'a Self::Config,
                ) -> $crate::BoxFuture<'a, ::std::io::Result<Self>> {
                    match &config.operator {
                        OperatorConfigVariant::$first_variant(inner) => {
                            let ctx = $crate::OperatorContext::new(
                                db,
                                name.to_owned(),
                                inner.clone(),
                            );
                            Box::pin(async move {
                                let operator = <$first_operator as $crate::Operator>::create(&ctx).await?;
                                Ok(OperatorInstance::$first_variant(operator, ctx))
                            })
                        }
                        $(
                            OperatorConfigVariant::$variant(inner) => {
                                let ctx = $crate::OperatorContext::new(
                                    db,
                                    name.to_owned(),
                                    inner.clone(),
                                );
                                Box::pin(async move {
                                    let operator = <$operator as $crate::Operator>::create(&ctx).await?;
                                    Ok(OperatorInstance::$variant(operator, ctx))
                                })
                            }
                        )*
                    }
                }

                fn process<'a>(
                    &'a mut self,
                    changes: Vec<$crate::Change>,
                    _db: ::std::sync::Weak<$crate::Database<Self>>,
                    _name: &'a str,
                    _config: &'a Self::Config,
                ) -> $crate::BoxFuture<'a, ::std::io::Result<$crate::OperatorDirective>> {
                    match self {
                        OperatorInstance::$first_variant(inner, ctx) => {
                            <$first_operator as $crate::Operator>::process(inner, changes, ctx)
                        }
                        $(
                            OperatorInstance::$variant(inner, ctx) => {
                                <$operator as $crate::Operator>::process(inner, changes, ctx)
                            }
                        )*
                    }
                }

                fn on_input_opened<'a>(
                    &'a mut self,
                    table: String,
                    _db: ::std::sync::Weak<$crate::Database<Self>>,
                    _name: &'a str,
                    _config: &'a Self::Config,
                ) -> $crate::BoxFuture<'a, ::std::io::Result<$crate::OperatorDirective>> {
                    match self {
                        OperatorInstance::$first_variant(inner, ctx) => {
                            <$first_operator as $crate::Operator>::on_input_opened(
                                inner, table, ctx,
                            )
                        }
                        $(
                            OperatorInstance::$variant(inner, ctx) => {
                                <$operator as $crate::Operator>::on_input_opened(
                                    inner, table, ctx,
                                )
                            }
                        )*
                    }
                }

                fn on_input_closed<'a>(
                    &'a mut self,
                    table: String,
                    _db: ::std::sync::Weak<$crate::Database<Self>>,
                    _name: &'a str,
                    _config: &'a Self::Config,
                ) -> $crate::BoxFuture<'a, ::std::io::Result<$crate::OperatorDirective>> {
                    match self {
                        OperatorInstance::$first_variant(inner, ctx) => {
                            <$first_operator as $crate::Operator>::on_input_closed(
                                inner, table, ctx,
                            )
                        }
                        $(
                            OperatorInstance::$variant(inner, ctx) => {
                                <$operator as $crate::Operator>::on_input_closed(
                                    inner, table, ctx,
                                )
                            }
                        )*
                    }
                }

                fn on_timer<'a>(
                    &'a mut self,
                    payload: Vec<u8>,
                    fire_at_ms: u64,
                    _db: ::std::sync::Weak<$crate::Database<Self>>,
                    _name: &'a str,
                    _config: &'a Self::Config,
                ) -> $crate::BoxFuture<'a, ::std::io::Result<$crate::OperatorDirective>> {
                    match self {
                        OperatorInstance::$first_variant(inner, ctx) => {
                            Box::pin(async move {
                                let timer: <$first_operator as $crate::Operator>::Timer =
                                    ::bincode::decode_from_slice(
                                        &payload,
                                        ::bincode::config::standard(),
                                    )
                                    .map(|(timer, _)| timer)
                                    .map_err(|error| {
                                        ::std::io::Error::new(
                                            ::std::io::ErrorKind::InvalidData,
                                            error.to_string(),
                                        )
                                    })?;
                                <$first_operator as $crate::Operator>::on_timer(
                                    inner, timer, fire_at_ms, ctx,
                                )
                                .await
                            })
                        }
                        $(
                            OperatorInstance::$variant(inner, ctx) => {
                                Box::pin(async move {
                                    let timer: <$operator as $crate::Operator>::Timer =
                                        ::bincode::decode_from_slice(
                                            &payload,
                                            ::bincode::config::standard(),
                                        )
                                        .map(|(timer, _)| timer)
                                        .map_err(|error| {
                                            ::std::io::Error::new(
                                                ::std::io::ErrorKind::InvalidData,
                                                error.to_string(),
                                            )
                                        })?;
                                    <$operator as $crate::Operator>::on_timer(
                                        inner, timer, fire_at_ms, ctx,
                                    )
                                    .await
                                })
                            }
                        )*
                    }
                }

                fn teardown<'a>(
                    &'a mut self,
                    reason: &'a $crate::TeardownReason,
                    _db: ::std::sync::Weak<$crate::Database<Self>>,
                    _name: &'a str,
                    _config: &'a Self::Config,
                ) -> $crate::BoxFuture<'a, ::std::io::Result<()>> {
                    match self {
                        OperatorInstance::$first_variant(inner, ctx) => {
                            <$first_operator as $crate::Operator>::teardown(inner, reason, ctx)
                        }
                        $(
                            OperatorInstance::$variant(inner, ctx) => {
                                <$operator as $crate::Operator>::teardown(inner, reason, ctx)
                            }
                        )*
                    }
                }
            }
        }
    };
}
