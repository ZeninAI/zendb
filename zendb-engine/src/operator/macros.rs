/// Generate a type-safe dispatch enum for a set of operators.
///
/// Produces `OperatorKind`, `OperatorConfig`, `OperatorConfigVariant`, and
/// `OperatorInstance` (the generated enum that implements [`DispatchOperator`]).
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
/// This creates `ops::OperatorInstance`, `ops::OperatorKind`,
/// `ops::OperatorConfig`, and `ops::OperatorConfigVariant`. Use it as the `D`
/// type parameter when creating a [`Database`].
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
            pub enum OperatorKind {
                $first_variant,
                $( $variant, )*
            }

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
                    Option<$crate::OperatorContext<$first_operator, OperatorInstance>>,
                ),
                $(
                    $variant(
                        $operator,
                        Option<$crate::OperatorContext<$operator, OperatorInstance>>,
                    ),
                )*
            }

            impl OperatorConfig {
                pub fn kind(&self) -> OperatorKind {
                    match &self.operator {
                        OperatorConfigVariant::$first_variant(_) => OperatorKind::$first_variant,
                        $( OperatorConfigVariant::$variant(_) => OperatorKind::$variant, )*
                    }
                }

                pub fn operator(&self) -> &OperatorConfigVariant {
                    &self.operator
                }
            }

            impl $crate::DispatchOperatorConfig for OperatorConfig {
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
                type DispatchConfig = OperatorConfig;

                fn new(config: &Self::DispatchConfig) -> ::std::io::Result<Self> {
                    match &config.operator {
                        OperatorConfigVariant::$first_variant(inner) => {
                            <$first_operator as $crate::Operator>::new(inner)
                                .map(|operator| OperatorInstance::$first_variant(operator, None))
                        }
                        $(
                            OperatorConfigVariant::$variant(inner) => {
                                <$operator as $crate::Operator>::new(inner)
                                    .map(|operator| OperatorInstance::$variant(operator, None))
                            }
                        )*
                    }
                }

                fn open<'a>(
                    &'a mut self,
                    db: ::std::sync::Weak<$crate::Database<Self>>,
                    name: &'a str,
                    config: &'a Self::DispatchConfig,
                ) -> $crate::BoxFuture<'a, ::std::io::Result<$crate::OperatorDirective>> {
                    match self {
                        OperatorInstance::$first_variant(inner, cached_ctx) => {
                            let typed_config = match &config.operator {
                                OperatorConfigVariant::$first_variant(c) => c.clone(),
                                _ => ::std::unreachable!("operator instance/config mismatch"),
                            };
                            *cached_ctx = Some($crate::OperatorContext::new(
                                db.clone(),
                                name.to_owned(),
                                typed_config,
                            ));
                            let typed_ctx = cached_ctx.as_ref().expect("operator context is cached");
                            <$first_operator as $crate::Operator>::open(inner, typed_ctx)
                        }
                        $(
                            OperatorInstance::$variant(inner, cached_ctx) => {
                                let typed_config = match &config.operator {
                                    OperatorConfigVariant::$variant(c) => c.clone(),
                                    _ => ::std::unreachable!("operator instance/config mismatch"),
                                };
                                *cached_ctx = Some($crate::OperatorContext::new(
                                    db.clone(),
                                    name.to_owned(),
                                    typed_config,
                                ));
                                let typed_ctx = cached_ctx.as_ref().expect("operator context is cached");
                                <$operator as $crate::Operator>::open(inner, typed_ctx)
                            }
                        )*
                    }
                }

                fn process<'a>(
                    &'a mut self,
                    changes: Vec<$crate::Change>,
                    db: ::std::sync::Weak<$crate::Database<Self>>,
                    name: &'a str,
                    config: &'a Self::DispatchConfig,
                ) -> $crate::BoxFuture<'a, ::std::io::Result<$crate::OperatorDirective>> {
                    let _ = (&db, name, config);
                    match self {
                        OperatorInstance::$first_variant(inner, cached_ctx) => {
                            let typed_ctx = cached_ctx
                                .as_ref()
                                .expect("operator context must be initialized by open");
                            <$first_operator as $crate::Operator>::process(inner, changes, typed_ctx)
                        }
                        $(
                            OperatorInstance::$variant(inner, cached_ctx) => {
                                let typed_ctx = cached_ctx
                                    .as_ref()
                                    .expect("operator context must be initialized by open");
                                <$operator as $crate::Operator>::process(inner, changes, typed_ctx)
                            }
                        )*
                    }
                }

                fn on_input_opened<'a>(
                    &'a mut self,
                    table: String,
                    db: ::std::sync::Weak<$crate::Database<Self>>,
                    name: &'a str,
                    config: &'a Self::DispatchConfig,
                ) -> $crate::BoxFuture<'a, ::std::io::Result<$crate::OperatorDirective>> {
                    let _ = (&db, name, config);
                    match self {
                        OperatorInstance::$first_variant(inner, cached_ctx) => {
                            let typed_ctx = cached_ctx
                                .as_ref()
                                .expect("operator context must be initialized by open");
                            <$first_operator as $crate::Operator>::on_input_opened(
                                inner, table, typed_ctx,
                            )
                        }
                        $(
                            OperatorInstance::$variant(inner, cached_ctx) => {
                                let typed_ctx = cached_ctx
                                    .as_ref()
                                    .expect("operator context must be initialized by open");
                                <$operator as $crate::Operator>::on_input_opened(
                                    inner, table, typed_ctx,
                                )
                            }
                        )*
                    }
                }

                fn on_input_closed<'a>(
                    &'a mut self,
                    table: String,
                    db: ::std::sync::Weak<$crate::Database<Self>>,
                    name: &'a str,
                    config: &'a Self::DispatchConfig,
                ) -> $crate::BoxFuture<'a, ::std::io::Result<$crate::OperatorDirective>> {
                    let _ = (&db, name, config);
                    match self {
                        OperatorInstance::$first_variant(inner, cached_ctx) => {
                            let typed_ctx = cached_ctx
                                .as_ref()
                                .expect("operator context must be initialized by open");
                            <$first_operator as $crate::Operator>::on_input_closed(
                                inner, table, typed_ctx,
                            )
                        }
                        $(
                            OperatorInstance::$variant(inner, cached_ctx) => {
                                let typed_ctx = cached_ctx
                                    .as_ref()
                                    .expect("operator context must be initialized by open");
                                <$operator as $crate::Operator>::on_input_closed(
                                    inner, table, typed_ctx,
                                )
                            }
                        )*
                    }
                }

                fn handle_timer<'a>(
                    &'a mut self,
                    payload: Vec<u8>,
                    fire_at_ms: u64,
                    db: ::std::sync::Weak<$crate::Database<Self>>,
                    name: &'a str,
                    config: &'a Self::DispatchConfig,
                ) -> $crate::BoxFuture<'a, ::std::io::Result<$crate::OperatorDirective>> {
                    let _ = (&db, name, config);
                    match self {
                        OperatorInstance::$first_variant(inner, cached_ctx) => {
                            let typed_ctx = cached_ctx
                                .as_ref()
                                .expect("operator context must be initialized by open");
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
                                <$first_operator as $crate::Operator>::handle_timer(
                                    inner, timer, fire_at_ms, typed_ctx,
                                )
                                .await
                            })
                        }
                        $(
                            OperatorInstance::$variant(inner, cached_ctx) => {
                                let typed_ctx = cached_ctx
                                    .as_ref()
                                    .expect("operator context must be initialized by open");
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
                                    <$operator as $crate::Operator>::handle_timer(
                                        inner, timer, fire_at_ms, typed_ctx,
                                    )
                                    .await
                                })
                            }
                        )*
                    }
                }

                fn finish<'a>(
                    &'a mut self,
                    db: ::std::sync::Weak<$crate::Database<Self>>,
                    name: &'a str,
                    config: &'a Self::DispatchConfig,
                ) -> $crate::BoxFuture<'a, ::std::io::Result<()>> {
                    let _ = (&db, name, config);
                    match self {
                        OperatorInstance::$first_variant(inner, cached_ctx) => {
                            let typed_ctx = cached_ctx
                                .as_ref()
                                .expect("operator context must be initialized by open");
                            <$first_operator as $crate::Operator>::finish(inner, typed_ctx)
                        }
                        $(
                            OperatorInstance::$variant(inner, cached_ctx) => {
                                let typed_ctx = cached_ctx
                                    .as_ref()
                                    .expect("operator context must be initialized by open");
                                <$operator as $crate::Operator>::finish(inner, typed_ctx)
                            }
                        )*
                    }
                }
            }
        }
    };
}
