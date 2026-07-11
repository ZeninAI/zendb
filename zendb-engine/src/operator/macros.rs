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
/// You can also use it with no custom operators (only prelude operators):
///
/// ```ignore
/// define_operator_set! {
///     pub mod ops {}
/// }
/// ```
#[macro_export]
macro_rules! define_operator_set {
    // Case with custom operators
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
    // Case with no custom operators (only prelude)
    (
        $vis:vis mod $module:ident {}
    ) => {
        $crate::__zendb_with_prelude_operators! {
            $crate::__zendb_define_operator_set,
            $vis mod $module {}
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
                $first_variant($first_operator),
                $( $variant($operator), )*
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

                    Ok(Self { operator, runtime })
                }
            }

            impl $crate::DispatchOperator for OperatorInstance {
                type Config = OperatorConfig;

                fn create<'a>(
                    db: &'a ::std::sync::Arc<$crate::Workspace<Self>>,
                    name: &'a str,
                    config: &'a Self::Config,
                ) -> $crate::BoxFuture<'a, ::std::io::Result<Self>> {
                    match &config.operator {
                        OperatorConfigVariant::$first_variant(inner) => {
                            Box::pin(async move {
                                let op = <$first_operator as $crate::Operator>::create(db, name, inner).await?;
                                Ok(OperatorInstance::$first_variant(op))
                            })
                        }
                        $(
                            OperatorConfigVariant::$variant(inner) => {
                                Box::pin(async move {
                                    let op = <$operator as $crate::Operator>::create(db, name, inner).await?;
                                    Ok(OperatorInstance::$variant(op))
                                })
                            }
                        )*
                    }
                }

                fn facet(&self) -> Box<dyn ::std::any::Any + Send + Sync> {
                    match self {
                        OperatorInstance::$first_variant(inner) => {
                            Box::new(<$first_operator as $crate::Operator>::facet(inner))
                        }
                        $(
                            OperatorInstance::$variant(inner) => {
                                Box::new(<$operator as $crate::Operator>::facet(inner))
                            }
                        )*
                    }
                }

                fn process<'a>(
                    &'a mut self,
                    changes: Vec<$crate::Change>,
                    db: &'a ::std::sync::Arc<$crate::Workspace<Self>>,
                    name: &'a str,
                    config: &'a Self::Config,
                ) -> $crate::BoxFuture<'a, ::std::io::Result<$crate::OperatorDirective>> {
                    match (self, &config.operator) {
                        (OperatorInstance::$first_variant(inner), OperatorConfigVariant::$first_variant(cfg)) => {
                            Box::pin(<$first_operator as $crate::Operator>::process(inner, changes, db, name, cfg))
                        }
                        $(
                            (OperatorInstance::$variant(inner), OperatorConfigVariant::$variant(cfg)) => {
                                Box::pin(<$operator as $crate::Operator>::process(inner, changes, db, name, cfg))
                            }
                        )*
                        _ => ::std::unreachable!("operator instance/config variant mismatch"),
                    }
                }

                fn on_input_opened<'a>(
                    &'a mut self,
                    table: String,
                    db: &'a ::std::sync::Arc<$crate::Workspace<Self>>,
                    name: &'a str,
                    config: &'a Self::Config,
                ) -> $crate::BoxFuture<'a, ::std::io::Result<$crate::OperatorDirective>> {
                    match (self, &config.operator) {
                        (OperatorInstance::$first_variant(inner), OperatorConfigVariant::$first_variant(cfg)) => {
                            Box::pin(<$first_operator as $crate::Operator>::on_input_opened(inner, table, db, name, cfg))
                        }
                        $(
                            (OperatorInstance::$variant(inner), OperatorConfigVariant::$variant(cfg)) => {
                                Box::pin(<$operator as $crate::Operator>::on_input_opened(inner, table, db, name, cfg))
                            }
                        )*
                        _ => ::std::unreachable!("operator instance/config variant mismatch"),
                    }
                }

                fn on_input_closed<'a>(
                    &'a mut self,
                    table: String,
                    db: &'a ::std::sync::Arc<$crate::Workspace<Self>>,
                    name: &'a str,
                    config: &'a Self::Config,
                ) -> $crate::BoxFuture<'a, ::std::io::Result<$crate::OperatorDirective>> {
                    match (self, &config.operator) {
                        (OperatorInstance::$first_variant(inner), OperatorConfigVariant::$first_variant(cfg)) => {
                            Box::pin(<$first_operator as $crate::Operator>::on_input_closed(inner, table, db, name, cfg))
                        }
                        $(
                            (OperatorInstance::$variant(inner), OperatorConfigVariant::$variant(cfg)) => {
                                Box::pin(<$operator as $crate::Operator>::on_input_closed(inner, table, db, name, cfg))
                            }
                        )*
                        _ => ::std::unreachable!("operator instance/config variant mismatch"),
                    }
                }

                fn on_timer<'a>(
                    &'a mut self,
                    payload: Vec<u8>,
                    fire_at_ms: u64,
                    db: &'a ::std::sync::Arc<$crate::Workspace<Self>>,
                    name: &'a str,
                    config: &'a Self::Config,
                ) -> $crate::BoxFuture<'a, ::std::io::Result<$crate::OperatorDirective>> {
                    match (self, &config.operator) {
                        (OperatorInstance::$first_variant(inner), OperatorConfigVariant::$first_variant(cfg)) => {
                            Box::pin(async move {
                                let timer: <$first_operator as $crate::Operator>::Timer =
                                    ::bincode::decode_from_slice(&payload, ::bincode::config::standard())
                                        .map(|(t, _)| t)
                                        .map_err(|e| ::std::io::Error::new(::std::io::ErrorKind::InvalidData, e.to_string()))?;
                                <$first_operator as $crate::Operator>::on_timer(inner, timer, fire_at_ms, db, name, cfg).await
                            })
                        }
                        $(
                            (OperatorInstance::$variant(inner), OperatorConfigVariant::$variant(cfg)) => {
                                Box::pin(async move {
                                    let timer: <$operator as $crate::Operator>::Timer =
                                        ::bincode::decode_from_slice(&payload, ::bincode::config::standard())
                                            .map(|(t, _)| t)
                                            .map_err(|e| ::std::io::Error::new(::std::io::ErrorKind::InvalidData, e.to_string()))?;
                                    <$operator as $crate::Operator>::on_timer(inner, timer, fire_at_ms, db, name, cfg).await
                                })
                            }
                        )*
                        _ => ::std::unreachable!("operator instance/config variant mismatch"),
                    }
                }

                fn teardown<'a>(
                    &'a mut self,
                    phase: &'a $crate::OperatorPhase,
                    db: &'a ::std::sync::Arc<$crate::Workspace<Self>>,
                    name: &'a str,
                    config: &'a Self::Config,
                ) -> $crate::BoxFuture<'a, ::std::io::Result<()>> {
                    match (self, &config.operator) {
                        (OperatorInstance::$first_variant(inner), OperatorConfigVariant::$first_variant(cfg)) => {
                            Box::pin(<$first_operator as $crate::Operator>::teardown(inner, phase, db, name, cfg))
                        }
                        $(
                            (OperatorInstance::$variant(inner), OperatorConfigVariant::$variant(cfg)) => {
                                Box::pin(<$operator as $crate::Operator>::teardown(inner, phase, db, name, cfg))
                            }
                        )*
                        _ => ::std::unreachable!("operator instance/config variant mismatch"),
                    }
                }
            }
        }
    };
}
