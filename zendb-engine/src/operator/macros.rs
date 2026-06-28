#[macro_export]
macro_rules! define_operator_set {
    (
        $vis:vis mod $module:ident {
            $( $variant:ident ( $operator:ty ) ),+ $(,)?
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
                $( $variant, )+
            }

            #[derive(Debug, Clone, PartialEq, ::bincode::Encode, ::bincode::Decode)]
            pub enum OperatorConfigVariant {
                $( $variant(<$operator as $crate::Operator>::Config), )+
            }

            #[derive(Debug, Clone, PartialEq, ::bincode::Encode, ::bincode::Decode)]
            pub struct OperatorConfig {
                pub operator: OperatorConfigVariant,
                pub runtime: $crate::OperatorRuntimeConfig,
            }

            pub enum OperatorInstance {
                $( $variant($operator), )+
            }

            impl OperatorConfig {
                pub fn kind(&self) -> OperatorKind {
                    match &self.operator {
                        $( OperatorConfigVariant::$variant(_) => OperatorKind::$variant, )+
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
            }

            impl $crate::Operator for OperatorInstance {
                type Config = OperatorConfig;
                type Timer = Vec<u8>;

                fn new(config: &Self::Config) -> ::std::io::Result<Self> {
                    match &config.operator {
                        $(
                            OperatorConfigVariant::$variant(inner) => {
                                <$operator as $crate::Operator>::new(inner)
                                    .map(OperatorInstance::$variant)
                            }
                        )+
                    }
                }

                fn open<'a, D>(
                    &'a mut self,
                    ctx: $crate::OperatorContext<'a, Self, D>,
                ) -> $crate::BoxFuture<'a, ::std::io::Result<$crate::OperatorDirective>>
                where
                    D: $crate::DispatchOperator,
                {
                    match self {
                        $(
                            OperatorInstance::$variant(inner) => {
                                let typed_config = match &ctx.config().operator {
                                    OperatorConfigVariant::$variant(c) => c.clone(),
                                    _ => ::std::unreachable!(),
                                };
                                let typed_ctx = $crate::OperatorContext {
                                    db: ctx.db.clone(),
                                    name: ctx.name,
                                    config: typed_config,
                                    _phantom: ::std::marker::PhantomData,
                                };
                                <$operator as $crate::Operator>::open(inner, typed_ctx)
                            }
                        )+
                    }
                }

                fn process<'a, D>(
                    &'a mut self,
                    changes: Vec<$crate::Change>,
                    ctx: $crate::OperatorContext<'a, Self, D>,
                ) -> $crate::BoxFuture<'a, ::std::io::Result<$crate::OperatorDirective>>
                where
                    D: $crate::DispatchOperator,
                {
                    match self {
                        $(
                            OperatorInstance::$variant(inner) => {
                                let typed_config = match &ctx.config().operator {
                                    OperatorConfigVariant::$variant(c) => c.clone(),
                                    _ => ::std::unreachable!(),
                                };
                                let typed_ctx = $crate::OperatorContext {
                                    db: ctx.db.clone(),
                                    name: ctx.name,
                                    config: typed_config,
                                    _phantom: ::std::marker::PhantomData,
                                };
                                <$operator as $crate::Operator>::process(inner, changes, typed_ctx)
                            }
                        )+
                    }
                }

                fn handle_timer<'a, D>(
                    &'a mut self,
                    payload: Vec<u8>,
                    ctx: $crate::OperatorContext<'a, Self, D>,
                ) -> $crate::BoxFuture<'a, ::std::io::Result<$crate::OperatorDirective>>
                where
                    D: $crate::DispatchOperator,
                {
                    match self {
                        $(
                            OperatorInstance::$variant(inner) => {
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
                                    let typed_config = match &ctx.config().operator {
                                        OperatorConfigVariant::$variant(c) => c.clone(),
                                        _ => ::std::unreachable!(),
                                    };
                                    let typed_ctx = $crate::OperatorContext {
                                        db: ctx.db.clone(),
                                        name: ctx.name,
                                        config: typed_config,
                                        _phantom: ::std::marker::PhantomData,
                                    };
                                    <$operator as $crate::Operator>::handle_timer(
                                        inner, timer, typed_ctx,
                                    )
                                    .await
                                })
                            }
                        )+
                    }
                }

                fn finish<'a, D>(
                    &'a mut self,
                    ctx: $crate::OperatorContext<'a, Self, D>,
                ) -> $crate::BoxFuture<'a, ::std::io::Result<()>>
                where
                    D: $crate::DispatchOperator,
                {
                    match self {
                        $(
                            OperatorInstance::$variant(inner) => {
                                let typed_config = match &ctx.config().operator {
                                    OperatorConfigVariant::$variant(c) => c.clone(),
                                    _ => ::std::unreachable!(),
                                };
                                let typed_ctx = $crate::OperatorContext {
                                    db: ctx.db.clone(),
                                    name: ctx.name,
                                    config: typed_config,
                                    _phantom: ::std::marker::PhantomData,
                                };
                                <$operator as $crate::Operator>::finish(inner, typed_ctx)
                            }
                        )+
                    }
                }
            }

            impl $crate::DispatchOperator for OperatorInstance {
                type DispatchConfig = OperatorConfig;
            }
        }
    };
}
