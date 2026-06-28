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
                $(
                    $variant(
                        $operator,
                        Option<$crate::OperatorContext<$operator, OperatorInstance>>,
                    ),
                )+
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

            impl $crate::DispatchOperator for OperatorInstance {
                type DispatchConfig = OperatorConfig;

                fn new(config: &Self::DispatchConfig) -> ::std::io::Result<Self> {
                    match &config.operator {
                        $(
                            OperatorConfigVariant::$variant(inner) => {
                                <$operator as $crate::Operator>::new(inner)
                                    .map(|operator| OperatorInstance::$variant(operator, None))
                            }
                        )+
                    }
                }

                fn open<'a>(
                    &'a mut self,
                    db: ::std::sync::Weak<$crate::Database<Self>>,
                    name: &'a str,
                    config: &'a Self::DispatchConfig,
                ) -> $crate::BoxFuture<'a, ::std::io::Result<$crate::OperatorDirective>> {
                    match self {
                        $(
                            OperatorInstance::$variant(inner, cached_ctx) => {
                                if cached_ctx.is_none() {
                                    let typed_config = match &config.operator {
                                        OperatorConfigVariant::$variant(c) => c.clone(),
                                        _ => ::std::unreachable!(),
                                    };
                                    *cached_ctx = Some($crate::OperatorContext {
                                        db: db.clone(),
                                        name: name.to_owned(),
                                        config: typed_config,
                                        _phantom: ::std::marker::PhantomData,
                                    });
                                }
                                let typed_ctx = cached_ctx.as_ref().expect("operator context is cached");
                                <$operator as $crate::Operator>::open(inner, typed_ctx)
                            }
                        )+
                    }
                }

                fn process<'a>(
                    &'a mut self,
                    changes: Vec<$crate::Change>,
                    _db: ::std::sync::Weak<$crate::Database<Self>>,
                    _name: &'a str,
                    _config: &'a Self::DispatchConfig,
                ) -> $crate::BoxFuture<'a, ::std::io::Result<$crate::OperatorDirective>> {
                    match self {
                        $(
                            OperatorInstance::$variant(inner, cached_ctx) => {
                                let typed_ctx = cached_ctx
                                    .as_ref()
                                    .expect("operator context must be initialized by open");
                                <$operator as $crate::Operator>::process(inner, changes, typed_ctx)
                            }
                        )+
                    }
                }

                fn handle_timer<'a>(
                    &'a mut self,
                    payload: Vec<u8>,
                    _db: ::std::sync::Weak<$crate::Database<Self>>,
                    _name: &'a str,
                    _config: &'a Self::DispatchConfig,
                ) -> $crate::BoxFuture<'a, ::std::io::Result<$crate::OperatorDirective>> {
                    match self {
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
                                        inner, timer, typed_ctx,
                                    )
                                    .await
                                })
                            }
                        )+
                    }
                }

                fn finish<'a>(
                    &'a mut self,
                    _db: ::std::sync::Weak<$crate::Database<Self>>,
                    _name: &'a str,
                    _config: &'a Self::DispatchConfig,
                ) -> $crate::BoxFuture<'a, ::std::io::Result<()>> {
                    match self {
                        $(
                            OperatorInstance::$variant(inner, cached_ctx) => {
                                let typed_ctx = cached_ctx
                                    .as_ref()
                                    .expect("operator context must be initialized by open");
                                <$operator as $crate::Operator>::finish(inner, typed_ctx)
                            }
                        )+
                    }
                }
            }
        }
    };
}
