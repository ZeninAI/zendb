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
            pub enum Kind {
                $( $variant, )+
            }

            #[derive(Debug, Clone, PartialEq, ::bincode::Encode, ::bincode::Decode)]
            pub enum OperatorInstanceConfig {
                $( $variant(<$operator as $crate::Operator>::Config), )+
            }

            #[derive(Debug, Clone, PartialEq, ::bincode::Encode, ::bincode::Decode)]
            pub struct Config {
                pub operator: OperatorInstanceConfig,
                pub runtime: $crate::OperatorRuntimeConfig,
            }

            pub enum Instance {
                $( $variant($operator), )+
            }

            impl Config {
                pub fn kind(&self) -> Kind {
                    match &self.operator {
                        $( OperatorInstanceConfig::$variant(_) => Kind::$variant, )+
                    }
                }

                pub fn operator(&self) -> &OperatorInstanceConfig {
                    &self.operator
                }
            }

            impl $crate::GlobalOperatorConfig for Config {
                fn runtime_config(&self) -> &$crate::OperatorRuntimeConfig {
                    &self.runtime
                }
            }

            impl $crate::Operator for Instance {
                type Config = Config;
                type Timer = Vec<u8>;

                fn new(config: &Self::Config) -> ::std::io::Result<Self> {
                    match &config.operator {
                        $(
                            OperatorInstanceConfig::$variant(inner) => {
                                <$operator as $crate::Operator>::new(inner).map(Instance::$variant)
                            }
                        )+
                    }
                }

                fn open<'a, Ops>(
                    &'a mut self,
                    ctx: $crate::OperatorContext<Ops>,
                ) -> $crate::BoxFuture<'a, ::std::io::Result<$crate::OperatorStatus>>
                where
                    Ops: $crate::GlobalOperator,
                {
                    match self {
                        $(
                            Instance::$variant(inner) => {
                                <$operator as $crate::Operator>::open(inner, ctx)
                            }
                        )+
                    }
                }

                fn process<'a, Ops>(
                    &'a mut self,
                    changes: Vec<$crate::Change>,
                    ctx: $crate::OperatorContext<Ops>,
                ) -> $crate::BoxFuture<'a, ::std::io::Result<$crate::OperatorStatus>>
                where
                    Ops: $crate::GlobalOperator,
                {
                    match self {
                        $(
                            Instance::$variant(inner) => {
                                <$operator as $crate::Operator>::process(inner, changes, ctx)
                            }
                        )+
                    }
                }

                fn handle_timer<'a, Ops>(
                    &'a mut self,
                    payload: Vec<u8>,
                    ctx: $crate::OperatorContext<Ops>,
                ) -> $crate::BoxFuture<'a, ::std::io::Result<$crate::OperatorStatus>>
                where
                    Ops: $crate::GlobalOperator,
                {
                    match self {
                        $(
                            Instance::$variant(inner) => {
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
                                    <$operator as $crate::Operator>::handle_timer(inner, timer, ctx)
                                        .await
                                })
                            }
                        )+
                    }
                }

                fn finish<'a, Ops>(
                    &'a mut self,
                    ctx: $crate::OperatorContext<Ops>,
                ) -> $crate::BoxFuture<'a, ::std::io::Result<()>>
                where
                    Ops: $crate::GlobalOperator,
                {
                    match self {
                        $(
                            Instance::$variant(inner) => {
                                <$operator as $crate::Operator>::finish(inner, ctx)
                            }
                        )+
                    }
                }
            }
        }
    };
}
