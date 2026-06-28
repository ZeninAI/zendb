use std::{fmt::Debug, io};

use bincode::{Decode, Encode};

use super::{BoxFuture, Change, OperatorContext, OperatorRuntimeConfig, OperatorStatus};

pub trait Operator: Send + 'static {
    type Config: Debug + Clone + PartialEq + Encode + Decode<()> + 'static;
    type Timer: Encode + Decode<()> + 'static;

    fn new(config: &Self::Config) -> io::Result<Self>
    where
        Self: Sized;

    fn open<'a, Ops>(
        &'a mut self,
        _ctx: OperatorContext<Ops>,
    ) -> BoxFuture<'a, io::Result<OperatorStatus>>
    where
        Ops: GlobalOperator,
    {
        Box::pin(async { Ok(OperatorStatus::Continue) })
    }

    fn process<'a, Ops>(
        &'a mut self,
        changes: Vec<Change>,
        ctx: OperatorContext<Ops>,
    ) -> BoxFuture<'a, io::Result<OperatorStatus>>
    where
        Ops: GlobalOperator;

    fn handle_timer<'a, Ops>(
        &'a mut self,
        _payload: Self::Timer,
        _ctx: OperatorContext<Ops>,
    ) -> BoxFuture<'a, io::Result<OperatorStatus>>
    where
        Ops: GlobalOperator,
    {
        Box::pin(async { Ok(OperatorStatus::Continue) })
    }

    fn finish<'a, Ops>(&'a mut self, _ctx: OperatorContext<Ops>) -> BoxFuture<'a, io::Result<()>>
    where
        Ops: GlobalOperator,
    {
        Box::pin(async { Ok(()) })
    }
}

pub trait GlobalOperatorConfig:
    Debug + Clone + PartialEq + Encode + Decode<()> + Send + Sync + 'static
{
    fn runtime_config(&self) -> &OperatorRuntimeConfig;
}

pub trait GlobalOperator: Operator<Timer = Vec<u8>, Config = Self::GlobalConfig> {
    type GlobalConfig: GlobalOperatorConfig;
}

impl<T> GlobalOperator for T
where
    T: Operator<Timer = Vec<u8>>,
    T::Config: GlobalOperatorConfig,
{
    type GlobalConfig = T::Config;
}
