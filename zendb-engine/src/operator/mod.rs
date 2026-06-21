//! Operator definitions, registrations, typed state, and execution.

mod registry;
pub(crate) mod worker;

use std::{future::Future, io, pin::Pin, sync::Weak};

use bincode::{Decode, Encode};

pub use registry::OperatorRegistry;
pub use state::{ConcurrentState, State, StateKey, StateValue};
pub use zendb_storage::frontend::table::Change;

pub(crate) mod state;

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub enum Subscription {
    Table(String),
    AllTables,
}

impl Subscription {
    pub(crate) fn matches(&self, table: &str) -> bool {
        matches!(self, Self::AllTables) || matches!(self, Self::Table(name) if name == table)
    }
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct OperatorConfig {
    pub implementation: String,
    pub configuration: Vec<u8>,
    pub subscriptions: Vec<Subscription>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperatorStatus {
    Continue,
    Finish,
}

pub trait Operator: Send + 'static {
    fn open<'a>(&'a mut self, _database: Weak<crate::Database>) -> BoxFuture<'a, io::Result<()>> {
        Box::pin(async { Ok(()) })
    }

    fn process<'a>(
        &'a mut self,
        changes: Vec<Change>,
        database: Weak<crate::Database>,
    ) -> BoxFuture<'a, io::Result<OperatorStatus>>;

    fn finish<'a>(&'a mut self, _database: Weak<crate::Database>) -> BoxFuture<'a, io::Result<()>> {
        Box::pin(async { Ok(()) })
    }
}
