//! Computation definitions, registrations, typed state, and execution.

mod context;
mod registry;
pub(crate) mod worker;

use std::{future::Future, io, pin::Pin};

use bincode::{Decode, Encode};
use zendb_storage::frontend::state::StateConfig;

pub use context::ComputationContext;
pub use registry::ComputationRegistry;
pub use state::{StateKey, StateRef, StateValue};
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

#[derive(Debug, Clone, PartialEq, Eq, Hash, Encode, Decode)]
pub enum StateVisibility {
    Local,
    Shared,
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct StateDefinition {
    pub name: String,
    pub visibility: StateVisibility,
    /// Stable registry name identifying the concrete `State<K, V>` types.
    pub implementation: String,
    pub config: StateConfig,
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct ComputationConfig {
    pub implementation: String,
    pub configuration: Vec<u8>,
    pub subscriptions: Vec<Subscription>,
    pub states: Vec<StateDefinition>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ComputationStatus {
    Continue,
    Finish,
}

pub trait Computation: Send + 'static {
    fn open<'a>(&'a mut self, _context: ComputationContext) -> BoxFuture<'a, io::Result<()>> {
        Box::pin(async { Ok(()) })
    }

    fn process<'a>(
        &'a mut self,
        changes: Vec<Change>,
        context: ComputationContext,
    ) -> BoxFuture<'a, io::Result<ComputationStatus>>;
}
