//! Operator definitions, typed composition, state, and execution.

mod config;
mod lifecycle;
mod macros;
pub mod prelude;
mod traits;
pub(crate) mod worker;

use std::{future::Future, pin::Pin};

pub use config::{OperatorRuntimeConfig, RetryConfig, Subscription};
pub use lifecycle::{OperatorDirective, OperatorPhase};
pub use traits::{DispatchOperator, DispatchOperatorConfig, Operator, OperatorContext};
pub use zendb_storage::frontend::state::State;
pub use zendb_types::Change;

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
