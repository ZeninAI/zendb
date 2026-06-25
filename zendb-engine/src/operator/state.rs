//! Typed operator state aliases.

use bincode::{Decode, Encode};
use std::hash::Hash;

pub use zendb_storage::frontend::state::State;

pub trait StateKey: Encode + Decode<()> + Hash + Eq + Clone + Ord + Send + Sync + 'static {}

impl<T> StateKey for T where T: Encode + Decode<()> + Hash + Eq + Clone + Ord + Send + Sync + 'static
{}

pub trait StateValue: Encode + Decode<()> + Clone + Send + Sync + 'static {}

impl<T> StateValue for T where T: Encode + Decode<()> + Clone + Send + Sync + 'static {}
