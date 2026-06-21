//! Typed operator state aliases.

use std::{hash::Hash, sync::Arc};

use bincode::{Decode, Encode};
use parking_lot::RwLock;
use zendb_storage::frontend::state::State as FrontendState;

pub trait StateKey: Encode + Decode<()> + Hash + Eq + Clone + Ord + Send + Sync + 'static {}

impl<T> StateKey for T where T: Encode + Decode<()> + Hash + Eq + Clone + Ord + Send + Sync + 'static
{}

pub trait StateValue: Encode + Decode<()> + Clone + Send + Sync + 'static {}

impl<T> StateValue for T where T: Encode + Decode<()> + Clone + Send + Sync + 'static {}

pub type State<K, V> = FrontendState<K, V>;
pub type ConcurrentState<K, V> = Arc<RwLock<State<K, V>>>;
