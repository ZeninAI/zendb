//! Resources exposed to a running computation.

use std::{io, sync::Weak};

use crate::{
    database::{DatabaseInner, Table},
    StateKey, StateRef, StateValue,
};

#[derive(Clone)]
pub struct ComputationContext {
    pub(crate) database: Weak<DatabaseInner>,
    pub(crate) computation: String,
}

impl ComputationContext {
    pub fn table(&self, name: &str) -> io::Result<Table> {
        self.database
            .upgrade()
            .ok_or_else(database_closed)?
            .table(name)
    }

    pub fn local_state<K: StateKey, V: StateValue>(
        &self,
        name: &str,
    ) -> io::Result<StateRef<K, V>> {
        self.database
            .upgrade()
            .ok_or_else(database_closed)?
            .local_state(&self.computation, name)
    }

    pub fn shared_state<K: StateKey, V: StateValue>(
        &self,
        name: &str,
    ) -> io::Result<StateRef<K, V>> {
        self.database
            .upgrade()
            .ok_or_else(database_closed)?
            .shared_state(name)
    }
}

fn database_closed() -> io::Error {
    io::Error::new(io::ErrorKind::NotConnected, "database is closed")
}
