//! Operator lifecycle context.

use std::{io, sync::Arc, sync::Weak};

use bincode::{Decode, Encode};

use crate::{StateHandle, TableHandle, OperatorConfig};
use zendb_storage::frontend::{state::StateConfig, table::TableConfig};

/// Context passed to every [`super::Operator`] lifecycle method.
///
/// Provides scoped access to the database (tables, states, timers) without
/// exposing the raw `Weak<Database>` or requiring operators to know their
/// own registration name.
#[derive(Clone)]
pub struct OperatorContext {
    pub(crate) db: Weak<crate::Database>,
    pub(crate) name: Arc<str>,
    config: Arc<OperatorConfig>,
}

impl OperatorContext {
    pub(crate) fn new(db: Weak<crate::Database>, name: Arc<str>, config: Arc<OperatorConfig>) -> Self {
        Self { db, name, config }
    }

    /// The registration name of this operator.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The full registration config for this operator, including subscriptions,
    /// retry policy, and the raw encoded typed config.
    pub fn config(&self) -> &OperatorConfig {
        &self.config
    }

    /// Upgrade to a strong database reference, or `None` if closed.
    pub fn database(&self) -> Option<Arc<crate::Database>> {
        self.db.upgrade()
    }

    /// Get or create a table handle.
    pub fn table(&self, name: &str, config: Option<TableConfig>) -> io::Result<TableHandle> {
        self.require_db()?.table(name, config)
    }

    /// Get or create a typed state handle.
    pub fn state<K, V>(
        &self,
        name: &str,
        config: Option<StateConfig>,
    ) -> io::Result<StateHandle<K, V>>
    where
        K: Encode + Decode<()> + std::hash::Hash + Eq + Clone + Ord + Send + Sync + 'static,
        V: Encode + Decode<()> + Clone + Send + Sync + 'static,
    {
        self.require_db()?.state(name, config)
    }

    /// Register a processing-time timer with raw bytes as the payload.
    ///
    /// If a timer already exists for this operator at `fire_at_ms` it is
    /// replaced (last-write-wins — no FIFO guarantee for equal times).
    pub fn register_timer(&self, fire_at_ms: u64, payload: Vec<u8>) -> io::Result<()> {
        self.require_db()?
            .register_timer(&self.name, fire_at_ms, payload)
    }

    /// Register a processing-time timer, serialising `payload` with bincode.
    ///
    /// Use [`decode_timer`](Self::decode_timer) in `on_timer` to recover the
    /// value without writing any serialisation code directly.
    pub fn register_timer_typed<T: Encode>(
        &self,
        fire_at_ms: u64,
        payload: &T,
    ) -> io::Result<()> {
        let bytes = bincode::encode_to_vec(payload, bincode::config::standard())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
        self.register_timer(fire_at_ms, bytes)
    }

    /// Cancel a pending timer at `fire_at_ms` registered by this operator.
    pub fn cancel_timer(&self, fire_at_ms: u64) -> io::Result<()> {
        self.require_db()?.cancel_timer(&self.name, fire_at_ms)
    }

    /// Decode a timer payload previously written by [`register_timer_typed`](Self::register_timer_typed).
    pub fn decode_timer<T: Decode<()>>(&self, payload: &[u8]) -> io::Result<T> {
        bincode::decode_from_slice(payload, bincode::config::standard())
            .map(|(v, _)| v)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))
    }

    fn require_db(&self) -> io::Result<Arc<crate::Database>> {
        self.db
            .upgrade()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotConnected, "database is closed"))
    }
}
