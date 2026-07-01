use std::{
    io,
    marker::PhantomData,
    sync::{Arc, Weak},
};

use bincode::{Decode, Encode};
use zendb_storage::frontend::{state::StateConfig, table::TableConfig};

use crate::{Database, StateHandle, TableHandle};

use super::{DispatchOperator, Operator, OperatorPhase};

/// Context passed to every [`Operator`] lifecycle method.
///
/// Carries the operator's own typed `Config` (not the union enum) and
/// provides typed timer registration.
///
/// * `O` - the concrete operator type (e.g. `CountingOperator`).
/// * `D` - the dispatch operator type (the generated `OperatorInstance` enum).
pub struct OperatorContext<O, D>
where
    O: Operator + ?Sized,
    D: DispatchOperator,
{
    pub(crate) db: Weak<Database<D>>,
    pub(crate) name: String,
    pub(crate) config: O::Config,
    pub(crate) _phantom: PhantomData<fn(&O, &D)>,
}

impl<O, D> OperatorContext<O, D>
where
    O: Operator + ?Sized,
    D: DispatchOperator,
{
    pub fn new(db: Weak<Database<D>>, name: String, config: O::Config) -> Self {
        Self {
            db,
            name,
            config,
            _phantom: PhantomData,
        }
    }

    /// The registration name of this operator.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The operator's own typed config, not the union enum.
    pub fn config(&self) -> &O::Config {
        &self.config
    }

    /// Upgrade to a strong database reference, or `None` if closed.
    pub fn database(&self) -> Option<Arc<Database<D>>> {
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

    /// Register a processing-time timer. Payload is typed (`O::Timer`) so
    /// mismatches are caught at compile time. Replaces an existing timer at
    /// the same `fire_at_ms` (last-write-wins).
    pub fn register_timer(&self, fire_at_ms: u64, payload: &O::Timer) -> io::Result<()> {
        let bytes = bincode::encode_to_vec(payload, bincode::config::standard())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
        self.require_db()?
            .register_timer(&self.name, fire_at_ms, bytes)
    }

    /// Cancel a pending timer at `fire_at_ms` registered by this operator.
    pub fn cancel_timer(&self, fire_at_ms: u64) -> io::Result<()> {
        self.require_db()?.cancel_timer(&self.name, fire_at_ms)
    }

    /// Register or update an operator using a dispatch config.
    pub fn operator(
        &self,
        name: &str,
        config: D::DispatchConfig,
    ) -> io::Result<(OperatorPhase, D::DispatchConfig)> {
        self.require_db()?.operator(name, Some(config))
    }

    fn require_db(&self) -> io::Result<Arc<Database<D>>> {
        self.db
            .upgrade()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotConnected, "database is closed"))
    }
}
