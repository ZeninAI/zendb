use std::{any::Any, fmt::Debug, future::Future, io, sync::Arc};

use bincode::{Decode, Encode};

use crate::OperatorHost;

use super::{BoxFuture, Change, OperatorDirective, OperatorPhase, OperatorRuntimeConfig};

/// Native operator execution ABI implemented by every concrete operator.
///
/// This is a trusted, local application extension ABI. It is not a distributed
/// placement or lease interface. A future control plane may decide when to
/// instantiate it, but that contract is intentionally absent until ADR 008 is
/// implemented. Sandboxed runtimes should expose a narrower context instead
/// of this native host API.
///
/// # Lifecycle
///
/// ```text
/// ┌─────────┐
/// │ create  │  ← Workspace + config available; set up state/tables
/// └────┬────┘
///      │  (for each matching table already open)
///      ▼
/// ┌────────────────┐
/// │ on_input_opened│
/// └────────┬───────┘
///          │
///          ▼
/// ┌─────────────────────────────────────────────────────┐
/// │              ACTIVE LOOP                             │
/// │  ┌─────────┐   ┌──────────┐   ┌────────────────┐   │
/// │  │ process │   │ on_timer │   │ on_input_opened │   │
/// │  │ changes │   │  fires   │   │ / _closed       │   │
/// │  └─────────┘   └──────────┘   └────────────────┘   │
/// │                                                     │
/// │  Any method may return Finish ─────────────────────►│
/// └─────────────────────────────────┬───────────────────┘
///                                   │
///                                   ▼
///                          ┌────────────────┐
///                          │   teardown     │ ← reason: Active/Finished/Failed/Cancelled
///                          └────────────────┘
/// ```
///
/// Each lifecycle method receives:
/// - `&Arc<OperatorHost<D>>` - the local operator integration surface (tables,
///   states, timers); it is not a grant to bypass policy
/// - `&str` name — this operator's registration name
/// - `&Self::Config` — operator-specific typed configuration
///
/// # Ownership
///
/// | Layer | Owns | Does NOT own |
/// |-------|------|-------------|
/// | `Operator` (user code) | Business logic, state handles, timer payloads | Polling, commit offsets, event ordering |
/// | `Workspace` | Tables, states, timer store, operator catalog | Lifecycle transitions |
/// | `OperatorWorker` | Input attachment/detachment, timer inbox, spawn | Run loop details |
/// | `RunLoop` | Event queue, shutdown state machine, poll+commit, idle/wake | What the operator does with changes |
pub trait Operator: Send + 'static {
    /// Operator-specific configuration. Persisted in the operator catalog.
    type Config: Debug + Clone + PartialEq + Encode + Decode<()> + 'static;

    /// Typed timer payload. Use `()` if the operator does not use timers.
    type Timer: Encode + Decode<()> + 'static;

    /// Public query interface exposed to users while the operator is running.
    ///
    /// The facet is produced once after [`Operator::create`] succeeds and is
    /// available via [`Workspace::facet`] for the lifetime of the operator.
    /// Typically wraps [`StateHandle`](crate::StateHandle) instances so that
    /// queries read directly from the operator's persisted state without
    /// requiring a lock on the operator itself.
    ///
    /// Use `()` if the operator does not expose a query interface.
    type Facet: Send + Sync + 'static;

    /// Construct the operator with full workspace access.
    ///
    /// Called once when the operator is first spawned. Use this to open state
    /// handles, create output tables, register initial timers, etc.
    fn create<'a, D>(
        db: &'a Arc<OperatorHost<D>>,
        name: &'a str,
        config: &'a Self::Config,
    ) -> impl Future<Output = io::Result<Self>> + Send + 'a
    where
        D: DispatchOperator,
        Self: Sized;

    /// Produce the public query facet for this operator.
    ///
    /// Called once after [`Operator::create`] succeeds (and again after
    /// re-creation on a suspend race). The returned facet is stored in the
    /// operator worker and made available via [`Workspace::facet`].
    ///
    /// The default implementation returns `()` for operators that do not
    /// expose a query interface.
    fn facet(&self) -> Self::Facet;

    /// Process a batch of changes from subscribed tables.
    fn process<'a, D>(
        &'a mut self,
        changes: Vec<Change>,
        db: &'a Arc<OperatorHost<D>>,
        name: &'a str,
        config: &'a Self::Config,
    ) -> impl Future<Output = io::Result<OperatorDirective>> + Send + 'a
    where
        D: DispatchOperator,
    {
        let _ = (changes, db, name, config);
        async { Ok(OperatorDirective::Continue) }
    }

    /// Called when a new input table matching the subscription appears.
    fn on_input_opened<'a, D>(
        &'a mut self,
        table: String,
        db: &'a Arc<OperatorHost<D>>,
        name: &'a str,
        config: &'a Self::Config,
    ) -> impl Future<Output = io::Result<OperatorDirective>> + Send + 'a
    where
        D: DispatchOperator,
    {
        let _ = (table, db, name, config);
        async { Ok(OperatorDirective::Continue) }
    }

    /// Called when a previously open input table disappears.
    fn on_input_closed<'a, D>(
        &'a mut self,
        table: String,
        db: &'a Arc<OperatorHost<D>>,
        name: &'a str,
        config: &'a Self::Config,
    ) -> impl Future<Output = io::Result<OperatorDirective>> + Send + 'a
    where
        D: DispatchOperator,
    {
        let _ = (table, db, name, config);
        async { Ok(OperatorDirective::Continue) }
    }

    /// Called when a registered processing-time timer fires.
    fn on_timer<'a, D>(
        &'a mut self,
        payload: Self::Timer,
        fire_at_ms: u64,
        db: &'a Arc<OperatorHost<D>>,
        name: &'a str,
        config: &'a Self::Config,
    ) -> impl Future<Output = io::Result<OperatorDirective>> + Send + 'a
    where
        D: DispatchOperator,
    {
        let _ = (payload, fire_at_ms, db, name, config);
        async { Ok(OperatorDirective::Continue) }
    }

    /// Called when the operator is stopping.
    ///
    /// The `phase` indicates why:
    /// - `Active` — suspending (no inputs remain, may be respawned later)
    /// - `Finished` — operator returned [`OperatorDirective::Finish`]
    /// - `Failed` — an unrecoverable error occurred
    /// - `Cancelled` — permanently cancelled by the workspace owner
    fn teardown<'a, D>(
        &'a mut self,
        phase: &'a OperatorPhase,
        db: &'a Arc<OperatorHost<D>>,
        name: &'a str,
        config: &'a Self::Config,
    ) -> impl Future<Output = io::Result<()>> + Send + 'a
    where
        D: DispatchOperator,
    {
        let _ = (phase, db, name, config);
        async { Ok(()) }
    }
}

/// Dispatch configuration that combines a registered operator's typed payload
/// with a derived local [`OperatorRuntimeConfig`].
///
/// The generated enum is only a local type-erasure adapter.
pub trait DispatchConfig:
    Debug + Clone + PartialEq + Encode + Decode<()> + Send + Sync + 'static
{
    fn runtime_config(&self) -> &OperatorRuntimeConfig;

    fn new<O>(config: O::Config, runtime: OperatorRuntimeConfig) -> io::Result<Self>
    where
        O: Operator;
}

/// Dispatch trait for operator-set enums generated by [`define_operator_set!`].
///
/// Type-erased dispatch layer. The run loop calls these methods; the generated
/// enum decodes typed payloads and delegates to [`Operator`] methods.
pub trait DispatchOperator: Send + 'static {
    type Config: DispatchConfig;

    /// Create the operator instance.
    fn create<'a>(
        db: &'a Arc<OperatorHost<Self>>,
        name: &'a str,
        config: &'a Self::Config,
    ) -> BoxFuture<'a, io::Result<Self>>
    where
        Self: Sized;

    /// Produce a type-erased facet for external queries.
    fn facet(&self) -> Box<dyn Any + Send + Sync>;

    fn process<'a>(
        &'a mut self,
        changes: Vec<Change>,
        db: &'a Arc<OperatorHost<Self>>,
        name: &'a str,
        config: &'a Self::Config,
    ) -> BoxFuture<'a, io::Result<OperatorDirective>>
    where
        Self: Sized;

    fn on_input_opened<'a>(
        &'a mut self,
        table: String,
        db: &'a Arc<OperatorHost<Self>>,
        name: &'a str,
        config: &'a Self::Config,
    ) -> BoxFuture<'a, io::Result<OperatorDirective>>
    where
        Self: Sized;

    fn on_input_closed<'a>(
        &'a mut self,
        table: String,
        db: &'a Arc<OperatorHost<Self>>,
        name: &'a str,
        config: &'a Self::Config,
    ) -> BoxFuture<'a, io::Result<OperatorDirective>>
    where
        Self: Sized;

    fn on_timer<'a>(
        &'a mut self,
        payload: Vec<u8>,
        fire_at_ms: u64,
        db: &'a Arc<OperatorHost<Self>>,
        name: &'a str,
        config: &'a Self::Config,
    ) -> BoxFuture<'a, io::Result<OperatorDirective>>
    where
        Self: Sized;

    fn teardown<'a>(
        &'a mut self,
        phase: &'a OperatorPhase,
        db: &'a Arc<OperatorHost<Self>>,
        name: &'a str,
        config: &'a Self::Config,
    ) -> BoxFuture<'a, io::Result<()>>
    where
        Self: Sized;
}
