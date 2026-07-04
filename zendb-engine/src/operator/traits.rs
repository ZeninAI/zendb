use std::{fmt::Debug, io, sync::Weak};

use bincode::{Decode, Encode};

use crate::Database;

use super::{BoxFuture, Change, OperatorContext, OperatorDirective, OperatorRuntimeConfig, TeardownReason};

/// Core operator trait implemented by every concrete operator.
///
/// # Lifecycle
///
/// ```text
/// ┌─────────┐
/// │ create  │  ← Context available, set up state/tables
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
///                          │   teardown     │ ← reason: Finished/Failed/Cancelled
///                          └────────────────┘
/// ```
///
/// Each operator declares its own typed [`Config`](Operator::Config) and
/// [`Timer`](Operator::Timer) payload. The generic `D` parameter is the
/// dispatch operator type (the generated `OperatorInstance` enum) and flows
/// through the [`OperatorContext`] so that the operator can interact with the
/// database without knowing its concrete type.
///
/// # Ownership
///
/// | Layer | Owns | Does NOT own |
/// |-------|------|-------------|
/// | `Operator` (user code) | Business logic, state handles, timer payloads | Polling, commit offsets, event ordering |
/// | `OperatorContext` | DB access, timer registration, table/state creation | Lifecycle transitions |
/// | `OperatorWorker` | Input attachment/detachment, spawn | Run loop details |
/// | `RunLoop` | Event queue, shutdown state machine, poll+commit, idle/wake | What the operator does with changes |
pub trait Operator: Send + 'static {
    /// Operator-specific configuration. Persisted in the operator catalog.
    type Config: Debug + Clone + PartialEq + Encode + Decode<()> + 'static;

    /// Typed timer payload. Use `()` if the operator does not use timers.
    type Timer: Encode + Decode<()> + 'static;

    /// Construct the operator with full context available.
    ///
    /// Unlike a plain constructor, `create` receives the [`OperatorContext`]
    /// so you can immediately open state handles and tables without the
    /// `Option<Handle>` pattern:
    ///
    /// ```ignore
    /// fn create<'a, D>(ctx: &'a OperatorContext<Self, D>) -> BoxFuture<'a, io::Result<Self>>
    /// where
    ///     D: DispatchOperator,
    ///     Self: Sized,
    /// {
    ///     Box::pin(async move {
    ///         let index = ctx.state("index", Some(StateConfig::default()))?;
    ///         Ok(Self { index })
    ///     })
    /// }
    /// ```
    fn create<'a, D>(
        ctx: &'a OperatorContext<Self, D>,
    ) -> BoxFuture<'a, io::Result<Self>>
    where
        D: DispatchOperator,
        Self: Sized;

    /// Process a batch of changes from subscribed tables.
    ///
    /// Called repeatedly while the operator is active and inputs produce data.
    /// Return [`OperatorDirective::Continue`] to keep polling or
    /// [`OperatorDirective::Finish`] to trigger teardown.
    fn process<'a, D>(
        &'a mut self,
        changes: Vec<Change>,
        ctx: &'a OperatorContext<Self, D>,
    ) -> BoxFuture<'a, io::Result<OperatorDirective>>
    where
        D: DispatchOperator,
    {
        let _ = (changes, ctx);
        Box::pin(async { Ok(OperatorDirective::Continue) })
    }

    /// Called when a new input table matching the subscription appears.
    ///
    /// The table name is passed so operators can react to dynamic inputs
    /// (e.g. creating per-table state). Default: no-op, continue.
    fn on_input_opened<'a, D>(
        &'a mut self,
        table: String,
        ctx: &'a OperatorContext<Self, D>,
    ) -> BoxFuture<'a, io::Result<OperatorDirective>>
    where
        D: DispatchOperator,
    {
        let _ = (table, ctx);
        Box::pin(async { Ok(OperatorDirective::Continue) })
    }

    /// Called when a previously open input table disappears.
    ///
    /// Default: no-op, continue.
    fn on_input_closed<'a, D>(
        &'a mut self,
        table: String,
        ctx: &'a OperatorContext<Self, D>,
    ) -> BoxFuture<'a, io::Result<OperatorDirective>>
    where
        D: DispatchOperator,
    {
        let _ = (table, ctx);
        Box::pin(async { Ok(OperatorDirective::Continue) })
    }

    /// Called when a registered processing-time timer fires.
    ///
    /// The `payload` is the typed value passed to
    /// [`OperatorContext::register_timer`]. `fire_at_ms` is the scheduled
    /// time. Return `Finish` to begin teardown.
    fn on_timer<'a, D>(
        &'a mut self,
        payload: Self::Timer,
        fire_at_ms: u64,
        ctx: &'a OperatorContext<Self, D>,
    ) -> BoxFuture<'a, io::Result<OperatorDirective>>
    where
        D: DispatchOperator,
    {
        let _ = (payload, fire_at_ms, ctx);
        Box::pin(async { Ok(OperatorDirective::Continue) })
    }

    /// Called exactly once when the operator is being permanently removed.
    ///
    /// Guaranteed to fire regardless of whether the operator finished
    /// successfully, failed, or was cancelled. Use `reason` to distinguish.
    /// Release resources (drop state handles, flush buffers, etc.) here.
    fn teardown<'a, D>(
        &'a mut self,
        reason: &'a TeardownReason,
        ctx: &'a OperatorContext<Self, D>,
    ) -> BoxFuture<'a, io::Result<()>>
    where
        D: DispatchOperator,
    {
        let _ = (reason, ctx);
        Box::pin(async { Ok(()) })
    }
}

/// Config types that carry an [`OperatorRuntimeConfig`].
///
/// The generated `OperatorConfig` struct implements this; every operator-set
/// config is required to so that the database and worker can access
/// subscriptions and poll size generically.
pub trait DispatchConfig:
    Debug + Clone + PartialEq + Encode + Decode<()> + Send + Sync + 'static
{
    fn runtime_config(&self) -> &OperatorRuntimeConfig;

    /// Construct the dispatch config from a typed operator config and runtime
    /// config. The operator type must be part of this dispatch set.
    fn new<O>(config: O::Config, runtime: OperatorRuntimeConfig) -> io::Result<Self>
    where
        O: Operator;
}

/// Dispatch trait for operator-set enums generated by [`define_operator_set!`].
///
/// `DispatchOperator` is the type-erased dispatch layer that the database,
/// worker, and scheduler know about. Timers flow through this layer as opaque
/// byte payloads, while the generated `OperatorInstance` enum decodes them and
/// delegates to the typed [`Operator`] methods.
///
/// # Methods map to `Operator` lifecycle:
///
/// | Dispatch method | Delegates to |
/// |-----------------|--------------|
/// | `create` | [`Operator::create`] — constructs the operator with context |
/// | `process` | [`Operator::process`] — handles a batch of changes |
/// | `on_input_opened` | [`Operator::on_input_opened`] — new table appeared |
/// | `on_input_closed` | [`Operator::on_input_closed`] — table disappeared |
/// | `on_timer` | [`Operator::on_timer`] — timer fired |
/// | `teardown` | [`Operator::teardown`] — permanent removal |
pub trait DispatchOperator: Send + 'static {
    type Config: DispatchConfig;

    /// Instantiate the concrete operator with full context.
    ///
    /// Builds the [`OperatorContext`] and delegates to [`Operator::create`].
    fn create<'a>(
        db: Weak<Database<Self>>,
        name: &'a str,
        config: &'a Self::Config,
    ) -> BoxFuture<'a, io::Result<Self>>
    where
        Self: Sized;

    /// Delegates to [`Operator::process`] with the typed context.
    fn process<'a>(
        &'a mut self,
        changes: Vec<Change>,
        db: Weak<Database<Self>>,
        name: &'a str,
        config: &'a Self::Config,
    ) -> BoxFuture<'a, io::Result<OperatorDirective>>
    where
        Self: Sized;

    /// Delegates to [`Operator::on_input_opened`].
    fn on_input_opened<'a>(
        &'a mut self,
        table: String,
        db: Weak<Database<Self>>,
        name: &'a str,
        config: &'a Self::Config,
    ) -> BoxFuture<'a, io::Result<OperatorDirective>>
    where
        Self: Sized;

    /// Delegates to [`Operator::on_input_closed`].
    fn on_input_closed<'a>(
        &'a mut self,
        table: String,
        db: Weak<Database<Self>>,
        name: &'a str,
        config: &'a Self::Config,
    ) -> BoxFuture<'a, io::Result<OperatorDirective>>
    where
        Self: Sized;

    /// Decodes the opaque `payload` bytes and delegates to
    /// [`Operator::on_timer`].
    fn on_timer<'a>(
        &'a mut self,
        payload: Vec<u8>,
        fire_at_ms: u64,
        db: Weak<Database<Self>>,
        name: &'a str,
        config: &'a Self::Config,
    ) -> BoxFuture<'a, io::Result<OperatorDirective>>
    where
        Self: Sized;

    /// Delegates to [`Operator::teardown`] for permanent removal.
    fn teardown<'a>(
        &'a mut self,
        reason: &'a TeardownReason,
        db: Weak<Database<Self>>,
        name: &'a str,
        config: &'a Self::Config,
    ) -> BoxFuture<'a, io::Result<()>>
    where
        Self: Sized;
}
