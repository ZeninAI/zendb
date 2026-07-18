//! Rhai scripting operator implementation.

use std::future::Future;
use std::io;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use rhai::{Array, CallFnOptions, Dynamic, Scope, AST};
use zendb_types::{Event, Hlc, Op, Path};

use crate::{
    Change, DispatchOperator, Operator, OperatorDirective, OperatorHost, OperatorPhase, TableConfig,
};

use super::api::ScriptContext;
use super::config::RhaiOperatorConfig;
use super::engine::{create_engine, handlers};
use super::types::ScriptChange;

/// Rhai scripting operator.
///
/// Executes user-provided Rhai scripts in response to operator lifecycle events.
/// Scripts can define handlers for various events and use the `db` module to
/// interact with the local workspace.
pub struct RhaiOperator {
    /// The Rhai engine instance.
    engine: rhai::Engine,
    /// Compiled script AST.
    ast: AST,
    /// Script execution scope (maintains state between calls).
    scope: Scope<'static>,
    /// Script context for workspace operations.
    context: ScriptContext,
}

impl RhaiOperator {
    /// Check if a handler function is defined in the script.
    fn has_handler(&self, name: &str) -> bool {
        self.ast.iter_functions().any(|f| f.name == name)
    }

    /// Call a handler function with no arguments.
    fn call_handler(&mut self, name: &str) -> io::Result<OperatorDirective> {
        if !self.has_handler(name) {
            return Ok(OperatorDirective::Continue);
        }

        let ctx = self.context.clone();
        let options = CallFnOptions::new().with_tag(Dynamic::from(ctx));
        let result = self.engine.call_fn_with_options::<Dynamic>(
            options,
            &mut self.scope,
            &self.ast,
            name,
            (),
        );

        self.handle_result(result)
    }

    /// Call a handler function with one argument.
    fn call_handler_1<A: Clone + Send + Sync + 'static>(
        &mut self,
        name: &str,
        arg: A,
    ) -> io::Result<OperatorDirective> {
        if !self.has_handler(name) {
            return Ok(OperatorDirective::Continue);
        }

        let ctx = self.context.clone();
        let options = CallFnOptions::new().with_tag(Dynamic::from(ctx));
        let result = self.engine.call_fn_with_options::<Dynamic>(
            options,
            &mut self.scope,
            &self.ast,
            name,
            (arg,),
        );

        self.handle_result(result)
    }

    /// Handle the result of a Rhai function call.
    fn handle_result(
        &self,
        result: Result<Dynamic, Box<rhai::EvalAltResult>>,
    ) -> io::Result<OperatorDirective> {
        match result {
            Ok(value) => {
                // Check if the handler returned a directive
                if value.is_string() {
                    let s: String = value.cast();
                    match s.as_str() {
                        "finish" | "Finish" | "FINISH" => return Ok(OperatorDirective::Finish),
                        "continue" | "Continue" | "CONTINUE" => {
                            return Ok(OperatorDirective::Continue)
                        }
                        _ => {}
                    }
                }
                Ok(OperatorDirective::Continue)
            }
            Err(e) => {
                log::error!("[Rhai] Script error: {}", e);
                Err(io::Error::new(
                    io::ErrorKind::Other,
                    format!("Rhai script error: {}", e),
                ))
            }
        }
    }

    /// Apply pending writes to the workspace.
    fn apply_pending<D>(&mut self, db: &Arc<OperatorHost<D>>, name: &str) -> io::Result<()>
    where
        D: DispatchOperator,
    {
        let pending = self.context.take_pending();

        // Get current time for HLC
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        // Reject the whole batch before applying any local event. Otherwise a
        // denied shared request later in the queue could leave earlier local
        // requests partially committed.
        if pending.events.iter().any(|event| event.sync) {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "shared Rhai writes require a policy-aware effect gate",
            ));
        }

        // Apply pending events
        for event in pending.events {
            // Ensure target table exists
            let table = db
                .table(&event.table)
                .config(TableConfig::default())
                .create()?;
            let table_guard = table.get()?;

            // Create and apply the event
            let op = match event.value {
                Some(value) => Op::Replace { value },
                None => Op::Delete,
            };

            let hlc = Hlc::new(now_ms, 0)
                .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "Failed to create HLC"))?;

            let zen_event = Event {
                table_id: event.table.clone(),
                primary_key: event.key.clone(),
                path: Path::new(),
                op,
                hlc,
            };

            table_guard.write().insert_event(zen_event)?;
        }

        // Apply pending timers
        for timer in pending.timers {
            let fire_at = now_ms + timer.delay_ms;
            db.register_timer(name, fire_at, &timer.payload)?;
        }

        Ok(())
    }
}

/// Public query interface for the Rhai operator.
///
/// Currently provides no query interface as Rhai operators are fully
/// script-driven. Future versions may expose script-defined queries.
#[derive(Clone)]
pub struct RhaiFacet;

impl Operator for RhaiOperator {
    type Config = RhaiOperatorConfig;
    type Timer = Vec<u8>;
    type Facet = RhaiFacet;

    fn create<'a, D>(
        db: &'a Arc<OperatorHost<D>>,
        name: &'a str,
        config: &'a Self::Config,
    ) -> impl Future<Output = io::Result<Self>> + Send + 'a
    where
        D: DispatchOperator,
        Self: Sized,
    {
        async move {
            // Create the engine
            let engine = create_engine(&config.policy);

            // Compile the script
            let ast = engine.compile(&config.script).map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Failed to compile Rhai script: {}", e),
                )
            })?;

            // Create scope with initial state
            let mut scope = Scope::new();

            // Add operator name and config to scope
            scope.push_constant("OPERATOR_NAME", name.to_owned());

            // Create script context
            let context = ScriptContext::new(name.to_owned(), config.policy.write_mode);

            let mut op = Self {
                engine,
                ast,
                scope,
                context,
            };

            // Call on_create handler
            op.call_handler(handlers::ON_CREATE)?;

            // Apply any pending writes from on_create
            op.apply_pending(db, name)?;

            Ok(op)
        }
    }

    fn facet(&self) -> Self::Facet {
        RhaiFacet
    }

    fn process<'a, D>(
        &'a mut self,
        changes: Vec<Change>,
        db: &'a Arc<OperatorHost<D>>,
        name: &'a str,
        _config: &'a Self::Config,
    ) -> impl Future<Output = io::Result<OperatorDirective>> + Send + 'a
    where
        D: DispatchOperator,
    {
        async move {
            if !self.has_handler(handlers::ON_PROCESS) {
                return Ok(OperatorDirective::Continue);
            }

            // Convert changes to Rhai array
            let script_changes: Array = changes
                .into_iter()
                .map(|c| Dynamic::from(ScriptChange::new(c)))
                .collect();

            // Call handler
            let directive = self.call_handler_1(handlers::ON_PROCESS, script_changes)?;

            // Apply pending writes
            self.apply_pending(db, name)?;

            Ok(directive)
        }
    }

    fn on_input_opened<'a, D>(
        &'a mut self,
        table: String,
        db: &'a Arc<OperatorHost<D>>,
        name: &'a str,
        _config: &'a Self::Config,
    ) -> impl Future<Output = io::Result<OperatorDirective>> + Send + 'a
    where
        D: DispatchOperator,
    {
        async move {
            let directive = self.call_handler_1(handlers::ON_INPUT_OPENED, table)?;
            self.apply_pending(db, name)?;
            Ok(directive)
        }
    }

    fn on_input_closed<'a, D>(
        &'a mut self,
        table: String,
        db: &'a Arc<OperatorHost<D>>,
        name: &'a str,
        _config: &'a Self::Config,
    ) -> impl Future<Output = io::Result<OperatorDirective>> + Send + 'a
    where
        D: DispatchOperator,
    {
        async move {
            let directive = self.call_handler_1(handlers::ON_INPUT_CLOSED, table)?;
            self.apply_pending(db, name)?;
            Ok(directive)
        }
    }

    fn on_timer<'a, D>(
        &'a mut self,
        payload: Self::Timer,
        _fire_at_ms: u64,
        db: &'a Arc<OperatorHost<D>>,
        name: &'a str,
        _config: &'a Self::Config,
    ) -> impl Future<Output = io::Result<OperatorDirective>> + Send + 'a
    where
        D: DispatchOperator,
    {
        async move {
            if !self.has_handler(handlers::ON_TIMER) {
                return Ok(OperatorDirective::Continue);
            }

            // Convert payload to string (or keep as bytes)
            let payload_str = String::from_utf8_lossy(&payload).to_string();

            let directive = self.call_handler_1(handlers::ON_TIMER, payload_str)?;
            self.apply_pending(db, name)?;
            Ok(directive)
        }
    }

    fn teardown<'a, D>(
        &'a mut self,
        phase: &'a OperatorPhase,
        db: &'a Arc<OperatorHost<D>>,
        name: &'a str,
        _config: &'a Self::Config,
    ) -> impl Future<Output = io::Result<()>> + Send + 'a
    where
        D: DispatchOperator,
    {
        async move {
            if !self.has_handler(handlers::ON_TEARDOWN) {
                return Ok(());
            }

            // Convert phase to string
            let phase_str = match phase {
                OperatorPhase::Active => "active",
                OperatorPhase::Finished => "finished",
                OperatorPhase::Failed { .. } => "failed",
                OperatorPhase::Cancelled => "cancelled",
            };

            let _ = self.call_handler_1(handlers::ON_TEARDOWN, phase_str.to_owned());
            self.apply_pending(db, name)?;
            Ok(())
        }
    }
}
