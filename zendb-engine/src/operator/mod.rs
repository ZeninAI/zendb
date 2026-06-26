//! Operator definitions, registrations, typed state, and execution.

mod context;
mod registry;
pub(crate) mod state;
pub(crate) mod worker;

use std::{future::Future, io, pin::Pin};

use bincode::{Decode, Encode};

pub use context::OperatorContext;
pub use registry::OperatorRegistry;
pub use state::State;
pub use zendb_types::Change;

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

// ── Subscription ─────────────────────────────────────────────────────────────

/// A glob pattern that determines which tables an operator subscribes to.
///
/// | Pattern    | Meaning                                           |
/// |------------|---------------------------------------------------|
/// | `"*"`      | Every table (`Subscription::all()`)               |
/// | `"wiki-*"` | Tables whose name starts with `"wiki-"`           |
/// | `"*-log"`  | Tables whose name ends with `"-log"`              |
/// | `"users"`  | Exactly the table `"users"`                       |
///
/// Multiple `*` wildcards are allowed; the pattern is **not** a regex.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct Subscription(pub String);

impl Subscription {
    /// Subscribe to every table (equivalent to pattern `"*"`).
    pub fn all() -> Self {
        Self("*".into())
    }

    /// Subscribe to tables matching a glob pattern.
    pub fn pattern(pattern: impl Into<String>) -> Self {
        Self(pattern.into())
    }

    /// Returns `true` if `table` is matched by this subscription's pattern.
    pub(crate) fn matches(&self, table: &str) -> bool {
        glob_matches(&self.0, table)
    }

    /// Returns `true` if the pattern contains at least one `*`, meaning it
    /// may match tables that do not yet exist.
    pub(crate) fn is_wildcard(&self) -> bool {
        self.0.contains('*')
    }
}

/// Match `value` against a simple `*`-only glob `pattern`.
fn glob_matches(pattern: &str, value: &str) -> bool {
    let segments: Vec<&str> = pattern.split('*').collect();
    if segments.len() == 1 {
        return pattern == value;
    }
    let mut remaining = value;
    for (i, segment) in segments.iter().enumerate() {
        if segment.is_empty() {
            continue;
        }
        if i == 0 {
            if !remaining.starts_with(segment) {
                return false;
            }
            remaining = &remaining[segment.len()..];
        } else if i == segments.len() - 1 {
            return remaining.ends_with(segment);
        } else {
            match remaining.find(segment) {
                Some(pos) => remaining = &remaining[pos + segment.len()..],
                None => return false,
            }
        }
    }
    true
}

// ── RetryConfig ───────────────────────────────────────────────────────────────

/// Retry policy for a failing operator `process` call.
#[derive(Debug, Clone, Encode, Decode)]
pub struct RetryConfig {
    /// Maximum consecutive failures before the operator is permanently retired.
    /// `0` means unlimited retries.
    pub max_attempts: usize,
    /// Initial retry delay in milliseconds.
    pub initial_delay_ms: u64,
    /// Upper bound on the retry delay in milliseconds.
    pub max_delay_ms: u64,
    /// Random jitter factor: `0.0` = none, `1.0` = full.
    /// Actual delay = `computed_delay × (1 + jitter_factor × random[0,1))`.
    pub jitter_factor: f64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 0,
            initial_delay_ms: 100,
            max_delay_ms: 30_000,
            jitter_factor: 0.25,
        }
    }
}

// ── OperatorPhase ─────────────────────────────────────────────────────────────

/// Persisted lifecycle phase of an operator in the catalog.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub enum OperatorPhase {
    /// The operator is actively running (or will be started on next table open).
    Running,
    /// Explicitly stopped (e.g. via `close_operator`). Restarts on next activation.
    Stopped,
    /// The operator returned [`OperatorStatus::Finish`] and exited cleanly.
    Finished,
    /// The operator exhausted its retry budget and was permanently retired.
    Failed { error: String },
}

// ── OperatorConfig ────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Encode, Decode)]
pub struct OperatorConfig {
    pub implementation: String,
    /// Raw (bincode-serialised) operator configuration bytes. Internal only —
    /// users construct configs via [`OperatorConfig::for_operator`] and never
    /// touch the bytes directly.
    pub(crate) configuration: Vec<u8>,
    pub subscriptions: Vec<Subscription>,
    pub retry: RetryConfig,
    /// Maximum number of changes consumed from subscribed tables per poll cycle.
    pub poll_size: usize,
}

// ── OperatorStatus ────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperatorStatus {
    Continue,
    Finish,
}

// ── ErasedOperator (internal dispatch) ───────────────────────────────────────

pub(crate) trait ErasedOperator: Send + 'static {
    fn open<'a>(&'a mut self, ctx: OperatorContext) -> BoxFuture<'a, io::Result<()>>;
    fn process<'a>(&'a mut self, changes: Vec<Change>, ctx: OperatorContext) -> BoxFuture<'a, io::Result<OperatorStatus>>;
    fn on_timer<'a>(&'a mut self, payload: Vec<u8>, ctx: OperatorContext) -> BoxFuture<'a, io::Result<()>>;
    fn finish<'a>(&'a mut self, ctx: OperatorContext) -> BoxFuture<'a, io::Result<()>>;
}

// ── Operator trait ────────────────────────────────────────────────────────────

/// A stateful computation bound to a database.
///
/// Declare `type Config` for the operator's stored configuration and
/// `type Timer` for the timer payload type. The engine decodes both
/// automatically — `handle_timer` receives the decoded value directly.
/// Use `type Timer = ()` for operators that never register timers.
///
/// Register with [`OperatorRegistry::register_operator`]; build configs with
/// [`OperatorConfig::for_operator`].
pub trait Operator: Send + 'static {
    type Config: Encode + Decode<()> + 'static;
    type Timer: Encode + Decode<()> + 'static;

    fn open<'a>(&'a mut self, _ctx: OperatorContext) -> BoxFuture<'a, io::Result<()>> {
        Box::pin(async { Ok(()) })
    }

    fn process<'a>(
        &'a mut self,
        changes: Vec<Change>,
        ctx: OperatorContext,
    ) -> BoxFuture<'a, io::Result<OperatorStatus>>;

    fn handle_timer<'a>(
        &'a mut self,
        _payload: Self::Timer,
        _ctx: OperatorContext,
    ) -> BoxFuture<'a, io::Result<()>> {
        Box::pin(async { Ok(()) })
    }

    fn finish<'a>(&'a mut self, _ctx: OperatorContext) -> BoxFuture<'a, io::Result<()>> {
        Box::pin(async { Ok(()) })
    }
}

impl<T: Operator> ErasedOperator for T {
    fn open<'a>(&'a mut self, ctx: OperatorContext) -> BoxFuture<'a, io::Result<()>> {
        Operator::open(self, ctx)
    }

    fn process<'a>(
        &'a mut self,
        changes: Vec<Change>,
        ctx: OperatorContext,
    ) -> BoxFuture<'a, io::Result<OperatorStatus>> {
        Operator::process(self, changes, ctx)
    }

    fn on_timer<'a>(
        &'a mut self,
        payload: Vec<u8>,
        ctx: OperatorContext,
    ) -> BoxFuture<'a, io::Result<()>> {
        Box::pin(async move {
            let timer: T::Timer = ctx.decode(&payload)?;
            Operator::handle_timer(self, timer, ctx).await
        })
    }

    fn finish<'a>(&'a mut self, ctx: OperatorContext) -> BoxFuture<'a, io::Result<()>> {
        Operator::finish(self, ctx)
    }
}

// ── OperatorConfig helpers ────────────────────────────────────────────────────

impl OperatorConfig {
    pub fn for_operator<O: Operator>(
        implementation: impl Into<String>,
        config: &O::Config,
        subscriptions: Vec<Subscription>,
        retry: RetryConfig,
    ) -> io::Result<Self> {
        let configuration = bincode::encode_to_vec(config, bincode::config::standard())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
        Ok(Self {
            implementation: implementation.into(),
            configuration,
            subscriptions,
            retry,
            poll_size: 128,
        })
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn glob_exact() {
        assert!(glob_matches("users", "users"));
        assert!(!glob_matches("users", "posts"));
    }

    #[test]
    fn glob_star_all() {
        assert!(glob_matches("*", "users"));
        assert!(glob_matches("*", ""));
    }

    #[test]
    fn glob_prefix() {
        assert!(glob_matches("wiki-*", "wiki-pages"));
        assert!(glob_matches("wiki-*", "wiki-"));
        assert!(!glob_matches("wiki-*", "other"));
    }

    #[test]
    fn glob_suffix() {
        assert!(glob_matches("*-log", "users-log"));
        assert!(!glob_matches("*-log", "users-data"));
    }

    #[test]
    fn glob_multi_star() {
        assert!(glob_matches("user-*-data", "user-john-data"));
        assert!(!glob_matches("user-*-data", "user-john-other"));
    }
}

