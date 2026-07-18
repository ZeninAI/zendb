//! Application-executor-backed asynchronous operator runtime.

use std::{future::Future, pin::Pin, time::Duration};

pub type RuntimeFuture = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

/// Async runtime abstraction. Allows the engine to run on any executor
/// (tokio, smol, a thread-per-task scheduler, etc.).
pub trait Executor: Send + Sync + 'static {
    /// Spawn a future onto the executor.
    fn spawn(&self, future: RuntimeFuture);

    /// Yield back to the executor when the operator has no work pending.
    fn idle(&self) -> RuntimeFuture;

    /// Sleep for approximately `duration`. Thread-per-task executors may
    /// implement this as a blocking `std::thread::sleep`; async runtimes
    /// should use their native sleep primitive.
    fn sleep(&self, duration: Duration) -> RuntimeFuture;
}
