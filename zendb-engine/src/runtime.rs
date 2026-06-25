//! Application-executor-backed asynchronous operator runtime.

use std::{future::Future, pin::Pin, time::Duration};

pub type RuntimeFuture = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

pub trait Executor: Send + Sync + 'static {
    fn spawn(&self, future: RuntimeFuture);

    /// Yield an idle operator polling loop back to the application runtime.
    fn idle(&self) -> RuntimeFuture;

    /// Sleep for approximately `duration`. Thread-per-task executors may
    /// implement this as a blocking `std::thread::sleep`; async runtimes
    /// should use their native sleep primitive.
    fn sleep(&self, duration: Duration) -> RuntimeFuture;
}
