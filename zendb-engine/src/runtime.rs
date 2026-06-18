//! Application-executor-backed asynchronous computation runtime.

use std::{future::Future, pin::Pin};

pub type RuntimeFuture = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

pub trait Executor: Send + Sync + 'static {
    fn spawn(&self, future: RuntimeFuture);

    /// Yield an idle computation polling loop back to the application runtime.
    fn idle(&self) -> RuntimeFuture;
}
