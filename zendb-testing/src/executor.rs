use std::time::Duration;

pub(crate) struct ThreadExecutor;

impl zendb_operator::runtime::Executor for ThreadExecutor {
    fn spawn(&self, future: zendb_operator::runtime::RuntimeFuture) {
        std::thread::spawn(move || futures::executor::block_on(future));
    }

    fn idle(&self) -> zendb_operator::runtime::RuntimeFuture {
        Box::pin(async { std::thread::sleep(Duration::from_millis(1)) })
    }

    fn sleep(&self, duration: Duration) -> zendb_operator::runtime::RuntimeFuture {
        Box::pin(async move { std::thread::sleep(duration) })
    }
}
