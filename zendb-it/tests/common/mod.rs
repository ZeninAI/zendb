//! Shared initialization used by the integration-test executables.

pub fn init_logging() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing_subscriber::filter::LevelFilter::DEBUG)
        .with_test_writer()
        .try_init();
}
