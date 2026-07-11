use super::support::*;
use crate::TableConfig;
use std::sync::{atomic::Ordering, Arc};

#[test]
fn processing_time_timers_fire() {
    let path = tmp("timers");
    let (tracker, fired) = new_tracker("timers");
    let db = TestWorkspace::create(&path, Arc::new(ThreadExecutor), workspace_config()).unwrap();
    db.table("users", Some(TableConfig::default())).unwrap();
    db.dispatch_operator::<TimerOperator>("ticker", timer_config(tracker), timer_runtime_config())
        .unwrap();

    wait_until(|| fired.load(Ordering::Relaxed) >= 1);
}
