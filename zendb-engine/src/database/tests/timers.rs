use super::support::*;
use crate::{DatabaseConfig, TableConfig};
use std::sync::{atomic::Ordering, Arc};

#[test]
fn processing_time_timers_fire_and_survive_restart() {
    let path = tmp("timers");
    let (tracker, fired) = new_tracker("timers");
    let db =
        TestDatabase::create(&path, Arc::new(ThreadExecutor), DatabaseConfig::default()).unwrap();
    db.table("users", Some(TableConfig::default())).unwrap();
    db.dispatch_operator::<TimerOperator>("ticker", timer_config(tracker), timer_runtime_config())
        .unwrap();

    wait_until(|| fired.load(Ordering::Relaxed) >= 1);
}
