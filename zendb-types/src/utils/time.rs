//! Wall-clock helpers shared across crates.

use std::time::{SystemTime, UNIX_EPOCH};

/// Current wall-clock time as milliseconds since the Unix epoch.
///
/// Centralizes the `SystemTime` → `u64` conversion and overflow handling so
/// callers do not each inline `SystemTime::now().duration_since(UNIX_EPOCH)`.
/// Returns `None` if the system clock is before the epoch or the millisecond
/// count does not fit in `u64`.
pub fn physical_ms() -> Option<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()?
        .as_millis()
        .try_into()
        .ok()
}
