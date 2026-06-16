//! Time-conversion helpers shared across the workspace.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Convert an `f64` of seconds-since-the-Unix-epoch into a [`SystemTime`].
///
/// Returns an error instead of panicking on non-finite, negative, or
/// out-of-range input. This matters because the value often arrives straight
/// off the wire or from a Python caller, and [`Duration::from_secs_f64`] panics
/// on exactly those cases.
pub fn system_time_from_secs(secs: f64) -> Result<SystemTime, String> {
    let dur =
        Duration::try_from_secs_f64(secs).map_err(|e| format!("invalid timestamp {secs}: {e}"))?;
    UNIX_EPOCH
        .checked_add(dur)
        .ok_or_else(|| format!("timestamp out of range: {secs}"))
}
