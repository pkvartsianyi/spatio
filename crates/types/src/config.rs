use serde::{Deserialize, Serialize};
use std::time::SystemTime;

/// Synchronization policy for persistence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum SyncPolicy {
    /// Never `fsync`; rely on the OS to flush. Fastest, least durable.
    Never,
    /// `fsync` at most once per second. The interval is checked on each write,
    /// so an idle database does not sync until the next write (or close).
    #[default]
    EverySecond,
    /// `fsync` every `sync_batch_size` writes. Most durable, slowest.
    Always,
}

/// File synchronization strategy (fsync vs fdatasync).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum SyncMode {
    #[default]
    All,
    Data,
}

/// Options for setting values.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SetOptions {
    /// Optional timestamp for the update (defaults to now if None)
    pub timestamp: Option<SystemTime>,
}

impl SetOptions {
    pub fn with_timestamp(timestamp: SystemTime) -> Self {
        Self {
            timestamp: Some(timestamp),
        }
    }
}
