use serde::{Deserialize, Serialize};
use std::time::SystemTime;

/// Synchronization policy for persistence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum SyncPolicy {
    Never,
    #[default]
    EverySecond,
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
