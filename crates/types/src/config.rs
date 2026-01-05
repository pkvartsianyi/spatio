use serde::{Deserialize, Serialize};
use std::time::{Duration, SystemTime};

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

/// Options for setting values with TTL.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SetOptions {
    /// Time-to-live for this item
    pub ttl: Option<Duration>,
    /// Absolute expiration time (takes precedence over TTL)
    pub expires_at: Option<SystemTime>,
    /// Optional timestamp for the update (defaults to now if None)
    pub timestamp: Option<SystemTime>,
}

impl SetOptions {
    pub fn with_ttl(ttl: Duration) -> Self {
        Self {
            ttl: Some(ttl),
            expires_at: None,
            timestamp: None,
        }
    }

    pub fn with_expiration(expires_at: SystemTime) -> Self {
        Self {
            ttl: None,
            expires_at: Some(expires_at),
            timestamp: None,
        }
    }

    pub fn with_timestamp(timestamp: SystemTime) -> Self {
        Self {
            ttl: None,
            expires_at: None,
            timestamp: Some(timestamp),
        }
    }

    pub fn effective_expires_at(&self) -> Option<SystemTime> {
        self.expires_at
            .or_else(|| self.ttl.map(|ttl| SystemTime::now() + ttl))
    }
}
