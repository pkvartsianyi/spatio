//! Simplified types and configuration for Spatio
//!
//! This module provides streamlined, serializable types for configuration
//! and data management with minimal complexity.
use bytes::Bytes;
use geo::Point;
use serde::de::Error;
use serde::{Deserialize, Serialize};
use std::time::{Duration, SystemTime};

/// A geographic point with an associated timestamp.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TemporalPoint {
    pub point: Point,
    pub timestamp: SystemTime,
}

/// Synchronization policy for persistence
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum SyncPolicy {
    /// Never sync to disk (fastest, least safe)
    Never,
    /// Sync every second (recommended default)
    #[default]
    EverySecond,
    /// Sync after every write (slowest, safest)
    Always,
}

/// File synchronization strategy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum SyncMode {
    /// Call `fsync` / `File::sync_all` to persist metadata + data.
    #[default]
    All,
    /// Call `fdatasync` / `File::sync_data` to persist data only.
    Data,
}

/// Simplified database configuration
///
/// This configuration is designed to be easily serializable and loadable
/// from JSON, TOML, or other formats while keeping complexity minimal.
///
/// # Example
///
/// ```rust
/// use spatio::{Config, SyncPolicy};
/// use std::time::Duration;
///
/// // Create default config
/// let config = Config::default();
///
/// // Load from JSON
/// let json = r#"{
///     "sync_policy": "always",
///     "default_ttl_seconds": 3600,
///     "geohash_precision": 10
/// }"#;
/// let config: Config = serde_json::from_str(json).unwrap();
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// How often data is synced to disk
    #[serde(default)]
    pub sync_policy: SyncPolicy,

    /// Default TTL for items in seconds (None means no default TTL)
    #[serde(default)]
    pub default_ttl_seconds: Option<f64>,

    /// Geohash precision for spatial indexing (1-12, default: 8)
    /// Higher values = more precision but more memory usage
    #[serde(default = "Config::default_geohash_precision")]
    pub geohash_precision: usize,

    /// Controls whether the database issues `fsync` or `fdatasync`.
    #[serde(default)]
    pub sync_mode: SyncMode,

    /// Number of writes to batch before forcing a sync when `SyncPolicy::Always`.
    #[serde(default = "Config::default_sync_batch_size")]
    pub sync_batch_size: usize,

    /// Optional history capacity per key (number of events to retain)
    #[cfg(feature = "time-index")]
    #[serde(default)]
    pub history_capacity: Option<usize>,

    /// Batch size for amortized cleanup
    #[serde(default = "Config::default_amortized_cleanup_batch_size")]
    pub amortized_cleanup_batch: usize,
}

impl Config {
    const fn default_geohash_precision() -> usize {
        8
    }

    const fn default_sync_batch_size() -> usize {
        1
    }

    const fn default_amortized_cleanup_batch_size() -> usize {
        0
    }

    pub fn with_amortized_cleanup(mut self, batch_size: usize) -> Self {
        self.amortized_cleanup_batch = batch_size;
        self
    }

    pub fn with_geohash_precision(precision: usize) -> Self {
        assert!(
            (1..=12).contains(&precision),
            "Geohash precision must be between 1 and 12"
        );

        Self {
            sync_policy: SyncPolicy::default(),
            default_ttl_seconds: None,
            geohash_precision: precision,
            sync_mode: SyncMode::default(),
            sync_batch_size: Self::default_sync_batch_size(),
            #[cfg(feature = "time-index")]
            history_capacity: None,
            amortized_cleanup_batch: Self::default_amortized_cleanup_batch_size(),
        }
    }

    pub fn with_default_ttl(mut self, ttl: Duration) -> Self {
        self.default_ttl_seconds = Some(ttl.as_secs_f64());
        self
    }

    pub fn with_sync_policy(mut self, policy: SyncPolicy) -> Self {
        self.sync_policy = policy;
        self
    }

    pub fn with_sync_mode(mut self, mode: SyncMode) -> Self {
        self.sync_mode = mode;
        self
    }

    /// Adjust the number of writes to batch before syncing when `SyncPolicy::Always`.
    pub fn with_sync_batch_size(mut self, batch_size: usize) -> Self {
        assert!(batch_size > 0, "Sync batch size must be greater than zero");
        self.sync_batch_size = batch_size;
        self
    }

    /// Enable update history with a maximum number of entries per key.
    #[cfg(feature = "time-index")]
    pub fn with_history_capacity(mut self, capacity: usize) -> Self {
        assert!(capacity > 0, "History capacity must be greater than zero");
        self.history_capacity = Some(capacity);
        self
    }

    /// Get default TTL as Duration
    pub fn default_ttl(&self) -> Option<Duration> {
        self.default_ttl_seconds.and_then(|ttl| {
            if ttl.is_finite() && ttl > 0.0 && ttl <= u64::MAX as f64 {
                Some(Duration::from_secs_f64(ttl))
            } else {
                None
            }
        })
    }

    /// Validate configuration values
    pub fn validate(&self) -> Result<(), String> {
        if self.geohash_precision < 1 || self.geohash_precision > 12 {
            return Err("Geohash precision must be between 1 and 12".to_string());
        }

        if let Some(ttl) = self.default_ttl_seconds {
            if !ttl.is_finite() {
                return Err("Default TTL must be finite (not NaN or infinity)".to_string());
            }
            if ttl <= 0.0 {
                return Err("Default TTL must be positive".to_string());
            }
            if ttl > u64::MAX as f64 {
                return Err("Default TTL is too large".to_string());
            }
        }

        #[cfg(feature = "time-index")]
        if let Some(capacity) = self.history_capacity
            && capacity == 0
        {
            return Err("History capacity must be greater than zero".to_string());
        }

        if self.sync_batch_size == 0 {
            return Err("Sync batch size must be greater than zero".to_string());
        }

        Ok(())
    }

    /// Load configuration from JSON string
    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        let config: Config = serde_json::from_str(json)?;
        if let Err(e) = config.validate() {
            return Err(Error::custom(e));
        }
        Ok(config)
    }

    /// Save configuration as JSON string
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }

    /// Load configuration from TOML string (requires toml feature)
    #[cfg(feature = "toml")]
    pub fn from_toml(toml_str: &str) -> Result<Self, toml::de::Error> {
        let config: Config = toml::from_str(toml_str)?;
        if let Err(e) = config.validate() {
            return Err(toml::de::Error::custom(e));
        }
        Ok(config)
    }

    /// Save configuration as TOML string (requires toml feature)
    #[cfg(feature = "toml")]
    pub fn to_toml(&self) -> Result<String, toml::ser::Error> {
        toml::to_string_pretty(self)
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            sync_policy: SyncPolicy::default(),
            default_ttl_seconds: None,
            geohash_precision: Self::default_geohash_precision(),
            sync_mode: SyncMode::default(),
            sync_batch_size: Self::default_sync_batch_size(),
            #[cfg(feature = "time-index")]
            history_capacity: None,
            amortized_cleanup_batch: Self::default_amortized_cleanup_batch_size(),
        }
    }
}

/// Options for setting values with optional TTL
#[derive(Debug, Clone, Default)]
pub struct SetOptions {
    /// Time-to-live for this item
    pub ttl: Option<Duration>,
    /// Absolute expiration time (takes precedence over TTL)
    pub expires_at: Option<SystemTime>,
}

impl SetOptions {
    /// Create options with TTL
    pub fn with_ttl(ttl: Duration) -> Self {
        Self {
            ttl: Some(ttl),
            expires_at: None,
        }
    }

    /// Create options with absolute expiration time
    pub fn with_expiration(expires_at: SystemTime) -> Self {
        Self {
            ttl: None,
            expires_at: Some(expires_at),
        }
    }

    /// Get the effective expiration time
    pub fn effective_expires_at(&self) -> Option<SystemTime> {
        self.expires_at
            .or_else(|| self.ttl.map(|ttl| SystemTime::now() + ttl))
    }
}

/// Internal representation of a database item
#[derive(Debug, Clone)]
pub struct DbItem {
    /// The value bytes
    pub value: Bytes,
    pub created_at: SystemTime,
    /// Expiration time (if any)
    pub expires_at: Option<SystemTime>,
}

/// Operation types captured in history tracking.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HistoryEventKind {
    Set,
    Delete,
}

/// Historical record for key mutations.
#[derive(Debug, Clone)]
pub struct HistoryEntry {
    pub timestamp: SystemTime,
    pub kind: HistoryEventKind,
    pub value: Option<Bytes>,
    pub expires_at: Option<SystemTime>,
}

impl DbItem {
    /// Create a new item without expiration
    pub fn new(value: impl Into<Bytes>) -> Self {
        Self {
            value: value.into(),
            created_at: SystemTime::now(),
            expires_at: None,
        }
    }

    /// Create an item with absolute expiration
    pub fn with_expiration(value: impl Into<Bytes>, expires_at: SystemTime) -> Self {
        Self {
            value: value.into(),
            created_at: SystemTime::now(),
            expires_at: Some(expires_at),
        }
    }

    /// Create an item with TTL
    pub fn with_ttl(value: impl Into<Bytes>, ttl: Duration) -> Self {
        let expires_at = SystemTime::now() + ttl;
        Self::with_expiration(value, expires_at)
    }

    /// Create from SetOptions
    pub fn from_options(value: impl Into<Bytes>, options: Option<&SetOptions>) -> Self {
        let value = value.into();

        match options {
            Some(opts) => {
                let expires_at = opts.effective_expires_at();
                Self {
                    value,
                    created_at: SystemTime::now(),
                    expires_at,
                }
            }
            None => Self::new(value),
        }
    }

    /// Check if this item has expired
    pub fn is_expired(&self) -> bool {
        self.is_expired_at(SystemTime::now())
    }

    /// Check if this item has expired at a specific time
    pub fn is_expired_at(&self, now: SystemTime) -> bool {
        match self.expires_at {
            Some(expires_at) => now >= expires_at,
            None => false,
        }
    }

    /// Get remaining TTL
    pub fn remaining_ttl(&self) -> Option<Duration> {
        self.remaining_ttl_at(SystemTime::now())
    }

    /// Get remaining TTL at a specific time
    pub fn remaining_ttl_at(&self, now: SystemTime) -> Option<Duration> {
        match self.expires_at {
            Some(expires_at) => {
                if now < expires_at {
                    expires_at.duration_since(now).ok()
                } else {
                    Some(Duration::ZERO)
                }
            }
            None => None,
        }
    }
}

/// Database statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DbStats {
    /// Number of keys in the database
    pub key_count: usize,
    /// Number of items that have expired
    pub expired_count: u64,
    /// Total number of operations performed
    pub operations_count: u64,
    /// Total size in bytes (approximate)
    pub size_bytes: usize,
}

impl DbStats {
    /// Create new empty statistics
    pub fn new() -> Self {
        Self::default()
    }

    /// Record an operation
    pub fn record_operation(&mut self) {
        self.operations_count += 1;
    }

    /// Record expired items cleanup
    pub fn record_expired(&mut self, count: u64) {
        self.expired_count += count;
    }

    /// Update key count
    pub fn set_key_count(&mut self, count: usize) {
        self.key_count = count;
    }

    /// Update size estimate
    pub fn set_size_bytes(&mut self, bytes: usize) {
        self.size_bytes = bytes;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_config_default() {
        let config = Config::default();
        assert_eq!(config.sync_policy, SyncPolicy::EverySecond);
        assert_eq!(config.sync_mode, SyncMode::All);
        assert_eq!(config.sync_batch_size, 1);
        assert_eq!(config.geohash_precision, 8);
        assert!(config.default_ttl_seconds.is_none());
        #[cfg(feature = "time-index")]
        assert!(config.history_capacity.is_none());
    }

    #[test]
    fn test_config_with_geohash_precision() {
        let config = Config::with_geohash_precision(10);
        assert_eq!(config.geohash_precision, 10);
    }

    #[test]
    #[should_panic(expected = "Geohash precision must be between 1 and 12")]
    fn test_config_invalid_precision() {
        Config::with_geohash_precision(15);
    }

    #[test]
    fn test_config_serialization() {
        let config = Config::with_geohash_precision(10)
            .with_default_ttl(Duration::from_secs(3600))
            .with_sync_policy(SyncPolicy::Always)
            .with_sync_mode(SyncMode::Data)
            .with_sync_batch_size(8);

        let json = config.to_json().unwrap();
        let deserialized: Config = Config::from_json(&json).unwrap();

        assert_eq!(deserialized.geohash_precision, 10);
        assert_eq!(deserialized.sync_policy, SyncPolicy::Always);
        assert_eq!(deserialized.sync_mode, SyncMode::Data);
        assert_eq!(deserialized.sync_batch_size, 8);
        assert_eq!(
            deserialized.default_ttl().unwrap(),
            Duration::from_secs(3600)
        );
    }

    #[cfg(feature = "time-index")]
    #[test]
    fn test_config_history_capacity() {
        let config = Config::default().with_history_capacity(5);
        assert_eq!(config.history_capacity, Some(5));
    }

    #[test]
    fn test_set_options() {
        let ttl_opts = SetOptions::with_ttl(Duration::from_secs(60));
        assert!(ttl_opts.ttl.is_some());
        assert!(ttl_opts.expires_at.is_none());

        let exp_opts = SetOptions::with_expiration(SystemTime::now());
        assert!(exp_opts.ttl.is_none());
        assert!(exp_opts.expires_at.is_some());
    }

    #[test]
    fn test_db_item_expiration() {
        let item = DbItem::new("test");
        assert!(!item.is_expired());

        let past = SystemTime::now() - Duration::from_secs(60);
        let expired_item = DbItem::with_expiration("test", past);
        assert!(expired_item.is_expired());

        let future = SystemTime::now() + Duration::from_secs(60);
        let future_item = DbItem::with_expiration("test", future);
        assert!(!future_item.is_expired());
    }

    #[test]
    fn test_db_item_ttl() {
        let item = DbItem::with_ttl("test", Duration::from_secs(60));
        let remaining = item.remaining_ttl().unwrap();

        // Should be close to 60 seconds (allowing for small timing differences)
        assert!(remaining.as_secs() >= 59 && remaining.as_secs() <= 60);
    }

    #[test]
    fn test_db_item_from_options() {
        let opts = SetOptions::with_ttl(Duration::from_secs(300));
        let item = DbItem::from_options("test", Some(&opts));

        assert!(item.expires_at.is_some());
        assert!(!item.is_expired());
    }

    #[test]
    fn test_db_stats() {
        let mut stats = DbStats::new();
        assert_eq!(stats.operations_count, 0);

        stats.record_operation();
        assert_eq!(stats.operations_count, 1);

        stats.record_expired(5);
        assert_eq!(stats.expired_count, 5);

        stats.set_key_count(100);
        assert_eq!(stats.key_count, 100);
    }

    #[test]
    fn test_config_validation() {
        let mut config = Config::default();
        assert!(config.validate().is_ok());

        config.geohash_precision = 15;
        assert!(config.validate().is_err());

        config.geohash_precision = 8;
        config.default_ttl_seconds = Some(-1.0);
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_ttl_validation() {
        let mut config = Config::default();
        assert!(config.validate().is_ok());

        // Valid TTL
        config = Config {
            default_ttl_seconds: Some(60.0),
            ..Default::default()
        };
        assert!(config.validate().is_ok());

        // Negative TTL
        config.default_ttl_seconds = Some(-1.0);
        assert!(config.validate().is_err());

        // Zero TTL
        config.default_ttl_seconds = Some(0.0);
        assert!(config.validate().is_err());

        // NaN TTL
        config.default_ttl_seconds = Some(f64::NAN);
        assert!(config.validate().is_err());

        // Positive infinity TTL
        config.default_ttl_seconds = Some(f64::INFINITY);
        assert!(config.validate().is_err());

        // Negative infinity TTL
        config.default_ttl_seconds = Some(f64::NEG_INFINITY);
        assert!(config.validate().is_err());

        // Too large TTL (use 1e20 which is definitely larger than u64::MAX as f64)
        config.default_ttl_seconds = Some(1e20);
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_default_ttl_safe_conversion() {
        let mut config = Config {
            default_ttl_seconds: Some(60.0),
            ..Default::default()
        };

        // Valid TTL should convert successfully
        assert!(config.default_ttl().is_some());

        // NaN should return None (safe fallback)
        config.default_ttl_seconds = Some(f64::NAN);
        assert!(config.default_ttl().is_none());

        // Infinity should return None (safe fallback)
        config.default_ttl_seconds = Some(f64::INFINITY);
        assert!(config.default_ttl().is_none());

        // Negative values should return None (safe fallback)
        config.default_ttl_seconds = Some(-1.0);
        assert!(config.default_ttl().is_none());

        // Too large values should return None (safe fallback)
        config.default_ttl_seconds = Some(1e20);
        assert!(config.default_ttl().is_none());

        // Zero should return None (safe fallback)
        config.default_ttl_seconds = Some(0.0);
        assert!(config.default_ttl().is_none());
    }
}
