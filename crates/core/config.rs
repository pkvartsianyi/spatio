//! Configuration and database settings for Spatio
//!
//! This module provides configuration types and re-exports spatial types
//! from the `spatio-types` crate for convenience.
use serde::de::Error;

pub use spatio_types::bbox::{
    BoundingBox2D, BoundingBox3D, TemporalBoundingBox2D, TemporalBoundingBox3D,
};
pub use spatio_types::point::{Point3d, TemporalPoint, TemporalPoint3D};
pub use spatio_types::polygon::{Polygon3D, PolygonDynamic, PolygonDynamic3D};

pub use spatio_types::config::{SyncMode, SyncPolicy};

/// Database configuration
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default = "Config::default_sync_policy")]
    pub sync_policy: SyncPolicy,

    #[serde(default)]
    pub sync_mode: SyncMode,

    #[serde(default = "Config::default_sync_batch_size")]
    pub sync_batch_size: usize,

    /// Buffer capacity per object for recent history in ColdState
    #[serde(default = "Config::default_buffer_capacity")]
    pub buffer_capacity: usize,

    /// Persistence configuration
    #[serde(default)]
    pub persistence: PersistenceConfig,
}

/// Configuration for data persistence and durability
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PersistenceConfig {
    /// Number of writes to buffer in memory before flushing to disk
    #[serde(default = "PersistenceConfig::default_buffer_size")]
    pub buffer_size: usize,
}

impl PersistenceConfig {
    const fn default_buffer_size() -> usize {
        512
    }
}

impl Default for PersistenceConfig {
    fn default() -> Self {
        Self {
            buffer_size: Self::default_buffer_size(),
        }
    }
}

impl Config {
    const fn default_sync_batch_size() -> usize {
        1
    }

    const fn default_sync_policy() -> SyncPolicy {
        SyncPolicy::EverySecond
    }

    pub fn with_sync_policy(mut self, policy: SyncPolicy) -> Self {
        self.sync_policy = policy;
        self
    }

    pub fn with_sync_mode(mut self, mode: SyncMode) -> Self {
        self.sync_mode = mode;
        self
    }

    pub fn with_sync_batch_size(mut self, batch_size: usize) -> Self {
        assert!(batch_size > 0, "Sync batch size must be greater than zero");
        self.sync_batch_size = batch_size;
        self
    }

    const fn default_buffer_capacity() -> usize {
        100
    }

    pub fn with_buffer_capacity(mut self, capacity: usize) -> Self {
        assert!(capacity > 0, "Buffer capacity must be greater than zero");
        self.buffer_capacity = capacity;
        self
    }

    pub fn with_persistence(mut self, config: PersistenceConfig) -> Self {
        self.persistence = config;
        self
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.sync_batch_size == 0 {
            return Err("Sync batch size must be greater than zero".to_string());
        }

        if self.buffer_capacity == 0 {
            return Err("Buffer capacity must be greater than zero".to_string());
        }

        Ok(())
    }

    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        let config: Config = serde_json::from_str(json)?;
        if let Err(e) = config.validate() {
            return Err(Error::custom(e));
        }
        Ok(config)
    }

    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }

    #[cfg(feature = "toml")]
    pub fn from_toml(toml_str: &str) -> Result<Self, toml::de::Error> {
        let config: Config = toml::from_str(toml_str)?;
        if let Err(e) = config.validate() {
            return Err(toml::de::Error::custom(e));
        }
        Ok(config)
    }

    #[cfg(feature = "toml")]
    pub fn to_toml(&self) -> Result<String, toml::ser::Error> {
        toml::to_string_pretty(self)
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            sync_policy: SyncPolicy::default(),
            sync_mode: SyncMode::default(),
            sync_batch_size: Self::default_sync_batch_size(),
            buffer_capacity: Self::default_buffer_capacity(),
            persistence: PersistenceConfig::default(),
        }
    }
}

pub use spatio_types::config::SetOptions;

pub use spatio_types::stats::DbStats;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_default() {
        let config = Config::default();
        assert_eq!(config.sync_policy, SyncPolicy::EverySecond);
        assert_eq!(config.sync_mode, SyncMode::All);
        assert_eq!(config.sync_batch_size, 1);
    }

    #[test]
    fn test_config_serialization() {
        let config = Config::default()
            .with_sync_policy(SyncPolicy::Always)
            .with_sync_mode(SyncMode::Data)
            .with_sync_batch_size(8);

        let json = config.to_json().unwrap();
        let deserialized: Config = Config::from_json(&json).unwrap();

        assert_eq!(deserialized.sync_policy, SyncPolicy::Always);
        assert_eq!(deserialized.sync_mode, SyncMode::Data);
        assert_eq!(deserialized.sync_batch_size, 8);
    }

    #[test]
    fn test_set_options() {
        let opts = SetOptions::with_timestamp(std::time::SystemTime::now());
        assert!(opts.timestamp.is_some());
    }

    #[test]
    fn test_config_validation() {
        let config = Config::default();
        assert!(config.validate().is_ok());
        assert!(
            Config::from_json(r#"{"buffer_capacity": 0}"#).is_err(),
            "zero buffer capacity must be rejected"
        );
    }
}
