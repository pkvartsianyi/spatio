//! Database builder for flexible configuration
//!
//! This module provides a builder pattern for creating databases with
//! advanced configuration options including custom persistence paths.

use crate::config::Config;
use crate::db::DB;
use crate::error::Result;
use std::path::PathBuf;

/// Builder for database configuration with custom persistence paths and settings.
#[derive(Debug)]
pub struct DBBuilder {
    path: Option<PathBuf>,
    config: Config,
    in_memory: bool,
}

impl DBBuilder {
    /// Create a new builder with default in-memory configuration.
    pub fn new() -> Self {
        Self {
            path: None,
            config: Config::default(),
            in_memory: true,
        }
    }

    /// Set the path for persistence (Cold State trajectory log).
    pub fn path<P: Into<PathBuf>>(mut self, path: P) -> Self {
        self.path = Some(path.into());
        self.in_memory = false;
        self
    }

    /// Legacy method for AOF path, maps to persistence path.
    #[cfg(feature = "aof")]
    pub fn aof_path<P: Into<PathBuf>>(self, path: P) -> Self {
        self.path(path)
    }

    /// Legacy method for snapshot path, currently ignored/unsupported in new architecture.
    #[cfg(feature = "snapshot")]
    pub fn snapshot_path<P: Into<PathBuf>>(self, _path: P) -> Self {
        // Snapshots are not yet implemented in the new architecture
        self
    }

    /// Configure for in-memory storage with no persistence.
    pub fn in_memory(mut self) -> Self {
        self.in_memory = true;
        self.path = None;
        self
    }

    /// Set the database configuration.
    pub fn config(mut self, config: Config) -> Self {
        self.config = config;
        self
    }

    /// Enable history tracking with a fixed per-key capacity.
    #[cfg(feature = "time-index")]
    pub fn history_capacity(mut self, capacity: usize) -> Self {
        self.config = self.config.clone().with_history_capacity(capacity);
        self
    }

    /// Build the database.
    pub fn build(self) -> Result<DB> {
        if self.in_memory {
            DB::memory_with_config(self.config)
        } else if let Some(path) = self.path {
            DB::open_with_config(path, self.config)
        } else {
            // Default to memory if no path provided but in_memory is false (shouldn't happen with current API usage but safe fallback)
            DB::memory_with_config(self.config)
        }
    }
}

impl Default for DBBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use spatio_types::point::Point3d;

    #[test]
    fn test_builder_default() {
        let builder = DBBuilder::new();
        assert!(builder.in_memory);
    }

    #[test]
    fn test_builder_in_memory() {
        let db = DBBuilder::new().in_memory().build().unwrap();
        // Verify basic operation
        db.update_location(
            "ns",
            "obj",
            Point3d::new(0.0, 0.0, 0.0),
            serde_json::json!({}),
        )
        .unwrap();
    }

    #[test]
    fn test_builder_with_path() {
        let temp_dir = std::env::temp_dir();
        let path = temp_dir.join("test_builder_new.db");
        let _ = std::fs::remove_dir_all(&path); // ColdState expects a directory for now or file? 
        // ColdState::new takes a path. If it's a file path, it uses it.

        let db = DBBuilder::new().path(&path).build().unwrap();
        db.update_location(
            "ns",
            "obj",
            Point3d::new(0.0, 0.0, 0.0),
            serde_json::json!({}),
        )
        .unwrap();

        // Cleanup
        if path.is_dir() {
            let _ = std::fs::remove_dir_all(path);
        } else {
            let _ = std::fs::remove_file(path);
        }
    }
}
