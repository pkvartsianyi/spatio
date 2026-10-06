//! Database builder

use crate::config::Config;
use crate::db::DB;
use crate::error::Result;
use std::path::PathBuf;

/// Builder for database configuration with custom persistence paths and settings.
#[derive(Debug)]
pub struct DBBuilder {
    path: Option<PathBuf>,
    config: Config,
}

impl DBBuilder {
    /// Create a new builder with default in-memory configuration.
    pub fn new() -> Self {
        Self {
            path: None,
            config: Config::default(),
        }
    }

    /// Set the path for persistence (Cold State trajectory log).
    pub fn path<P: Into<PathBuf>>(mut self, path: P) -> Self {
        self.path = Some(path.into());
        self
    }

    /// Configure for in-memory storage with no persistence.
    pub fn in_memory(mut self) -> Self {
        self.path = None;
        self
    }

    /// Set the database configuration.
    pub fn config(mut self, config: Config) -> Self {
        self.config = config;
        self
    }

    /// Build the database.
    pub fn build(self) -> Result<DB> {
        match self.path {
            Some(path) => DB::open_with_config(path, self.config),
            None => DB::memory_with_config(self.config),
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
        assert!(builder.path.is_none());
    }

    #[test]
    fn test_builder_in_memory() {
        let db = DBBuilder::new().in_memory().build().unwrap();
        // Verify basic operation
        db.upsert(
            "ns",
            "obj",
            Point3d::new(0.0, 0.0, 0.0),
            serde_json::json!({}),
            None,
        )
        .unwrap();
    }

    #[test]
    fn test_builder_with_path() {
        let temp_dir = std::env::temp_dir();
        let path = temp_dir.join("test_builder_new.db");
        let _ = std::fs::remove_dir_all(&path);

        let db = DBBuilder::new().path(&path).build().unwrap();
        db.upsert(
            "ns",
            "obj",
            Point3d::new(0.0, 0.0, 0.0),
            serde_json::json!({}),
            None,
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
