//! Database builder for flexible configuration
//!
//! This module provides a builder pattern for creating databases with
//! advanced configuration options including custom AOF paths.

use crate::db::{DBInner, DB};
use crate::error::Result;
use crate::index::IndexManager;
use crate::persistence::AOFFile;
use crate::types::{Config, DbStats};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

/// Builder for creating database instances with custom configuration.
///
/// The `DBBuilder` provides a flexible way to configure databases with
/// options for:
/// - Custom AOF (Append-Only File) paths separate from the logical database path
/// - In-memory databases
/// - Full configuration control
/// - Automatic startup replay
///
/// # Examples
///
/// ## Basic usage with custom AOF path
/// ```rust
/// use spatio::DBBuilder;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let temp_path = std::env::temp_dir().join("test_db.aof");
/// let db = DBBuilder::new()
///     .aof_path(&temp_path)
///     .build()?;
///
/// db.insert("key", b"value", None)?;
/// # std::fs::remove_file(temp_path)?;
/// # Ok(())
/// # }
/// ```
///
/// ## In-memory database
/// ```rust
/// use spatio::DBBuilder;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let db = DBBuilder::new()
///     .in_memory()
///     .build()?;
/// # Ok(())
/// # }
/// ```
///
/// ## Full configuration
/// ```rust
/// use spatio::{DBBuilder, Config, SyncPolicy};
/// use std::time::Duration;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let config = Config::with_geohash_precision(10)
///     .with_sync_policy(SyncPolicy::Always)
///     .with_default_ttl(Duration::from_secs(3600));
///
/// let temp_path = std::env::temp_dir().join("high_precision.aof");
/// let db = DBBuilder::new()
///     .aof_path(&temp_path)
///     .config(config)
///     .build()?;
/// # std::fs::remove_file(temp_path)?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct DBBuilder {
    aof_path: Option<PathBuf>,
    config: Config,
    in_memory: bool,
}

impl DBBuilder {
    /// Create a new database builder with default configuration.
    ///
    /// By default, creates an in-memory database. Use `aof_path()` to
    /// enable persistence.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spatio::DBBuilder;
    ///
    /// let builder = DBBuilder::new();
    /// ```
    pub fn new() -> Self {
        Self {
            aof_path: None,
            config: Config::default(),
            in_memory: true,
        }
    }

    /// Set the AOF (Append-Only File) path for persistence.
    ///
    /// When an AOF path is set:
    /// - The database will persist all writes to this file
    /// - On startup, the AOF will be replayed to restore state
    /// - The database is durable across restarts
    ///
    /// If the file doesn't exist, it will be created. If it exists,
    /// it will be opened and replayed to restore previous state.
    ///
    /// # Arguments
    ///
    /// * `path` - File system path for the AOF file
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spatio::DBBuilder;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let temp_path = std::env::temp_dir().join("myapp_data.aof");
    /// let db = DBBuilder::new()
    ///     .aof_path(&temp_path)
    ///     .build()?;
    /// # std::fs::remove_file(temp_path)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn aof_path<P: Into<PathBuf>>(mut self, path: P) -> Self {
        self.aof_path = Some(path.into());
        self.in_memory = false;
        self
    }

    /// Create an in-memory database with no persistence.
    ///
    /// In-memory databases:
    /// - Are extremely fast (no disk I/O)
    /// - Do not persist data across restarts
    /// - Are ideal for caching, testing, and ephemeral data
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spatio::DBBuilder;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let db = DBBuilder::new()
    ///     .in_memory()
    ///     .build()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn in_memory(mut self) -> Self {
        self.in_memory = true;
        self.aof_path = None;
        self
    }

    /// Set the database configuration.
    ///
    /// The configuration controls:
    /// - Geohash precision for spatial indexing
    /// - Sync policy (durability vs performance tradeoff)
    /// - Default TTL for automatic expiration
    ///
    /// # Arguments
    ///
    /// * `config` - Database configuration
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spatio::{DBBuilder, Config, SyncPolicy};
    /// use std::time::Duration;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let config = Config::with_geohash_precision(10)
    ///     .with_sync_policy(SyncPolicy::Always)
    ///     .with_default_ttl(Duration::from_secs(3600));
    ///
    /// let temp_path = std::env::temp_dir().join("high_precision.aof");
    /// let db = DBBuilder::new()
    ///     .aof_path(&temp_path)
    ///     .config(config)
    ///     .build()?;
    /// # std::fs::remove_file(temp_path)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn config(mut self, config: Config) -> Self {
        self.config = config;
        self
    }

    /// Build the database with the configured options.
    ///
    /// This method:
    /// 1. Creates the database instance
    /// 2. Opens the AOF file (if persistence is enabled)
    /// 3. Replays the AOF to restore previous state (startup replay)
    /// 4. Rebuilds spatial indexes
    /// 5. Returns a ready-to-use database
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The AOF file cannot be opened or created
    /// - The AOF file is corrupted and cannot be replayed
    /// - File system permissions prevent access
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spatio::DBBuilder;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let temp_path = std::env::temp_dir().join("my_data.aof");
    /// let db = DBBuilder::new()
    ///     .aof_path(&temp_path)
    ///     .build()?;
    ///
    /// db.insert("key", b"value", None)?;
    /// # std::fs::remove_file(temp_path)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn build(self) -> Result<DB> {
        let mut inner = DBInner {
            keys: BTreeMap::new(),
            expirations: BTreeMap::new(),
            index_manager: IndexManager::with_config(&self.config),
            aof_file: None,
            closed: false,
            stats: DbStats::default(),
            config: self.config.clone(),
        };

        // Initialize persistence if AOF path is specified
        if !self.in_memory {
            if let Some(aof_path) = self.aof_path {
                let mut aof_file = AOFFile::open(&aof_path)?;
                // Automatic startup replay to restore previous state
                inner.load_from_aof(&mut aof_file)?;
                inner.aof_file = Some(aof_file);
            }
        }

        Ok(DB {
            inner: Arc::new(RwLock::new(inner)),
        })
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
    use crate::types::SyncPolicy;
    use std::time::Duration;

    #[test]
    fn test_builder_default() {
        let builder = DBBuilder::new();
        assert!(builder.in_memory);
        assert!(builder.aof_path.is_none());
    }

    #[test]
    fn test_builder_in_memory() {
        let db = DBBuilder::new().in_memory().build().unwrap();
        db.insert("test", b"value", None).unwrap();
        assert_eq!(db.get("test").unwrap().unwrap().as_ref(), b"value");
    }

    #[test]
    fn test_builder_with_config() {
        let config = Config::with_geohash_precision(10)
            .with_sync_policy(SyncPolicy::Always)
            .with_default_ttl(Duration::from_secs(3600));

        let db = DBBuilder::new().config(config).build().unwrap();
        db.insert("test", b"value", None).unwrap();
    }

    #[test]
    fn test_builder_aof_path() {
        let temp_dir = std::env::temp_dir();
        let aof_path = temp_dir.join("test_builder.aof");

        // Clean up any existing file
        let _ = std::fs::remove_file(&aof_path);

        let db = DBBuilder::new().aof_path(&aof_path).build().unwrap();

        db.insert("persistent", b"data", None).unwrap();
        drop(db);

        // Reopen and verify data persisted
        let db2 = DBBuilder::new().aof_path(&aof_path).build().unwrap();

        assert_eq!(db2.get("persistent").unwrap().unwrap().as_ref(), b"data");

        // Clean up
        let _ = std::fs::remove_file(aof_path);
    }

    #[test]
    fn test_builder_aof_path_disables_in_memory() {
        let temp_dir = std::env::temp_dir();
        let aof_path = temp_dir.join("test_builder2.aof");
        let _ = std::fs::remove_file(&aof_path);

        let builder = DBBuilder::new().in_memory().aof_path(&aof_path);

        assert!(!builder.in_memory);
        assert!(builder.aof_path.is_some());

        // Clean up
        let _ = std::fs::remove_file(aof_path);
    }

    #[test]
    fn test_builder_in_memory_clears_aof_path() {
        let temp_dir = std::env::temp_dir();
        let aof_path = temp_dir.join("test_builder3.aof");

        let builder = DBBuilder::new().aof_path(aof_path).in_memory();

        assert!(builder.in_memory);
        assert!(builder.aof_path.is_none());
    }
}
