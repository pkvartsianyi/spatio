//! Database builder for flexible configuration
//!
//! This module provides a builder pattern for creating databases with
//! advanced configuration options including custom persistence paths.

use crate::config::Config;
use crate::db::{DB, DBInner};
use crate::error::Result;
#[cfg(feature = "aof")]
use crate::storage::AOFFile;
#[cfg(feature = "snapshot")]
use crate::storage::SnapshotConfig;
#[cfg(feature = "snapshot")]
use crate::storage::SnapshotFile;
use std::path::PathBuf;

/// Builder for database configuration with custom persistence paths and settings.
#[derive(Debug)]
pub struct DBBuilder {
    #[cfg(feature = "aof")]
    aof_path: Option<PathBuf>,
    #[cfg(feature = "snapshot")]
    snapshot_path: Option<PathBuf>,
    config: Config,
    in_memory: bool,
}

impl DBBuilder {
    /// Create a new builder with default in-memory configuration.
    pub fn new() -> Self {
        Self {
            #[cfg(feature = "aof")]
            aof_path: None,
            #[cfg(feature = "snapshot")]
            snapshot_path: None,
            config: Config::default(),
            in_memory: true,
        }
    }

    /// Set the AOF path for persistence. File is created if needed and replayed on startup.
    #[cfg(feature = "aof")]
    pub fn aof_path<P: Into<PathBuf>>(mut self, path: P) -> Self {
        self.aof_path = Some(path.into());
        self.in_memory = false;
        self
    }

    /// Set the snapshot path for persistence. File is created if needed and loaded on startup.
    #[cfg(feature = "snapshot")]
    pub fn snapshot_path<P: Into<PathBuf>>(mut self, path: P) -> Self {
        self.snapshot_path = Some(path.into());
        self.in_memory = false;
        self
    }

    /// Configure for in-memory storage with no persistence.
    pub fn in_memory(mut self) -> Self {
        self.in_memory = true;
        #[cfg(feature = "aof")]
        {
            self.aof_path = None;
        }
        #[cfg(feature = "snapshot")]
        {
            self.snapshot_path = None;
        }
        self
    }

    /// Set the database configuration (sync policy, TTL, etc.).
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

    /// Build the database. Opens persistence file if configured and loads state.
    pub fn build(self) -> Result<DB> {
        let mut inner = DBInner::new_with_config(&self.config);

        if !self.in_memory {
            #[cfg(feature = "aof")]
            if let Some(aof_path) = self.aof_path {
                let mut aof_file = AOFFile::open(&aof_path)?;
                inner.load_from_aof(&mut aof_file)?;
                inner.aof_file = Some(aof_file);
            }

            #[cfg(feature = "snapshot")]
            if let Some(snapshot_path) = self.snapshot_path {
                let snapshot_config = SnapshotConfig {
                    auto_snapshot_ops: self.config.snapshot_auto_ops,
                };
                let snapshot_file = SnapshotFile::new(&snapshot_path, snapshot_config);
                inner.load_from_snapshot(&snapshot_file)?;
                inner.snapshot_file = Some(snapshot_file);
            }
        }

        Ok(DB {
            inner,
            #[cfg(not(feature = "sync"))]
            _not_send_sync: std::marker::PhantomData,
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
    #[cfg(feature = "time-index")]
    use crate::config::HistoryEventKind;
    use crate::config::SyncPolicy;
    use std::time::Duration;

    #[test]
    fn test_builder_default() {
        let builder = DBBuilder::new();
        assert!(builder.in_memory);
    }

    #[test]
    fn test_builder_in_memory() {
        let mut db = DBBuilder::new().in_memory().build().unwrap();
        db.insert("test", b"value", None).unwrap();
        assert_eq!(db.get("test").unwrap().unwrap().as_ref(), b"value");
    }

    #[test]
    fn test_builder_with_config() {
        let config = Config::default()
            .with_sync_policy(SyncPolicy::Always)
            .with_default_ttl(Duration::from_secs(3600));

        let mut db = DBBuilder::new().config(config).build().unwrap();
        db.insert("test", b"value", None).unwrap();
    }

    #[cfg(feature = "time-index")]
    #[test]
    fn test_builder_history_capacity() {
        let mut db = DBBuilder::new().history_capacity(2).build().unwrap();

        db.insert("key", b"v1", None).unwrap();
        db.insert("key", b"v2", None).unwrap();
        db.delete("key").unwrap();

        let history = db.history("key").unwrap();
        assert_eq!(history.len(), 2);
        assert_eq!(history[0].kind, HistoryEventKind::Set);
        assert_eq!(history[1].kind, HistoryEventKind::Delete);
    }

    #[cfg(feature = "aof")]
    #[test]
    fn test_builder_aof_path() {
        let temp_dir = std::env::temp_dir();
        let aof_path = temp_dir.join("test_builder.aof");

        // Clean up any existing file
        let _ = std::fs::remove_file(&aof_path);

        let mut db = DBBuilder::new().aof_path(&aof_path).build().unwrap();

        db.insert("persistent", b"data", None).unwrap();
        drop(db);

        // Reopen and verify data persisted
        let db2 = DBBuilder::new().aof_path(&aof_path).build().unwrap();

        assert_eq!(db2.get("persistent").unwrap().unwrap().as_ref(), b"data");

        // Clean up
        let _ = std::fs::remove_file(aof_path);
    }

    #[cfg(feature = "aof")]
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

    #[cfg(feature = "aof")]
    #[test]
    fn test_builder_in_memory_clears_aof_path() {
        let temp_dir = std::env::temp_dir();
        let aof_path = temp_dir.join("test_builder3.aof");

        let builder = DBBuilder::new().aof_path(aof_path).in_memory();

        assert!(builder.in_memory);
        assert!(builder.aof_path.is_none());
    }
}
