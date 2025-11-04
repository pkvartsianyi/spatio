//! Core database implementation for Spatio.
//!
//! This module defines the main `DB` type along with spatio-temporal helpers and
//! persistence wiring that power the public `Spatio` API.

use crate::compute::spatial::rtree::SpatialIndexManager;
use crate::config::{Config, DbItem, DbStats, SetOptions};
#[cfg(feature = "time-index")]
use crate::config::{HistoryEntry, HistoryEventKind};
use crate::error::{Result, SpatioError};
#[cfg(feature = "aof")]
use crate::storage::AOFFile;
use bytes::Bytes;
use std::collections::BTreeMap;
#[cfg(feature = "time-index")]
use std::collections::{BTreeSet, HashMap, VecDeque};
#[cfg(not(feature = "sync"))]
use std::marker::PhantomData;
use std::path::Path;

use std::time::SystemTime;

mod batch;
mod internal;
mod namespace;

#[cfg(feature = "sync")]
mod sync;

pub use batch::AtomicBatch;
pub use namespace::{Namespace, NamespaceManager};

#[cfg(feature = "sync")]
pub use sync::SyncDB;

/// Embedded spatio-temporal database.
///
/// Provides key-value storage with spatial indexing, TTL support, and optional persistence.
/// Single-threaded by default. Use the `sync` feature for thread-safe access via `SyncDB`.
pub struct DB {
    pub(crate) inner: DBInner,
    #[cfg(not(feature = "sync"))]
    pub(crate) _not_send_sync: PhantomData<*const ()>,
}

pub(crate) struct DBInner {
    /// Main key-value storage (B-tree for ordered access)
    pub keys: BTreeMap<Bytes, DbItem>,
    #[cfg(feature = "time-index")]
    /// Items indexed by creation time for time-range queries
    pub created_index: BTreeMap<SystemTime, BTreeSet<Bytes>>,
    /// Spatial index manager for 2D and 3D spatial operations (R-tree based)
    pub spatial_index: SpatialIndexManager,
    /// Append-only file for persistence
    pub aof_file: Option<AOFFile>,
    #[cfg(feature = "time-index")]
    /// Optional per-key history tracker
    pub history: Option<HistoryTracker>,
    /// Whether the database is closed
    pub closed: bool,
    /// Database statistics
    pub stats: DbStats,
    /// Configuration
    pub config: Config,
    /// Number of writes since last forced sync (SyncPolicy::Always only)
    sync_ops_since_flush: usize,
}

impl DB {
    /// Open or create a database at the given path. Use ":memory:" for in-memory storage.
    /// Existing AOF files are automatically replayed on startup.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::open_with_config(path, Config::default())
    }

    /// Open or create a database with custom configuration.
    pub fn open_with_config<P: AsRef<Path>>(path: P, config: Config) -> Result<Self> {
        let path = path.as_ref();
        let is_memory = path.to_str() == Some(":memory:");

        let mut inner = DBInner::new_with_config(&config);

        if !is_memory {
            let mut aof_file = AOFFile::open(path)?;
            inner.load_from_aof(&mut aof_file)?;
            inner.aof_file = Some(aof_file);
        }

        Ok(DB {
            inner,
            #[cfg(not(feature = "sync"))]
            _not_send_sync: PhantomData,
        })
    }

    /// Create an in-memory database with default configuration.
    pub fn memory() -> Result<Self> {
        Self::open(":memory:")
    }

    /// Create an in-memory database with custom configuration.
    pub fn memory_with_config(config: Config) -> Result<Self> {
        Self::open_with_config(":memory:", config)
    }

    /// Create a builder for advanced configuration options.
    pub fn builder() -> crate::builder::DBBuilder {
        crate::builder::DBBuilder::new()
    }

    /// Get database statistics.
    pub fn stats(&self) -> DbStats {
        self.inner.stats.clone()
    }

    /// Insert a key-value pair, optionally with TTL settings.
    pub fn insert(
        &mut self,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
        opts: Option<SetOptions>,
    ) -> Result<Option<Bytes>> {
        if self.inner.closed {
            return Err(SpatioError::DatabaseClosed);
        }

        let key_bytes = Bytes::copy_from_slice(key.as_ref());
        let value_bytes = Bytes::copy_from_slice(value.as_ref());

        let item = match opts {
            Some(SetOptions { ttl: Some(ttl), .. }) => DbItem::with_ttl(value_bytes, ttl),
            Some(SetOptions {
                expires_at: Some(expires_at),
                ..
            }) => DbItem::with_expiration(value_bytes, expires_at),
            _ => DbItem::new(value_bytes),
        };
        let created_at = item.created_at;

        let old = self.inner.insert_item(key_bytes.clone(), item);
        self.inner
            .write_to_aof_if_needed(&key_bytes, value.as_ref(), opts.as_ref(), created_at)?;
        Ok(old.map(|item| item.value))
    }

    /// Get a value by key.
    ///
    /// Returns `None` if the key doesn't exist or has expired.
    /// **Note**: Expired items are not physically deleted here (get() is immutable).
    /// They remain in storage until overwritten or manually cleaned with `cleanup_expired()`.
    pub fn get(&self, key: impl AsRef<[u8]>) -> Result<Option<Bytes>> {
        if self.inner.closed {
            return Err(SpatioError::DatabaseClosed);
        }

        let key_bytes = Bytes::copy_from_slice(key.as_ref());

        if let Some(item) = self.inner.get_item(&key_bytes) {
            if item.is_expired() {
                // Lazy expiration: treat as non-existent without physically deleting
                // (get() is immutable, cannot modify storage)
                return Ok(None);
            }
            return Ok(Some(item.value.clone()));
        }

        Ok(None)
    }

    /// Delete a key.
    pub fn delete(&mut self, key: impl AsRef<[u8]>) -> Result<Option<Bytes>> {
        if self.inner.closed {
            return Err(SpatioError::DatabaseClosed);
        }

        let key_bytes = Bytes::copy_from_slice(key.as_ref());

        if let Some(item) = self.inner.remove_item(&key_bytes) {
            self.inner.write_delete_to_aof_if_needed(&key_bytes)?;
            Ok(Some(item.value))
        } else {
            Ok(None)
        }
    }

    /// Execute multiple operations atomically.
    pub fn atomic<F, R>(&mut self, f: F) -> Result<R>
    where
        F: FnOnce(&mut AtomicBatch) -> Result<R>,
    {
        let mut batch = AtomicBatch::new(&mut self.inner);
        let result = f(&mut batch)?;
        batch.commit()?;
        Ok(result)
    }

    /// Force fsync of all pending writes to disk.
    pub fn sync(&mut self) -> Result<()> {
        let sync_mode = self.inner.config.sync_mode;
        if let Some(ref mut aof_file) = self.inner.aof_file {
            aof_file.sync_with_mode(sync_mode)?;
            self.inner.sync_ops_since_flush = 0;
        }
        Ok(())
    }

    /// Close the database and sync all pending writes.
    /// Subsequent operations will return errors.
    pub fn close(&mut self) -> Result<()> {
        if self.inner.closed {
            return Err(SpatioError::DatabaseClosed);
        }

        self.inner.closed = true;
        let sync_mode = self.inner.config.sync_mode;
        if let Some(ref mut aof_file) = self.inner.aof_file {
            aof_file.sync_with_mode(sync_mode)?;
            self.inner.sync_ops_since_flush = 0;
        }
        Ok(())
    }
}

#[cfg(feature = "time-index")]
#[derive(Debug)]
pub(crate) struct HistoryTracker {
    capacity: usize,
    entries: HashMap<Bytes, VecDeque<HistoryEntry>>,
}

#[cfg(feature = "time-index")]
impl HistoryTracker {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            entries: HashMap::new(),
        }
    }

    fn record_set(
        &mut self,
        key: &Bytes,
        value: Bytes,
        timestamp: SystemTime,
        expires_at: Option<SystemTime>,
    ) {
        let capacity = self.capacity;
        let deque = self.entries.entry(key.clone()).or_default();
        deque.push_back(HistoryEntry {
            timestamp,
            kind: HistoryEventKind::Set,
            value: Some(value),
            expires_at,
        });
        while deque.len() > capacity {
            deque.pop_front();
        }
    }

    fn record_delete(&mut self, key: &Bytes, timestamp: SystemTime, value: Option<Bytes>) {
        let capacity = self.capacity;
        let deque = self.entries.entry(key.clone()).or_default();
        deque.push_back(HistoryEntry {
            timestamp,
            kind: HistoryEventKind::Delete,
            value,
            expires_at: None,
        });
        while deque.len() > capacity {
            deque.pop_front();
        }
    }

    pub(crate) fn history_for(&self, key: &Bytes) -> Option<Vec<HistoryEntry>> {
        self.entries
            .get(key)
            .map(|deque| deque.iter().cloned().collect())
    }
}

/// Sync pending writes on drop (best effort, errors ignored).
impl Drop for DB {
    fn drop(&mut self) {
        if self.inner.closed {
            return;
        }

        let sync_mode = self.inner.config.sync_mode;
        if let Some(ref mut aof_file) = self.inner.aof_file
            && aof_file.sync_with_mode(sync_mode).is_ok()
        {
            self.inner.sync_ops_since_flush = 0;
        }
    }
}

pub use DB as Spatio;

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(feature = "time-index")]
    use bytes::Bytes;
    use std::thread::sleep;
    use std::time::Duration;

    #[test]
    fn test_drop_syncs_to_disk() {
        use std::fs;
        let temp_path = std::env::temp_dir().join("test_drop_sync.aof");
        let _ = fs::remove_file(&temp_path);

        {
            let mut db = DB::open(&temp_path).unwrap();
            db.insert("key1", b"value1", None).unwrap();
            // DB dropped here, should sync
        }

        let db = DB::open(&temp_path).unwrap();
        assert_eq!(db.get("key1").unwrap().unwrap().as_ref(), b"value1");

        let _ = fs::remove_file(temp_path);
    }

    #[test]
    fn test_explicit_close_prevents_operations() {
        let mut db = DB::memory().unwrap();
        db.insert("key", b"value", None).unwrap();

        db.close().unwrap();

        assert!(db.insert("key2", b"value2", None).is_err());
        assert!(db.get("key").is_err());
        assert!(db.delete("key").is_err());
    }

    // Note: DB is no longer Clone - removed test_clone_shares_state

    #[test]
    fn test_cleanup_expired_removes_keys() {
        let mut db = DB::memory().unwrap();
        db.insert(
            "ttl",
            b"value",
            Some(SetOptions::with_ttl(Duration::from_millis(20))),
        )
        .unwrap();

        sleep(Duration::from_millis(40));

        let removed = db.cleanup_expired().unwrap();
        assert_eq!(removed, 1);
        assert!(db.get("ttl").unwrap().is_none());
        assert_eq!(db.cleanup_expired().unwrap(), 0);
    }

    #[cfg(feature = "time-index")]
    #[test]
    fn test_keys_created_time_filters() {
        let mut db = DB::memory().unwrap();
        db.insert("old", b"1", None).unwrap();
        sleep(Duration::from_millis(30));
        db.insert("recent", b"2", None).unwrap();

        let recent_keys = db.keys_created_since(Duration::from_millis(20)).unwrap();
        assert!(recent_keys.iter().any(|k| k.as_ref() == b"recent"));
        assert!(!recent_keys.iter().any(|k| k.as_ref() == b"old"));

        let old_key = Bytes::copy_from_slice(b"old");
        let new_key = Bytes::copy_from_slice(b"recent");
        let old_created = db.inner.get_item(&old_key).unwrap().created_at;
        let new_created = db.inner.get_item(&new_key).unwrap().created_at;

        let between = db.keys_created_between(old_created, new_created).unwrap();
        assert!(between.iter().any(|k| k.as_ref() == b"old"));
        assert!(between.iter().any(|k| k.as_ref() == b"recent"));
    }

    #[cfg(feature = "time-index")]
    #[test]
    fn test_history_tracking_with_capacity() {
        let config = Config::default().with_history_capacity(2);
        let mut db = DB::open_with_config(":memory:", config).unwrap();

        db.insert("key", b"v1", None).unwrap();
        db.insert("key", b"v2", None).unwrap();
        db.delete("key").unwrap();

        let history = db.history("key").unwrap();
        assert_eq!(history.len(), 2);
        assert_eq!(history[0].kind, HistoryEventKind::Set);
        assert_eq!(history[0].value.as_ref().unwrap().as_ref(), b"v2");
        assert_eq!(history[1].kind, HistoryEventKind::Delete);
    }
}
