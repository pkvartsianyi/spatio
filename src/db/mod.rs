//! Core database implementation for Spatio.
//!
//! This module defines the main `DB` type along with spatio-temporal helpers and
//! persistence wiring that power the public `Spatio` API.

use crate::compute::spatial::SpatialIndexManager;
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

/// Main Spatio database structure (single-threaded by design).
///
/// The `DB` struct is the core of Spatio, offering:
/// - Key-value storage with spatio-temporal indexing
/// - Geographic point operations with automatic spatial indexing
/// - Trajectory tracking for moving objects
/// - Time-to-live (TTL) support for temporal data
/// - Atomic batch operations
/// - Optional persistence with append-only file (AOF) format
///
/// # Thread Safety
///
/// **`DB` is NOT thread-safe by default.** It cannot be sent between threads
/// or shared without synchronization. This design choice provides:
///
/// - **Maximum performance** for single-threaded use cases
/// - **Flexibility** to choose your own concurrency model
/// - **Compile-time safety** - you cannot accidentally share `DB` unsafely
///
/// ## Options for Multi-Threaded Use
///
/// ### Option 1: Use the `sync` feature (easiest)
///
/// ```toml
/// [dependencies]
/// spatio = { version = "2.0", features = ["sync"] }
/// ```
///
/// ```rust,ignore
/// use spatio::SyncDB;
/// use std::thread;
///
/// let db = SyncDB::open("data.db").unwrap();
/// let db_clone = db.clone();
///
/// thread::spawn(move || {
///     db_clone.insert("key", b"value", None).unwrap();
/// });
/// ```
///
/// ### Option 2: Manual wrapping with RwLock/Mutex
///
/// ```rust,ignore
/// use spatio::Spatio;
/// use parking_lot::RwLock;
/// use std::sync::Arc;
///
/// let db = Arc::new(RwLock::new(Spatio::open("data.db").unwrap()));
/// let db_clone = db.clone();
///
/// std::thread::spawn(move || {
///     db_clone.write().insert("key", b"value", None).unwrap();
/// });
/// ```
///
/// ### Option 3: Actor pattern (recommended for async)
///
/// For async applications, use an actor/channel pattern.
/// See `examples/actor_pattern.rs` for details.
///
/// # Examples
///
/// ## Single-threaded usage (no wrapper needed)
///
/// ```rust
/// use spatio::{Spatio, Point, SetOptions};
/// use std::time::Duration;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// // Create an in-memory database
/// let mut db = Spatio::memory()?;
///
/// // Store a simple key-value pair
/// db.insert("key1", b"value1", None)?;
///
/// // Store data with TTL
/// let opts = SetOptions::with_ttl(Duration::from_secs(300));
/// db.insert("temp_key", b"expires_in_5_minutes", Some(opts))?;
/// # Ok(())
/// # }
/// ```
///
/// ## Spatial Operations
///
/// ```rust
/// use spatio::{Spatio, Point};
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let mut db = Spatio::memory()?;
///
/// // Store geographic points (automatically indexed)
/// let nyc = Point::new(-74.0060, 40.7128);
/// let london = Point::new(-0.1278, 51.5074);
///
/// db.insert_point("cities", &nyc, b"New York", None)?;
/// db.insert_point("cities", &london, b"London", None)?;
///
/// // Find nearby cities within 100km
/// let nearby = db.query_within_radius("cities", &nyc, 100_000.0, 10)?;
/// println!("Found {} cities within 100km", nearby.len());
/// # Ok(())
/// # }
/// ```
///
/// ## Atomic batching
///
/// ```rust
/// use spatio::Spatio;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let mut db = Spatio::memory()?;
///
/// // All operations succeed or fail together
/// db.atomic(|batch| {
///     batch.insert("key1", b"value1", None)?;
///     batch.insert("key2", b"value2", None)?;
///     batch.delete("old_key")?;
///     Ok(())
/// })?;
/// # Ok(())
/// # }
/// ```
pub struct DB {
    pub(crate) inner: DBInner,
    #[cfg(not(feature = "sync"))]
    pub(crate) _not_send_sync: PhantomData<*const ()>,
}

pub(crate) struct DBInner {
    /// Main key-value storage (B-tree for ordered access)
    pub keys: BTreeMap<Bytes, DbItem>,
    /// Items ordered by expiration time
    pub expirations: BTreeMap<SystemTime, Vec<Bytes>>,
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
    /// Opens a Spatio database from a file path or creates a new one.
    ///
    /// When opening an existing database, this method automatically replays the
    /// append-only file (AOF) to restore all data and spatial indexes to their
    /// previous state. This ensures durability across restarts.
    ///
    /// # Startup Replay
    ///
    /// The database performs the following steps on startup:
    /// 1. Opens the AOF file at the specified path (creates if doesn't exist)
    /// 2. Replays all commands from the AOF to restore state
    /// 3. Rebuilds spatial indexes for all geographic data
    /// 4. Ready for new operations
    ///
    /// # Arguments
    ///
    /// * `path` - File system path or ":memory:" for in-memory storage
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spatio::Spatio;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let _ = std::fs::remove_file("my_data.db");
    /// // Create persistent database with automatic AOF replay on open
    /// let persistent_db = Spatio::open("my_data.db")?;
    ///
    /// // Create in-memory database (no persistence)
    /// let mem_db = Spatio::open(":memory:")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::open_with_config(path, Config::default())
    }

    /// Creates a new Spatio database with custom configuration.
    ///
    /// This method provides full control over database behavior including:
    /// - Geohash precision for spatial indexing
    /// - Sync policy for durability vs performance tradeoff
    /// - Default TTL for automatic expiration
    ///
    /// Like `open()`, this method automatically replays the AOF on startup
    /// to restore previous state.
    ///
    /// # Arguments
    ///
    /// * `path` - File path for the database (use ":memory:" for in-memory)
    /// * `config` - Database configuration including geohash precision settings
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spatio::{Spatio, Config, SyncPolicy};
    /// use std::time::Duration;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// // High-precision config for dense urban areas
    /// let config = Config::with_geohash_precision(10)
    ///     .with_sync_policy(SyncPolicy::Always)
    ///     .with_default_ttl(Duration::from_secs(3600));
    /// # let _ = std::fs::remove_file("my_database.db");
    ///
    /// let mut db = Spatio::open_with_config("my_database.db", config)?;
    /// # Ok(())
    /// # }
    /// ```
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

    /// Creates a new in-memory Spatio database.
    pub fn memory() -> Result<Self> {
        Self::open(":memory:")
    }

    /// Create an in-memory database with custom configuration
    pub fn memory_with_config(config: Config) -> Result<Self> {
        Self::open_with_config(":memory:", config)
    }

    /// Create a database builder for advanced configuration.
    ///
    /// The builder provides full control over database configuration including:
    /// - Custom AOF (Append-Only File) paths
    /// - In-memory vs persistent storage
    /// - Full configuration options
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spatio::Spatio;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// // Create database with custom AOF path
    /// let temp_path = std::env::temp_dir().join("builder_demo.aof");
    /// let mut db = Spatio::builder()
    ///     .aof_path(&temp_path)
    ///     .build()?;
    ///
    /// db.insert("key", b"value", None)?;
    /// # std::fs::remove_file(temp_path)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn builder() -> crate::builder::DBBuilder {
        crate::builder::DBBuilder::new()
    }

    /// Get database statistics
    pub fn stats(&self) -> DbStats {
        self.inner.stats.clone()
    }

    /// Inserts a key-value pair into the database.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to store
    /// * `value` - The value to associate with the key
    /// * `opts` - Optional settings like TTL
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spatio::{Spatio, SetOptions};
    /// use std::time::Duration;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut db = Spatio::memory()?;
    ///
    /// // Simple insert
    /// db.insert("user:123", b"John Doe", None)?;
    ///
    /// // Insert with TTL
    /// let opts = SetOptions::with_ttl(Duration::from_secs(300));
    /// db.insert("session:abc", b"user_data", Some(opts))?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn insert(
        &mut self,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
        opts: Option<SetOptions>,
    ) -> Result<Option<Bytes>> {
        if self.inner.closed {
            return Err(SpatioError::DatabaseClosed);
        }

        let cleanup_batch = self.inner.config.amortized_cleanup_batch;
        if cleanup_batch > 0 {
            let _ = self.inner.amortized_cleanup(cleanup_batch)?;
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

    /// Get a value by key
    pub fn get(&self, key: impl AsRef<[u8]>) -> Result<Option<Bytes>> {
        if self.inner.closed {
            return Err(SpatioError::DatabaseClosed);
        }

        let key_bytes = Bytes::copy_from_slice(key.as_ref());

        if let Some(item) = self.inner.get_item(&key_bytes)
            && !item.is_expired()
        {
            return Ok(Some(item.value.clone()));
        }

        Ok(None)
    }

    /// Delete a key atomically
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

    /// Execute multiple operations atomically
    pub fn atomic<F, R>(&mut self, f: F) -> Result<R>
    where
        F: FnOnce(&mut AtomicBatch) -> Result<R>,
    {
        let mut batch = AtomicBatch::new(&mut self.inner);
        let result = f(&mut batch)?;
        batch.commit()?;
        Ok(result)
    }

    /// Force sync all pending writes to disk.
    ///
    /// This method flushes the AOF buffer and calls fsync to ensure all data
    /// is durably written to disk. Useful before critical operations or when
    /// you need to guarantee data persistence.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spatio::Spatio;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut db = Spatio::open("my_data.db")?;
    /// db.insert("critical_key", b"important_data", None)?;
    ///
    /// // Ensure data is on disk before continuing
    /// db.sync()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn sync(&mut self) -> Result<()> {
        let sync_mode = self.inner.config.sync_mode;
        if let Some(ref mut aof_file) = self.inner.aof_file {
            aof_file.sync_with_mode(sync_mode)?;
            self.inner.sync_ops_since_flush = 0;
        }
        Ok(())
    }

    /// Gracefully close the database.
    ///
    /// This method performs a clean shutdown by:
    /// 1. Marking the database as closed (rejecting new operations)
    /// 2. Flushing any pending writes to the AOF
    /// 3. Syncing the AOF to disk (fsync)
    /// 4. Releasing resources
    ///
    /// After calling `close()`, any further operations on this database
    /// instance will return `DatabaseClosed` errors.
    ///
    /// **Note:** The database is also automatically closed when dropped,
    /// so explicitly calling `close()` is optional but recommended for
    /// explicit error handling.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spatio::Spatio;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let _ = std::fs::remove_file("my_data.db");
    /// let mut db = Spatio::open("my_data.db")?;
    /// db.insert("key", b"value", None)?;
    ///
    /// // Explicitly close and handle errors
    /// db.close()?;
    /// # Ok(())
    /// # }
    /// ```
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

/// Automatic graceful shutdown on drop.
///
/// When the last reference to the database is dropped, it automatically performs a graceful shutdown:
/// - Flushes pending writes
/// - Syncs to disk (best effort, errors are silently ignored)
/// - Releases resources
///
/// Note: Since DB uses Arc internally, this syncs only when the last clone is dropped.
/// The database is NOT marked as closed here to allow other clones to continue operating.
/// Use `close()` explicitly if you need to prevent further operations.
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
