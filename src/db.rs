use crate::batch::AtomicBatch;
use crate::error::{Result, SpatioError};
use crate::index::IndexManager;
use crate::persistence::{AOFCommand, AOFFile};
use crate::spatial::{Point, SpatialKey};
use crate::types::{Config, DbItem, DbStats, SetOptions};
use bytes::Bytes;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::SystemTime;

/// Main Spatio database structure providing spatial and temporal data storage.
///
/// The `DB` struct is the core of Spatio, offering:
/// - Key-value storage with spatial indexing
/// - Geographic point operations with automatic spatial indexing
/// - Trajectory tracking for moving objects
/// - Time-to-live (TTL) support for temporal data
/// - Atomic batch operations
/// - Optional persistence with append-only file (AOF) format
///
/// # Examples
///
/// ## Basic Usage
/// ```rust
/// use spatio::{Spatio, Point, SetOptions};
/// use std::time::Duration;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// // Create an in-memory database
/// let db = Spatio::memory()?;
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
/// ```rust
/// use spatio::{Spatio, Point};
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let db = Spatio::memory()?;
///
/// // Store geographic points (automatically indexed)
/// let nyc = Point::new(40.7128, -74.0060);
/// let london = Point::new(51.5074, -0.1278);
///
/// db.insert_point("cities", &nyc, b"New York", None)?;
/// db.insert_point("cities", &london, b"London", None)?;
///
/// // Find nearby cities within 100km
/// let nearby = db.find_nearby("cities", &nyc, 100_000.0, 10)?;
/// println!("Found {} cities within 100km", nearby.len());
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct DB {
    pub(crate) inner: Arc<RwLock<DBInner>>,
}

pub(crate) struct DBInner {
    /// Main key-value storage (B-tree for ordered access)
    pub keys: BTreeMap<Bytes, DbItem>,
    /// Items ordered by expiration time
    pub expirations: BTreeMap<SystemTime, Vec<Bytes>>,
    /// Index manager for spatial operations
    pub index_manager: IndexManager,
    /// Append-only file for persistence
    pub aof_file: Option<AOFFile>,
    /// Whether the database is closed
    pub closed: bool,
    /// Database statistics
    pub stats: DbStats,
    /// Configuration
    pub config: Config,
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
    ///
    /// let db = Spatio::open_with_config("my_database.db", config)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn open_with_config<P: AsRef<Path>>(path: P, config: Config) -> Result<Self> {
        let path = path.as_ref();
        let is_memory = path.to_str() == Some(":memory:");

        let mut inner = DBInner {
            keys: BTreeMap::new(),
            expirations: BTreeMap::new(),
            index_manager: IndexManager::with_config(&config),
            aof_file: None,
            closed: false,
            stats: DbStats::default(),
            config: config.clone(),
        };

        // Initialize persistence if not in-memory
        // This automatically replays the AOF to restore previous state
        if !is_memory {
            let mut aof_file = AOFFile::open(path)?;
            inner.load_from_aof(&mut aof_file)?;
            inner.aof_file = Some(aof_file);
        }

        Ok(DB {
            inner: Arc::new(RwLock::new(inner)),
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
    /// let db = Spatio::builder()
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
    pub fn stats(&self) -> Result<DbStats> {
        let inner = self.read()?;
        Ok(inner.stats.clone())
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
    /// let db = Spatio::memory()?;
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
        &self,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
        opts: Option<SetOptions>,
    ) -> Result<Option<Bytes>> {
        let mut inner = self.write()?;
        if inner.closed {
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

        let old = inner.insert_item(key_bytes.clone(), item);
        inner.write_to_aof_if_needed(&key_bytes, value.as_ref(), opts.as_ref())?;
        Ok(old.map(|item| item.value))
    }

    /// Get a value by key
    pub fn get(&self, key: impl AsRef<[u8]>) -> Result<Option<Bytes>> {
        let inner = self.read()?;
        if inner.closed {
            return Err(SpatioError::DatabaseClosed);
        }

        let key_bytes = Bytes::copy_from_slice(key.as_ref());

        if let Some(item) = inner.get_item(&key_bytes) {
            if !item.is_expired() {
                return Ok(Some(item.value.clone()));
            }
        }
        Ok(None)
    }

    /// Delete a key atomically
    pub fn delete(&self, key: impl AsRef<[u8]>) -> Result<Option<Bytes>> {
        let mut inner = self.write()?;
        if inner.closed {
            return Err(SpatioError::DatabaseClosed);
        }

        let key_bytes = Bytes::copy_from_slice(key.as_ref());

        if let Some(item) = inner.remove_item(&key_bytes) {
            inner.write_delete_to_aof_if_needed(&key_bytes)?;
            Ok(Some(item.value))
        } else {
            Ok(None)
        }
    }

    /// Execute multiple operations atomically
    pub fn atomic<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce(&mut AtomicBatch) -> Result<R>,
    {
        let mut batch = AtomicBatch::new(self.clone());
        let result = f(&mut batch)?;
        batch.commit()?;
        Ok(result)
    }

    /// Insert a geographic point with automatic spatial indexing.
    ///
    /// Points are automatically indexed for spatial queries. The system
    /// chooses the optimal indexing strategy based on data patterns.
    ///
    /// # Arguments
    ///
    /// * `prefix` - Namespace for the point (e.g., "cities", "sensors")
    /// * `point` - Geographic coordinates
    /// * `data` - Associated data to store with the point
    /// * `opts` - Optional settings like TTL
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spatio::{Spatio, Point};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let db = Spatio::memory()?;
    /// let nyc = Point::new(40.7128, -74.0060);
    ///
    /// db.insert_point("cities", &nyc, b"New York City", None)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn insert_point(
        &self,
        prefix: &str,
        point: &Point,
        value: &[u8],
        opts: Option<SetOptions>,
    ) -> Result<()> {
        let data_bytes = value;
        let data_ref = Bytes::copy_from_slice(data_bytes);

        // Generate geohash key for automatic indexing
        let geohash = point
            .to_geohash(8)
            .map_err(|_| SpatioError::InvalidGeohash)?;
        let key = SpatialKey::geohash(prefix, &geohash);
        let key_bytes = Bytes::copy_from_slice(key.as_bytes());

        // Single lock acquisition for both operations
        let mut inner = self.write()?;

        // Insert into main storage
        let item = match opts {
            Some(SetOptions { ttl: Some(ttl), .. }) => DbItem::with_ttl(data_ref.clone(), ttl),
            Some(SetOptions {
                expires_at: Some(expires_at),
                ..
            }) => DbItem::with_expiration(data_ref.clone(), expires_at),
            _ => DbItem::new(data_ref.clone()),
        };

        inner.insert_item(key_bytes.clone(), item);

        // Add to spatial index
        inner.index_manager.insert_point(prefix, point, &data_ref)?;

        inner.write_to_aof_if_needed(&key_bytes, value, opts.as_ref())?;
        Ok(())
    }

    /// Find nearby points within a radius.
    ///
    /// Uses spatial indexing for efficient queries. Results are ordered
    /// by distance from the query point.
    ///
    /// # Arguments
    ///
    /// * `prefix` - Namespace to search in
    /// * `center` - Center point for the search
    /// * `radius_meters` - Search radius in meters
    /// * `limit` - Maximum number of results to return
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spatio::{Spatio, Point};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let db = Spatio::memory()?;
    /// let center = Point::new(40.7128, -74.0060);
    ///
    /// // Find up to 10 points within 1km
    /// let nearby = db.find_nearby("cities", &center, 1000.0, 10)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn find_nearby(
        &self,
        prefix: &str,
        center: &Point,
        radius_meters: f64,
        limit: usize,
    ) -> Result<Vec<(Point, Bytes)>> {
        let inner = self.read()?;
        inner
            .index_manager
            .find_nearby(prefix, center, radius_meters, limit)
    }

    /// Insert a trajectory (sequence of points over time).
    ///
    /// Trajectories represent the movement of objects over time. Each
    /// point in the trajectory has a timestamp for temporal queries.
    ///
    /// # Arguments
    ///
    /// * `object_id` - Unique identifier for the moving object
    /// * `trajectory` - Sequence of (Point, timestamp) pairs
    /// * `opts` - Optional settings like TTL for the entire trajectory
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spatio::{Spatio, Point};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let db = Spatio::memory()?;
    ///
    /// let trajectory = vec![
    ///     (Point::new(40.7128, -74.0060), 1640995200), // Start
    ///     (Point::new(40.7150, -74.0040), 1640995260), // 1 min later
    ///     (Point::new(40.7172, -74.0020), 1640995320), // 2 min later
    /// ];
    ///
    /// db.insert_trajectory("vehicle:truck001", &trajectory, None)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn insert_trajectory(
        &self,
        object_id: &str,
        trajectory: &[(Point, u64)],
        opts: Option<SetOptions>,
    ) -> Result<()> {
        for (i, (point, timestamp)) in trajectory.iter().enumerate() {
            let key = format!("traj:{}:{:010}:{:06}", object_id, timestamp, i);
            let point_data = bincode::serialize(&(point, timestamp)).map_err(|e| {
                SpatioError::SerializationErrorWithContext(format!(
                    "Failed to serialize trajectory point for object '{}': {}",
                    object_id, e
                ))
            })?;

            self.insert(&key, &point_data, opts.clone())?;
        }
        Ok(())
    }

    /// Query trajectory between timestamps.
    ///
    /// Returns all trajectory points for an object within the specified
    /// time range, ordered by timestamp.
    ///
    /// # Arguments
    ///
    /// * `object_id` - The object to query
    /// * `start_time` - Start of time range (unix timestamp)
    /// * `end_time` - End of time range (unix timestamp)
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spatio::{Spatio, Point};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let db = Spatio::memory()?;
    ///
    /// // Query trajectory for first hour
    /// let path = db.query_trajectory("vehicle:truck001", 1640995200, 1640998800)?;
    /// println!("Found {} trajectory points", path.len());
    /// # Ok(())
    /// # }
    /// ```
    pub fn query_trajectory(
        &self,
        object_id: &str,
        start_time: u64,
        end_time: u64,
    ) -> Result<Vec<(Point, u64)>> {
        let mut results = Vec::new();
        let prefix = format!("traj:{}:", object_id);

        let inner = self.read()?;
        for (key, item) in inner.keys.range(Bytes::from(prefix.clone())..) {
            if !key.starts_with(prefix.as_bytes()) {
                break;
            }

            if item.is_expired() {
                continue;
            }

            match bincode::deserialize::<(Point, u64)>(&item.value) {
                Ok((point, timestamp)) => {
                    if timestamp >= start_time && timestamp <= end_time {
                        results.push((point, timestamp));
                    }
                }
                Err(e) => {
                    // Log deserialization error but continue processing other points
                    eprintln!(
                        "Warning: Failed to deserialize trajectory point for object '{}': {}",
                        object_id, e
                    );
                }
            }
        }

        results.sort_by_key(|(_, timestamp)| *timestamp);
        Ok(results)
    }

    /// Check if there are any points within a circular region.
    ///
    /// This method checks if any points exist within the specified distance
    /// from a center point in the given namespace.
    ///
    /// # Arguments
    ///
    /// * `prefix` - Namespace to search in
    /// * `center` - Center point of the circular region
    /// * `radius_meters` - Radius in meters
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spatio::{Spatio, Point};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let db = Spatio::memory()?;
    /// let center = Point::new(40.7128, -74.0060);
    ///
    /// // Check if there are any cities within 50km
    /// let has_nearby = db.contains_point("cities", &center, 50_000.0)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn contains_point(&self, prefix: &str, center: &Point, radius_meters: f64) -> Result<bool> {
        let inner = self.read()?;
        inner
            .index_manager
            .contains_point(prefix, center, radius_meters)
    }

    /// Check if there are any points within a bounding box.
    ///
    /// This method checks if any points exist within the specified
    /// rectangular region in the given namespace.
    ///
    /// # Arguments
    ///
    /// * `prefix` - Namespace to search in
    /// * `min_lat` - Minimum latitude of bounding box
    /// * `min_lon` - Minimum longitude of bounding box
    /// * `max_lat` - Maximum latitude of bounding box
    /// * `max_lon` - Maximum longitude of bounding box
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spatio::{Spatio, Point};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let db = Spatio::memory()?;
    ///
    /// // Check if there are any points in Manhattan area
    /// let has_points = db.intersects_bounds("sensors", 40.7, -74.1, 40.8, -73.9)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn intersects_bounds(
        &self,
        prefix: &str,
        min_lat: f64,
        min_lon: f64,
        max_lat: f64,
        max_lon: f64,
    ) -> Result<bool> {
        let inner = self.read()?;
        inner
            .index_manager
            .intersects_bounds(prefix, min_lat, min_lon, max_lat, max_lon)
    }

    /// Count points within a distance from a center point.
    ///
    /// This method counts how many points exist within the specified
    /// distance from a center point without returning the actual points.
    /// More efficient than `find_nearby` when you only need the count.
    ///
    /// # Arguments
    ///
    /// * `prefix` - Namespace to search in
    /// * `center` - Center point for the search
    /// * `radius_meters` - Search radius in meters
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spatio::{Spatio, Point};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let db = Spatio::memory()?;
    /// let center = Point::new(40.7128, -74.0060);
    ///
    /// // Count how many sensors are within 1km
    /// let count = db.count_within_distance("sensors", &center, 1000.0)?;
    /// println!("Found {} sensors within 1km", count);
    /// # Ok(())
    /// # }
    /// ```
    pub fn count_within_distance(
        &self,
        prefix: &str,
        center: &Point,
        radius_meters: f64,
    ) -> Result<usize> {
        let inner = self.read()?;
        inner
            .index_manager
            .count_within_distance(prefix, center, radius_meters)
    }

    /// Find all points within a bounding box.
    ///
    /// This method returns all points that fall within the specified
    /// rectangular region, up to the specified limit.
    ///
    /// # Arguments
    ///
    /// * `prefix` - Namespace to search in
    /// * `min_lat` - Minimum latitude of bounding box
    /// * `min_lon` - Minimum longitude of bounding box
    /// * `max_lat` - Maximum latitude of bounding box
    /// * `max_lon` - Maximum longitude of bounding box
    /// * `limit` - Maximum number of results to return
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spatio::{Spatio, Point};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let db = Spatio::memory()?;
    ///
    /// // Find all sensors in Manhattan area
    /// let points = db.find_within_bounds("sensors", 40.7, -74.1, 40.8, -73.9, 100)?;
    /// println!("Found {} sensors in Manhattan", points.len());
    /// # Ok(())
    /// # }
    /// ```
    pub fn find_within_bounds(
        &self,
        prefix: &str,
        min_lat: f64,
        min_lon: f64,
        max_lat: f64,
        max_lon: f64,
        limit: usize,
    ) -> Result<Vec<(Point, Bytes)>> {
        let inner = self.read()?;
        inner
            .index_manager
            .find_within_bounds(prefix, min_lat, min_lon, max_lat, max_lon, limit)
    }

    /// Force sync to disk
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
    /// let db = Spatio::open("my_data.db")?;
    /// db.insert("critical_key", b"important_data", None)?;
    ///
    /// // Ensure data is on disk before continuing
    /// db.sync()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn sync(&self) -> Result<()> {
        let mut inner = self.write()?;
        if let Some(ref mut aof_file) = inner.aof_file {
            aof_file.sync()?;
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
    /// let mut db = Spatio::open("my_data.db")?;
    /// db.insert("key", b"value", None)?;
    ///
    /// // Explicitly close and handle errors
    /// db.close()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn close(&mut self) -> Result<()> {
        let mut inner = self.write()?;
        if inner.closed {
            return Err(SpatioError::DatabaseClosed);
        }

        inner.closed = true;
        if let Some(ref mut aof_file) = inner.aof_file {
            aof_file.sync()?;
        }
        Ok(())
    }

    // Internal helper methods
    fn read(&self) -> Result<RwLockReadGuard<'_, DBInner>> {
        self.inner.read().map_err(|_| SpatioError::LockError)
    }

    pub(crate) fn write(&self) -> Result<RwLockWriteGuard<'_, DBInner>> {
        self.inner.write().map_err(|_| SpatioError::LockError)
    }
}

/// Automatic graceful shutdown on drop.
///
/// When the last reference to the database is dropped, it automatically performs a graceful shutdown:
/// - Flushes pending writes
/// - Syncs to disk (best effort, errors are silently ignored)
/// - Releases resources
///
/// Note: Since DB uses Arc internally, this only triggers when all clones are dropped.
/// The database is NOT marked as closed here to allow clones to continue operating.
/// Use `close()` explicitly if you need to prevent further operations.
impl Drop for DB {
    fn drop(&mut self) {
        // Best-effort sync on drop
        // We don't mark the database as closed since DB is Arc-wrapped and may have other clones
        // Only attempt sync if this is potentially the last reference
        if let Ok(mut inner) = self.inner.write() {
            if !inner.closed {
                if let Some(ref mut aof_file) = inner.aof_file {
                    // Attempt to sync on drop, but don't panic if it fails
                    let _ = aof_file.sync();
                }
            }
        }
    }
}

impl DBInner {
    /// Insert an item into the database
    pub fn insert_item(&mut self, key: Bytes, item: DbItem) -> Option<DbItem> {
        // Remove from old expiration index if updating
        let old_item = if let Some(old) = self.keys.get(&key) {
            if let Some(expires_at) = old.expires_at {
                if let Some(keys) = self.expirations.get_mut(&expires_at) {
                    keys.retain(|k| k != &key);
                    if keys.is_empty() {
                        self.expirations.remove(&expires_at);
                    }
                }
            }
            Some(old.clone())
        } else {
            None
        };

        // Add to expiration index if TTL is set
        if let Some(expires_at) = item.expires_at {
            self.expirations
                .entry(expires_at)
                .or_default()
                .push(key.clone());
        }

        // Insert into main storage
        self.keys.insert(key, item);
        self.stats.key_count = self.keys.len();

        old_item
    }

    /// Remove an item from the database
    pub fn remove_item(&mut self, key: &Bytes) -> Option<DbItem> {
        if let Some(item) = self.keys.remove(key) {
            // Remove from expiration index
            if let Some(expires_at) = item.expires_at {
                if let Some(keys) = self.expirations.get_mut(&expires_at) {
                    keys.retain(|k| k != key);
                    if keys.is_empty() {
                        self.expirations.remove(&expires_at);
                    }
                }
            }

            self.stats.key_count = self.keys.len();
            Some(item)
        } else {
            None
        }
    }

    /// Get an item from the database
    pub fn get_item(&self, key: &Bytes) -> Option<&DbItem> {
        self.keys.get(key)
    }

    /// Load data from AOF file
    /// Load database state from the AOF file (startup replay).
    ///
    /// This method replays all commands from the append-only file to restore
    /// the database to its previous state. It's called automatically during
    /// database initialization.
    ///
    /// The replay process:
    /// 1. Reads all commands from the AOF sequentially
    /// 2. Applies each SET and DELETE command to rebuild state
    /// 3. Reconstructs spatial indexes from geographic data
    /// 4. Updates statistics (key counts, etc.)
    ///
    /// # Error Handling
    ///
    /// If the AOF is corrupted or unreadable, this method returns an error
    /// and the database will not open. To recover from corruption:
    /// - Restore from backup if available
    /// - Or delete the AOF file to start fresh (data loss)
    pub fn load_from_aof(&mut self, aof_file: &mut AOFFile) -> Result<()> {
        let commands = aof_file.replay()?;

        for command in commands {
            match command {
                AOFCommand::Set {
                    key,
                    value,
                    expires_at,
                } => {
                    let item = DbItem {
                        value: value.clone(),
                        expires_at,
                    };
                    self.keys.insert(key.clone(), item);

                    // Rebuild spatial index if this is a spatial key
                    if let Ok(key_str) = std::str::from_utf8(&key) {
                        if let Some((prefix, geohash)) = self.parse_spatial_key(key_str) {
                            if let Ok(point) = self.decode_geohash_to_point(geohash) {
                                let _ = self.index_manager.insert_point(prefix, &point, &value);
                            }
                        }
                    }
                }
                AOFCommand::Delete { key } => {
                    self.keys.remove(&key);

                    // Remove from spatial index if this was a spatial key
                    if let Ok(key_str) = std::str::from_utf8(&key) {
                        if let Some((prefix, geohash)) = self.parse_spatial_key(key_str) {
                            if let Ok(point) = self.decode_geohash_to_point(geohash) {
                                let _ = self.index_manager.remove_point(prefix, &point);
                            }
                        }
                    }
                }
            }
        }

        self.stats.key_count = self.keys.len();
        Ok(())
    }

    /// Parse a spatial key to extract prefix and geohash
    fn parse_spatial_key<'a>(&self, key: &'a str) -> Option<(&'a str, &'a str)> {
        // Spatial keys have format: "prefix:gh:geohash" for geographic points
        let parts: Vec<&str> = key.split(':').collect();
        if parts.len() >= 3 && parts[1] == "gh" {
            let prefix = parts[0];
            let geohash = parts[2];
            Some((prefix, geohash))
        } else {
            None
        }
    }

    /// Decode a geohash back to a Point
    fn decode_geohash_to_point(&self, geohash: &str) -> Result<Point> {
        let (coord, _lat_err, _lon_err) =
            geohash::decode(geohash).map_err(|_| SpatioError::InvalidGeohash)?;
        Ok(Point::new(coord.y, coord.x))
    }

    /// Write to AOF file if needed
    pub fn write_to_aof_if_needed(
        &mut self,
        key: &Bytes,
        value: &[u8],
        options: Option<&SetOptions>,
    ) -> Result<()> {
        if let Some(ref mut aof_file) = self.aof_file {
            let value_bytes = Bytes::copy_from_slice(value);
            aof_file.write_set(key, &value_bytes, options)?;

            // Flush based on sync policy
            match self.config.sync_policy {
                crate::types::SyncPolicy::Always => {
                    aof_file.sync()?;
                }
                crate::types::SyncPolicy::EverySecond => {
                    aof_file.flush()?;
                }
                crate::types::SyncPolicy::Never => {
                    // Don't flush
                }
            }
        }
        Ok(())
    }

    /// Write delete operation to AOF if needed
    pub fn write_delete_to_aof_if_needed(&mut self, key: &Bytes) -> Result<()> {
        if let Some(ref mut aof_file) = self.aof_file {
            aof_file.write_delete(key)?;

            // Flush based on sync policy
            match self.config.sync_policy {
                crate::types::SyncPolicy::Always => {
                    aof_file.sync()?;
                }
                crate::types::SyncPolicy::EverySecond => {
                    aof_file.flush()?;
                }
                crate::types::SyncPolicy::Never => {
                    // Don't flush
                }
            }
        }
        Ok(())
    }
}

// Re-export for convenience
pub use DB as Spatio;
