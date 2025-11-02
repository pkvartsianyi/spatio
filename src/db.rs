//! Core database implementation for Spatio.
//!
//! This module defines the main `DB` type along with spatio-temporal helpers and
//! persistence wiring that power the public `Spatio` API.

use crate::batch::AtomicBatch;
use crate::error::{Result, SpatioError};
use crate::persistence::{AOFCommand, AOFFile};
use crate::spatial_index::SpatialIndexManager;
use crate::types::{
    BoundingBox2D, BoundingBox3D, Config, DbItem, DbStats, Point3d, SetOptions, TemporalPoint,
};
#[cfg(feature = "time-index")]
use crate::types::{HistoryEntry, HistoryEventKind};
use bytes::Bytes;
use geo::Point;
use std::collections::BTreeMap;
#[cfg(feature = "time-index")]
use std::collections::{BTreeSet, HashMap, VecDeque};
use std::path::Path;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};
#[cfg(feature = "time-index")]
use std::time::Duration;
use std::time::SystemTime;

/// Main Spatio database structure providing spatio-temporal data storage.
///
/// The `DB` struct is the core of Spatio, offering:
/// - Key-value storage with spatio-temporal indexing
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
#[derive(Clone)]
pub struct DB {
    pub(crate) inner: Arc<RwLock<DBInner>>,
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
    /// let db = Spatio::open_with_config("my_database.db", config)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn open_with_config<P: AsRef<Path>>(path: P, config: Config) -> Result<Self> {
        let path = path.as_ref();
        let is_memory = path.to_str() == Some(":memory:");

        let mut inner = DBInner::new_with_config(&config);

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

        let cleanup_batch = inner.config.amortized_cleanup_batch;
        if cleanup_batch > 0 {
            let _ = inner.amortized_cleanup(cleanup_batch)?;
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

        // NOTE: We hold the write lock throughout the insertion, including any
        // amortised cleanup. This guarantees that AOF appends (inserts + deletes)
        // remain strictly ordered for deterministic replay.
        let old = inner.insert_item(key_bytes.clone(), item);
        inner.write_to_aof_if_needed(&key_bytes, value.as_ref(), opts.as_ref(), created_at)?;
        Ok(old.map(|item| item.value))
    }

    /// Get a value by key
    pub fn get(&self, key: impl AsRef<[u8]>) -> Result<Option<Bytes>> {
        let key_bytes = Bytes::copy_from_slice(key.as_ref());

        // Fast path: read-only lock
        {
            let inner = self.read()?;
            if inner.closed {
                return Err(SpatioError::DatabaseClosed);
            }

            if let Some(item) = inner.get_item(&key_bytes) {
                if !item.is_expired() {
                    return Ok(Some(item.value.clone()));
                }
            } else {
                return Ok(None);
            }
        }

        // NOTE: expired-key removal (and any amortised cleanup it triggers)
        // runs while holding the same exclusive lock, so AOF delete entries are
        // ordered consistently with preceding writes.

        // Slow path: expired item needs removal
        let mut inner = self.write()?;
        if inner.closed {
            return Err(SpatioError::DatabaseClosed);
        }

        if let Some(item) = inner.get_item(&key_bytes) {
            if item.is_expired() {
                if let Some(_old) = inner.remove_item(&key_bytes) {
                    inner.write_delete_to_aof_if_needed(&key_bytes)?;

                    let cleanup_batch = inner.config.amortized_cleanup_batch;
                    if cleanup_batch > 0 {
                        let _ = inner.amortized_cleanup(cleanup_batch)?;
                    }
                }
                return Ok(None);
            }
            return Ok(Some(item.value.clone()));
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

    /// Remove all expired keys and compact indexes.
    pub fn cleanup_expired(&self) -> Result<usize> {
        let mut inner = self.write()?;
        if inner.closed {
            return Err(SpatioError::DatabaseClosed);
        }

        let now = SystemTime::now();
        let expired_times: Vec<SystemTime> =
            inner.expirations.range(..=now).map(|(&ts, _)| ts).collect();

        let mut removed = 0;
        for ts in expired_times {
            if let Some(keys) = inner.expirations.remove(&ts) {
                for key in keys {
                    if let Some(_item) = inner.remove_item(&key) {
                        inner.write_delete_to_aof_if_needed(&key)?;
                        removed += 1;
                    }
                }
            }
        }

        Ok(removed)
    }

    #[cfg(feature = "time-index")]
    /// Return keys whose last update occurred within the given duration.
    pub fn keys_created_since(&self, duration: Duration) -> Result<Vec<Bytes>> {
        let inner = self.read()?;
        if inner.closed {
            return Err(SpatioError::DatabaseClosed);
        }

        let end = SystemTime::now();
        let start = end.checked_sub(duration).unwrap_or(SystemTime::UNIX_EPOCH);

        Ok(inner.collect_keys_created_between(start, end))
    }

    #[cfg(feature = "time-index")]
    /// Return keys whose last update timestamp falls within the specified interval.
    pub fn keys_created_between(&self, start: SystemTime, end: SystemTime) -> Result<Vec<Bytes>> {
        let inner = self.read()?;
        if inner.closed {
            return Err(SpatioError::DatabaseClosed);
        }

        let (start, end) = if start <= end {
            (start, end)
        } else {
            (end, start)
        };
        Ok(inner.collect_keys_created_between(start, end))
    }

    #[cfg(feature = "time-index")]
    /// Retrieve the recent history of mutations for a specific key.
    pub fn history(&self, key: impl AsRef<[u8]>) -> Result<Vec<HistoryEntry>> {
        let inner = self.read()?;
        if inner.closed {
            return Err(SpatioError::DatabaseClosed);
        }

        if let Some(ref tracker) = inner.history {
            let key_bytes = Bytes::copy_from_slice(key.as_ref());
            Ok(tracker.history_for(&key_bytes).unwrap_or_default())
        } else {
            Ok(Vec::new())
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
    /// let nyc = Point::new(-74.0060, 40.7128);
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
        let data_ref = Bytes::copy_from_slice(value);

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
        let created_at = item.created_at;

        // Generate key with coordinates encoded for AOF replay
        // Format: "prefix:lat_hex:lon_hex:z_hex:timestamp_nanos:uuid"
        let key = format!(
            "{}:{:x}:{:x}:0:{:x}:{}",
            prefix,
            point.y().to_bits(),
            point.x().to_bits(),
            created_at
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos(),
            uuid::Uuid::new_v4()
        );
        let key_bytes = Bytes::copy_from_slice(key.as_bytes());

        inner.insert_item(key_bytes.clone(), item);

        // Add to spatial index (2D with z=0)
        inner.spatial_index.insert_point_2d(
            prefix,
            point.x(),
            point.y(),
            key.clone(),
            data_ref.clone(),
        );

        inner.write_to_aof_if_needed(&key_bytes, value, opts.as_ref(), created_at)?;
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
    /// let center = Point::new(-74.0060, 40.7128);
    ///
    /// // Find up to 10 points within 1km
    /// let nearby = db.query_within_radius("cities", &center, 1000.0, 10)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn query_within_radius(
        &self,
        prefix: &str,
        center: &Point,
        radius_meters: f64,
        limit: usize,
    ) -> Result<Vec<(Point, Bytes)>> {
        let inner = self.read()?;

        let results = inner.spatial_index.query_within_radius_2d(
            prefix,
            center.x(),
            center.y(),
            radius_meters,
            limit,
        );

        // Convert results back to Point (we don't have the original points stored,
        // so we need to retrieve them from the index)
        let mut points = Vec::new();
        for (key, data, _distance) in results {
            if let Some(tree) = inner.spatial_index.indexes.get(prefix) {
                for indexed_point in tree.iter() {
                    if indexed_point.key == key {
                        let point = Point::new(indexed_point.x, indexed_point.y);
                        points.push((point, data.clone()));
                        break;
                    }
                }
            }
        }

        Ok(points)
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
    /// use spatio::{Spatio, Point, TemporalPoint};
    /// use std::time::{Duration, SystemTime, UNIX_EPOCH};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let db = Spatio::memory()?;
    ///
    /// let trajectory = vec![
    ///     TemporalPoint { point: Point::new(-74.0060, 40.7128), timestamp: UNIX_EPOCH + Duration::from_secs(1640995200) }, // Start
    ///     TemporalPoint { point: Point::new(-74.0040, 40.7150), timestamp: UNIX_EPOCH + Duration::from_secs(1640995260) }, // 1 min later
    ///     TemporalPoint { point: Point::new(-74.0020, 40.7172), timestamp: UNIX_EPOCH + Duration::from_secs(1640995320) }, // 2 min later
    /// ];
    ///
    /// db.insert_trajectory("vehicle:truck001", &trajectory, None)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn insert_trajectory(
        &self,
        object_id: &str,
        trajectory: &[TemporalPoint],
        opts: Option<SetOptions>,
    ) -> Result<()> {
        for (i, temporal_point) in trajectory.iter().enumerate() {
            let key = format!(
                "traj:{}:{:010}:{:06}",
                object_id,
                temporal_point
                    .timestamp
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .map_err(|_| SpatioError::InvalidTimestamp)?
                    .as_secs(),
                i
            );
            let point_data = bincode::serialize(&temporal_point).map_err(|e| {
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
    /// use spatio::{Spatio, TemporalPoint};
    /// use std::time::{Duration, SystemTime, UNIX_EPOCH};
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
    ) -> Result<Vec<TemporalPoint>> {
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

            match bincode::deserialize::<TemporalPoint>(&item.value) {
                Ok(temporal_point) => {
                    let timestamp_secs = temporal_point
                        .timestamp
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .map_err(|_| SpatioError::InvalidTimestamp)?
                        .as_secs();
                    if timestamp_secs >= start_time && timestamp_secs <= end_time {
                        results.push(temporal_point);
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

        results.sort_by_key(|tp| tp.timestamp);
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
    /// let center = Point::new(-74.0060, 40.7128);
    ///
    /// // Check if there are any cities within 50km
    /// let has_nearby = db.contains_point("cities", &center, 50_000.0)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn contains_point(&self, prefix: &str, center: &Point, radius_meters: f64) -> Result<bool> {
        let inner = self.read()?;
        Ok(inner
            .spatial_index
            .contains_point_2d(prefix, center.x(), center.y(), radius_meters))
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
        let results = inner
            .spatial_index
            .query_within_bbox_2d(prefix, min_lon, min_lat, max_lon, max_lat);
        Ok(!results.is_empty())
    }

    /// Count points within a distance from a center point.
    ///
    /// This method counts how many points exist within the specified
    /// distance from a center point without returning the actual points.
    /// More efficient than `query_within_radius` when you only need the count.
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
    /// let center = Point::new(-74.0060, 40.7128);
    ///
    /// // Count how many sensors are within 1km
    /// let count = db.count_within_radius("sensors", &center, 1000.0)?;
    /// println!("Found {} sensors within 1km", count);
    /// # Ok(())
    /// # }
    /// ```
    pub fn count_within_radius(
        &self,
        prefix: &str,
        center: &Point,
        radius_meters: f64,
    ) -> Result<usize> {
        let inner = self.read()?;
        Ok(inner.spatial_index.count_within_radius_2d(
            prefix,
            center.x(),
            center.y(),
            radius_meters,
        ))
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
        let results = inner
            .spatial_index
            .query_within_bbox_2d(prefix, min_lon, min_lat, max_lon, max_lat);

        let mut points = Vec::new();
        for (key, data) in results.into_iter().take(limit) {
            if let Some(tree) = inner.spatial_index.indexes.get(prefix) {
                for indexed_point in tree.iter() {
                    if indexed_point.key == key {
                        let point = Point::new(indexed_point.x, indexed_point.y);
                        points.push((point, data.clone()));
                        break;
                    }
                }
            }
        }
        Ok(points)
    }

    /// Calculate the distance between two points using a specified metric.
    ///
    /// This is a convenience method that wraps geo crate distance calculations.
    /// For most lon/lat use cases, Haversine is recommended.
    ///
    /// # Arguments
    ///
    /// * `point1` - First point
    /// * `point2` - Second point
    /// * `metric` - Distance metric (Haversine, Geodesic, Rhumb, or Euclidean)
    ///
    /// # Returns
    ///
    /// Distance in meters
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spatio::{Spatio, Point, spatial::DistanceMetric};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let db = Spatio::memory()?;
    ///
    /// let nyc = Point::new(-74.0060, 40.7128);
    /// let la = Point::new(-118.2437, 34.0522);
    ///
    /// let distance = db.distance_between(&nyc, &la, DistanceMetric::Haversine)?;
    /// println!("Distance: {} meters", distance);
    /// # Ok(())
    /// # }
    /// ```
    pub fn distance_between(
        &self,
        point1: &Point,
        point2: &Point,
        metric: crate::spatial::DistanceMetric,
    ) -> Result<f64> {
        Ok(crate::spatial::distance_between(point1, point2, metric))
    }

    /// Find the K nearest points to a query point within a namespace.
    ///
    /// This performs a K-nearest-neighbor search using the spatial index.
    /// It first queries a radius, then refines to the K nearest points.
    ///
    /// # Arguments
    ///
    /// * `prefix` - Namespace to search in
    /// * `center` - Query point
    /// * `k` - Number of nearest neighbors to return
    /// * `max_radius` - Maximum search radius in meters
    /// * `metric` - Distance metric to use
    ///
    /// # Returns
    ///
    /// Vector of (Point, Bytes, distance) tuples sorted by distance (nearest first)
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spatio::{Spatio, Point, spatial::DistanceMetric};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let db = Spatio::memory()?;
    ///
    /// let nyc = Point::new(-74.0060, 40.7128);
    /// db.insert_point("cities", &nyc, b"New York", None)?;
    ///
    /// let query = Point::new(-74.0, 40.7);
    /// let nearest = db.knn("cities", &query, 5, 100_000.0, DistanceMetric::Haversine)?;
    ///
    /// for (point, data, distance) in nearest {
    ///     println!("Found city at {}m", distance);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn knn(
        &self,
        prefix: &str,
        center: &Point,
        k: usize,
        max_radius: f64,
        metric: crate::spatial::DistanceMetric,
    ) -> Result<Vec<(Point, Bytes, f64)>> {
        // Query all points within max_radius
        let candidates = self.query_within_radius(prefix, center, max_radius, usize::MAX)?;

        // Convert to format expected by knn function
        let points: Vec<(Point, Bytes)> = candidates;

        // Use the spatial module's knn function
        let results = crate::spatial::knn(center, &points, k, metric);

        // Convert back to include data
        Ok(results
            .into_iter()
            .map(|(pt, dist, data)| (pt, data, dist))
            .collect())
    }

    /// Query points within a polygon boundary.
    ///
    /// This finds all points that are contained within the given polygon.
    /// It uses the polygon's bounding box for initial filtering via the
    /// spatial index, then performs precise point-in-polygon tests.
    ///
    /// # Arguments
    ///
    /// * `prefix` - Namespace to search in
    /// * `polygon` - The polygon boundary
    /// * `limit` - Maximum number of results to return
    ///
    /// # Returns
    ///
    /// Vector of (Point, Bytes) tuples for points within the polygon
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spatio::{Spatio, Point};
    /// use geo::{polygon, Polygon};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let db = Spatio::memory()?;
    ///
    /// let poly: Polygon = polygon![
    ///     (x: -74.0, y: 40.7),
    ///     (x: -73.9, y: 40.7),
    ///     (x: -73.9, y: 40.8),
    ///     (x: -74.0, y: 40.8),
    /// ];
    ///
    /// let results = db.query_within_polygon("cities", &poly, 100)?;
    /// println!("Found {} cities in polygon", results.len());
    /// # Ok(())
    /// # }
    /// ```
    pub fn query_within_polygon(
        &self,
        prefix: &str,
        polygon: &geo::Polygon,
        limit: usize,
    ) -> Result<Vec<(Point, Bytes)>> {
        use geo::BoundingRect;

        // Get bounding box of polygon for initial filtering
        let bbox = polygon
            .bounding_rect()
            .ok_or_else(|| SpatioError::InvalidInput("Polygon has no bounding box".to_string()))?;

        // Query all points within the bounding box
        let candidates = self.find_within_bounds(
            prefix,
            bbox.min().y,
            bbox.min().x,
            bbox.max().y,
            bbox.max().x,
            usize::MAX,
        )?;

        // Filter to only points actually within the polygon
        let mut results = Vec::new();
        for (point, data) in candidates {
            if crate::spatial::point_in_polygon(polygon, &point) {
                results.push((point, data));
                if results.len() >= limit {
                    break;
                }
            }
        }

        Ok(results)
    }

    /// Query all points within a bounding box.
    ///
    /// Returns all spatial points that fall within the specified 2D bounding box,
    /// ordered by their distance from the box's center.
    ///
    /// # Arguments
    ///
    /// * `prefix` - Optional key prefix to filter results
    /// * `bbox` - The bounding box to search within
    /// * `limit` - Maximum number of results to return
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spatio::{Spatio, Point, BoundingBox2D};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let db = Spatio::memory()?;
    ///
    /// // Store some points
    /// db.insert_point("poi", &Point::new(-73.9855, 40.7580), b"times_square", None)?;
    /// db.insert_point("poi", &Point::new(-73.9665, 40.7829), b"central_park", None)?;
    /// db.insert_point("poi", &Point::new(-73.9442, 40.6782), b"brooklyn", None)?;
    ///
    /// // Query points in Manhattan
    /// let manhattan = BoundingBox2D::new(-74.0479, 40.6829, -73.9067, 40.8820);
    /// let results = db.query_within_bbox("poi", &manhattan, 100)?;
    ///
    /// println!("Found {} points in Manhattan", results.len());
    /// # Ok(())
    /// # }
    /// ```
    pub fn query_within_bbox(
        &self,
        prefix: &str,
        bbox: &BoundingBox2D,
        limit: usize,
    ) -> Result<Vec<(Point, Bytes)>> {
        let center = bbox.center();

        // Get candidates using radius search from center
        // Calculate approximate radius from center to corner
        let dx = bbox.width() / 2.0;
        let dy = bbox.height() / 2.0;
        let radius_deg = (dx * dx + dy * dy).sqrt();

        // Convert to meters (rough approximation: 1 degree ≈ 111km)
        let radius_meters = radius_deg * 111_000.0;

        let candidates = self.query_within_radius(prefix, &center, radius_meters, limit * 2)?;

        // Filter to only points actually within the bounding box
        let mut results = Vec::new();
        for (point, data) in candidates {
            if bbox.contains_point(&point) {
                results.push((point, data));
                if results.len() >= limit {
                    break;
                }
            }
        }

        Ok(results)
    }

    /// Store a bounding box with a key.
    ///
    /// Serializes and stores a bounding box, making it retrievable later.
    /// Useful for storing geographic regions, service areas, or zones.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to store the bounding box under
    /// * `bbox` - The bounding box to store
    /// * `opts` - Optional settings like TTL
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spatio::{Spatio, BoundingBox2D};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let db = Spatio::memory()?;
    ///
    /// let manhattan = BoundingBox2D::new(-74.0479, 40.6829, -73.9067, 40.8820);
    /// db.insert_bbox("zones:manhattan", &manhattan, None)?;
    ///
    /// # Ok(())
    /// # }
    /// ```
    pub fn insert_bbox(
        &self,
        key: impl AsRef<[u8]>,
        bbox: &BoundingBox2D,
        opts: Option<SetOptions>,
    ) -> Result<()> {
        let serialized = bincode::serialize(bbox)
            .map_err(|e| SpatioError::SerializationErrorWithContext(e.to_string()))?;
        self.insert(key, serialized, opts)?;
        Ok(())
    }

    /// Retrieve a bounding box by key.
    ///
    /// Deserializes and returns a previously stored bounding box.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to retrieve
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spatio::{Spatio, BoundingBox2D};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let db = Spatio::memory()?;
    ///
    /// let manhattan = BoundingBox2D::new(-74.0479, 40.6829, -73.9067, 40.8820);
    /// db.insert_bbox("zones:manhattan", &manhattan, None)?;
    ///
    /// if let Some(bbox) = db.get_bbox("zones:manhattan")? {
    ///     println!("Manhattan area: {}°×{}°", bbox.width(), bbox.height());
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn get_bbox(&self, key: impl AsRef<[u8]>) -> Result<Option<BoundingBox2D>> {
        match self.get(key)? {
            Some(data) => {
                let bbox = bincode::deserialize(&data)
                    .map_err(|e| SpatioError::SerializationErrorWithContext(e.to_string()))?;
                Ok(Some(bbox))
            }
            None => Ok(None),
        }
    }

    /// Find all bounding boxes that intersect with a given bounding box.
    ///
    /// Returns all stored bounding boxes (with the specified prefix) that
    /// intersect with the query bounding box.
    ///
    /// # Arguments
    ///
    /// * `prefix` - Key prefix to filter results (e.g., "zones:")
    /// * `bbox` - The bounding box to check for intersections
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spatio::{Spatio, BoundingBox2D};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let db = Spatio::memory()?;
    ///
    /// db.insert_bbox("zones:manhattan", &BoundingBox2D::new(-74.0479, 40.6829, -73.9067, 40.8820), None)?;
    /// db.insert_bbox("zones:brooklyn", &BoundingBox2D::new(-74.0421, 40.5707, -73.8333, 40.7395), None)?;
    ///
    /// let query = BoundingBox2D::new(-74.01, 40.70, -73.95, 40.75);
    /// let intersecting = db.find_intersecting_bboxes("zones:", &query)?;
    ///
    /// println!("Found {} intersecting zones", intersecting.len());
    /// # Ok(())
    /// # }
    /// ```
    pub fn find_intersecting_bboxes(
        &self,
        prefix: &str,
        bbox: &BoundingBox2D,
    ) -> Result<Vec<(String, BoundingBox2D)>> {
        let inner = self.read()?;
        let prefix_bytes = Bytes::from(prefix.to_owned());
        let mut results = Vec::new();

        for (key, item) in inner.keys.range(prefix_bytes.clone()..) {
            if !key.starts_with(prefix.as_bytes()) {
                break;
            }

            if item.is_expired() {
                continue;
            }

            // Try to deserialize as bounding box
            if let Ok(stored_bbox) = bincode::deserialize::<BoundingBox2D>(&item.value) {
                if stored_bbox.intersects(bbox) {
                    let key_str = String::from_utf8_lossy(key).to_string();
                    results.push((key_str, stored_bbox));
                }
            }
        }

        Ok(results)
    }

    /// Insert a 3D point (with altitude) into the database.
    ///
    /// This method stores a 3D geographic point with altitude/elevation information
    /// and automatically adds it to the 3D spatial index for altitude-aware queries.
    ///
    /// # Arguments
    ///
    /// * `prefix` - The prefix/namespace for organizing related points
    /// * `point` - The 3D point with x, y, z coordinates
    /// * `value` - The data to associate with this point
    /// * `options` - Optional TTL and other storage options
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spatio::{Spatio, Point3d};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let db = Spatio::memory()?;
    ///
    /// // Track a drone at 100 meters altitude
    /// let drone_pos = Point3d::new(-74.0060, 40.7128, 100.0);
    /// db.insert_point_3d("drones", &drone_pos, b"Drone-001", None)?;
    ///
    /// // Track an aircraft at 10,000 meters
    /// let aircraft_pos = Point3d::new(-74.0070, 40.7138, 10000.0);
    /// db.insert_point_3d("aircraft", &aircraft_pos, b"Flight-AA123", None)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn insert_point_3d(
        &self,
        prefix: &str,
        point: &Point3d,
        value: &[u8],
        opts: Option<SetOptions>,
    ) -> Result<()> {
        let data_ref = Bytes::copy_from_slice(value);

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
        let created_at = item.created_at;

        // Generate key with coordinates encoded for AOF replay
        // Format: "prefix:lat_hex:lon_hex:z_hex:timestamp_nanos:uuid"
        let key = format!(
            "{}:{:x}:{:x}:{:x}:{:x}:{}",
            prefix,
            point.point_2d().y().to_bits(),
            point.point_2d().x().to_bits(),
            point.z().to_bits(),
            created_at
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos(),
            uuid::Uuid::new_v4()
        );
        let key_bytes = Bytes::copy_from_slice(key.as_bytes());

        inner.insert_item(key_bytes.clone(), item);

        // Add to 3D spatial index
        inner.spatial_index.insert_point(
            prefix,
            point.x(),
            point.y(),
            point.z(),
            key.clone(),
            data_ref.clone(),
        );

        inner.write_to_aof_if_needed(&key_bytes, value, opts.as_ref(), created_at)?;
        Ok(())
    }

    /// Query points within a 3D spherical radius.
    ///
    /// Finds all points within a spherical distance from the center point,
    /// taking altitude differences into account using 3D distance calculation.
    ///
    /// # Arguments
    ///
    /// * `prefix` - The prefix/namespace to search
    /// * `center` - The center point for the search
    /// * `radius` - Radius in meters (3D distance)
    /// * `limit` - Maximum number of results to return
    ///
    /// # Returns
    ///
    /// Vector of (point, data, distance) tuples sorted by distance.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spatio::{Spatio, Point3d};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let db = Spatio::memory()?;
    ///
    /// let drone1 = Point3d::new(-74.0060, 40.7128, 100.0);
    /// let drone2 = Point3d::new(-74.0070, 40.7138, 150.0);
    ///
    /// db.insert_point_3d("drones", &drone1, b"Drone-1", None)?;
    /// db.insert_point_3d("drones", &drone2, b"Drone-2", None)?;
    ///
    /// // Find drones within 500m radius (3D)
    /// let center = Point3d::new(-74.0065, 40.7133, 125.0);
    /// let nearby = db.query_within_sphere_3d("drones", &center, 500.0, 10)?;
    ///
    /// for (point, data, distance) in nearby {
    ///     println!("Found drone at {}m distance", distance);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn query_within_sphere_3d(
        &self,
        prefix: &str,
        center: &Point3d,
        radius: f64,
        limit: usize,
    ) -> Result<Vec<(Point3d, Bytes, f64)>> {
        let inner = self.read()?;

        let results = inner.spatial_index.query_within_sphere(
            prefix,
            center.x(),
            center.y(),
            center.z(),
            radius,
            limit,
        );

        // Parse keys to extract coordinates and reconstruct Point3d
        let mut points = Vec::new();
        for (key, data, distance) in results {
            if let Some(stored_point) = inner.spatial_index.indexes.get(prefix) {
                for indexed_point in stored_point.iter() {
                    if indexed_point.key == key {
                        let point = Point3d::new(indexed_point.x, indexed_point.y, indexed_point.z);
                        points.push((point, data.clone(), distance));
                        break;
                    }
                }
            }
        }

        Ok(points)
    }

    /// Query points within a 3D bounding box.
    ///
    /// # Arguments
    ///
    /// * `prefix` - The prefix/namespace to search
    /// * `bbox` - The 3D bounding box to search within
    /// * `limit` - Maximum number of results to return
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spatio::{Spatio, Point3d, BoundingBox3D};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let db = Spatio::memory()?;
    ///
    /// let drone = Point3d::new(-74.0060, 40.7128, 100.0);
    /// db.insert_point_3d("drones", &drone, b"Drone-1", None)?;
    ///
    /// // Search in a 3D box
    /// let bbox = BoundingBox3D::new(-74.01, 40.71, 50.0, -74.00, 40.72, 150.0);
    /// let results = db.query_within_bbox_3d("drones", &bbox, 100)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn query_within_bbox_3d(
        &self,
        prefix: &str,
        bbox: &BoundingBox3D,
        limit: usize,
    ) -> Result<Vec<(Point3d, Bytes)>> {
        let inner = self.read()?;

        let results = inner.spatial_index.query_within_bbox(
            prefix, bbox.min_x, bbox.min_y, bbox.min_z, bbox.max_x, bbox.max_y, bbox.max_z,
        );

        let mut points = Vec::new();
        for (key, data) in results.into_iter().take(limit) {
            if let Some(stored_point) = inner.spatial_index.indexes.get(prefix) {
                for indexed_point in stored_point.iter() {
                    if indexed_point.key == key {
                        let point = Point3d::new(indexed_point.x, indexed_point.y, indexed_point.z);
                        points.push((point, data.clone()));
                        break;
                    }
                }
            }
        }

        Ok(points)
    }

    /// Query points within a cylindrical volume.
    ///
    /// This is useful for altitude-constrained radius queries, such as finding
    /// all aircraft within a certain horizontal distance and altitude range.
    ///
    /// # Arguments
    ///
    /// * `prefix` - The prefix/namespace to search
    /// * `center` - The center point (only x, y used for horizontal center)
    /// * `min_altitude` - Minimum altitude/z coordinate
    /// * `max_altitude` - Maximum altitude/z coordinate
    /// * `horizontal_radius` - Horizontal radius in meters
    /// * `limit` - Maximum number of results
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spatio::{Spatio, Point3d};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let db = Spatio::memory()?;
    ///
    /// let aircraft1 = Point3d::new(-74.0060, 40.7128, 5000.0);
    /// let aircraft2 = Point3d::new(-74.0070, 40.7138, 10000.0);
    ///
    /// db.insert_point_3d("aircraft", &aircraft1, b"Flight-1", None)?;
    /// db.insert_point_3d("aircraft", &aircraft2, b"Flight-2", None)?;
    ///
    /// // Find aircraft between 3000m and 7000m altitude within 10km horizontal
    /// let center = Point3d::new(-74.0065, 40.7133, 0.0);
    /// let results = db.query_within_cylinder_3d(
    ///     "aircraft",
    ///     &center,
    ///     3000.0,
    ///     7000.0,
    ///     10000.0,
    ///     100
    /// )?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn query_within_cylinder_3d(
        &self,
        prefix: &str,
        center: &Point3d,
        min_altitude: f64,
        max_altitude: f64,
        horizontal_radius: f64,
        limit: usize,
    ) -> Result<Vec<(Point3d, Bytes, f64)>> {
        let inner = self.read()?;

        let results = inner.spatial_index.query_within_cylinder(
            prefix,
            center.x(),
            center.y(),
            min_altitude,
            max_altitude,
            horizontal_radius,
            limit,
        );

        let mut points = Vec::new();
        for (key, data, h_dist) in results {
            if let Some(stored_point) = inner.spatial_index.indexes.get(prefix) {
                for indexed_point in stored_point.iter() {
                    if indexed_point.key == key {
                        let point = Point3d::new(indexed_point.x, indexed_point.y, indexed_point.z);
                        points.push((point, data.clone(), h_dist));
                        break;
                    }
                }
            }
        }

        Ok(points)
    }

    /// Find the k nearest neighbors in 3D space.
    ///
    /// # Arguments
    ///
    /// * `prefix` - The prefix/namespace to search
    /// * `point` - The query point
    /// * `k` - Number of nearest neighbors to find
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spatio::{Spatio, Point3d};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let db = Spatio::memory()?;
    ///
    /// db.insert_point_3d("drones", &Point3d::new(-74.00, 40.71, 100.0), b"D1", None)?;
    /// db.insert_point_3d("drones", &Point3d::new(-74.01, 40.72, 200.0), b"D2", None)?;
    /// db.insert_point_3d("drones", &Point3d::new(-74.02, 40.73, 300.0), b"D3", None)?;
    ///
    /// // Find 2 nearest drones in 3D space
    /// let query = Point3d::new(-74.005, 40.715, 150.0);
    /// let nearest = db.knn_3d("drones", &query, 2)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn knn_3d(
        &self,
        prefix: &str,
        point: &Point3d,
        k: usize,
    ) -> Result<Vec<(Point3d, Bytes, f64)>> {
        let inner = self.read()?;

        let results = inner
            .spatial_index
            .knn_3d(prefix, point.x(), point.y(), point.z(), k);

        let mut points = Vec::new();
        for (key, data, distance) in results {
            if let Some(stored_point) = inner.spatial_index.indexes.get(prefix) {
                for indexed_point in stored_point.iter() {
                    if indexed_point.key == key {
                        let point = Point3d::new(indexed_point.x, indexed_point.y, indexed_point.z);
                        points.push((point, data.clone(), distance));
                        break;
                    }
                }
            }
        }

        Ok(points)
    }

    /// Calculate the 3D distance between two points.
    ///
    /// Uses haversine formula for horizontal distance and incorporates
    /// altitude difference using the Pythagorean theorem.
    ///
    /// # Returns
    ///
    /// Distance in meters.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spatio::{Spatio, Point3d};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let db = Spatio::memory()?;
    ///
    /// let p1 = Point3d::new(-74.0060, 40.7128, 0.0);
    /// let p2 = Point3d::new(-74.0070, 40.7138, 100.0);
    ///
    /// let distance = db.distance_between_3d(&p1, &p2)?;
    /// println!("3D distance: {} meters", distance);
    /// # Ok(())
    /// # }
    /// ```
    pub fn distance_between_3d(&self, p1: &Point3d, p2: &Point3d) -> Result<f64> {
        Ok(p1.haversine_3d(p2))
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
        let sync_mode = inner.config.sync_mode;
        if let Some(ref mut aof_file) = inner.aof_file {
            aof_file.sync_with_mode(sync_mode)?;
            inner.sync_ops_since_flush = 0;
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
        let mut inner = self.write()?;
        if inner.closed {
            return Err(SpatioError::DatabaseClosed);
        }

        inner.closed = true;
        let sync_mode = inner.config.sync_mode;
        if let Some(ref mut aof_file) = inner.aof_file {
            aof_file.sync_with_mode(sync_mode)?;
            inner.sync_ops_since_flush = 0;
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

    fn history_for(&self, key: &Bytes) -> Option<Vec<HistoryEntry>> {
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
        // Only sync if this is the last reference to the database
        if Arc::strong_count(&self.inner) != 1 {
            return;
        }

        // Best-effort sync on final drop
        if let Ok(mut inner) = self.inner.write() {
            if inner.closed {
                return;
            }

            let sync_mode = inner.config.sync_mode;
            if let Some(ref mut aof_file) = inner.aof_file {
                // Attempt to sync on drop, but don't panic if it fails
                if aof_file.sync_with_mode(sync_mode).is_ok() {
                    inner.sync_ops_since_flush = 0;
                }
            }
        }
    }
}

impl DBInner {
    pub(crate) fn new_with_config(config: &Config) -> Self {
        Self {
            keys: BTreeMap::new(),
            expirations: BTreeMap::new(),
            spatial_index: SpatialIndexManager::new(),
            aof_file: None,
            closed: false,
            stats: DbStats::default(),
            config: config.clone(),
            sync_ops_since_flush: 0,
            #[cfg(feature = "time-index")]
            created_index: BTreeMap::new(),
            #[cfg(feature = "time-index")]
            history: config.history_capacity.map(HistoryTracker::new),
        }
    }

    fn add_expiration(&mut self, key: &Bytes, expires_at: Option<SystemTime>) {
        if let Some(exp) = expires_at {
            self.expirations.entry(exp).or_default().push(key.clone());
        }
    }

    fn remove_expiration_entry(&mut self, key: &Bytes, item: &DbItem) {
        if let Some(exp) = item.expires_at
            && let Some(keys) = self.expirations.get_mut(&exp)
        {
            keys.retain(|k| k != key);
            if keys.is_empty() {
                self.expirations.remove(&exp);
            }
        }
    }

    #[cfg(feature = "time-index")]
    fn add_created_index(&mut self, key: &Bytes, created_at: SystemTime) {
        self.created_index
            .entry(created_at)
            .or_default()
            .insert(key.clone());
    }

    #[cfg(feature = "time-index")]
    fn remove_created_index(&mut self, key: &Bytes, item: &DbItem) {
        if let Some(keys) = self.created_index.get_mut(&item.created_at) {
            keys.remove(key);
            if keys.is_empty() {
                self.created_index.remove(&item.created_at);
            }
        }
    }

    /// Insert an item into the database
    pub fn insert_item(&mut self, key: Bytes, item: DbItem) -> Option<DbItem> {
        let expires_at = item.expires_at;
        #[cfg(feature = "time-index")]
        let created_at = item.created_at;
        #[cfg(feature = "time-index")]
        let history_value = self.history.as_ref().map(|_| item.value.clone());

        let old_item = self.keys.insert(key.clone(), item);
        if let Some(ref old) = old_item {
            self.remove_expiration_entry(&key, old);
            #[cfg(feature = "time-index")]
            self.remove_created_index(&key, old);
        }

        self.add_expiration(&key, expires_at);
        #[cfg(feature = "time-index")]
        self.add_created_index(&key, created_at);

        #[cfg(feature = "time-index")]
        if let Some(history) = self.history.as_mut()
            && let Some(value) = history_value
        {
            history.record_set(&key, value, created_at, expires_at);
        }

        self.stats.key_count = self.keys.len();
        old_item
    }

    /// Remove an item from the database
    pub fn remove_item(&mut self, key: &Bytes) -> Option<DbItem> {
        if let Some(item) = self.keys.remove(key) {
            #[cfg(feature = "time-index")]
            let history_value = self.history.as_ref().map(|_| item.value.clone());
            self.remove_expiration_entry(key, &item);
            #[cfg(feature = "time-index")]
            self.remove_created_index(key, &item);

            if let Ok(key_str) = std::str::from_utf8(key) {
                if let Some(prefix) = key_str.split(':').next() {
                    let _ = self.spatial_index.remove_entry(prefix, key_str);
                }
            }

            #[cfg(feature = "time-index")]
            if let Some(history) = self.history.as_mut() {
                history.record_delete(key, SystemTime::now(), history_value);
            }

            self.stats.key_count = self.keys.len();
            Some(item)
        } else {
            None
        }
    }

    fn amortized_cleanup(&mut self, max_items: usize) -> Result<usize> {
        if max_items == 0 {
            return Ok(0);
        }

        let now = SystemTime::now();
        let mut removed = 0;

        while removed < max_items {
            let Some(ts) = self.expirations.range(..=now).next().map(|(ts, _)| *ts) else {
                break;
            };

            let mut keys = match self.expirations.remove(&ts) {
                Some(keys) => keys,
                None => continue,
            };

            while removed < max_items {
                let Some(key) = keys.pop() else {
                    break;
                };

                if let Some(_item) = self.remove_item(&key) {
                    self.write_delete_to_aof_if_needed(&key)?;
                    removed += 1;
                }
            }

            if !keys.is_empty() {
                self.expirations.insert(ts, keys);
            }
        }

        Ok(removed)
    }

    /// Get an item from the database
    pub fn get_item(&self, key: &Bytes) -> Option<&DbItem> {
        self.keys.get(key)
    }

    #[cfg(feature = "time-index")]
    fn collect_keys_created_between(&self, start: SystemTime, end: SystemTime) -> Vec<Bytes> {
        let mut results = Vec::new();
        let now = SystemTime::now();
        for (_timestamp, keys) in self.created_index.range(start..=end) {
            for key in keys {
                if let Some(item) = self.keys.get(key)
                    && !item.is_expired_at(now)
                {
                    results.push(key.clone());
                }
            }
        }
        results
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
        for command in aof_file.replay()? {
            match command {
                AOFCommand::Set {
                    key,
                    value,
                    created_at,
                    expires_at,
                } => {
                    self.apply_set_from_aof(key, value, created_at, expires_at)?;
                }
                AOFCommand::Delete { key } => {
                    self.apply_delete_from_aof(key)?;
                }
            }
        }

        self.stats.key_count = self.keys.len();
        Ok(())
    }

    fn apply_set_from_aof(
        &mut self,
        key: Bytes,
        value: Bytes,
        created_at: SystemTime,
        expires_at: Option<SystemTime>,
    ) -> Result<()> {
        let item = DbItem {
            value: value.clone(),
            created_at,
            expires_at,
        };

        if let Some(old) = self.keys.insert(key.clone(), item) {
            self.remove_expiration_entry(&key, &old);
            #[cfg(feature = "time-index")]
            self.remove_created_index(&key, &old);
        }

        self.add_expiration(&key, expires_at);
        #[cfg(feature = "time-index")]
        self.add_created_index(&key, created_at);

        #[cfg(feature = "time-index")]
        if let Some(history) = self.history.as_mut() {
            history.record_set(&key, value.clone(), created_at, expires_at);
        }

        self.rebuild_spatial_index(&key, &value);
        Ok(())
    }

    fn apply_delete_from_aof(&mut self, key: Bytes) -> Result<()> {
        if let Some(item) = self.keys.remove(&key) {
            #[cfg(feature = "time-index")]
            let deleted_value = item.value.clone();
            self.remove_expiration_entry(&key, &item);
            #[cfg(feature = "time-index")]
            self.remove_created_index(&key, &item);

            #[cfg(feature = "time-index")]
            if let Some(history) = self.history.as_mut() {
                history.record_delete(&key, SystemTime::now(), Some(deleted_value));
            }
        }

        self.remove_from_spatial_index(&key);
        Ok(())
    }

    fn rebuild_spatial_index(&mut self, key: &Bytes, value: &Bytes) {
        // During AOF replay, rebuild spatial index from encoded keys
        // Key format: "prefix:lat_hex:lon_hex:z_hex:timestamp_nanos:uuid"

        if let Ok(key_str) = std::str::from_utf8(key) {
            let parts: Vec<&str> = key_str.split(':').collect();

            // Check if this is a spatial key (has at least 6 parts with coordinates)
            if parts.len() >= 6 {
                if let (Ok(lat_bits), Ok(lon_bits), Ok(z_bits)) = (
                    u64::from_str_radix(parts[1], 16),
                    u64::from_str_radix(parts[2], 16),
                    u64::from_str_radix(parts[3], 16),
                ) {
                    let prefix = parts[0];
                    let lat = f64::from_bits(lat_bits);
                    let lon = f64::from_bits(lon_bits);
                    let z = f64::from_bits(z_bits);

                    // Insert into spatial index
                    if z == 0.0 {
                        // 2D point
                        self.spatial_index.insert_point_2d(
                            prefix,
                            lon,
                            lat,
                            key_str.to_string(),
                            value.clone(),
                        );
                    } else {
                        // 3D point
                        self.spatial_index.insert_point(
                            prefix,
                            lon,
                            lat,
                            z,
                            key_str.to_string(),
                            value.clone(),
                        );
                    }
                }
            }
        }
    }

    fn remove_from_spatial_index(&mut self, key: &Bytes) {
        if let Ok(key_str) = std::str::from_utf8(key) {
            if let Some(prefix) = key_str.split(':').next() {
                let _ = self.spatial_index.remove_entry(prefix, key_str);
            }
        }
    }

    /// Write to AOF file if needed
    pub fn write_to_aof_if_needed(
        &mut self,
        key: &Bytes,
        value: &[u8],
        options: Option<&SetOptions>,
        created_at: SystemTime,
    ) -> Result<()> {
        let Some(aof_file) = self.aof_file.as_mut() else {
            return Ok(());
        };

        let sync_policy = self.config.sync_policy;
        let sync_mode = self.config.sync_mode;
        let batch_size = self.config.sync_batch_size;
        let value_bytes = Bytes::copy_from_slice(value);

        aof_file.write_set(key, &value_bytes, options, created_at)?;
        self.maybe_flush_or_sync(sync_policy, sync_mode, batch_size)?;
        Ok(())
    }

    /// Write delete operation to AOF if needed
    pub fn write_delete_to_aof_if_needed(&mut self, key: &Bytes) -> Result<()> {
        let Some(aof_file) = self.aof_file.as_mut() else {
            return Ok(());
        };

        let sync_policy = self.config.sync_policy;
        let sync_mode = self.config.sync_mode;
        let batch_size = self.config.sync_batch_size;

        aof_file.write_delete(key)?;
        self.maybe_flush_or_sync(sync_policy, sync_mode, batch_size)?;
        Ok(())
    }

    fn maybe_flush_or_sync(
        &mut self,
        policy: crate::types::SyncPolicy,
        mode: crate::types::SyncMode,
        batch_size: usize,
    ) -> Result<()> {
        use crate::types::SyncPolicy;

        let Some(aof_file) = self.aof_file.as_mut() else {
            return Ok(());
        };

        match policy {
            SyncPolicy::Always => {
                self.sync_ops_since_flush += 1;
                if self.sync_ops_since_flush >= batch_size {
                    aof_file.sync_with_mode(mode)?;
                    self.sync_ops_since_flush = 0;
                } else {
                    aof_file.flush()?;
                }
            }
            SyncPolicy::EverySecond => {
                aof_file.flush()?;
            }
            SyncPolicy::Never => {}
        }

        Ok(())
    }
}

// Re-export for convenience
pub use DB as Spatio;

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(feature = "time-index")]
    use bytes::Bytes;
    use std::sync::Arc;
    use std::thread::sleep;
    use std::time::Duration;

    #[test]
    fn test_drop_only_syncs_on_last_reference() {
        use std::fs;
        let temp_path = std::env::temp_dir().join("test_drop_sync.aof");
        let _ = fs::remove_file(&temp_path);

        // Create database
        let db = DB::open(&temp_path).unwrap();
        db.insert("key1", b"value1", None).unwrap();

        // Create clones
        let db2 = db.clone();
        let db3 = db.clone();

        // Check strong count
        assert_eq!(Arc::strong_count(&db.inner), 3);

        // Drop one clone - should NOT sync (still 2 references)
        drop(db2);
        assert_eq!(Arc::strong_count(&db.inner), 2);

        // Drop another clone - should NOT sync (still 1 reference)
        drop(db3);
        assert_eq!(Arc::strong_count(&db.inner), 1);

        // Drop last reference - SHOULD sync
        drop(db);

        // Reopen and verify data persisted
        let db = DB::open(&temp_path).unwrap();
        assert_eq!(db.get("key1").unwrap().unwrap().as_ref(), b"value1");

        // Cleanup
        let _ = fs::remove_file(temp_path);
    }

    #[test]
    fn test_explicit_close_prevents_operations() {
        let mut db = DB::memory().unwrap();
        db.insert("key", b"value", None).unwrap();

        // Close the database
        db.close().unwrap();

        // Operations should fail
        assert!(db.insert("key2", b"value2", None).is_err());
        assert!(db.get("key").is_err());
        assert!(db.delete("key").is_err());
    }

    #[test]
    fn test_clone_shares_state() {
        let db = DB::memory().unwrap();
        let db2 = db.clone();

        db.insert("key1", b"value1", None).unwrap();
        db2.insert("key2", b"value2", None).unwrap();

        // Both clones see both keys
        assert_eq!(db.get("key1").unwrap().unwrap().as_ref(), b"value1");
        assert_eq!(db.get("key2").unwrap().unwrap().as_ref(), b"value2");
        assert_eq!(db2.get("key1").unwrap().unwrap().as_ref(), b"value1");
        assert_eq!(db2.get("key2").unwrap().unwrap().as_ref(), b"value2");
    }

    #[test]
    fn test_cleanup_expired_removes_keys() {
        let db = DB::memory().unwrap();
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
        let db = DB::memory().unwrap();
        db.insert("old", b"1", None).unwrap();
        sleep(Duration::from_millis(30));
        db.insert("recent", b"2", None).unwrap();

        let recent_keys = db.keys_created_since(Duration::from_millis(20)).unwrap();
        assert!(recent_keys.iter().any(|k| k.as_ref() == b"recent"));
        assert!(!recent_keys.iter().any(|k| k.as_ref() == b"old"));

        let old_key = Bytes::copy_from_slice(b"old");
        let new_key = Bytes::copy_from_slice(b"recent");
        let inner = db.read().unwrap();
        let old_created = inner.get_item(&old_key).unwrap().created_at;
        let new_created = inner.get_item(&new_key).unwrap().created_at;
        drop(inner);

        let between = db.keys_created_between(old_created, new_created).unwrap();
        assert!(between.iter().any(|k| k.as_ref() == b"old"));
        assert!(between.iter().any(|k| k.as_ref() == b"recent"));
    }

    #[cfg(feature = "time-index")]
    #[test]
    fn test_history_tracking_with_capacity() {
        let config = Config::default().with_history_capacity(2);
        let db = DB::open_with_config(":memory:", config).unwrap();

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
