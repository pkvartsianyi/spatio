//! Thread-safe wrapper for concurrent database access.
//!
//! This module provides `SyncDB`, a thread-safe wrapper around `DB` that uses
//! `Arc<RwLock<DB>>` internally to allow safe concurrent access from multiple threads.
//!
//! # Features
//!
//! Enable the `sync` feature to use this module:
//!
//! ```toml
//! [dependencies]
//! spatio = { version = "0.1", features = ["sync"] }
//! ```
//!
//! # Examples
//!
//! ```rust
//! use spatio::SyncDB;
//! use spatio::Point;
//! use std::thread;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! // Create a thread-safe database
//! let db = SyncDB::memory()?;
//!
//! // Clone for use in another thread
//! let db_clone = db.clone();
//!
//! // Spawn a thread that writes data
//! let handle = thread::spawn(move || {
//!     db_clone.insert("key", b"value", None).unwrap();
//! });
//!
//! // Read from the main thread
//! db.insert("another_key", b"another_value", None)?;
//!
//! handle.join().unwrap();
//! # Ok(())
//! # }
//! ```

use super::DB;
use crate::{
    AtomicBatch, BoundingBox2D, BoundingBox3D, Config, Point, Point3d, Polygon, Result, SetOptions,
    TemporalPoint, Trajectory,
};
use bytes::Bytes;
use parking_lot::RwLock;
use std::path::Path;
use std::sync::Arc;
use crate::compute::spatial::DistanceMetric;

/// Thread-safe wrapper around `DB` using `Arc<RwLock<DB>>`.
///
/// `SyncDB` provides concurrent access to the database by wrapping `DB` in a read-write lock.
/// Multiple threads can read simultaneously, but writes require exclusive access.
///
/// # Performance Considerations
///
/// - **Read-heavy workloads**: Good performance due to concurrent reads
/// - **Write-heavy workloads**: May experience contention; consider actor pattern instead
/// - **Mixed workloads**: Reasonable performance for moderate concurrency
///
/// # Thread Safety
///
/// - Implements `Clone` for easy sharing between threads
/// - All operations are thread-safe
/// - Read operations (`get`, queries) allow concurrent access
/// - Write operations (`insert`, `delete`, `atomic`) require exclusive access
#[derive(Clone)]
pub struct SyncDB {
    inner: Arc<RwLock<DB>>,
}

impl SyncDB {
    /// Creates a new in-memory database with default configuration.
    pub fn memory() -> Result<Self> {
        Ok(Self {
            inner: Arc::new(RwLock::new(DB::memory()?)),
        })
    }

    /// Creates a new in-memory database with custom configuration.
    pub fn memory_with_config(config: Config) -> Result<Self> {
        Ok(Self {
            inner: Arc::new(RwLock::new(DB::memory_with_config(config)?)),
        })
    }

    /// Opens a database with AOF persistence at the specified path.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        Ok(Self {
            inner: Arc::new(RwLock::new(DB::open(path)?)),
        })
    }

    /// Opens a database with AOF persistence and custom configuration.
    pub fn open_with_config<P: AsRef<Path>>(path: P, config: Config) -> Result<Self> {
        Ok(Self {
            inner: Arc::new(RwLock::new(DB::open_with_config(path, config)?)),
        })
    }

    // ===== Key-Value Operations =====

    /// Inserts a key-value pair into the database.
    pub fn insert(
        &self,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
        opts: Option<SetOptions>,
    ) -> Result<Option<Bytes>> {
        self.inner.write().insert(key, value, opts)
    }

    /// Retrieves a value by key.
    pub fn get(&self, key: impl AsRef<[u8]>) -> Result<Option<Bytes>> {
        self.inner.read().get(key)
    }

    /// Deletes a key-value pair from the database.
    pub fn delete(&self, key: impl AsRef<[u8]>) -> Result<Option<Bytes>> {
        self.inner.write().delete(key)
    }

    // ===== Spatial Operations (2D) =====

    /// Inserts a 2D point with associated data.
    pub fn insert_point(
        &self,
        prefix: &str,
        point: &Point,
        value: &[u8],
        opts: Option<SetOptions>,
    ) -> Result<()> {
        self.inner.write().insert_point(prefix, point, value, opts)
    }

    /// Queries points within a radius.
    pub fn query_within_radius(
        &self,
        prefix: &str,
        center: &Point,
        radius_meters: f64,
        max_results: usize,
    ) -> Result<Vec<(Point, Bytes)>> {
        self.inner
            .read()
            .query_within_radius(prefix, center, radius_meters, max_results)
    }

    /// Queries points within a bounding box.
    pub fn query_within_bbox(
        &self,
        prefix: &str,
        bbox: &BoundingBox2D,
        max_results: usize,
    ) -> Result<Vec<(Point, Bytes)>> {
        self.inner
            .read()
            .query_within_bbox(prefix, bbox, max_results)
    }

    /// Finds k nearest neighbors to a point.
    pub fn knn(
        &self,
        prefix: &str,
        point: &Point,
        k: usize,
        max_radius: f64,
        metric: DistanceMetric,
    ) -> Result<Vec<(Point, Bytes, f64)>> {
        self.inner.read().knn(prefix, point, k, max_radius, metric)
    }

    /// Checks if there are any points within a radius.
    pub fn contains_point(&self, prefix: &str, center: &Point, radius_meters: f64) -> Result<bool> {
        self.inner
            .read()
            .contains_point(prefix, center, radius_meters)
    }

    /// Counts points within a radius.
    pub fn count_within_radius(
        &self,
        prefix: &str,
        center: &Point,
        radius_meters: f64,
    ) -> Result<usize> {
        self.inner
            .read()
            .count_within_radius(prefix, center, radius_meters)
    }

    /// Finds points within a polygon.
    pub fn query_within_polygon(
        &self,
        prefix: &str,
        polygon: &Polygon,
        max_results: usize,
    ) -> Result<Vec<(Point, Bytes)>> {
        self.inner
            .read()
            .query_within_polygon(prefix, polygon, max_results)
    }

    /// Finds points within geographic bounds.
    pub fn find_within_bounds(
        &self,
        prefix: &str,
        min_lat: f64,
        min_lon: f64,
        max_lat: f64,
        max_lon: f64,
        max_results: usize,
    ) -> Result<Vec<(Point, Bytes)>> {
        self.inner.read().find_within_bounds(
            prefix,
            min_lat,
            min_lon,
            max_lat,
            max_lon,
            max_results,
        )
    }

    /// Calculates distance between two points.
    pub fn distance_between(&self, p1: &Point, p2: &Point, metric: DistanceMetric) -> Result<f64> {
        self.inner.read().distance_between(p1, p2, metric)
    }

    /// Checks if bounds intersect with any points.
    pub fn intersects_bounds(
        &self,
        prefix: &str,
        min_lat: f64,
        min_lon: f64,
        max_lat: f64,
        max_lon: f64,
    ) -> Result<bool> {
        self.inner
            .read()
            .intersects_bounds(prefix, min_lat, min_lon, max_lat, max_lon)
    }

    // ===== Bounding Box Operations =====

    /// Inserts a 2D bounding box.
    pub fn insert_bbox(
        &self,
        prefix: &str,
        bbox: &BoundingBox2D,
        opts: Option<SetOptions>,
    ) -> Result<()> {
        self.inner.write().insert_bbox(prefix, bbox, opts)
    }

    /// Retrieves a bounding box by key.
    pub fn get_bbox(&self, key: &str) -> Result<Option<BoundingBox2D>> {
        self.inner.read().get_bbox(key)
    }

    /// Finds bounding boxes that intersect with a given bounding box.
    pub fn find_intersecting_bboxes(
        &self,
        prefix: &str,
        query_bbox: &BoundingBox2D,
    ) -> Result<Vec<(String, BoundingBox2D)>> {
        self.inner
            .read()
            .find_intersecting_bboxes(prefix, query_bbox)
    }

    // ===== 3D Spatial Operations =====

    /// Inserts a 3D point.
    pub fn insert_point_3d(
        &self,
        prefix: &str,
        point: &Point3d,
        value: &[u8],
        opts: Option<SetOptions>,
    ) -> Result<()> {
        self.inner
            .write()
            .insert_point_3d(prefix, point, value, opts)
    }

    /// Queries points within a sphere.
    pub fn query_within_sphere_3d(
        &self,
        prefix: &str,
        center: &Point3d,
        radius_meters: f64,
        max_results: usize,
    ) -> Result<Vec<(Point3d, Bytes, f64)>> {
        self.inner
            .read()
            .query_within_sphere_3d(prefix, center, radius_meters, max_results)
    }

    /// Queries points within a 3D bounding box.
    pub fn query_within_bbox_3d(
        &self,
        prefix: &str,
        bbox: &BoundingBox3D,
        max_results: usize,
    ) -> Result<Vec<(Point3d, Bytes)>> {
        self.inner
            .read()
            .query_within_bbox_3d(prefix, bbox, max_results)
    }

    /// Queries points within a cylinder.
    pub fn query_within_cylinder_3d(
        &self,
        prefix: &str,
        center: &Point3d,
        radius_meters: f64,
        min_altitude: f64,
        max_altitude: f64,
        max_results: usize,
    ) -> Result<Vec<(Point3d, Bytes, f64)>> {
        self.inner.read().query_within_cylinder_3d(
            prefix,
            center,
            radius_meters,
            min_altitude,
            max_altitude,
            max_results,
        )
    }

    /// Finds k nearest neighbors in 3D space.
    pub fn knn_3d(
        &self,
        prefix: &str,
        point: &Point3d,
        k: usize,
    ) -> Result<Vec<(Point3d, Bytes, f64)>> {
        self.inner.read().knn_3d(prefix, point, k)
    }

    /// Calculates 3D distance between two points.
    pub fn distance_between_3d(&self, p1: &Point3d, p2: &Point3d) -> Result<f64> {
        self.inner.read().distance_between_3d(p1, p2)
    }

    // ===== Trajectory Operations =====

    /// Inserts a 2D trajectory.
    pub fn insert_trajectory(
        &self,
        object_id: &str,
        trajectory: &Trajectory,
        opts: Option<SetOptions>,
    ) -> Result<()> {
        self.inner
            .write()
            .insert_trajectory(object_id, trajectory, opts)
    }

    /// Queries a 2D trajectory.
    pub fn query_trajectory(
        &self,
        object_id: &str,
        start_time: u64,
        end_time: u64,
    ) -> Result<Vec<TemporalPoint>> {
        self.inner
            .read()
            .query_trajectory(object_id, start_time, end_time)
    }

    // ===== Atomic Operations =====

    /// Executes multiple operations atomically.
    pub fn atomic<F>(&self, f: F) -> Result<()>
    where
        F: FnOnce(&mut AtomicBatch) -> Result<()>,
    {
        self.inner.write().atomic(f)
    }

    // ===== Maintenance Operations =====

    /// Removes expired keys from the database.
    pub fn cleanup_expired(&self) -> Result<usize> {
        self.inner.write().cleanup_expired()
    }

    /// Forces a sync to disk (if AOF is enabled).
    pub fn sync(&self) -> Result<()> {
        self.inner.write().sync()
    }

    /// Closes the database.
    pub fn close(&self) -> Result<()> {
        self.inner.write().close()
    }

    /// Returns database statistics.
    pub fn stats(&self) -> crate::config::DbStats {
        self.inner.read().stats()
    }

    /// Returns the current configuration.
    pub fn config(&self) -> Config {
        self.inner.read().inner.config.clone()
    }

    /// Acquires a read lock for direct access to the database.
    ///
    /// This allows multiple read operations under a single lock.
    pub fn read(&self) -> parking_lot::RwLockReadGuard<'_, DB> {
        self.inner.read()
    }

    /// Acquires a write lock for direct access to the database.
    ///
    /// This allows multiple write operations under a single lock.
    pub fn write(&self) -> parking_lot::RwLockWriteGuard<'_, DB> {
        self.inner.write()
    }
}

// Ensure SyncDB is Send + Sync
const _: () = {
    const fn assert_send_sync<T: Send + Sync>() {}
    let _ = assert_send_sync::<SyncDB>;
};

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_basic_operations() {
        let db = SyncDB::memory().unwrap();
        db.insert("key", b"value", None).unwrap();
        let value = db.get("key").unwrap().unwrap();
        assert_eq!(value.as_ref(), b"value");
    }

    #[test]
    fn test_concurrent_reads() {
        let db = SyncDB::memory().unwrap();
        db.insert("key", b"value", None).unwrap();

        let handles: Vec<_> = (0..10)
            .map(|_| {
                let db = db.clone();
                thread::spawn(move || {
                    for _ in 0..100 {
                        let value = db.get("key").unwrap().unwrap();
                        assert_eq!(value.as_ref(), b"value");
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }
    }

    #[test]
    fn test_concurrent_writes() {
        let db = SyncDB::memory().unwrap();

        let handles: Vec<_> = (0..5)
            .map(|i| {
                let db = db.clone();
                thread::spawn(move || {
                    for j in 0..20 {
                        let key = format!("thread_{}_{}", i, j);
                        db.insert(&key, b"value", None).unwrap();
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        let stats = db.stats();
        assert_eq!(stats.key_count, 100);
    }

    #[test]
    fn test_concurrent_reads_and_writes() {
        let db = SyncDB::memory().unwrap();

        // Pre-populate
        for i in 0..50 {
            db.insert(format!("key_{}", i), b"value", None).unwrap();
        }

        let mut handles = vec![];

        // Spawn readers
        for _ in 0..5 {
            let db = db.clone();
            handles.push(thread::spawn(move || {
                for i in 0..50 {
                    let key = format!("key_{}", i);
                    let _ = db.get(&key);
                }
            }));
        }

        // Spawn writers
        for i in 0..3 {
            let db = db.clone();
            handles.push(thread::spawn(move || {
                for j in 0..20 {
                    let key = format!("writer_{}_{}", i, j);
                    db.insert(&key, b"value", None).unwrap();
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let stats = db.stats();
        assert!(stats.key_count >= 110);
    }

    #[test]
    fn test_spatial_operations() {
        let db = SyncDB::memory().unwrap();

        let handles: Vec<_> = (0..5)
            .map(|i| {
                let db = db.clone();
                thread::spawn(move || {
                    for j in 0..10 {
                        let lat = 40.0 + (i as f64 * 0.01);
                        let lon = -74.0 + (j as f64 * 0.01);
                        let point = Point::new(lon, lat);
                        db.insert_point(
                            "cities",
                            &point,
                            format!("city_{}_{}", i, j).as_bytes(),
                            None,
                        )
                        .unwrap();
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        let center = Point::new(-74.0, 40.0);
        let results = db
            .query_within_radius("cities", &center, 5000.0, 100)
            .unwrap();
        assert!(!results.is_empty());
    }

    #[test]
    fn test_atomic_operations() {
        let db = SyncDB::memory().unwrap();

        let handles: Vec<_> = (0..5)
            .map(|i| {
                let db = db.clone();
                thread::spawn(move || {
                    db.atomic(|batch| {
                        for j in 0..10 {
                            let key = format!("batch_{}_{}", i, j);
                            batch.insert(&key, b"value", None)?;
                        }
                        Ok(())
                    })
                    .unwrap();
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        let stats = db.stats();
        assert_eq!(stats.key_count, 50);
    }

    #[test]
    fn test_clone_shares_state() {
        let db = SyncDB::memory().unwrap();
        db.insert("key", b"value", None).unwrap();

        let db_clone = db.clone();
        let value = db_clone.get("key").unwrap().unwrap();
        assert_eq!(value.as_ref(), b"value");

        db_clone.insert("key2", b"value2", None).unwrap();
        let value2 = db.get("key2").unwrap().unwrap();
        assert_eq!(value2.as_ref(), b"value2");
    }

    #[test]
    fn test_close_prevents_operations() {
        let db = SyncDB::memory().unwrap();
        db.insert("key", b"value", None).unwrap();

        db.close().unwrap();

        let result = db.insert("key2", b"value2", None);
        assert!(result.is_err());
    }
}
