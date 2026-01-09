//! Core database implementation for Spatio.
//!
//! This module defines the main `DB` type along with spatio-temporal helpers and
//! persistence wiring that power the public `Spatio` API.

use crate::config::{Config, DbStats, SetOptions, TemporalPoint};
use crate::error::{Result, SpatioError};
use std::path::Path;

use std::time::SystemTime;

mod cold_state;
mod hot_state;
mod namespace;

#[cfg(feature = "sync")]
mod sync;

pub use cold_state::{ColdState, LocationUpdate};
pub use hot_state::{CurrentLocation, HotState};
pub use namespace::{Namespace, NamespaceManager};

#[cfg(feature = "sync")]
pub use sync::SyncDB;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

/// Embedded spatio-temporal database.
///
/// Optimized for tracking moving objects with hot/cold data separation.
/// - **Hot State**: Current locations in memory (DashMap)
/// - **Cold State**: Historical trajectories on disk (Append-only log)
///
/// Thread-safe by default (uses internal locking/lock-free structures).
#[derive(Clone)]
pub struct DB {
    pub(crate) hot: Arc<HotState>,
    pub(crate) cold: Arc<ColdState>,
    pub(crate) closed: Arc<AtomicBool>,
    pub(crate) ops_count: Arc<AtomicU64>,
    #[allow(dead_code)] // Will be used for snapshot checkpoints
    pub(crate) config: Config,
}

impl DB {
    /// Open or create a database at the given path. Use ":memory:" for in-memory storage.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::open_with_config(path, Config::default())
    }

    pub fn builder() -> crate::builder::DBBuilder {
        crate::builder::DBBuilder::new()
    }

    /// Open or create a database with custom configuration.
    pub fn open_with_config<P: AsRef<Path>>(path: P, config: Config) -> Result<Self> {
        let path_ref = path.as_ref();
        let hot = Arc::new(HotState::new());

        let cold = if path_ref.to_str() == Some(":memory:") {
            let temp_dir =
                std::env::temp_dir().join(format!("spatio_mem_{}", uuid::Uuid::new_v4()));
            Arc::new(ColdState::new(
                &temp_dir.join("traj.log"),
                config.buffer_capacity,
            )?)
        } else {
            Arc::new(ColdState::new(path_ref, config.buffer_capacity)?)
        };

        // Recover current locations from cold storage (skip for :memory: mode)
        if path_ref.to_str() != Some(":memory:") {
            match cold.recover_current_locations() {
                Ok(recovered) => {
                    for (key, update) in recovered {
                        // Parse namespace and object_id from key "namespace::object_id"
                        if let Some(separator_idx) = key.find("::") {
                            let namespace = &key[..separator_idx];
                            let object_id = &key[separator_idx + 2..];

                            // Update hot state with recovered location
                            if let Err(e) = hot.update_location(
                                namespace,
                                object_id,
                                update.position,
                                update.metadata,
                                update.timestamp,
                            ) {
                                log::warn!("Failed to recover location for {}: {}", key, e);
                            }
                        }
                    }
                }
                Err(e) => {
                    log::warn!("Failed to recover current locations: {}", e);
                    // Continue anyway - partial recovery is acceptable
                }
            }
        }

        Ok(Self {
            hot,
            cold,
            closed: Arc::new(AtomicBool::new(false)),
            ops_count: Arc::new(AtomicU64::new(0)),
            config,
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

    /// Upsert an object's location.
    pub fn upsert(
        &self,
        namespace: &str,
        object_id: &str,
        position: spatio_types::point::Point3d,
        metadata: serde_json::Value,
        opts: Option<SetOptions>,
    ) -> Result<()> {
        if self.closed.load(Ordering::Acquire) {
            return Err(SpatioError::DatabaseClosed);
        }

        let ts = opts
            .as_ref()
            .and_then(|o| o.timestamp)
            .unwrap_or_else(SystemTime::now);

        // 1. Update hot state (replaces old position)
        self.hot
            .update_location(namespace, object_id, position.clone(), metadata.clone(), ts)?;

        // 2. Append to cold state
        self.cold
            .append_update(namespace, object_id, position, metadata, ts)?;

        self.ops_count.fetch_add(1, Ordering::Relaxed);

        Ok(())
    }

    /// Get current location of an object.
    pub fn get(&self, namespace: &str, object_id: &str) -> Result<Option<CurrentLocation>> {
        if self.closed.load(Ordering::Acquire) {
            return Err(SpatioError::DatabaseClosed);
        }
        Ok(self.hot.get_current_location(namespace, object_id))
    }

    /// Delete an object from the database.
    pub fn delete(&self, namespace: &str, object_id: &str) -> Result<()> {
        if self.closed.load(Ordering::Acquire) {
            return Err(SpatioError::DatabaseClosed);
        }
        self.hot.remove_object(namespace, object_id);
        Ok(())
    }

    /// Insert a trajectory (sequence of points)
    pub fn insert_trajectory(
        &self,
        namespace: &str,
        object_id: &str,
        trajectory: &[TemporalPoint],
    ) -> Result<()> {
        for tp in trajectory {
            let pos = spatio_types::point::Point3d::new(tp.point.x(), tp.point.y(), 0.0);
            self.upsert(
                namespace,
                object_id,
                pos,
                serde_json::json!({}),
                Some(SetOptions {
                    timestamp: Some(tp.timestamp),
                    ..Default::default()
                }),
            )?;
        }
        Ok(())
    }

    /// Query objects within radius, always returning (Location, distance).
    pub fn query_radius(
        &self,
        namespace: &str,
        center: &spatio_types::point::Point3d,
        radius: f64,
        limit: usize,
    ) -> Result<Vec<(CurrentLocation, f64)>> {
        if self.closed.load(Ordering::Acquire) {
            return Err(SpatioError::DatabaseClosed);
        }
        Ok(self
            .hot
            .query_within_radius(namespace, center, radius, limit))
    }

    /// Query current locations within a 2D bounding box (HOT PATH)
    pub fn query_bbox(
        &self,
        namespace: &str,
        min_x: f64,
        min_y: f64,
        max_x: f64,
        max_y: f64,
        limit: usize,
    ) -> Result<Vec<CurrentLocation>> {
        if self.closed.load(Ordering::Acquire) {
            return Err(SpatioError::DatabaseClosed);
        }
        Ok(self
            .hot
            .query_within_bbox(namespace, min_x, min_y, max_x, max_y, limit))
    }

    /// Query objects within a cylindrical volume (HOT PATH)
    pub fn query_within_cylinder(
        &self,
        namespace: &str,
        center: spatio_types::geo::Point,
        min_z: f64,
        max_z: f64,
        radius: f64,
        limit: usize,
    ) -> Result<Vec<(CurrentLocation, f64)>> {
        if self.closed.load(Ordering::Acquire) {
            return Err(SpatioError::DatabaseClosed);
        }
        Ok(self
            .hot
            .query_within_cylinder(namespace, center, min_z, max_z, radius, limit))
    }

    /// Find k nearest neighbors in 3D (HOT PATH)
    pub fn knn(
        &self,
        namespace: &str,
        center: &spatio_types::point::Point3d,
        k: usize,
    ) -> Result<Vec<(CurrentLocation, f64)>> {
        if self.closed.load(Ordering::Acquire) {
            return Err(SpatioError::DatabaseClosed);
        }
        Ok(self.hot.knn_3d(namespace, center, k))
    }

    /// Query objects within a 3D bounding box (HOT PATH)
    #[allow(clippy::too_many_arguments)]
    pub fn query_within_bbox_3d(
        &self,
        namespace: &str,
        min_x: f64,
        min_y: f64,
        min_z: f64,
        max_x: f64,
        max_y: f64,
        max_z: f64,
        limit: usize,
    ) -> Result<Vec<CurrentLocation>> {
        if self.closed.load(Ordering::Acquire) {
            return Err(SpatioError::DatabaseClosed);
        }
        Ok(self
            .hot
            .query_within_bbox_3d(namespace, min_x, min_y, min_z, max_x, max_y, max_z, limit))
    }

    /// Query objects near another object (by key). returns (Location, distance).
    pub fn query_near(
        &self,
        namespace: &str,
        object_id: &str,
        radius: f64,
        limit: usize,
    ) -> Result<Vec<(CurrentLocation, f64)>> {
        if self.closed.load(Ordering::Acquire) {
            return Err(SpatioError::DatabaseClosed);
        }

        // 1. Get target object's current position
        let target = self
            .hot
            .get_current_location(namespace, object_id)
            .ok_or(SpatioError::ObjectNotFound)?;

        // 2. Query around that position
        self.query_radius(namespace, &target.position, radius, limit)
    }

    /// Query objects within a bounding box relative to another object
    pub fn query_bbox_near_object(
        &self,
        namespace: &str,
        object_id: &str,
        width: f64,
        height: f64,
        limit: usize,
    ) -> Result<Vec<CurrentLocation>> {
        if self.closed.load(Ordering::Acquire) {
            return Err(SpatioError::DatabaseClosed);
        }

        let target = self
            .hot
            .get_current_location(namespace, object_id)
            .ok_or(SpatioError::ObjectNotFound)?;

        let half_width = width / 2.0;
        let half_height = height / 2.0;
        let center = &target.position;

        self.query_bbox(
            namespace,
            center.x() - half_width,
            center.y() - half_height,
            center.x() + half_width,
            center.y() + half_height,
            limit,
        )
    }

    /// Query objects within a cylindrical volume relative to another object
    pub fn query_cylinder_near_object(
        &self,
        namespace: &str,
        object_id: &str,
        min_z: f64,
        max_z: f64,
        radius: f64,
        limit: usize,
    ) -> Result<Vec<(CurrentLocation, f64)>> {
        if self.closed.load(Ordering::Acquire) {
            return Err(SpatioError::DatabaseClosed);
        }

        let target = self
            .hot
            .get_current_location(namespace, object_id)
            .ok_or(SpatioError::ObjectNotFound)?;

        let center = spatio_types::geo::Point::new(target.position.x(), target.position.y());

        self.query_within_cylinder(namespace, center, min_z, max_z, radius, limit)
    }

    /// Query objects within a 3D bounding box relative to another object
    pub fn query_bbox_3d_near_object(
        &self,
        namespace: &str,
        object_id: &str,
        width: f64,
        height: f64,
        depth: f64,
        limit: usize,
    ) -> Result<Vec<CurrentLocation>> {
        if self.closed.load(Ordering::Acquire) {
            return Err(SpatioError::DatabaseClosed);
        }

        let target = self
            .hot
            .get_current_location(namespace, object_id)
            .ok_or(SpatioError::ObjectNotFound)?;

        let half_width = width / 2.0;
        let half_height = height / 2.0;
        let half_depth = depth / 2.0;
        let center = &target.position;

        self.query_within_bbox_3d(
            namespace,
            center.x() - half_width,
            center.y() - half_height,
            center.z() - half_depth,
            center.x() + half_width,
            center.y() + half_height,
            center.z() + half_depth,
            limit,
        )
    }

    /// Find k nearest neighbors relative to another object
    pub fn knn_near_object(
        &self,
        namespace: &str,
        object_id: &str,
        k: usize,
    ) -> Result<Vec<(CurrentLocation, f64)>> {
        if self.closed.load(Ordering::Acquire) {
            return Err(SpatioError::DatabaseClosed);
        }

        let target = self
            .hot
            .get_current_location(namespace, object_id)
            .ok_or(SpatioError::ObjectNotFound)?;

        self.knn(namespace, &target.position, k)
    }

    /// Query historical trajectory (COLD PATH)
    pub fn query_trajectory(
        &self,
        namespace: &str,
        object_id: &str,
        start_time: SystemTime,
        end_time: SystemTime,
        limit: usize,
    ) -> Result<Vec<LocationUpdate>> {
        if self.closed.load(Ordering::Acquire) {
            return Err(SpatioError::DatabaseClosed);
        }
        self.cold
            .query_trajectory(namespace, object_id, start_time, end_time, limit)
    }

    /// Close the database
    pub fn close(&self) -> Result<()> {
        self.closed.store(true, Ordering::Release);
        Ok(())
    }

    /// Get database statistics
    pub fn stats(&self) -> DbStats {
        let (hot_objects, hot_memory) = self.hot.detailed_stats();
        let (cold_trajectories, cold_buffer_bytes) = self.cold.stats();

        DbStats {
            expired_count: 0, // Placeholder as we don't track expired cleanup count globally yet
            operations_count: self.ops_count.load(Ordering::Relaxed),
            size_bytes: hot_memory + cold_buffer_bytes,
            hot_state_objects: hot_objects,
            cold_state_trajectories: cold_trajectories,
            cold_state_buffer_bytes: cold_buffer_bytes,
            memory_usage_bytes: hot_memory + cold_buffer_bytes,
        }
    }
    /// Query objects within a polygon
    pub fn query_polygon(
        &self,
        namespace: &str,
        polygon: &spatio_types::geo::Polygon,
        limit: usize,
    ) -> Result<Vec<CurrentLocation>> {
        if self.closed.load(Ordering::Acquire) {
            return Err(SpatioError::DatabaseClosed);
        }
        Ok(self.hot.query_polygon(namespace, polygon, limit))
    }

    /// Calculate distance between two objects
    pub fn distance_between(
        &self,
        namespace: &str,
        id1: &str,
        id2: &str,
        metric: crate::compute::spatial::DistanceMetric,
    ) -> Result<Option<f64>> {
        if self.closed.load(Ordering::Acquire) {
            return Err(SpatioError::DatabaseClosed);
        }
        Ok(self.hot.distance_between(namespace, id1, id2, metric))
    }

    /// Calculate distance from object to point
    pub fn distance_to(
        &self,
        namespace: &str,
        id: &str,
        point: &spatio_types::geo::Point,
        metric: crate::compute::spatial::DistanceMetric,
    ) -> Result<Option<f64>> {
        if self.closed.load(Ordering::Acquire) {
            return Err(SpatioError::DatabaseClosed);
        }
        Ok(self.hot.distance_to(namespace, id, point, metric))
    }

    /// Compute convex hull of all objects in namespace
    pub fn convex_hull(&self, namespace: &str) -> Result<Option<spatio_types::geo::Polygon>> {
        if self.closed.load(Ordering::Acquire) {
            return Err(SpatioError::DatabaseClosed);
        }
        Ok(self.hot.convex_hull(namespace))
    }

    /// Compute bounding box of all objects in namespace
    pub fn bounding_box(&self, namespace: &str) -> Result<Option<geo::Rect>> {
        if self.closed.load(Ordering::Acquire) {
            return Err(SpatioError::DatabaseClosed);
        }
        Ok(self.hot.bounding_box(namespace))
    }
}

pub use DB as Spatio;

#[cfg(test)]
mod tests {
    use super::*;
    use spatio_types::point::Point3d;
    use std::thread::sleep;
    use std::time::Duration;

    #[test]
    fn test_update_and_query_location() {
        let db = DB::memory().unwrap();
        let namespace = "vehicles";
        let object_id = "car1";
        let pos1 = Point3d::new(10.0, 20.0, 0.0);
        let metadata1 = serde_json::json!({"engine": "on"});

        db.upsert(namespace, object_id, pos1.clone(), metadata1.clone(), None)
            .unwrap();

        let results = db.query_radius(namespace, &pos1, 1.0, 1).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0.object_id, object_id);
        assert_eq!(results[0].0.position, pos1);
        assert_eq!(results[0].0.metadata, metadata1);

        let pos2 = Point3d::new(10.1, 20.1, 0.0);
        let metadata2 = serde_json::json!({"engine": "off"});
        db.upsert(namespace, object_id, pos2.clone(), metadata2.clone(), None)
            .unwrap();

        let results = db.query_radius(namespace, &pos2, 1.0, 1).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0.object_id, object_id);
        assert_eq!(results[0].0.position, pos2);
        assert_eq!(results[0].0.metadata, metadata2);
    }

    #[test]
    fn test_query_near_object() {
        let db = DB::memory().unwrap();
        let namespace = "vehicles";

        let car1_pos = Point3d::new(0.0, 0.0, 0.0);
        db.upsert(namespace, "car1", car1_pos, serde_json::json!({}), None)
            .unwrap();

        let car2_pos = Point3d::new(0.00001, 0.0, 0.0); // ~1 meter away
        db.upsert(namespace, "car2", car2_pos, serde_json::json!({}), None)
            .unwrap();

        let car3_pos = Point3d::new(10.0, 0.0, 0.0); // 10 units away
        db.upsert(namespace, "car3", car3_pos, serde_json::json!({}), None)
            .unwrap();

        let near_car1 = db.query_near(namespace, "car1", 1.5, 10).unwrap();
        assert_eq!(near_car1.len(), 2); // car1 and car2
        assert!(near_car1.iter().any(|(loc, _)| loc.object_id == "car1"));
        assert!(near_car1.iter().any(|(loc, _)| loc.object_id == "car2"));
        assert!(!near_car1.iter().any(|(loc, _)| loc.object_id == "car3"));

        let near_car1_limit_1 = db.query_near(namespace, "car1", 1.5, 1).unwrap();
        assert_eq!(near_car1_limit_1.len(), 1);
    }

    #[test]
    fn test_query_trajectory() {
        let db = DB::memory().unwrap();
        let namespace = "planes";
        let object_id = "plane1";

        let start_time = SystemTime::now();
        sleep(Duration::from_millis(10));
        db.upsert(
            namespace,
            object_id,
            Point3d::new(0.0, 0.0, 0.0),
            serde_json::json!({"status": "takeoff"}),
            None,
        )
        .unwrap();
        sleep(Duration::from_millis(10));
        db.upsert(
            namespace,
            object_id,
            Point3d::new(10.0, 10.0, 1000.0),
            serde_json::json!({"status": "climb"}),
            None,
        )
        .unwrap();
        sleep(Duration::from_millis(10));
        db.upsert(
            namespace,
            object_id,
            Point3d::new(20.0, 20.0, 2000.0),
            serde_json::json!({"status": "cruise"}),
            None,
        )
        .unwrap();
        sleep(Duration::from_millis(10));
        let end_time = SystemTime::now();

        let trajectory = db
            .query_trajectory(namespace, object_id, start_time, end_time, 10)
            .unwrap();
        assert_eq!(trajectory.len(), 3);
        // Results are newest first
        assert_eq!(trajectory[0].position, Point3d::new(20.0, 20.0, 2000.0));
        assert_eq!(trajectory[1].position, Point3d::new(10.0, 10.0, 1000.0));
        assert_eq!(trajectory[2].position, Point3d::new(0.0, 0.0, 0.0));

        // Test limit
        let limited_trajectory = db
            .query_trajectory(namespace, object_id, start_time, end_time, 2)
            .unwrap();
        assert_eq!(limited_trajectory.len(), 2);
    }

    #[test]
    fn test_database_closed_operations() {
        let db = DB::memory().unwrap();
        db.close().unwrap();

        let namespace = "test";
        let object_id = "obj1";
        let pos = Point3d::new(0.0, 0.0, 0.0);
        let metadata = serde_json::json!({"data": "data"});

        assert!(
            db.upsert(namespace, object_id, pos.clone(), metadata, None)
                .is_err()
        );
        assert!(db.query_radius(namespace, &pos, 1.0, 1).is_err());
        assert!(db.query_near(namespace, object_id, 1.0, 1).is_err());
        assert!(
            db.query_trajectory(
                namespace,
                object_id,
                SystemTime::UNIX_EPOCH,
                SystemTime::now(),
                1
            )
            .is_err()
        );
    }
}
