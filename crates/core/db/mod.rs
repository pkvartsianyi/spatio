//! Core database implementation for Spatio.
//!
//! This module defines the main `DB` type along with spatio-temporal helpers and
//! persistence wiring that power the public `Spatio` API.

use crate::config::{Config, DbStats, TemporalPoint};
use crate::error::{Result, SpatioError};
use bytes::Bytes;
use std::path::Path;

use std::time::SystemTime;

mod batch;
mod cold_state;
mod hot_state;
mod internal;
mod namespace;

#[cfg(feature = "sync")]
mod sync;

pub use batch::AtomicBatch;
pub use cold_state::{ColdState, LocationUpdate};
pub use hot_state::{CurrentLocation, HotState};
pub use namespace::{Namespace, NamespaceManager};

#[cfg(feature = "sync")]
pub use sync::SyncDB;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

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
}

impl DB {
    /// Open or create a database at the given path. Use ":memory:" for in-memory storage.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::open_with_config(path, Config::default())
    }

    pub fn builder() -> crate::DBBuilder {
        crate::DBBuilder::new()
    }

    /// Open or create a database with custom configuration.
    pub fn open_with_config<P: AsRef<Path>>(path: P, _config: Config) -> Result<Self> {
        let path = path.as_ref();
        let hot = Arc::new(HotState::new());

        let cold = if path.to_str() == Some(":memory:") {
            let temp_dir =
                std::env::temp_dir().join(format!("spatio_mem_{}", uuid::Uuid::new_v4()));
            Arc::new(ColdState::new(&temp_dir.join("traj.log"), 100)?)
        } else {
            Arc::new(ColdState::new(path, 100)?)
        };

        Ok(Self {
            hot,
            cold,
            closed: Arc::new(AtomicBool::new(false)),
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

    /// Update object's current location
    pub fn update_location(
        &self,
        namespace: &str,
        object_id: &str,
        position: spatio_types::point::Point3d,
        metadata: impl AsRef<[u8]>,
    ) -> Result<()> {
        if self.closed.load(Ordering::Acquire) {
            return Err(SpatioError::DatabaseClosed);
        }

        let metadata_bytes = Bytes::copy_from_slice(metadata.as_ref());
        let timestamp = SystemTime::now();

        // 1. Update hot state (replaces old position)
        self.hot.update_location(
            namespace,
            object_id,
            position.clone(),
            metadata_bytes.clone(),
            timestamp,
        )?;

        // 2. Append to cold state
        self.cold
            .append_update(namespace, object_id, position, metadata_bytes, timestamp)?;

        Ok(())
    }

    /// Update an object's location with a specific timestamp (useful for backfilling)
    pub fn update_location_at(
        &self,
        namespace: &str,
        object_id: &str,
        position: spatio_types::point::Point3d,
        metadata: impl AsRef<[u8]>,
        timestamp: SystemTime,
    ) -> Result<()> {
        if self.closed.load(Ordering::Acquire) {
            return Err(SpatioError::DatabaseClosed);
        }

        let meta_bytes = Bytes::copy_from_slice(metadata.as_ref());

        // 1. Update Hot State (Current Location)
        // We only update hot state if this is a recent update (e.g. within last minute)
        // or if it's newer than what we have.
        // For simplicity in this version, we always update hot state if it's "current" enough.
        // But really, hot state should reflect the *latest* known position.
        // TODO: Check timestamp against current hot state timestamp

        self.hot.update_location(
            namespace,
            object_id,
            position.clone(),
            meta_bytes.clone(),
            timestamp,
        )?;

        // 2. Append to Cold State (History)
        self.cold
            .append_update(namespace, object_id, position, meta_bytes, timestamp)?;

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
            self.update_location_at(namespace, object_id, pos, [], tp.timestamp)?;
        }
        Ok(())
    }

    /// Query current locations within radius (HOT PATH)
    pub fn query_current_within_radius(
        &self,
        namespace: &str,
        center: &spatio_types::point::Point3d,
        radius: f64,
        limit: usize,
    ) -> Result<Vec<CurrentLocation>> {
        if self.closed.load(Ordering::Acquire) {
            return Err(SpatioError::DatabaseClosed);
        }
        Ok(self
            .hot
            .query_within_radius(namespace, center, radius, limit))
    }

    /// Query current locations within a 2D bounding box (HOT PATH)
    pub fn query_current_within_bbox(
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
    pub fn knn_3d(
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

    /// Query objects near another object (relative query)
    pub fn query_near_object(
        &self,
        namespace: &str,
        object_id: &str,
        radius: f64,
        limit: usize,
    ) -> Result<Vec<CurrentLocation>> {
        if self.closed.load(Ordering::Acquire) {
            return Err(SpatioError::DatabaseClosed);
        }

        // 1. Get target object's current position
        let target = self
            .hot
            .get_current_location(namespace, object_id)
            .ok_or(SpatioError::ObjectNotFound)?;

        // 2. Query around that position
        self.query_current_within_radius(namespace, &target.position, radius, limit)
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
        // TODO: Implement stats for Hot/Cold architecture
        DbStats::default()
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
        let metadata1 = b"engine_on";

        db.update_location(namespace, object_id, pos1.clone(), metadata1)
            .unwrap();

        let results = db
            .query_current_within_radius(namespace, &pos1, 1.0, 1)
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].object_id, object_id);
        assert_eq!(results[0].position, pos1);
        assert_eq!(results[0].metadata.as_ref(), metadata1);

        let pos2 = Point3d::new(10.1, 20.1, 0.0);
        let metadata2 = b"engine_off";
        db.update_location(namespace, object_id, pos2.clone(), metadata2)
            .unwrap();

        let results = db
            .query_current_within_radius(namespace, &pos2, 1.0, 1)
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].object_id, object_id);
        assert_eq!(results[0].position, pos2); // Should be updated to pos2
        assert_eq!(results[0].metadata.as_ref(), metadata2);
    }

    #[test]
    fn test_query_near_object() {
        let db = DB::memory().unwrap();
        let namespace = "vehicles";

        let car1_pos = Point3d::new(0.0, 0.0, 0.0);
        db.update_location(namespace, "car1", car1_pos, b"")
            .unwrap();

        let car2_pos = Point3d::new(0.00001, 0.0, 0.0); // ~1 meter away
        db.update_location(namespace, "car2", car2_pos, b"")
            .unwrap();

        let car3_pos = Point3d::new(10.0, 0.0, 0.0); // 10 units away
        db.update_location(namespace, "car3", car3_pos, b"")
            .unwrap();

        let near_car1 = db.query_near_object(namespace, "car1", 1.5, 10).unwrap();
        assert_eq!(near_car1.len(), 2); // car1 and car2
        assert!(near_car1.iter().any(|loc| loc.object_id == "car1"));
        assert!(near_car1.iter().any(|loc| loc.object_id == "car2"));
        assert!(!near_car1.iter().any(|loc| loc.object_id == "car3"));

        let near_car1_limit_1 = db.query_near_object(namespace, "car1", 1.5, 1).unwrap();
        assert_eq!(near_car1_limit_1.len(), 1);
    }

    #[test]
    fn test_query_trajectory() {
        let db = DB::memory().unwrap();
        let namespace = "planes";
        let object_id = "plane1";

        let start_time = SystemTime::now();
        sleep(Duration::from_millis(10));
        db.update_location(
            namespace,
            object_id,
            Point3d::new(0.0, 0.0, 0.0),
            b"takeoff",
        )
        .unwrap();
        sleep(Duration::from_millis(10));
        db.update_location(
            namespace,
            object_id,
            Point3d::new(10.0, 10.0, 1000.0),
            b"climb",
        )
        .unwrap();
        sleep(Duration::from_millis(10));
        db.update_location(
            namespace,
            object_id,
            Point3d::new(20.0, 20.0, 2000.0),
            b"cruise",
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
        let metadata = b"data";

        assert!(
            db.update_location(namespace, object_id, pos.clone(), metadata)
                .is_err()
        );
        assert!(
            db.query_current_within_radius(namespace, &pos, 1.0, 1)
                .is_err()
        );
        assert!(db.query_near_object(namespace, object_id, 1.0, 1).is_err());
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
