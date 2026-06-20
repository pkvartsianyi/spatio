//! Core database implementation for Spatio.
//!
//! This module defines the main `DB` type along with spatio-temporal helpers and
//! persistence wiring that power the public `Spatio` API.

use crate::compute::validation;
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

/// Reject namespace/object_id values that would corrupt the append-only log or
/// alias the `namespace::object_id` composite key.
///
/// The log is a pipe-delimited, newline-terminated text format and the in-memory
/// keys are joined with `::`, so `|`, CR/LF, and `::` are structurally unsafe and
/// must be rejected at the write boundary rather than silently mangled.
fn validate_identifier(kind: &str, value: &str) -> Result<()> {
    if value.is_empty() {
        return Err(SpatioError::InvalidInput(format!(
            "{kind} must not be empty"
        )));
    }
    if value.contains('|') || value.contains('\n') || value.contains('\r') || value.contains("::") {
        return Err(SpatioError::InvalidInput(format!(
            "{kind} must not contain '|', a newline, or '::' (got {value:?})"
        )));
    }
    Ok(())
}

/// Embedded spatio-temporal database.
///
/// Optimized for tracking moving objects with hot/cold data separation.
/// - **Hot State**: Current locations in memory (DashMap)
/// - **Cold State**: Historical trajectories on disk (Append-only log).
///   `:memory:` databases keep this log in memory and never touch the filesystem.
///
/// Thread-safe by default (uses internal locking/lock-free structures).
#[derive(Clone)]
pub struct DB {
    pub(crate) hot: Arc<HotState>,
    pub(crate) cold: Arc<ColdState>,
    pub(crate) closed: Arc<AtomicBool>,
    pub(crate) ops_count: Arc<AtomicU64>,
    #[allow(dead_code)] // retained for configuration introspection
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

        let sync = cold_state::SyncSettings {
            policy: config.sync_policy,
            mode: config.sync_mode,
            batch_size: config.sync_batch_size,
        };

        let cold = if path_ref.to_str() == Some(":memory:") {
            // Pure in-memory: no temp dir, no file, no serialization on writes.
            Arc::new(ColdState::new_memory(config.buffer_capacity))
        } else {
            Arc::new(ColdState::new(
                path_ref,
                config.buffer_capacity,
                config.persistence.clone(),
                sync,
            )?)
        };

        // Recover current locations from cold storage (skip for :memory: mode)
        if path_ref.to_str() != Some(":memory:") {
            match cold.recover_current_locations() {
                Ok(recovered) => {
                    // Persist a fresh checkpoint covering everything recovered so
                    // the next startup replays only newly appended records. The
                    // full history log is left intact. Best-effort: a failure here
                    // only means the next recovery is slower, not incorrect.
                    if let Err(e) = cold.write_checkpoint(&recovered) {
                        log::warn!("Failed to write recovery checkpoint: {}", e);
                    }

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
        validate_identifier("namespace", namespace)?;
        validate_identifier("object_id", object_id)?;
        // Reject NaN/Inf/out-of-range coordinates before they poison the index.
        validation::validate_geographic_point_3d(&position)?;

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
    pub fn get(&self, namespace: &str, object_id: &str) -> Result<Option<Arc<CurrentLocation>>> {
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
        validate_identifier("namespace", namespace)?;
        validate_identifier("object_id", object_id)?;
        self.cold.append_tombstone(namespace, object_id)?;
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
    ) -> Result<Vec<(Arc<CurrentLocation>, f64)>> {
        if self.closed.load(Ordering::Acquire) {
            return Err(SpatioError::DatabaseClosed);
        }
        validation::validate_geographic_point_3d(center)?;
        validation::validate_radius(radius)?;
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
    ) -> Result<Vec<Arc<CurrentLocation>>> {
        if self.closed.load(Ordering::Acquire) {
            return Err(SpatioError::DatabaseClosed);
        }
        validation::validate_bbox(min_x, min_y, max_x, max_y)?;
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
    ) -> Result<Vec<(Arc<CurrentLocation>, f64)>> {
        if self.closed.load(Ordering::Acquire) {
            return Err(SpatioError::DatabaseClosed);
        }
        validation::validate_geographic_point(&center)?;
        validation::validate_radius(radius)?;
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
    ) -> Result<Vec<(Arc<CurrentLocation>, f64)>> {
        if self.closed.load(Ordering::Acquire) {
            return Err(SpatioError::DatabaseClosed);
        }
        validation::validate_geographic_point_3d(center)?;
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
    ) -> Result<Vec<Arc<CurrentLocation>>> {
        if self.closed.load(Ordering::Acquire) {
            return Err(SpatioError::DatabaseClosed);
        }
        validation::validate_bbox_3d(min_x, min_y, min_z, max_x, max_y, max_z)?;
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
    ) -> Result<Vec<(Arc<CurrentLocation>, f64)>> {
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
    ) -> Result<Vec<Arc<CurrentLocation>>> {
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
    ) -> Result<Vec<(Arc<CurrentLocation>, f64)>> {
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
    ) -> Result<Vec<Arc<CurrentLocation>>> {
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
    ) -> Result<Vec<(Arc<CurrentLocation>, f64)>> {
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

    /// Close the database, flushing and syncing any buffered writes to disk.
    pub fn close(&self) -> Result<()> {
        self.closed.store(true, Ordering::Release);
        self.cold.flush()
    }

    /// Get database statistics
    pub fn stats(&self) -> DbStats {
        let (hot_objects, hot_memory) = self.hot.detailed_stats();
        let (cold_trajectories, cold_buffer_bytes) = self.cold.stats();

        DbStats {
            expired_count: 0, // TTL/expiry is not implemented; always zero
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
    ) -> Result<Vec<Arc<CurrentLocation>>> {
        if self.closed.load(Ordering::Acquire) {
            return Err(SpatioError::DatabaseClosed);
        }
        validation::validate_polygon(polygon)?;
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
    fn test_delete_does_not_survive_restart() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");

        // First session: insert then delete.
        {
            let db = DB::open(&db_path).unwrap();
            db.upsert(
                "ns",
                "obj",
                Point3d::new(1.0, 2.0, 0.0),
                serde_json::json!({}),
                None,
            )
            .unwrap();
            db.delete("ns", "obj").unwrap();
            db.close().unwrap();
        }

        // Second session: object must not reappear.
        {
            let db = DB::open(&db_path).unwrap();
            assert!(
                db.get("ns", "obj").unwrap().is_none(),
                "deleted object must not reappear after restart"
            );
        }
    }

    #[test]
    fn test_delete_then_reinsert_survives_restart() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test2.db");

        let pos2 = Point3d::new(9.0, 8.0, 0.0);

        {
            let db = DB::open(&db_path).unwrap();
            db.upsert(
                "ns",
                "obj",
                Point3d::new(1.0, 2.0, 0.0),
                serde_json::json!({}),
                None,
            )
            .unwrap();
            db.delete("ns", "obj").unwrap();
            sleep(Duration::from_millis(1)); // ensure re-insert timestamp > tombstone
            db.upsert("ns", "obj", pos2.clone(), serde_json::json!({"v": 2}), None)
                .unwrap();
            db.close().unwrap();
        }

        {
            let db = DB::open(&db_path).unwrap();
            let loc = db
                .get("ns", "obj")
                .unwrap()
                .expect("re-inserted object must survive restart");
            assert_eq!(loc.position, pos2);
        }
    }

    #[test]
    fn test_memory_db_serves_trajectory_history_in_memory() {
        // A :memory: DB must not touch the filesystem yet still answer
        // trajectory queries (history kept in the in-memory log) beyond the
        // recent buffer window.
        let db = DB::memory().unwrap();

        let t0 = SystemTime::now();
        for i in 0..5u64 {
            db.upsert(
                "ns",
                "obj",
                Point3d::new(i as f64, i as f64, 0.0),
                serde_json::json!({ "i": i }),
                Some(SetOptions {
                    timestamp: Some(t0 + Duration::from_millis(i)),
                }),
            )
            .unwrap();
        }

        // Current position reflects the latest update.
        let current = db.get("ns", "obj").unwrap().unwrap();
        assert_eq!(current.position.x(), 4.0);

        // Full trajectory is queryable from the in-memory log. Use a window that
        // safely brackets all records: stored timestamps are truncated to micros,
        // so a raw-now() lower bound could exclude the boundary record.
        let traj = db
            .query_trajectory(
                "ns",
                "obj",
                t0 - Duration::from_secs(1),
                t0 + Duration::from_secs(1),
                10,
            )
            .unwrap();
        assert_eq!(traj.len(), 5, "all in-memory history must be queryable");
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

    #[test]
    fn test_metadata_with_pipe_survives_reopen() {
        // A '|' inside metadata must not corrupt the log record: the value has
        // to survive a full close/reopen recovery cycle on a file-backed DB.
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("pipe.db");

        {
            let db = DB::open(&db_path).unwrap();
            db.upsert(
                "ns",
                "obj",
                Point3d::new(1.0, 2.0, 0.0),
                serde_json::json!({"note": "a|b|c", "n": 1}),
                None,
            )
            .unwrap();
            db.close().unwrap();
        }
        {
            let db = DB::open(&db_path).unwrap();
            let loc = db
                .get("ns", "obj")
                .unwrap()
                .expect("record with '|' in metadata must survive reopen");
            assert_eq!(loc.metadata, serde_json::json!({"note": "a|b|c", "n": 1}));
        }
    }

    #[test]
    fn test_checkpoint_preserves_history_and_writes_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("traj.db");
        let snap_path = dir.path().join("traj.db.snap");

        let t1 = SystemTime::now();
        let t2 = t1 + Duration::from_millis(5);

        {
            let db = DB::open(&db_path).unwrap();
            db.upsert(
                "ns",
                "a",
                Point3d::new(1.0, 1.0, 0.0),
                serde_json::json!({"s": 1}),
                Some(SetOptions {
                    timestamp: Some(t1),
                }),
            )
            .unwrap();
            db.upsert(
                "ns",
                "a",
                Point3d::new(2.0, 2.0, 0.0),
                serde_json::json!({"s": 2}),
                Some(SetOptions {
                    timestamp: Some(t2),
                }),
            )
            .unwrap();
            db.upsert(
                "ns",
                "b",
                Point3d::new(9.0, 9.0, 0.0),
                serde_json::json!({}),
                None,
            )
            .unwrap();
            db.close().unwrap();
        }
        {
            let db = DB::open(&db_path).unwrap();
            // Current state recovered correctly.
            assert_eq!(db.get("ns", "a").unwrap().unwrap().position.x(), 2.0);
            assert!(db.get("ns", "b").unwrap().is_some());
            // Trajectory history is NOT discarded by the checkpoint. Bracket the
            // window generously: stored timestamps are micro-truncated, so a raw
            // lower bound could exclude the first record.
            let traj = db
                .query_trajectory(
                    "ns",
                    "a",
                    t1 - Duration::from_secs(1),
                    t2 + Duration::from_secs(1),
                    10,
                )
                .unwrap();
            assert_eq!(
                traj.len(),
                2,
                "checkpoint must preserve full trajectory history"
            );
        }
        // A checkpoint snapshot was written beside the log.
        assert!(snap_path.exists(), "checkpoint snapshot should exist");
    }

    #[test]
    fn test_corrupt_snapshot_falls_back_to_full_replay() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("traj.db");
        let snap_path = dir.path().join("traj.db.snap");

        {
            let db = DB::open(&db_path).unwrap();
            db.upsert(
                "ns",
                "a",
                Point3d::new(1.0, 1.0, 0.0),
                serde_json::json!({}),
                None,
            )
            .unwrap();
            db.upsert(
                "ns",
                "a",
                Point3d::new(2.0, 2.0, 0.0),
                serde_json::json!({}),
                None,
            )
            .unwrap();
            db.close().unwrap();
        }
        // Open once more so the snapshot covers the records, then corrupt it.
        {
            let db = DB::open(&db_path).unwrap();
            db.close().unwrap();
        }
        // Valid header, but a record with a bad CRC -> snapshot must be rejected.
        std::fs::write(&snap_path, "#spatio-snap v1 0\n00000000|garbage-record\n").unwrap();

        let db = DB::open(&db_path).unwrap();
        let loc = db
            .get("ns", "a")
            .unwrap()
            .expect("state must still recover via full log replay");
        assert_eq!(loc.position.x(), 2.0);
    }

    #[test]
    fn test_recovery_after_torn_final_write() {
        // Simulate a crash mid-append: truncate the log inside the last record.
        // Recovery must skip the torn record (CRC) and return the valid prefix
        // without error.
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("torn.db");
        let t0 = SystemTime::now();

        {
            let db = DB::open(&db_path).unwrap();
            for i in 0..3u64 {
                db.upsert(
                    "ns",
                    "a",
                    Point3d::new(i as f64, 0.0, 0.0),
                    serde_json::json!({ "i": i }),
                    Some(SetOptions {
                        timestamp: Some(t0 + Duration::from_millis(i)),
                    }),
                )
                .unwrap();
            }
            db.close().unwrap();
        }

        // Lop off the tail of the last record (leave earlier records intact).
        let len = std::fs::metadata(&db_path).unwrap().len();
        let f = std::fs::OpenOptions::new()
            .write(true)
            .open(&db_path)
            .unwrap();
        f.set_len(len - 4).unwrap();
        drop(f);

        let db = DB::open(&db_path).unwrap();
        let loc = db
            .get("ns", "a")
            .unwrap()
            .expect("a complete earlier record must still recover");
        // The torn last record (i=2) is dropped; the last intact one is i=1.
        assert_eq!(loc.position.x(), 1.0);
    }

    #[test]
    fn test_concurrent_writes_same_object_converge() {
        use std::sync::Arc;
        use std::thread;

        // Many threads hammer the same object with increasing timestamps while
        // readers query concurrently. No panic; final value is the latest write.
        let db = Arc::new(DB::memory().unwrap());
        let base = SystemTime::now();
        let writers = 8u64;
        let per = 200u64;

        let mut handles = Vec::new();
        for w in 0..writers {
            let db = Arc::clone(&db);
            handles.push(thread::spawn(move || {
                for i in 0..per {
                    let ms = w * per + i; // globally unique, increasing timestamp
                    // Position stays a valid coordinate; ordering is by timestamp.
                    let _ = db.upsert(
                        "ns",
                        "hot",
                        Point3d::new(1.0, 2.0, 0.0),
                        serde_json::json!({ "ms": ms }),
                        Some(SetOptions {
                            timestamp: Some(base + Duration::from_millis(ms)),
                        }),
                    );
                }
            }));
        }
        // Concurrent readers — must never panic or deadlock.
        for _ in 0..2 {
            let db = Arc::clone(&db);
            handles.push(thread::spawn(move || {
                for _ in 0..per {
                    let _ = db.get("ns", "hot");
                    let _ = db.query_radius("ns", &Point3d::new(0.0, 0.0, 0.0), 1.0e6, 10);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

        // Last-writer-wins by timestamp: the highest-timestamp write survives.
        let max_ms = writers * per - 1;
        let loc = db.get("ns", "hot").unwrap().unwrap();
        assert_eq!(loc.timestamp, base + Duration::from_millis(max_ms));
        assert_eq!(loc.metadata, serde_json::json!({ "ms": max_ms }));
    }

    #[test]
    fn test_invalid_coordinates_are_rejected() {
        let db = DB::memory().unwrap();
        let meta = serde_json::json!({});

        // NaN / Inf / out-of-range coordinates must never reach the index.
        for bad in [
            Point3d::new(f64::NAN, 0.0, 0.0),
            Point3d::new(0.0, f64::INFINITY, 0.0),
            Point3d::new(200.0, 0.0, 0.0), // lon > 180
            Point3d::new(0.0, 95.0, 0.0),  // lat > 90
            Point3d::new(0.0, 0.0, 1.0e9), // absurd altitude
        ] {
            assert!(
                db.upsert("ns", "o", bad, meta.clone(), None).is_err(),
                "invalid coordinate must be rejected on upsert"
            );
        }
        // A valid point still works, and the bad ones left no trace.
        assert!(
            db.upsert("ns", "o", Point3d::new(1.0, 2.0, 0.0), meta, None)
                .is_ok()
        );
        assert_eq!(db.stats().hot_state_objects, 1);
    }

    #[test]
    fn test_invalid_query_inputs_are_rejected() {
        let db = DB::memory().unwrap();
        let c = Point3d::new(0.0, 0.0, 0.0);

        assert!(
            db.query_radius("ns", &c, 0.0, 10).is_err(),
            "radius 0 rejected"
        );
        assert!(
            db.query_radius("ns", &c, -5.0, 10).is_err(),
            "negative radius rejected"
        );
        assert!(
            db.query_radius("ns", &Point3d::new(f64::NAN, 0.0, 0.0), 1.0, 10)
                .is_err()
        );
        assert!(
            db.query_bbox("ns", 10.0, 0.0, 5.0, 10.0, 10).is_err(),
            "min>=max rejected"
        );
        assert!(
            db.knn("ns", &Point3d::new(0.0, 200.0, 0.0), 5).is_err(),
            "bad center rejected"
        );
    }

    #[test]
    fn test_unsafe_identifiers_are_rejected() {
        let db = DB::memory().unwrap();
        let pos = Point3d::new(0.0, 0.0, 0.0);
        let meta = serde_json::json!({});

        // Delimiter / ambiguity hazards must be rejected, not silently mangled.
        for bad in ["a|b", "a\nb", "a\rb", "a::b", ""] {
            assert!(
                db.upsert(bad, "obj", pos.clone(), meta.clone(), None)
                    .is_err(),
                "namespace {bad:?} must be rejected"
            );
            assert!(
                db.upsert("ns", bad, pos.clone(), meta.clone(), None)
                    .is_err(),
                "object_id {bad:?} must be rejected"
            );
            assert!(
                db.delete("ns", bad).is_err(),
                "delete {bad:?} must be rejected"
            );
        }

        // A normal key still works.
        assert!(db.upsert("ns", "ok", pos, meta, None).is_ok());
    }
}
