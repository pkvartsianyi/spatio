//! Hot state: current locations of tracked objects
//!
//! This module manages the current state of moving objects, optimized for
//! frequent updates and spatial queries. Each object has exactly one current
//! position, which replaces the previous position on update.

use bytes::Bytes;
use dashmap::DashMap;
use spatio_types::point::Point3d;
use std::time::SystemTime;

use crate::compute::spatial::rtree::SpatialIndexManager;
use crate::error::Result;
use parking_lot::RwLock;

/// Current location of a tracked object
#[derive(Debug, Clone)]
pub struct CurrentLocation {
    pub object_id: String,
    pub namespace: String,
    pub position: Point3d,
    pub metadata: Bytes,
    pub last_updated: SystemTime,
}

/// Hot state: current locations only
///
/// Optimized for:
/// - Frequent updates (replaces old position)
/// - Spatial queries on current state
/// - Lock-free concurrent access
///
/// Memory footprint: ~200 bytes per object
pub struct HotState {
    /// One entry per object (keyed by "namespace::object_id")
    current_locations: DashMap<String, CurrentLocation>,

    /// Spatial index manager (wrapped in RwLock for interior mutability)
    spatial_index: RwLock<SpatialIndexManager>,
}

impl HotState {
    /// Create a new hot state
    pub fn new() -> Self {
        Self {
            current_locations: DashMap::new(),
            spatial_index: RwLock::new(SpatialIndexManager::new()),
        }
    }

    /// Create a composite key from namespace and object ID
    #[inline]
    fn make_key(namespace: &str, object_id: &str) -> String {
        format!("{}::{}", namespace, object_id)
    }

    /// Update object's current location (replaces old position)
    ///
    /// This is the main write path for the hot state. It:
    /// 1. Updates the current_locations map (atomic via DashMap)
    /// 2. Removes old position from spatial index
    /// 3. Inserts new position into spatial index
    ///
    /// Returns the previous location if it existed.
    pub fn update_location(
        &self,
        namespace: &str,
        object_id: &str,
        position: Point3d,
        metadata: Bytes,
        timestamp: SystemTime,
    ) -> Result<Option<CurrentLocation>> {
        let full_key = Self::make_key(namespace, object_id);

        let new_location = CurrentLocation {
            object_id: object_id.to_string(),
            namespace: namespace.to_string(),
            position,
            metadata: metadata.clone(),
            last_updated: timestamp,
        };

        // Extract coordinates before moving new_location
        let pos_x = new_location.position.x();
        let pos_y = new_location.position.y();
        let pos_z = new_location.position.z();

        // Atomic update in main map (DashMap handles concurrency)
        let old_location = self
            .current_locations
            .insert(full_key.clone(), new_location);

        // Update spatial index
        // Remove old position if exists
        if let Some(ref _old) = old_location {
            let old_key = Self::make_key(namespace, object_id);
            let mut spatial_idx = self.spatial_index.write();
            spatial_idx.remove_entry(namespace, &old_key);
        }

        // Insert new position
        let mut spatial_idx = self.spatial_index.write();
        spatial_idx.insert_point(namespace, pos_x, pos_y, pos_z, full_key, metadata);

        Ok(old_location)
    }

    /// Get current location of an object
    pub fn get_current_location(
        &self,
        namespace: &str,
        object_id: &str,
    ) -> Option<CurrentLocation> {
        let key = Self::make_key(namespace, object_id);
        self.current_locations.get(&key).map(|v| v.clone())
    }

    /// Query objects within radius
    pub fn query_within_radius(
        &self,
        namespace: &str,
        center: &Point3d,
        radius: f64,
        limit: usize,
    ) -> Vec<CurrentLocation> {
        let spatial_idx = self.spatial_index.read();
        let results = spatial_idx.query_within_sphere(namespace, center, radius, limit);

        results
            .into_iter()
            .filter_map(|(key, _data, _dist)| self.current_locations.get(&key).map(|v| v.clone()))
            .collect()
    }

    /// Query objects within a 2D bounding box
    pub fn query_within_bbox(
        &self,
        namespace: &str,
        min_x: f64,
        min_y: f64,
        max_x: f64,
        max_y: f64,
        limit: usize,
    ) -> Vec<CurrentLocation> {
        let spatial_idx = self.spatial_index.read();
        let results =
            spatial_idx.query_within_bbox_2d_points(namespace, min_x, min_y, max_x, max_y, limit);

        results
            .into_iter()
            .filter_map(|(_x, _y, key, _data)| self.current_locations.get(&key).map(|v| v.clone()))
            .collect()
    }

    /// Remove an object
    pub fn remove_object(&self, namespace: &str, object_id: &str) -> Option<CurrentLocation> {
        let key = Self::make_key(namespace, object_id);

        // Remove from map
        let removed = self.current_locations.remove(&key).map(|(_, v)| v);

        // Remove from spatial index
        if removed.is_some() {
            let mut spatial_idx = self.spatial_index.write();
            spatial_idx.remove_entry(namespace, &key);
        }

        removed
    }

    /// Query objects within a cylindrical volume
    pub fn query_within_cylinder(
        &self,
        namespace: &str,
        center: spatio_types::geo::Point,
        min_z: f64,
        max_z: f64,
        radius: f64,
        limit: usize,
    ) -> Vec<(CurrentLocation, f64)> {
        let spatial_idx = self.spatial_index.read();
        let query = crate::compute::spatial::rtree::CylinderQuery {
            center,
            min_z,
            max_z,
            radius,
        };
        let results = spatial_idx.query_within_cylinder(namespace, query, limit);

        results
            .into_iter()
            .filter_map(|(key, _data, dist)| {
                self.current_locations.get(&key).map(|v| (v.clone(), dist))
            })
            .collect()
    }

    /// Find k nearest neighbors in 3D
    pub fn knn_3d(
        &self,
        namespace: &str,
        center: &Point3d,
        k: usize,
    ) -> Vec<(CurrentLocation, f64)> {
        let spatial_idx = self.spatial_index.read();
        let results = spatial_idx.knn_3d(namespace, center, k);

        results
            .into_iter()
            .filter_map(|(key, _data, dist)| {
                self.current_locations.get(&key).map(|v| (v.clone(), dist))
            })
            .collect()
    }

    /// Query objects within a 3D bounding box
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
    ) -> Vec<CurrentLocation> {
        let spatial_idx = self.spatial_index.read();
        let query = crate::compute::spatial::rtree::BBoxQuery {
            min_x,
            min_y,
            min_z,
            max_x,
            max_y,
            max_z,
        };
        let results = spatial_idx.query_within_bbox(namespace, query);

        results
            .into_iter()
            .take(limit)
            .filter_map(|(key, _data)| self.current_locations.get(&key).map(|v| v.clone()))
            .collect()
    }

    /// Get total number of tracked objects
    pub fn object_count(&self) -> usize {
        self.current_locations.len()
    }

    /// Get number of objects in a specific namespace
    pub fn namespace_count(&self, namespace: &str) -> usize {
        let prefix = format!("{}::", namespace);
        self.current_locations
            .iter()
            .filter(|entry| entry.key().starts_with(&prefix))
            .count()
    }

    /// Clear all objects from hot state
    pub fn clear(&mut self) {
        self.current_locations.clear();
        let mut spatial_idx = self.spatial_index.write();
        spatial_idx.clear();
    }
}

impl Default for HotState {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_update_replaces_old_position() {
        let hot = HotState::new();

        let pos1 = Point3d::new(-74.0, 40.7, 0.0);
        let pos2 = Point3d::new(-74.1, 40.8, 0.0);

        // First update
        let old = hot
            .update_location(
                "vehicles",
                "truck_001",
                pos1,
                Bytes::from("meta1"),
                SystemTime::now(),
            )
            .unwrap();
        assert!(old.is_none());

        // Second update should replace
        let old = hot
            .update_location(
                "vehicles",
                "truck_001",
                pos2,
                Bytes::from("meta2"),
                SystemTime::now(),
            )
            .unwrap();
        assert!(old.is_some());
        let old_loc = old.unwrap();
        // Compare coordinates instead of position directly
        assert_eq!(old_loc.position.x(), -74.0);
        assert_eq!(old_loc.position.y(), 40.7);

        // Verify current position
        let current = hot.get_current_location("vehicles", "truck_001").unwrap();
        assert_eq!(current.position.x(), -74.1);
        assert_eq!(current.position.y(), 40.8);
        assert_eq!(current.metadata.as_ref(), b"meta2");
    }

    #[test]
    fn test_concurrent_updates_different_objects() {
        use std::sync::Arc;
        use std::thread;

        let hot = Arc::new(HotState::new());

        let handles: Vec<_> = (0..10)
            .map(|i| {
                let hot = hot.clone();
                thread::spawn(move || {
                    for j in 0..100 {
                        let pos = Point3d::new(-74.0 + i as f64 * 0.01, 40.7, 0.0);
                        hot.update_location(
                            "vehicles",
                            &format!("truck_{:03}", i),
                            pos,
                            Bytes::from(format!("data_{}", j)),
                            SystemTime::now(),
                        )
                        .unwrap();
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        // All objects should have current position
        assert_eq!(hot.object_count(), 10);

        // Each object should have its latest data
        for i in 0..10 {
            let loc = hot
                .get_current_location("vehicles", &format!("truck_{:03}", i))
                .unwrap();
            assert_eq!(loc.metadata.as_ref(), b"data_99");
        }
    }

    #[test]
    fn test_namespace_isolation() {
        let hot = HotState::new();

        let pos = Point3d::new(-74.0, 40.7, 0.0);
        hot.update_location(
            "vehicles",
            "truck_001",
            pos.clone(),
            Bytes::from("v1"),
            SystemTime::now(),
        )
        .unwrap();
        hot.update_location(
            "drones",
            "truck_001",
            pos,
            Bytes::from("d1"),
            SystemTime::now(),
        )
        .unwrap();

        // Same object_id, different namespaces
        let vehicle = hot.get_current_location("vehicles", "truck_001").unwrap();
        let drone = hot.get_current_location("drones", "truck_001").unwrap();

        assert_eq!(vehicle.namespace, "vehicles");
        assert_eq!(drone.namespace, "drones");
        assert_eq!(vehicle.metadata.as_ref(), b"v1");
        assert_eq!(drone.metadata.as_ref(), b"d1");
    }

    #[test]
    fn test_remove_object() {
        let hot = HotState::new();

        let pos = Point3d::new(-74.0, 40.7, 0.0);
        hot.update_location(
            "vehicles",
            "truck_001",
            pos,
            Bytes::from("data"),
            SystemTime::now(),
        )
        .unwrap();

        assert_eq!(hot.object_count(), 1);

        let removed = hot.remove_object("vehicles", "truck_001");
        assert!(removed.is_some());
        assert_eq!(hot.object_count(), 0);

        // Should be gone
        assert!(hot.get_current_location("vehicles", "truck_001").is_none());
    }

    #[test]
    fn test_spatial_query() {
        let hot = HotState::new();

        // Add objects at different locations
        hot.update_location(
            "vehicles",
            "truck_001",
            Point3d::new(-74.0, 40.7, 0.0),
            Bytes::from("near"),
            SystemTime::now(),
        )
        .unwrap();

        hot.update_location(
            "vehicles",
            "truck_002",
            Point3d::new(-74.001, 40.701, 0.0), // ~150m away
            Bytes::from("near"),
            SystemTime::now(),
        )
        .unwrap();

        hot.update_location(
            "vehicles",
            "truck_003",
            Point3d::new(-75.0, 41.0, 0.0), // ~100km away
            Bytes::from("far"),
            SystemTime::now(),
        )
        .unwrap();

        // Query within 1km radius
        let center = Point3d::new(-74.0, 40.7, 0.0);
        let nearby = hot.query_within_radius("vehicles", &center, 1000.0, 10);

        // Should find truck_001 and truck_002, not truck_003
        assert!(nearby.len() >= 2);
        assert!(nearby.iter().any(|l| l.object_id == "truck_001"));
        assert!(nearby.iter().any(|l| l.object_id == "truck_002"));
    }
}
