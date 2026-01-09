//! Hot state: current locations of tracked objects
//!
//! This module manages the current state of moving objects, optimized for
//! frequent updates and spatial queries. Each object has exactly one current
//! position, which replaces the previous position on update.

use dashmap::DashMap;
use spatio_types::point::Point3d;
use std::time::SystemTime;

use crate::compute::spatial::rtree::SpatialIndexManager;
use crate::error::Result;
use parking_lot::RwLock;

/// Current location of a tracked object
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CurrentLocation {
    pub object_id: String,
    pub namespace: String,
    pub position: Point3d,
    pub metadata: serde_json::Value,
    pub timestamp: SystemTime,
}

/// Hot state: current locations only
///
/// Optimized for:
/// - Frequent updates (replaces old position)
/// - Spatial queries on current state
/// - Lock-free concurrent access
pub struct HotState {
    /// One entry per object (keyed by "namespace::object_id")
    current_locations: DashMap<String, CurrentLocation>,

    /// Spatial index manager (wrapped in RwLock for interior mutability)
    spatial_index: RwLock<SpatialIndexManager>,
}

impl HotState {
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
    /// Update an object's current location
    pub fn update_location(
        &self,
        namespace: &str,
        object_id: &str,
        position: Point3d,
        metadata: serde_json::Value,
        timestamp: SystemTime,
    ) -> Result<Option<CurrentLocation>> {
        let full_key = Self::make_key(namespace, object_id);

        let new_location = CurrentLocation {
            object_id: object_id.to_string(),
            namespace: namespace.to_string(),
            position,
            metadata: metadata.clone(),
            timestamp,
        };

        // Extract coordinates before moving new_location
        let pos_x = new_location.position.x();
        let pos_y = new_location.position.y();
        let pos_z = new_location.position.z();

        // Atomic update in main map (DashMap handles concurrency)
        // We only update if the new timestamp is newer than or equal to existing
        enum UpdateAction {
            Updated(CurrentLocation),
            Inserted,
            Ignored,
        }

        let action = match self.current_locations.entry(full_key.clone()) {
            dashmap::mapref::entry::Entry::Occupied(mut entry) => {
                if entry.get().timestamp <= timestamp {
                    let old = entry.insert(new_location);
                    UpdateAction::Updated(old)
                } else {
                    UpdateAction::Ignored
                }
            }
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                entry.insert(new_location);
                UpdateAction::Inserted
            }
        };

        match action {
            UpdateAction::Updated(old_location) => {
                // Update spatial index
                // Remove old position
                let mut spatial_idx = self.spatial_index.write();
                spatial_idx.remove_entry(namespace, &full_key);

                // Insert new position
                spatial_idx.insert_point(namespace, pos_x, pos_y, pos_z, full_key);

                Ok(Some(old_location))
            }
            UpdateAction::Inserted => {
                // Insert new position
                let mut spatial_idx = self.spatial_index.write();
                spatial_idx.insert_point(namespace, pos_x, pos_y, pos_z, full_key);
                Ok(None)
            }
            UpdateAction::Ignored => {
                // Return None to indicate no change (or we could return the current value?)
                // For now, None mimics "no old value replaced" which is technically true
                Ok(None)
            }
        }
    }

    /// Get current location of an object
    pub fn get_current_location(
        &self,
        namespace: &str,
        object_id: &str,
    ) -> Option<CurrentLocation> {
        let key = Self::make_key(namespace, object_id);
        self.current_locations.get(&key).map(|v| v.value().clone())
    }

    /// Query objects within radius, returning (location, distance)
    pub fn query_within_radius(
        &self,
        namespace: &str,
        center: &Point3d,
        radius: f64,
        limit: usize,
    ) -> Vec<(CurrentLocation, f64)> {
        let spatial_idx = self.spatial_index.read();
        let results = spatial_idx.query_within_sphere(namespace, center, radius, limit);

        results
            .into_iter()
            .filter_map(|(key, dist)| {
                self.current_locations
                    .get(&key)
                    .map(|v| (v.value().clone(), dist))
            })
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
            .filter_map(|(_x, _y, key)| self.current_locations.get(&key).map(|v| v.value().clone()))
            .take(limit)
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
            .filter_map(|(key, dist)| self.current_locations.get(&key).map(|v| (v.clone(), dist)))
            .collect()
    }

    /// Find k nearest neighbors in 3D
    pub fn knn_3d(
        &self,
        namespace: &str,
        center: &Point3d,
        k: usize,
    ) -> Vec<(CurrentLocation, f64)> {
        let keys = self.spatial_index.read().knn_3d(namespace, center, k);
        keys.into_iter()
            .filter_map(|(key, distance)| {
                self.current_locations
                    .get(&key)
                    .map(|v| (v.clone(), distance))
            })
            .collect()
    }

    /// Query objects within a 3D bounding box
    #[allow(clippy::too_many_arguments)]
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
            .filter_map(|(key,)| self.current_locations.get(&key).map(|v| v.value().clone()))
            .collect()
    }

    /// Query objects within a polygon (2D only)
    pub fn query_polygon(
        &self,
        namespace: &str,
        polygon: &spatio_types::geo::Polygon,
        limit: usize,
    ) -> Vec<CurrentLocation> {
        use geo::BoundingRect;
        // 1. Get polygon bbox for broad phase
        let Some(bbox) = polygon.inner().bounding_rect() else {
            return Vec::new();
        };

        let min = bbox.min();
        let max = bbox.max();

        // 2. Query spatial index with bbox
        let candidates = self.query_within_bbox(
            namespace,
            min.x,
            min.y,
            max.x,
            max.y,
            // Fetch more than limit because we'll filter
            limit * 2,
        );

        // 3. Precise filter
        candidates
            .into_iter()
            .filter(|loc| {
                let pt = spatio_types::geo::Point::new(loc.position.x(), loc.position.y());
                polygon.contains(&pt)
            })
            .take(limit)
            .collect()
    }

    /// Calculate distance between two objects
    pub fn distance_between(
        &self,
        namespace: &str,
        id1: &str,
        id2: &str,
        metric: crate::compute::spatial::DistanceMetric,
    ) -> Option<f64> {
        let loc1 = self.get_current_location(namespace, id1)?;
        let loc2 = self.get_current_location(namespace, id2)?;

        let p1 = spatio_types::geo::Point::new(loc1.position.x(), loc1.position.y());
        let p2 = spatio_types::geo::Point::new(loc2.position.x(), loc2.position.y());

        Some(crate::compute::spatial::distance_between(&p1, &p2, metric))
    }

    /// Calculate distance from object to point
    pub fn distance_to(
        &self,
        namespace: &str,
        id: &str,
        point: &spatio_types::geo::Point,
        metric: crate::compute::spatial::DistanceMetric,
    ) -> Option<f64> {
        let loc = self.get_current_location(namespace, id)?;
        let p1 = spatio_types::geo::Point::new(loc.position.x(), loc.position.y());

        Some(crate::compute::spatial::distance_between(
            &p1, point, metric,
        ))
    }

    /// Compute convex hull of all objects in namespace
    pub fn convex_hull(&self, namespace: &str) -> Option<spatio_types::geo::Polygon> {
        let prefix = Self::make_key(namespace, "");
        // Only strip the "::" suffix if make_key adds it, but make_key is "namespace::id".
        // make_key(namespace, "") -> "namespace::"

        let points: Vec<spatio_types::geo::Point> = self
            .current_locations
            .iter()
            .filter(|entry| entry.key().starts_with(&prefix))
            .map(|entry| {
                let pos = &entry.value().position;
                spatio_types::geo::Point::new(pos.x(), pos.y())
            })
            .collect();

        crate::compute::spatial::convex_hull(&points)
    }

    /// Compute bounding box of all objects in namespace
    pub fn bounding_box(&self, namespace: &str) -> Option<geo::Rect> {
        let prefix = Self::make_key(namespace, "");
        let points: Vec<spatio_types::geo::Point> = self
            .current_locations
            .iter()
            .filter(|entry| entry.key().starts_with(&prefix))
            .map(|entry| {
                let pos = &entry.value().position;
                spatio_types::geo::Point::new(pos.x(), pos.y())
            })
            .collect();

        crate::compute::spatial::bounding_rect_for_points(&points)
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

    /// Get detailed statistics including per-namespace breakdown
    pub fn detailed_stats(&self) -> (usize, usize) {
        let total_objects = self.current_locations.len();
        // Estimate: ~200 bytes per object (key + Point3d + metadata + overhead)
        let estimated_memory = total_objects * 200;
        (total_objects, estimated_memory)
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
                serde_json::json!({"meta": "meta1"}),
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
                serde_json::json!({"meta": "meta2"}),
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
        assert_eq!(current.metadata, serde_json::json!({"meta": "meta2"}));
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
                            serde_json::json!({"data": format!("data_{}", j)}),
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
            assert_eq!(loc.metadata, serde_json::json!({"data": "data_99"}));
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
            serde_json::json!({"v": "v1"}),
            SystemTime::now(),
        )
        .unwrap();
        hot.update_location(
            "drones",
            "truck_001",
            pos,
            serde_json::json!({"d": "d1"}),
            SystemTime::now(),
        )
        .unwrap();

        // Same object_id, different namespaces
        let vehicle = hot.get_current_location("vehicles", "truck_001").unwrap();
        let drone = hot.get_current_location("drones", "truck_001").unwrap();

        assert_eq!(vehicle.namespace, "vehicles");
        assert_eq!(drone.namespace, "drones");
        assert_eq!(vehicle.metadata, serde_json::json!({"v": "v1"}));
        assert_eq!(drone.metadata, serde_json::json!({"d": "d1"}));
    }

    #[test]
    fn test_remove_object() {
        let hot = HotState::new();

        let pos = Point3d::new(-74.0, 40.7, 0.0);
        hot.update_location(
            "vehicles",
            "truck_001",
            pos,
            serde_json::json!({"data": "data"}),
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
            serde_json::json!({"type": "near"}),
            SystemTime::now(),
        )
        .unwrap();

        hot.update_location(
            "vehicles",
            "truck_002",
            Point3d::new(-74.001, 40.701, 0.0), // ~150m away
            serde_json::json!({"type": "near"}),
            SystemTime::now(),
        )
        .unwrap();

        hot.update_location(
            "vehicles",
            "truck_003",
            Point3d::new(-75.0, 41.0, 0.0), // ~100km away
            serde_json::json!({"type": "far"}),
            SystemTime::now(),
        )
        .unwrap();

        // Query within 1km radius
        let center = Point3d::new(-74.0, 40.7, 0.0);
        let nearby = hot.query_within_radius("vehicles", &center, 1000.0, 10);

        // Should find truck_001 and truck_002, not truck_003
        assert!(nearby.len() >= 2);
        assert!(nearby.iter().any(|(l, _)| l.object_id == "truck_001"));
        assert!(nearby.iter().any(|(l, _)| l.object_id == "truck_002"));
    }
}
