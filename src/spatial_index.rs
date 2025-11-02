//! 3D Spatial Index implementation using R-tree for altitude-aware queries.
//!
//! This module provides a 3D spatial index that can efficiently query points
//! in 3D space, supporting altitude-aware operations for applications like
//! drone tracking, aviation, or multi-floor building navigation.

use bytes::Bytes;
use rstar::{Point as RstarPoint, RTree};
use rustc_hash::FxHashMap;

/// Earth radius in meters for haversine distance calculations
const EARTH_RADIUS_METERS: f64 = 6_371_000.0;

/// A 3D point wrapper for use with the R-tree spatial index.
#[derive(Debug, Clone, PartialEq)]
pub struct IndexedPoint3D {
    /// X coordinate (longitude)
    pub x: f64,
    /// Y coordinate (latitude)
    pub y: f64,
    /// Z coordinate (altitude/elevation)
    pub z: f64,
    /// Associated data key
    pub key: String,
    /// Associated data (stored for backward compatibility, consider removing)
    pub data: Bytes,
}

impl IndexedPoint3D {
    /// Create a new indexed 3D point.
    pub fn new(x: f64, y: f64, z: f64, key: String, data: Bytes) -> Self {
        Self { x, y, z, key, data }
    }
}

impl RstarPoint for IndexedPoint3D {
    type Scalar = f64;
    const DIMENSIONS: usize = 3;

    fn generate(mut generator: impl FnMut(usize) -> Self::Scalar) -> Self {
        Self {
            x: generator(0),
            y: generator(1),
            z: generator(2),
            key: String::new(),
            data: Bytes::new(),
        }
    }

    fn nth(&self, index: usize) -> Self::Scalar {
        match index {
            0 => self.x,
            1 => self.y,
            2 => self.z,
            _ => unreachable!(),
        }
    }

    fn nth_mut(&mut self, index: usize) -> &mut Self::Scalar {
        match index {
            0 => &mut self.x,
            1 => &mut self.y,
            2 => &mut self.z,
            _ => unreachable!(),
        }
    }
}

/// Spatial index manager using R-tree for both 2D and 3D queries.
///
/// This unified index handles both 2D points (with z=0) and 3D points
/// using a single R-tree implementation for optimal performance.
pub struct SpatialIndexManager {
    /// Spatial indexes organized by prefix
    pub(crate) indexes: FxHashMap<String, RTree<IndexedPoint3D>>,
}

impl SpatialIndexManager {
    /// Create a new spatial index manager.
    pub fn new() -> Self {
        Self {
            indexes: FxHashMap::default(),
        }
    }

    /// Insert a 2D point into the index (with z=0).
    ///
    /// # Arguments
    ///
    /// * `prefix` - The prefix/namespace for this point
    /// * `x` - X coordinate (longitude)
    /// * `y` - Y coordinate (latitude)
    /// * `key` - Unique key for this point
    /// * `data` - Associated data
    pub fn insert_point_2d(&mut self, prefix: &str, x: f64, y: f64, key: String, data: Bytes) {
        self.insert_point(prefix, x, y, 0.0, key, data);
    }

    /// Insert a 3D point into the index.
    ///
    /// # Arguments
    ///
    /// * `prefix` - The prefix/namespace for this point
    /// * `x` - X coordinate (longitude)
    /// * `y` - Y coordinate (latitude)
    /// * `z` - Z coordinate (altitude/elevation)
    /// * `key` - Unique key for this point
    /// * `data` - Associated data
    pub fn insert_point(&mut self, prefix: &str, x: f64, y: f64, z: f64, key: String, data: Bytes) {
        let point = IndexedPoint3D::new(x, y, z, key, data);

        self.indexes
            .entry(prefix.to_string())
            .or_default()
            .insert(point);
    }

    /// Query points within a 3D spherical radius.
    ///
    /// This finds all points within a spherical distance from the center point,
    /// considering altitude differences.
    ///
    /// # Arguments
    ///
    /// * `prefix` - The prefix/namespace to search
    /// * `center_x` - Center X coordinate
    /// * `center_y` - Center Y coordinate
    /// * `center_z` - Center Z coordinate (altitude)
    /// * `radius` - Radius in meters (3D distance)
    /// * `limit` - Maximum number of results to return
    ///
    /// # Returns
    ///
    /// Vector of (key, data, distance) tuples within the spherical radius, sorted by distance.
    pub fn query_within_sphere(
        &self,
        prefix: &str,
        center_x: f64,
        center_y: f64,
        center_z: f64,
        radius: f64,
        limit: usize,
    ) -> Vec<(String, Bytes, f64)> {
        let Some(tree) = self.indexes.get(prefix) else {
            return Vec::new();
        };

        // Pre-compute bounding box for quick rejection
        // Approximate: radius in meters to degrees (1 degree ≈ 111km)
        let radius_deg = radius / 111_000.0;
        let min_x = center_x - radius_deg;
        let max_x = center_x + radius_deg;
        let min_y = center_y - radius_deg;
        let max_y = center_y + radius_deg;
        let min_z = center_z - radius;
        let max_z = center_z + radius;

        // Iterate over all points and filter by distance
        let mut results: Vec<_> = tree
            .iter()
            .filter_map(|point| {
                // Quick bounding box rejection (cheap)
                if point.x < min_x
                    || point.x > max_x
                    || point.y < min_y
                    || point.y > max_y
                    || point.z < min_z
                    || point.z > max_z
                {
                    return None;
                }

                // Precise distance check (expensive)
                let distance =
                    haversine_3d_distance(center_x, center_y, center_z, point.x, point.y, point.z);
                if distance <= radius {
                    Some((point.key.clone(), point.data.clone(), distance))
                } else {
                    None
                }
            })
            .collect();

        // Sort by distance
        results.sort_by(|a, b| a.2.partial_cmp(&b.2).unwrap_or(std::cmp::Ordering::Equal));

        // Apply limit
        results.truncate(limit);
        results
    }

    /// Query points within a 2D radius (ignoring altitude).
    ///
    /// # Arguments
    ///
    /// * `prefix` - The prefix/namespace to search
    /// * `center_x` - Center X coordinate
    /// * `center_y` - Center Y coordinate
    /// * `radius` - Radius in meters (2D distance)
    /// * `limit` - Maximum number of results to return
    ///
    /// # Returns
    ///
    /// Vector of (x, y, key, data, distance) tuples within the radius, sorted by distance.
    pub fn query_within_radius_2d(
        &self,
        prefix: &str,
        center_x: f64,
        center_y: f64,
        radius: f64,
        limit: usize,
    ) -> Vec<(f64, f64, String, Bytes, f64)> {
        let Some(tree) = self.indexes.get(prefix) else {
            return Vec::new();
        };

        // Iterate over all points and filter by 2D distance
        let mut results: Vec<_> = tree
            .iter()
            .filter_map(|point| {
                let distance = haversine_2d_distance(center_x, center_y, point.x, point.y);
                if distance <= radius {
                    Some((
                        point.x,
                        point.y,
                        point.key.clone(),
                        point.data.clone(),
                        distance,
                    ))
                } else {
                    None
                }
            })
            .collect();

        // Sort by distance
        results.sort_by(|a, b| a.4.partial_cmp(&b.4).unwrap_or(std::cmp::Ordering::Equal));

        // Apply limit
        results.truncate(limit);
        results
    }

    /// Query points within a 3D bounding box.
    ///
    /// # Arguments
    ///
    /// * `prefix` - The prefix/namespace to search
    /// * `min_x`, `min_y`, `min_z` - Minimum coordinates of the bounding box
    /// * `max_x`, `max_y`, `max_z` - Maximum coordinates of the bounding box
    ///
    /// # Returns
    ///
    /// Vector of (key, data) tuples within the bounding box.
    pub fn query_within_bbox(
        &self,
        prefix: &str,
        mut min_x: f64,
        mut min_y: f64,
        mut min_z: f64,
        mut max_x: f64,
        mut max_y: f64,
        mut max_z: f64,
    ) -> Vec<(String, Bytes)> {
        // Validate all coordinates are finite
        if ![min_x, min_y, min_z, max_x, max_y, max_z]
            .iter()
            .all(|v| v.is_finite())
        {
            log::warn!("Rejecting bounding box query with non-finite coordinates");
            return Vec::new();
        }

        // Auto-correct if needed (swap min/max)
        if min_x > max_x {
            std::mem::swap(&mut min_x, &mut max_x);
        }
        if min_y > max_y {
            std::mem::swap(&mut min_y, &mut max_y);
        }
        if min_z > max_z {
            std::mem::swap(&mut min_z, &mut max_z);
        }

        let Some(tree) = self.indexes.get(prefix) else {
            return Vec::new();
        };

        tree.iter()
            .filter(|point| {
                point.x >= min_x
                    && point.x <= max_x
                    && point.y >= min_y
                    && point.y <= max_y
                    && point.z >= min_z
                    && point.z <= max_z
            })
            .map(|point| (point.key.clone(), point.data.clone()))
            .collect()
    }

    /// Query points within a 2D bounding box (rectangle).
    ///
    /// # Arguments
    ///
    /// * `prefix` - The prefix/namespace to search
    /// * `min_x`, `min_y` - Minimum coordinates of the bounding box
    /// * `max_x`, `max_y` - Maximum coordinates of the bounding box
    ///
    /// # Returns
    ///
    /// Vector of (key, data) tuples within the bounding box.
    pub fn query_within_bbox_2d(
        &self,
        prefix: &str,
        min_x: f64,
        min_y: f64,
        max_x: f64,
        max_y: f64,
    ) -> Vec<(String, Bytes)> {
        self.query_within_bbox(prefix, min_x, min_y, f64::MIN, max_x, max_y, f64::MAX)
    }

    /// Count points within a 2D radius.
    ///
    /// # Arguments
    ///
    /// * `prefix` - The prefix/namespace to search
    /// * `center_x` - Center X coordinate
    /// * `center_y` - Center Y coordinate
    /// * `radius` - Radius in meters
    ///
    /// # Returns
    ///
    /// Number of points within the radius.
    pub fn count_within_radius_2d(
        &self,
        prefix: &str,
        center_x: f64,
        center_y: f64,
        radius: f64,
    ) -> usize {
        let Some(tree) = self.indexes.get(prefix) else {
            return 0;
        };

        tree.iter()
            .filter(|point| {
                let distance = haversine_2d_distance(center_x, center_y, point.x, point.y);
                distance <= radius
            })
            .count()
    }

    /// Check if any points exist within a 2D radius.
    ///
    /// # Arguments
    ///
    /// * `prefix` - The prefix/namespace to search
    /// * `center_x` - Center X coordinate
    /// * `center_y` - Center Y coordinate
    /// * `radius` - Radius in meters
    pub fn contains_point_2d(
        &self,
        prefix: &str,
        center_x: f64,
        center_y: f64,
        radius: f64,
    ) -> bool {
        let Some(tree) = self.indexes.get(prefix) else {
            return false;
        };

        tree.iter().any(|point| {
            let distance = haversine_2d_distance(center_x, center_y, point.x, point.y);
            distance <= radius
        })
    }

    /// Find the k nearest neighbors in 2D space (ignoring altitude).
    ///
    /// # Arguments
    ///
    /// * `prefix` - The prefix/namespace to search
    /// * `x` - Query point X coordinate
    /// * `y` - Query point Y coordinate
    /// * `k` - Number of nearest neighbors to find
    ///
    /// # Returns
    ///
    /// Vector of (x, y, key, data, distance) tuples for the k nearest points.
    pub fn knn_2d(
        &self,
        prefix: &str,
        x: f64,
        y: f64,
        k: usize,
    ) -> Vec<(f64, f64, String, Bytes, f64)> {
        let Some(tree) = self.indexes.get(prefix) else {
            return Vec::new();
        };

        let query_point = IndexedPoint3D::generate(|i| match i {
            0 => x,
            1 => y,
            2 => 0.0,
            _ => 0.0,
        });

        tree.nearest_neighbor_iter(&query_point)
            .take(k)
            .map(|point| {
                let distance = haversine_2d_distance(x, y, point.x, point.y);
                (
                    point.x,
                    point.y,
                    point.key.clone(),
                    point.data.clone(),
                    distance,
                )
            })
            .collect()
    }

    /// Find the k nearest neighbors in 2D space with optional max distance filter.
    ///
    /// # Arguments
    ///
    /// * `prefix` - The prefix/namespace to search
    /// * `x` - Query point X coordinate
    /// * `y` - Query point Y coordinate
    /// * `k` - Number of nearest neighbors to find
    /// * `max_distance` - Optional maximum distance in meters
    ///
    /// # Returns
    ///
    /// Vector of (x, y, key, data, distance) tuples for the k nearest points within max_distance.
    pub fn knn_2d_with_max_distance(
        &self,
        prefix: &str,
        x: f64,
        y: f64,
        k: usize,
        max_distance: Option<f64>,
    ) -> Vec<(f64, f64, String, Bytes, f64)> {
        let Some(tree) = self.indexes.get(prefix) else {
            return Vec::new();
        };

        let query_point = IndexedPoint3D::generate(|i| match i {
            0 => x,
            1 => y,
            2 => 0.0,
            _ => 0.0,
        });

        tree.nearest_neighbor_iter(&query_point)
            .filter_map(|point| {
                let distance = haversine_2d_distance(x, y, point.x, point.y);
                if let Some(max_dist) = max_distance
                    && distance > max_dist
                {
                    return None;
                }
                Some((
                    point.x,
                    point.y,
                    point.key.clone(),
                    point.data.clone(),
                    distance,
                ))
            })
            .take(k)
            .collect()
    }

    /// Query points within a cylindrical volume.
    ///
    /// This is useful for altitude-constrained radius queries, such as finding
    /// all aircraft within a certain horizontal distance and altitude range.
    ///
    /// # Arguments
    ///
    /// * `prefix` - The prefix/namespace to search
    /// * `center_x` - Center X coordinate
    /// * `center_y` - Center Y coordinate
    /// * `min_z` - Minimum altitude
    /// * `max_z` - Maximum altitude
    /// * `horizontal_radius` - Horizontal radius in meters
    /// * `limit` - Maximum number of results
    ///
    /// # Returns
    ///
    /// Vector of (key, data, horizontal_distance) tuples within the cylinder, sorted by distance.
    pub fn query_within_cylinder(
        &self,
        prefix: &str,
        center_x: f64,
        center_y: f64,
        min_z: f64,
        max_z: f64,
        horizontal_radius: f64,
        limit: usize,
    ) -> Vec<(String, Bytes, f64)> {
        self.query_within_cylinder_internal(
            prefix,
            center_x,
            center_y,
            min_z,
            max_z,
            horizontal_radius,
            limit,
            true,
        )
    }

    /// Internal cylinder query with optional sorting.
    fn query_within_cylinder_internal(
        &self,
        prefix: &str,
        center_x: f64,
        center_y: f64,
        min_z: f64,
        max_z: f64,
        horizontal_radius: f64,
        limit: usize,
        sort_by_distance: bool,
    ) -> Vec<(String, Bytes, f64)> {
        let Some(tree) = self.indexes.get(prefix) else {
            return Vec::new();
        };

        let mut results: Vec<_> = tree
            .iter()
            .filter_map(|point| {
                // Check altitude constraint
                if point.z < min_z || point.z > max_z {
                    return None;
                }

                // Check horizontal distance
                let h_dist = haversine_2d_distance(center_x, center_y, point.x, point.y);
                if h_dist <= horizontal_radius {
                    Some((point.key.clone(), point.data.clone(), h_dist))
                } else {
                    None
                }
            })
            .collect();

        if sort_by_distance {
            // Sort by horizontal distance
            results.sort_by(|a, b| a.2.partial_cmp(&b.2).unwrap_or(std::cmp::Ordering::Equal));
        }

        // Apply limit
        results.truncate(limit);
        results
    }

    /// Find the k nearest neighbors in 3D space.
    ///
    /// # Arguments
    ///
    /// * `prefix` - The prefix/namespace to search
    /// * `x` - Query point X coordinate
    /// * `y` - Query point Y coordinate
    /// * `z` - Query point Z coordinate
    /// * `k` - Number of nearest neighbors to find
    ///
    /// # Returns
    ///
    /// Vector of (key, data, distance) tuples for the k nearest points.
    pub fn knn_3d(
        &self,
        prefix: &str,
        x: f64,
        y: f64,
        z: f64,
        k: usize,
    ) -> Vec<(String, Bytes, f64)> {
        let Some(tree) = self.indexes.get(prefix) else {
            return Vec::new();
        };

        let query_point = IndexedPoint3D::generate(|i| match i {
            0 => x,
            1 => y,
            2 => z,
            _ => 0.0,
        });

        tree.nearest_neighbor_iter(&query_point)
            .take(k)
            .map(|point| {
                let distance = haversine_3d_distance(x, y, z, point.x, point.y, point.z);
                (point.key.clone(), point.data.clone(), distance)
            })
            .collect()
    }

    /// Check if a point exists within altitude range at given coordinates.
    ///
    /// # Arguments
    ///
    /// * `prefix` - The prefix/namespace to search
    /// * `x` - X coordinate
    /// * `y` - Y coordinate
    /// * `min_z` - Minimum altitude
    /// * `max_z` - Maximum altitude
    /// * `tolerance` - Horizontal coordinate tolerance
    pub fn contains_point_in_altitude_range(
        &self,
        prefix: &str,
        x: f64,
        y: f64,
        min_z: f64,
        max_z: f64,
        tolerance: f64,
    ) -> bool {
        let Some(tree) = self.indexes.get(prefix) else {
            return false;
        };

        tree.iter().any(|point| {
            (point.x - x).abs() <= tolerance
                && (point.y - y).abs() <= tolerance
                && point.z >= min_z
                && point.z <= max_z
        })
    }

    /// Remove a point from the index.
    ///
    /// # Arguments
    ///
    /// * `prefix` - The prefix/namespace
    /// * `key` - The key of the point to remove
    pub fn remove_entry(&mut self, prefix: &str, key: &str) -> bool {
        let Some(tree) = self.indexes.get_mut(prefix) else {
            return false;
        };

        // Find and remove all points with matching key
        let to_remove: Vec<_> = tree.iter().filter(|p| p.key == key).cloned().collect();

        let mut removed = false;
        for point in to_remove {
            removed |= tree.remove(&point).is_some();
        }

        removed
    }

    /// Get statistics about the spatial indexes.
    pub fn stats(&self) -> SpatialIndexStats {
        let mut total_points = 0;
        for tree in self.indexes.values() {
            total_points += tree.size();
        }

        SpatialIndexStats {
            index_count: self.indexes.len(),
            total_points,
        }
    }

    /// Clear all indexes.
    pub fn clear(&mut self) {
        self.indexes.clear();
    }

    /// Remove an entire index for a prefix.
    pub fn remove_index(&mut self, prefix: &str) -> bool {
        self.indexes.remove(prefix).is_some()
    }
}

impl Default for SpatialIndexManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Statistics about the spatial indexes.
#[derive(Debug, Clone)]
pub struct SpatialIndexStats {
    /// Number of prefix-based indexes
    pub index_count: usize,
    /// Total number of indexed points across all prefixes
    pub total_points: usize,
}

/// Calculate 3D haversine distance between two points.
///
/// Uses haversine formula for horizontal distance and Pythagorean theorem
/// to incorporate altitude difference.
///
/// # Returns
///
/// Distance in meters.
#[inline]
fn haversine_3d_distance(x1: f64, y1: f64, z1: f64, x2: f64, y2: f64, z2: f64) -> f64 {
    let h_dist = haversine_2d_distance(x1, y1, x2, y2);
    let z_diff = z2 - z1;
    (h_dist * h_dist + z_diff * z_diff).sqrt()
}

/// Calculate 2D haversine distance between two points.
///
/// # Returns
///
/// Distance in meters.
#[inline]
fn haversine_2d_distance(lon1: f64, lat1: f64, lon2: f64, lat2: f64) -> f64 {
    let lat1_rad = lat1.to_radians();
    let lat2_rad = lat2.to_radians();
    let delta_lat = (lat2 - lat1).to_radians();
    let delta_lon = (lon2 - lon1).to_radians();

    let a = (delta_lat / 2.0).sin().powi(2)
        + lat1_rad.cos() * lat2_rad.cos() * (delta_lon / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());

    EARTH_RADIUS_METERS * c
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_and_query_3d() {
        let mut index = SpatialIndexManager::new();

        index.insert_point(
            "drones",
            -74.0,
            40.7,
            100.0,
            "drone1".to_string(),
            Bytes::from("data1"),
        );
        index.insert_point(
            "drones",
            -74.001,
            40.701,
            150.0,
            "drone2".to_string(),
            Bytes::from("data2"),
        );
        index.insert_point(
            "drones",
            -74.0,
            40.7,
            50.0,
            "drone3".to_string(),
            Bytes::from("data3"),
        );

        let results = index.query_within_sphere("drones", -74.0, 40.7, 100.0, 1000.0, 10);
        assert!(results.len() >= 2);
    }

    #[test]
    fn test_query_within_bbox_3d() {
        let mut index = SpatialIndexManager::new();

        index.insert_point(
            "aircraft",
            -74.0,
            40.7,
            1000.0,
            "plane1".to_string(),
            Bytes::from("data1"),
        );
        index.insert_point(
            "aircraft",
            -74.1,
            40.8,
            2000.0,
            "plane2".to_string(),
            Bytes::from("data2"),
        );
        index.insert_point(
            "aircraft",
            -74.0,
            40.7,
            3000.0,
            "plane3".to_string(),
            Bytes::from("data3"),
        );

        let results =
            index.query_within_bbox("aircraft", -74.05, 40.65, 500.0, -73.95, 40.75, 1500.0);

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, "plane1");
    }

    #[test]
    fn test_query_within_cylinder() {
        let mut index = SpatialIndexManager::new();

        // Insert points at different altitudes
        index.insert_point(
            "aircraft",
            -74.0,
            40.7,
            1000.0,
            "low".to_string(),
            Bytes::from("data1"),
        );
        index.insert_point(
            "aircraft",
            -74.0,
            40.7,
            5000.0,
            "mid".to_string(),
            Bytes::from("data2"),
        );
        index.insert_point(
            "aircraft",
            -74.0,
            40.7,
            10000.0,
            "high".to_string(),
            Bytes::from("data3"),
        );

        // Query for mid-altitude aircraft
        let results =
            index.query_within_cylinder("aircraft", -74.0, 40.7, 3000.0, 7000.0, 10000.0, 10);

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, "mid");
    }

    #[test]
    fn test_knn_3d() {
        let mut index = SpatialIndexManager::new();

        index.insert_point(
            "points",
            0.0,
            0.0,
            0.0,
            "origin".to_string(),
            Bytes::from("data1"),
        );
        index.insert_point(
            "points",
            1.0,
            0.0,
            0.0,
            "x1".to_string(),
            Bytes::from("data2"),
        );
        index.insert_point(
            "points",
            0.0,
            1.0,
            0.0,
            "y1".to_string(),
            Bytes::from("data3"),
        );
        index.insert_point(
            "points",
            0.0,
            0.0,
            1.0,
            "z1".to_string(),
            Bytes::from("data4"),
        );

        let results = index.knn_3d("points", 0.0, 0.0, 0.0, 2);
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0, "origin");
    }

    #[test]
    fn test_remove_entry_3d() {
        let mut index = SpatialIndexManager::new();

        index.insert_point(
            "test",
            -74.0,
            40.7,
            100.0,
            "point1".to_string(),
            Bytes::from("data1"),
        );
        index.insert_point(
            "test",
            -74.0,
            40.7,
            200.0,
            "point2".to_string(),
            Bytes::from("data2"),
        );

        assert!(index.remove_entry("test", "point1"));

        let results = index.query_within_sphere("test", -74.0, 40.7, 100.0, 1000.0, 10);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, "point2");
    }

    #[test]
    fn test_altitude_range_check() {
        let mut index = SpatialIndexManager::new();

        index.insert_point(
            "test",
            -74.0,
            40.7,
            100.0,
            "point1".to_string(),
            Bytes::from("data1"),
        );

        assert!(index.contains_point_in_altitude_range("test", -74.0, 40.7, 50.0, 150.0, 0.01));
        assert!(!index.contains_point_in_altitude_range("test", -74.0, 40.7, 200.0, 300.0, 0.01));
    }

    #[test]
    fn test_haversine_3d_distance_calculation() {
        // Same location, different altitude
        let dist = haversine_3d_distance(-74.0, 40.7, 0.0, -74.0, 40.7, 100.0);
        assert!((dist - 100.0).abs() < 0.1);

        // Different location, same altitude
        let dist2 = haversine_3d_distance(-74.0, 40.7, 100.0, -74.0, 40.7, 100.0);
        assert!(dist2 < 0.1);
    }

    #[test]
    fn test_stats() {
        let mut index = SpatialIndexManager::new();

        index.insert_point(
            "prefix1",
            0.0,
            0.0,
            0.0,
            "p1".to_string(),
            Bytes::from("data1"),
        );
        index.insert_point(
            "prefix1",
            1.0,
            1.0,
            1.0,
            "p2".to_string(),
            Bytes::from("data2"),
        );
        index.insert_point(
            "prefix2",
            2.0,
            2.0,
            2.0,
            "p3".to_string(),
            Bytes::from("data3"),
        );

        let stats = index.stats();
        assert_eq!(stats.index_count, 2);
        assert_eq!(stats.total_points, 3);
    }

    #[test]
    fn test_2d_queries() {
        let mut index = SpatialIndexManager::new();

        // Insert 2D points (z=0)
        index.insert_point_2d(
            "cities",
            -74.0060,
            40.7128,
            "nyc".to_string(),
            Bytes::from("New York"),
        );
        index.insert_point_2d(
            "cities",
            -73.9442,
            40.6782,
            "brooklyn".to_string(),
            Bytes::from("Brooklyn"),
        );

        // Test 2D radius query
        let results = index.query_within_radius_2d("cities", -74.0060, 40.7128, 10000.0, 10);
        assert_eq!(results.len(), 2);

        // Test 2D bbox query
        let bbox_results = index.query_within_bbox_2d("cities", -74.01, 40.67, -73.94, 40.72);
        assert_eq!(bbox_results.len(), 2);

        // Test 2D knn
        let nearest = index.knn_2d("cities", -74.0, 40.71, 1);
        assert_eq!(nearest.len(), 1);
        assert_eq!(nearest[0].2, "nyc"); // Index 2 is the key

        // Test count
        let count = index.count_within_radius_2d("cities", -74.0060, 40.7128, 10000.0);
        assert_eq!(count, 2);

        // Test contains
        assert!(index.contains_point_2d("cities", -74.0060, 40.7128, 1000.0));
        assert!(!index.contains_point_2d("cities", -80.0, 50.0, 1000.0));
    }
}
