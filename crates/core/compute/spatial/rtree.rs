//! Unified spatial index using R*-tree for 2D and 3D queries.
//!
//! Provides R*-tree based spatial indexing with AABB envelope pruning for efficient
//! geographic queries. Handles both 2D points (z=0) and native 3D points.
//!
//! Uses Haversine distance for geographic accuracy and achieves O(log n) query
//! performance through spatial pruning before distance calculations.
//!
//! # Example
//!
//! ```rust
//! use spatio::Spatio;
//! use spatio::Point3d;
//!
//! let mut db = Spatio::memory().unwrap();
//! let point = Point3d::new(-74.0, 40.7, 5000.0);
//! db.insert_point_3d("aircraft", &point, b"data", None).unwrap();
//!
//! let center = Point3d::new(-74.0, 40.0, 5000.0);
//! let results = db.query_within_sphere_3d("aircraft", &center, 10000.0, 100).unwrap();
//! ```

use bytes::Bytes;
use geo::{Distance, Haversine, HaversineMeasure, Point as GeoPoint};
use rstar::{Point as RstarPoint, RTree};
use rustc_hash::FxHashMap;

/// Query parameters for bounding box queries.
#[derive(Debug, Clone, Copy)]
pub struct BBoxQuery {
    pub min_x: f64,
    pub min_y: f64,
    pub min_z: f64,
    pub max_x: f64,
    pub max_y: f64,
    pub max_z: f64,
}

/// Query parameters for cylindrical queries.
#[derive(Debug, Clone, Copy)]
pub struct CylinderQuery {
    pub center_x: f64,
    pub center_y: f64,
    pub min_z: f64,
    pub max_z: f64,
    pub radius: f64,
}

/// 3D point for R*-tree indexing.
#[derive(Debug, Clone, PartialEq)]
pub struct IndexedPoint3D {
    pub x: f64,
    pub y: f64,
    pub z: f64,
    pub key: String,
    pub data: Bytes,
}

impl IndexedPoint3D {
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

/// Unified spatial index manager for all spatial queries.
///
/// Maintains per-prefix 3D R*-trees that handle both 2D and 3D points efficiently.
/// 2D points are stored with z=0 coordinate in the 3D structure, allowing a single
/// index implementation to serve all spatial query types.
pub struct SpatialIndexManager {
    pub(crate) indexes: FxHashMap<String, RTree<IndexedPoint3D>>,
}

impl SpatialIndexManager {
    pub fn new() -> Self {
        Self {
            indexes: FxHashMap::default(),
        }
    }

    pub fn insert_point_2d(&mut self, prefix: &str, x: f64, y: f64, key: String, data: Bytes) {
        self.insert_point(prefix, x, y, 0.0, key, data);
    }

    pub fn insert_point(&mut self, prefix: &str, x: f64, y: f64, z: f64, key: String, data: Bytes) {
        let point = IndexedPoint3D::new(x, y, z, key, data);

        self.indexes
            .entry(prefix.to_string())
            .or_default()
            .insert(point);
    }

    /// Query points within a 3D spherical volume using envelope-based pruning.
    ///
    /// # Arguments
    ///
    /// * `prefix` - Namespace to search
    /// * `center_x` - Center longitude
    /// * `center_y` - Center latitude
    /// * `center_z` - Center altitude in meters
    /// * `radius` - Radius in meters
    /// * `limit` - Maximum results to return
    ///
    /// # Returns
    ///
    /// Vector of (key, data, distance) tuples sorted by distance
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

        let envelope = compute_spherical_envelope(center_x, center_y, center_z, radius);

        let mut results: Vec<_> = tree
            .locate_in_envelope_intersecting(&envelope)
            .filter_map(|point| {
                let distance =
                    haversine_3d_distance(center_x, center_y, center_z, point.x, point.y, point.z);
                if distance.is_finite() && distance <= radius {
                    Some((point.key.clone(), point.data.clone(), distance))
                } else {
                    None
                }
            })
            .collect();

        results.sort_by(|a, b| {
            a.2.partial_cmp(&b.2)
                .expect("Distance should be finite after filtering")
        });
        results.truncate(limit);
        results
    }

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

        let lat_degrees = (radius / HaversineMeasure::GRS80_MEAN_RADIUS.radius()).to_degrees();

        let lon_degrees = (radius
            / (HaversineMeasure::GRS80_MEAN_RADIUS.radius() * center_y.to_radians().cos()))
        .to_degrees();

        let min_x = center_x - lon_degrees;
        let max_x = center_x + lon_degrees;
        let min_y = center_y - lat_degrees;
        let max_y = center_y + lat_degrees;

        let min_corner = IndexedPoint3D::new(min_x, min_y, f64::MIN, String::new(), Bytes::new());
        let max_corner = IndexedPoint3D::new(max_x, max_y, f64::MAX, String::new(), Bytes::new());
        let envelope = rstar::AABB::from_corners(min_corner, max_corner);

        let mut results: Vec<_> = tree
            .locate_in_envelope_intersecting(&envelope)
            .filter_map(|point| {
                let distance = haversine_2d_distance(center_x, center_y, point.x, point.y);
                if distance.is_finite() && distance <= radius {
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

        results.sort_by(|a, b| {
            a.4.partial_cmp(&b.4)
                .expect("Distance should be finite after filtering")
        });
        results.truncate(limit);
        results
    }

    pub fn query_within_bbox(&self, prefix: &str, query: BBoxQuery) -> Vec<(String, Bytes)> {
        let mut min_x = query.min_x;
        let mut min_y = query.min_y;
        let mut min_z = query.min_z;
        let mut max_x = query.max_x;
        let mut max_y = query.max_y;
        let mut max_z = query.max_z;

        if ![min_x, min_y, min_z, max_x, max_y, max_z]
            .iter()
            .all(|v| v.is_finite())
        {
            log::warn!("Rejecting bounding box query with non-finite coordinates");
            return Vec::new();
        }

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

        let min_corner = IndexedPoint3D::new(min_x, min_y, min_z, String::new(), Bytes::new());
        let max_corner = IndexedPoint3D::new(max_x, max_y, max_z, String::new(), Bytes::new());
        let envelope = rstar::AABB::from_corners(min_corner, max_corner);
        tree.locate_in_envelope_intersecting(&envelope)
            .map(|point| (point.key.clone(), point.data.clone()))
            .collect()
    }

    /// Query points within a 2D bounding box.
    pub fn query_within_bbox_2d(
        &self,
        prefix: &str,
        min_x: f64,
        min_y: f64,
        max_x: f64,
        max_y: f64,
    ) -> Vec<(String, Bytes)> {
        self.query_within_bbox(
            prefix,
            BBoxQuery {
                min_x,
                min_y,
                min_z: f64::MIN,
                max_x,
                max_y,
                max_z: f64::MAX,
            },
        )
    }

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

        // Convert radius from meters to degrees for latitude
        let lat_degrees = (radius / HaversineMeasure::GRS80_MEAN_RADIUS.radius()).to_degrees();

        // Convert radius from meters to degrees for longitude
        let lon_degrees = (radius
            / (HaversineMeasure::GRS80_MEAN_RADIUS.radius() * center_y.to_radians().cos()))
        .to_degrees();

        let min_x = center_x - lon_degrees;
        let max_x = center_x + lon_degrees;
        let min_y = center_y - lat_degrees;
        let max_y = center_y + lat_degrees;

        let min_corner = IndexedPoint3D::new(min_x, min_y, f64::MIN, String::new(), Bytes::new());
        let max_corner = IndexedPoint3D::new(max_x, max_y, f64::MAX, String::new(), Bytes::new());
        let envelope = rstar::AABB::from_corners(min_corner, max_corner);

        tree.locate_in_envelope_intersecting(&envelope)
            .filter(|point| {
                let distance = haversine_2d_distance(center_x, center_y, point.x, point.y);
                distance <= radius
            })
            .count()
    }

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

        let lat_degrees = (radius / HaversineMeasure::GRS80_MEAN_RADIUS.radius()).to_degrees();

        let lon_degrees = (radius
            / (HaversineMeasure::GRS80_MEAN_RADIUS.radius() * center_y.to_radians().cos()))
        .to_degrees();

        let min_x = center_x - lon_degrees;
        let max_x = center_x + lon_degrees;
        let min_y = center_y - lat_degrees;
        let max_y = center_y + lat_degrees;

        let min_corner = IndexedPoint3D::new(min_x, min_y, f64::MIN, String::new(), Bytes::new());
        let max_corner = IndexedPoint3D::new(max_x, max_y, f64::MAX, String::new(), Bytes::new());
        let envelope = rstar::AABB::from_corners(min_corner, max_corner);

        tree.locate_in_envelope_intersecting(&envelope)
            .any(|point| {
                let distance = haversine_2d_distance(center_x, center_y, point.x, point.y);
                distance <= radius
            })
    }

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
            .filter_map(|point| {
                let distance = haversine_2d_distance(x, y, point.x, point.y);
                if distance.is_finite() {
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
            .collect()
    }

    /// Find k nearest neighbors in 2D with optional max distance filter.
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
                if !distance.is_finite() {
                    return None;
                }
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

    /// Query points within a cylindrical volume (altitude-constrained radius query).
    pub fn query_within_cylinder(
        &self,
        prefix: &str,
        query: CylinderQuery,
        limit: usize,
    ) -> Vec<(String, Bytes, f64)> {
        self.query_within_cylinder_internal(prefix, query, limit, true)
    }

    /// Internal cylinder query with optional sorting.
    fn query_within_cylinder_internal(
        &self,
        prefix: &str,
        query: CylinderQuery,
        limit: usize,
        sort_by_distance: bool,
    ) -> Vec<(String, Bytes, f64)> {
        let center_x = query.center_x;
        let center_y = query.center_y;
        let min_z = query.min_z;
        let max_z = query.max_z;
        let radius = query.radius;
        let Some(tree) = self.indexes.get(prefix) else {
            return Vec::new();
        };

        let envelope = compute_cylindrical_envelope(center_x, center_y, min_z, max_z, radius);

        let mut results: Vec<_> = tree
            .locate_in_envelope_intersecting(&envelope)
            .filter_map(|point| {
                if point.z < min_z || point.z > max_z {
                    return None;
                }

                let h_dist = haversine_2d_distance(center_x, center_y, point.x, point.y);
                if h_dist <= radius {
                    Some((point.key.clone(), point.data.clone(), h_dist))
                } else {
                    None
                }
            })
            .collect();

        if sort_by_distance {
            results.sort_by(|a, b| a.2.partial_cmp(&b.2).unwrap_or(std::cmp::Ordering::Equal));
        }

        results.truncate(limit);
        results
    }

    /// Find k nearest neighbors in 3D space.
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
            .filter_map(|point| {
                let distance = haversine_3d_distance(x, y, z, point.x, point.y, point.z);
                if distance.is_finite() {
                    Some((point.key.clone(), point.data.clone(), distance))
                } else {
                    None
                }
            })
            .collect()
    }

    /// Check if a point exists within altitude range at given coordinates.
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

        let envelope = compute_cylindrical_envelope(x, y, min_z, max_z, tolerance);

        tree.locate_in_envelope_intersecting(&envelope)
            .any(|point| {
                let horizontal_distance = haversine_2d_distance(x, y, point.x, point.y);
                horizontal_distance <= tolerance && point.z >= min_z && point.z <= max_z
            })
    }

    /// Remove a point from the index.
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

/// Compute AABB envelope for a spherical query volume.
#[inline]
fn compute_spherical_envelope(
    center_x: f64,
    center_y: f64,
    center_z: f64,
    radius: f64,
) -> rstar::AABB<IndexedPoint3D> {
    let lat_degrees = (radius / HaversineMeasure::GRS80_MEAN_RADIUS.radius()).to_degrees();
    let lon_degrees = (radius
        / (HaversineMeasure::GRS80_MEAN_RADIUS.radius() * center_y.to_radians().cos()))
    .to_degrees();

    let min_x = center_x - lon_degrees;
    let max_x = center_x + lon_degrees;
    let min_y = center_y - lat_degrees;
    let max_y = center_y + lat_degrees;
    let min_z = center_z - radius;
    let max_z = center_z + radius;

    let min_corner = IndexedPoint3D::new(min_x, min_y, min_z, String::new(), Bytes::new());
    let max_corner = IndexedPoint3D::new(max_x, max_y, max_z, String::new(), Bytes::new());
    rstar::AABB::from_corners(min_corner, max_corner)
}

/// Compute AABB envelope for a cylindrical query volume.
#[inline]
fn compute_cylindrical_envelope(
    center_x: f64,
    center_y: f64,
    min_z: f64,
    max_z: f64,
    radius: f64,
) -> rstar::AABB<IndexedPoint3D> {
    let lat_degrees = (radius / HaversineMeasure::GRS80_MEAN_RADIUS.radius()).to_degrees();
    let lon_degrees = (radius
        / (HaversineMeasure::GRS80_MEAN_RADIUS.radius() * center_y.to_radians().cos()))
    .to_degrees();

    let min_x = center_x - lon_degrees;
    let max_x = center_x + lon_degrees;
    let min_y = center_y - lat_degrees;
    let max_y = center_y + lat_degrees;

    let min_corner = IndexedPoint3D::new(min_x, min_y, min_z, String::new(), Bytes::new());
    let max_corner = IndexedPoint3D::new(max_x, max_y, max_z, String::new(), Bytes::new());
    rstar::AABB::from_corners(min_corner, max_corner)
}

/// Calculate 3D haversine distance between two points (meters).
#[inline]
fn haversine_3d_distance(lon1: f64, lat1: f64, alt1: f64, lon2: f64, lat2: f64, alt2: f64) -> f64 {
    let p1 = GeoPoint::new(lon1, lat1);
    let p2 = GeoPoint::new(lon2, lat2);
    let horizontal = Haversine.distance(p1, p2);
    let vertical = (alt2 - alt1).abs();
    (horizontal.powi(2) + vertical.powi(2)).sqrt()
}

/// Calculate 2D haversine distance between two points (meters).
#[inline]
fn haversine_2d_distance(lon1: f64, lat1: f64, lon2: f64, lat2: f64) -> f64 {
    let p1 = GeoPoint::new(lon1, lat1);
    let p2 = GeoPoint::new(lon2, lat2);
    Haversine.distance(p1, p2)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstar::Envelope;

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

        let results = index.query_within_bbox(
            "aircraft",
            BBoxQuery {
                min_x: -74.05,
                min_y: 40.65,
                min_z: 500.0,
                max_x: -73.95,
                max_y: 40.75,
                max_z: 1500.0,
            },
        );

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
        let results = index.query_within_cylinder(
            "aircraft",
            CylinderQuery {
                center_x: -74.0,
                center_y: 40.7,
                min_z: 3000.0,
                max_z: 7000.0,
                radius: 10000.0,
            },
            10,
        );

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
    fn test_envelope_pruning_spherical() {
        let mut index = SpatialIndexManager::new();

        // Insert points in a grid pattern
        for i in 0..100 {
            for j in 0..100 {
                let lat = 40.0 + (i as f64 * 0.001);
                let lon = -74.0 + (j as f64 * 0.001);
                let alt = (i + j) as f64 * 10.0;
                let key = format!("point_{}_{}", i, j);
                index.insert_point(
                    "grid",
                    lon,
                    lat,
                    alt,
                    key,
                    Bytes::from(format!("data_{}_{}", i, j)),
                );
            }
        }

        // Query with small radius - should prune most points
        let center_lon = -74.0;
        let center_lat = 40.0;
        let center_alt = 500.0;
        let radius = 5000.0;

        let results =
            index.query_within_sphere("grid", center_lon, center_lat, center_alt, radius, 1000);

        // Verify all results are actually within radius
        for (_, _, distance) in &results {
            assert!(
                *distance <= radius,
                "Found point outside radius: {} > {}",
                distance,
                radius
            );
        }

        // Should find some results but not all 10000 points
        assert!(!results.is_empty(), "Should find some results");
        assert!(
            results.len() < 10000,
            "Should not return all points due to pruning"
        );
    }

    #[test]
    fn test_envelope_pruning_cylindrical() {
        let mut index = SpatialIndexManager::new();

        // Insert aircraft at various altitudes
        for i in 0..1000 {
            let lat = 40.0 + ((i % 100) as f64 * 0.001);
            let lon = -74.0 + ((i / 100) as f64 * 0.001);
            let alt = i as f64 * 10.0;
            let key = format!("aircraft_{}", i);
            index.insert_point(
                "aircraft",
                lon,
                lat,
                alt,
                key,
                Bytes::from(format!("data_{}", i)),
            );
        }

        // Query for aircraft between 2000m and 5000m altitude within 5km radius
        let center_lon = -74.005;
        let center_lat = 40.005;
        let min_alt = 2000.0;
        let max_alt = 5000.0;
        let radius = 5000.0;

        let results = index.query_within_cylinder(
            "aircraft",
            CylinderQuery {
                center_x: center_lon,
                center_y: center_lat,
                min_z: min_alt,
                max_z: max_alt,
                radius,
            },
            1000,
        );

        // Verify all results match altitude constraint
        let tree = index.indexes.get("aircraft").unwrap();
        for (key, _, _) in &results {
            let point = tree.iter().find(|p| &p.key == key).unwrap();
            assert!(
                point.z >= min_alt,
                "Altitude {} below minimum {}",
                point.z,
                min_alt
            );
            assert!(
                point.z <= max_alt,
                "Altitude {} above maximum {}",
                point.z,
                max_alt
            );
        }

        // Should exclude points outside altitude range
        assert!(results.len() < 1000, "Should filter by altitude");
    }

    #[test]
    fn test_spherical_envelope_accuracy() {
        // Test that envelope correctly bounds spherical volume
        let center_x = -74.0;
        let center_y = 40.0;
        let center_z = 1000.0;
        let radius = 5000.0;

        let envelope = compute_spherical_envelope(center_x, center_y, center_z, radius);

        // Create test point at edge of sphere in each direction
        let test_points = vec![
            (center_x, center_y, center_z + radius), // Above
            (center_x, center_y, center_z - radius), // Below
        ];

        for (x, y, z) in test_points {
            let test_point = IndexedPoint3D::new(x, y, z, String::new(), Bytes::new());
            assert!(
                envelope.contains_point(&test_point),
                "Envelope should contain point at ({}, {}, {})",
                x,
                y,
                z
            );
        }
    }

    #[test]
    fn test_cylindrical_envelope_accuracy() {
        // Test that envelope correctly bounds cylindrical volume
        let center_x = -74.0;
        let center_y = 40.0;
        let min_z = 1000.0;
        let max_z = 5000.0;
        let radius = 10000.0;

        let envelope = compute_cylindrical_envelope(center_x, center_y, min_z, max_z, radius);

        // Test point at altitude boundaries
        let test_point_bottom =
            IndexedPoint3D::new(center_x, center_y, min_z, String::new(), Bytes::new());
        let test_point_top =
            IndexedPoint3D::new(center_x, center_y, max_z, String::new(), Bytes::new());

        assert!(
            envelope.contains_point(&test_point_bottom),
            "Envelope should contain point at minimum altitude"
        );
        assert!(
            envelope.contains_point(&test_point_top),
            "Envelope should contain point at maximum altitude"
        );

        // Point outside altitude range should not be in envelope
        let test_point_above = IndexedPoint3D::new(
            center_x,
            center_y,
            max_z + 1000.0,
            String::new(),
            Bytes::new(),
        );
        assert!(
            !envelope.contains_point(&test_point_above),
            "Envelope should not contain point above maximum altitude"
        );
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
