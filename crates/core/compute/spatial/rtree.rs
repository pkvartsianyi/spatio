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

use crate::config::BoundingBox2D;
use bytes::Bytes;
use geo::HaversineMeasure;
use rstar::{AABB, Point as RstarPoint, RTree};
use rustc_hash::FxHashMap;
use spatio_types::geo::Point as GeoPoint;
use spatio_types::point::Point3d;
use std::cmp::Ordering;
use std::collections::BinaryHeap;

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
    pub center: GeoPoint,
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

/// Indexed Bounding Box for R*-tree.
#[derive(Debug, Clone, PartialEq)]
pub struct IndexedBBox {
    pub min_x: f64,
    pub min_y: f64,
    pub max_x: f64,
    pub max_y: f64,
    pub key: String,
    pub data: Bytes, // Stores the serialized BoundingBox2D
}

impl rstar::RTreeObject for IndexedBBox {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        AABB::from_corners([self.min_x, self.min_y], [self.max_x, self.max_y])
    }
}

/// Helper struct for heap-based top-k selection (max-heap by distance)
#[derive(Clone)]
struct QueryCandidate {
    point: IndexedPoint3D,
    distance: f64,
}

impl PartialEq for QueryCandidate {
    fn eq(&self, other: &Self) -> bool {
        self.distance == other.distance
    }
}
impl Eq for QueryCandidate {}
impl PartialOrd for QueryCandidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for QueryCandidate {
    fn cmp(&self, other: &Self) -> Ordering {
        // Max-heap: larger distances have higher priority (so we can pop the worst)
        self.distance
            .partial_cmp(&other.distance)
            .unwrap_or(Ordering::Equal)
    }
}

/// Unified spatial index manager for all spatial queries.
///
/// Maintains per-prefix 3D R*-trees that handle both 2D and 3D points efficiently.
/// 2D points are stored with z=0 coordinate in the 3D structure, allowing a single
/// index implementation to serve all spatial query types.
pub struct SpatialIndexManager {
    pub(crate) indexes: FxHashMap<String, RTree<IndexedPoint3D>>,
    // Map from key to list of points (usually 1) for fast removal
    pub(crate) key_map: FxHashMap<String, Vec<IndexedPoint3D>>,
    // Separate index for bounding boxes
    pub(crate) bbox_indexes: FxHashMap<String, RTree<IndexedBBox>>,
}

impl SpatialIndexManager {
    pub fn new() -> Self {
        Self {
            indexes: FxHashMap::default(),
            key_map: FxHashMap::default(),
            bbox_indexes: FxHashMap::default(),
        }
    }

    pub fn insert_point_2d(&mut self, prefix: &str, x: f64, y: f64, key: String, data: Bytes) {
        self.insert_point(prefix, x, y, 0.0, key, data);
    }

    pub fn insert_point(&mut self, prefix: &str, x: f64, y: f64, z: f64, key: String, data: Bytes) {
        let point = IndexedPoint3D::new(x, y, z, key.clone(), data);

        self.indexes
            .entry(prefix.to_string())
            .or_default()
            .insert(point.clone());

        // Track for fast removal
        self.key_map.entry(key).or_default().push(point);
    }

    pub fn insert_bbox(&mut self, prefix: &str, bbox: &BoundingBox2D, key: String, data: Bytes) {
        let indexed_bbox = IndexedBBox {
            min_x: bbox.min_x(),
            min_y: bbox.min_y(),
            max_x: bbox.max_x(),
            max_y: bbox.max_y(),
            key,
            data,
        };

        self.bbox_indexes
            .entry(prefix.to_string())
            .or_default()
            .insert(indexed_bbox);
    }

    /// Query points within a 3D spherical volume using hybrid distance metric.
    ///
    /// Uses envelope-based pruning followed by exact distance filtering.
    ///
    /// # Distance Metric
    ///
    /// Hybrid 3D distance: `√(haversine_horizontal² + euclidean_vertical²)`
    ///
    /// # Assumptions
    ///
    /// This internal function assumes the caller has validated:
    /// - `center` coordinates are valid (lon: ±180°, lat: ±90°, alt: reasonable range)
    /// - `radius` is positive and finite
    /// - Public APIs perform validation
    ///
    /// # Limitations
    ///
    /// Not recommended for queries above ±80° latitude due to envelope expansion.
    pub fn query_within_sphere(
        &self,
        prefix: &str,
        center: &Point3d,
        radius: f64,
        limit: usize,
    ) -> Vec<(String, Bytes, f64)> {
        let Some(tree) = self.indexes.get(prefix) else {
            return Vec::new();
        };

        let envelope = compute_spherical_envelope(center, radius);
        let mut heap = BinaryHeap::with_capacity(limit);

        for point in tree.locate_in_envelope_intersecting(&envelope) {
            let p2 = Point3d::new(point.x, point.y, point.z);
            let distance = geographic_3d_distance(center, &p2);

            if distance.is_finite() && distance <= radius {
                if heap.len() < limit {
                    heap.push(QueryCandidate {
                        // TODO: Performance optimization: Avoid cloning here?
                        point: point.clone(),
                        distance,
                    });
                } else if let Some(worst) = heap.peek()
                    && distance < worst.distance
                {
                    heap.pop();
                    heap.push(QueryCandidate {
                        point: point.clone(),
                        distance,
                    });
                }
            }
        }

        // Convert heap to sorted vector (ascending distance)
        let mut results = Vec::with_capacity(heap.len());
        while let Some(candidate) = heap.pop() {
            results.push((
                candidate.point.key,
                candidate.point.data,
                candidate.distance,
            ));
        }
        results.reverse();
        results
    }

    /// Query 2D points within a circular radius (internal, assumes validated input).
    ///
    /// Returns points sorted by distance (ascending) up to the specified limit.
    ///
    /// # Assumptions
    ///
    /// This internal function assumes the caller has validated:
    /// - `center` has valid geographic coordinates (lon: ±180°, lat: ±90°)
    /// - `radius` is positive and finite
    /// - Public APIs use `validate_geographic_point()` and `validate_radius()`
    ///
    /// # Performance
    ///
    /// Uses envelope-based pruning to avoid computing distances for distant points.
    pub fn query_within_radius_2d(
        &self,
        prefix: &str,
        center: &GeoPoint,
        radius: f64,
        limit: usize,
    ) -> Vec<(f64, f64, String, Bytes, f64)> {
        let Some(tree) = self.indexes.get(prefix) else {
            return Vec::new();
        };

        let envelope = compute_2d_envelope(center, radius);
        let mut heap = BinaryHeap::with_capacity(limit);

        for point in tree.locate_in_envelope_intersecting(&envelope) {
            let p2 = GeoPoint::new(point.x, point.y);
            let distance = center.haversine_distance(&p2);
            if distance.is_finite() && distance <= radius {
                if heap.len() < limit {
                    heap.push(QueryCandidate {
                        point: point.clone(),
                        distance,
                    });
                } else if let Some(worst) = heap.peek()
                    && distance < worst.distance
                {
                    heap.pop();
                    heap.push(QueryCandidate {
                        point: point.clone(),
                        distance,
                    });
                }
            }
        }

        let mut results = Vec::with_capacity(heap.len());
        while let Some(candidate) = heap.pop() {
            results.push((
                candidate.point.x,
                candidate.point.y,
                candidate.point.key,
                candidate.point.data,
                candidate.distance,
            ));
        }
        results.reverse();
        results
    }

    /// Query points within a 3D bounding box.
    ///
    /// - Returns empty result if coordinates are non-finite.
    pub fn query_within_bbox(&self, prefix: &str, query: BBoxQuery) -> Vec<(String, Bytes)> {
        let min_x = query.min_x;
        let min_y = query.min_y;
        let min_z = query.min_z;
        let max_x = query.max_x;
        let max_y = query.max_y;
        let max_z = query.max_z;

        if ![min_x, min_y, min_z, max_x, max_y, max_z]
            .iter()
            .all(|v| v.is_finite())
        {
            log::warn!("Rejecting bounding box query with non-finite coordinates");
            return Vec::new();
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

    pub fn count_within_radius_2d(&self, prefix: &str, center: &GeoPoint, radius: f64) -> usize {
        let Some(tree) = self.indexes.get(prefix) else {
            return 0;
        };

        let envelope = compute_2d_envelope(center, radius);

        tree.locate_in_envelope_intersecting(&envelope)
            .filter(|point| {
                let p2 = GeoPoint::new(point.x, point.y);
                let distance = center.haversine_distance(&p2);
                distance <= radius
            })
            .count()
    }

    /// Check if any points exist within a circular radius.
    ///
    /// Returns `true` if at least one point in the spatial index falls within
    /// the specified radius of the center point.
    pub fn intersects_radius_2d(&self, prefix: &str, center: &GeoPoint, radius: f64) -> bool {
        let Some(tree) = self.indexes.get(prefix) else {
            return false;
        };

        let envelope = compute_2d_envelope(center, radius);

        tree.locate_in_envelope_intersecting(&envelope)
            .any(|point| {
                let p2 = GeoPoint::new(point.x, point.y);
                let distance = center.haversine_distance(&p2);
                distance <= radius
            })
    }

    pub fn knn_2d(
        &self,
        prefix: &str,
        center: &GeoPoint,
        k: usize,
    ) -> Vec<(f64, f64, String, Bytes, f64)> {
        let Some(tree) = self.indexes.get(prefix) else {
            return Vec::new();
        };

        let query_point = IndexedPoint3D::generate(|i| match i {
            0 => center.x(),
            1 => center.y(),
            2 => 0.0,
            _ => 0.0,
        });

        tree.nearest_neighbor_iter(&query_point)
            .take(k)
            .filter_map(|point| {
                let p2 = GeoPoint::new(point.x, point.y);
                let distance = center.haversine_distance(&p2);
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
        center: &GeoPoint,
        k: usize,
        max_distance: Option<f64>,
    ) -> Vec<(f64, f64, String, Bytes, f64)> {
        let Some(tree) = self.indexes.get(prefix) else {
            return Vec::new();
        };

        let query_point = IndexedPoint3D::generate(|i| match i {
            0 => center.x(),
            1 => center.y(),
            2 => 0.0,
            _ => 0.0,
        });

        tree.nearest_neighbor_iter(&query_point)
            .filter_map(|point| {
                let p2 = GeoPoint::new(point.x, point.y);
                let distance = center.haversine_distance(&p2);
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
        let center = query.center;
        let min_z = query.min_z;
        let max_z = query.max_z;
        let radius = query.radius;
        let Some(tree) = self.indexes.get(prefix) else {
            return Vec::new();
        };

        let envelope = compute_cylindrical_envelope(&center, min_z, max_z, radius);
        let mut heap = BinaryHeap::with_capacity(limit);

        for point in tree.locate_in_envelope_intersecting(&envelope) {
            if point.z < min_z || point.z > max_z {
                continue;
            }

            let p2 = GeoPoint::new(point.x, point.y);
            let h_dist = center.haversine_distance(&p2);
            if h_dist <= radius {
                if heap.len() < limit {
                    heap.push(QueryCandidate {
                        point: point.clone(),
                        distance: h_dist,
                    });
                } else if let Some(worst) = heap.peek()
                    && h_dist < worst.distance
                {
                    heap.pop();
                    heap.push(QueryCandidate {
                        point: point.clone(),
                        distance: h_dist,
                    });
                }
            }
        }

        let mut results = Vec::with_capacity(heap.len());
        while let Some(candidate) = heap.pop() {
            results.push((
                candidate.point.key,
                candidate.point.data,
                candidate.distance,
            ));
        }
        results.reverse();
        results
    }

    /// Find k nearest neighbors in 3D space.
    pub fn knn_3d(&self, prefix: &str, center: &Point3d, k: usize) -> Vec<(String, Bytes, f64)> {
        let Some(tree) = self.indexes.get(prefix) else {
            return Vec::new();
        };

        let query_point = IndexedPoint3D::generate(|i| match i {
            0 => center.x(),
            1 => center.y(),
            2 => center.z(),
            _ => 0.0,
        });

        tree.nearest_neighbor_iter(&query_point)
            .take(k)
            .filter_map(|point| {
                let p2 = Point3d::new(point.x, point.y, point.z);
                let distance = geographic_3d_distance(center, &p2);
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
        center: &GeoPoint,
        min_z: f64,
        max_z: f64,
        tolerance: f64,
    ) -> bool {
        let Some(tree) = self.indexes.get(prefix) else {
            return false;
        };

        let envelope = compute_cylindrical_envelope(center, min_z, max_z, tolerance);

        tree.locate_in_envelope_intersecting(&envelope)
            .any(|point| {
                let p2 = GeoPoint::new(point.x, point.y);
                let horizontal_distance = center.haversine_distance(&p2);
                horizontal_distance <= tolerance && point.z >= min_z && point.z <= max_z
            })
    }

    /// Remove a point from the index.
    pub fn remove_entry(&mut self, prefix: &str, key: &str) -> bool {
        let Some(points) = self.key_map.remove(key) else {
            return false;
        };

        let Some(tree) = self.indexes.get_mut(prefix) else {
            return false;
        };

        let mut removed = false;
        for point in points {
            removed |= tree.remove(&point).is_some();
        }

        // Also remove from bbox index if present
        if let Some(bbox_tree) = self.bbox_indexes.get_mut(prefix) {
            let to_remove: Vec<_> = bbox_tree.iter().filter(|b| b.key == key).cloned().collect();
            for bbox in to_remove {
                bbox_tree.remove(&bbox);
            }
        }

        removed
    }

    /// Find intersecting bounding boxes.
    pub fn find_intersecting_bboxes(
        &self,
        prefix: &str,
        bbox: &BoundingBox2D,
    ) -> Vec<(String, Bytes)> {
        let Some(tree) = self.bbox_indexes.get(prefix) else {
            return Vec::new();
        };

        let envelope =
            AABB::from_corners([bbox.min_x(), bbox.min_y()], [bbox.max_x(), bbox.max_y()]);

        tree.locate_in_envelope_intersecting(&envelope)
            .map(|entry| (entry.key.clone(), entry.data.clone()))
            .collect()
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
        self.key_map.clear();
        self.bbox_indexes.clear();
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

/// Compute approximate lat/lon degrees for a given radius at a latitude.
///
/// Uses geodesic approximations:
/// - Latitude: Simple linear (1° ≈ 111 km everywhere)
/// - Longitude: Cosine-corrected based on latitude
///
/// # Polar Region Handling
///
/// Near the poles, longitude degrees per meter increases dramatically as cos(latitude) → 0.
/// To prevent extreme expansion or division by zero:
/// - Latitude is clamped to ±89.9° for calculations
/// - At 89.9°, cos(lat) ≈ 0.00175, giving ~6.4° longitude per km
/// - Queries at exactly ±90° are handled safely
///
/// **Not recommended for queries above ±80° latitude** due to large envelope sizes.
fn compute_lat_lon_degrees(lat: f64, radius: f64) -> (f64, f64) {
    let lat_degrees = (radius / HaversineMeasure::GRS80_MEAN_RADIUS.radius()).to_degrees();

    // Clamp latitude to avoid extreme expansion near poles
    // At 89.9°, cos(lat) ≈ 0.00175 (conservative but prevents issues)
    // At exactly 90°, cos would be 0 (division by zero)
    let safe_lat = lat.abs().min(89.9);

    let lon_degrees = (radius
        / (HaversineMeasure::GRS80_MEAN_RADIUS.radius() * safe_lat.to_radians().cos()))
    .to_degrees();

    (lat_degrees, lon_degrees)
}

/// Compute AABB envelope for a 2D spherical query (circle).
#[inline]
fn compute_2d_envelope(center: &GeoPoint, radius: f64) -> rstar::AABB<IndexedPoint3D> {
    let (lat_degrees, lon_degrees) = compute_lat_lon_degrees(center.y(), radius);

    let min_x = center.x() - lon_degrees;
    let max_x = center.x() + lon_degrees;
    let min_y = center.y() - lat_degrees;
    let max_y = center.y() + lat_degrees;

    let min_corner = IndexedPoint3D::new(min_x, min_y, f64::MIN, String::new(), Bytes::new());
    let max_corner = IndexedPoint3D::new(max_x, max_y, f64::MAX, String::new(), Bytes::new());
    rstar::AABB::from_corners(min_corner, max_corner)
}

/// Compute AABB envelope for a spherical query volume.
///
/// # Distance Metric
///
/// This creates an envelope for a **hybrid 3D distance** query:
/// - Horizontal: Geodesic (haversine) distance on Earth's surface
/// - Vertical: Euclidean (straight-line) altitude difference
/// - Combined: `√(horizontal² + vertical²)`
///
/// # Envelope Approximation
///
/// The envelope uses ±radius for altitude bounds, which is an **over-approximation**:
/// - A point at `(center.lon, center.lat, center.alt ± radius)` has distance exactly `radius`
/// - But envelope also includes points far horizontally that are within altitude range
/// - All candidates are filtered by actual distance calculation
///
/// This ensures no false negatives while accepting some false positives in the envelope.
///
/// # Limitations
///
/// - Not recommended for queries above ±80° latitude (use cylindrical queries instead)
/// - Envelope may include many points that will be filtered out by distance check
#[inline]
fn compute_spherical_envelope(center: &Point3d, radius: f64) -> rstar::AABB<IndexedPoint3D> {
    let (lat_degrees, lon_degrees) = compute_lat_lon_degrees(center.y(), radius);

    let min_x = center.x() - lon_degrees;
    let max_x = center.x() + lon_degrees;
    let min_y = center.y() - lat_degrees;
    let max_y = center.y() + lat_degrees;
    let min_z = center.z() - radius;
    let max_z = center.z() + radius;

    let min_corner = IndexedPoint3D::new(min_x, min_y, min_z, String::new(), Bytes::new());
    let max_corner = IndexedPoint3D::new(max_x, max_y, max_z, String::new(), Bytes::new());
    rstar::AABB::from_corners(min_corner, max_corner)
}

/// Compute AABB envelope for a cylindrical query volume.
#[inline]
fn compute_cylindrical_envelope(
    center: &GeoPoint,
    min_z: f64,
    max_z: f64,
    radius: f64,
) -> rstar::AABB<IndexedPoint3D> {
    let (lat_degrees, lon_degrees) = compute_lat_lon_degrees(center.y(), radius);

    let min_x = center.x() - lon_degrees;
    let max_x = center.x() + lon_degrees;
    let min_y = center.y() - lat_degrees;
    let max_y = center.y() + lat_degrees;

    let min_corner = IndexedPoint3D::new(min_x, min_y, min_z, String::new(), Bytes::new());
    let max_corner = IndexedPoint3D::new(max_x, max_y, max_z, String::new(), Bytes::new());
    rstar::AABB::from_corners(min_corner, max_corner)
}

/// Calculate hybrid 3D distance between two points (meters).
///
/// - **Horizontal distance:** Haversine formula on Earth's surface (geodesic)
/// - **Vertical distance:** Euclidean distance (straight-line altitude difference)
///
/// The result is the Euclidean combination of these two components:
/// `sqrt(horizontal² + vertical²)`
#[inline]
fn geographic_3d_distance(p1: &Point3d, p2: &Point3d) -> f64 {
    let p1_geo = GeoPoint::new(p1.x(), p1.y());
    let p2_geo = GeoPoint::new(p2.x(), p2.y());
    let horizontal = p1_geo.haversine_distance(&p2_geo);
    let vertical = (p2.z() - p1.z()).abs();
    (horizontal.powi(2) + vertical.powi(2)).sqrt()
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

        let center = Point3d::new(-74.0, 40.7, 100.0);
        let results = index.query_within_sphere("drones", &center, 1000.0, 10);
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
                center: GeoPoint::new(-74.0, 40.7),
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
    fn test_bbox_indexing() {
        let mut index = SpatialIndexManager::new();
        let bbox = BoundingBox2D::new(-74.1, 40.6, -74.0, 40.7);
        index.insert_bbox("zones", &bbox, "zone1".to_string(), Bytes::from("data"));

        let query = BoundingBox2D::new(-74.05, 40.65, -74.04, 40.66);
        let results = index.find_intersecting_bboxes("zones", &query);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, "zone1");

        let query_miss = BoundingBox2D::new(-75.0, 41.0, -74.9, 41.1);
        let results_miss = index.find_intersecting_bboxes("zones", &query_miss);
        assert_eq!(results_miss.len(), 0);
    }

    #[test]
    fn test_polar_region_query_doesnt_panic() {
        // Test near North Pole
        let mut index = SpatialIndexManager::new();

        // Insert a point near the North Pole
        index.insert_point(
            "arctic",
            0.0,
            89.5,
            1000.0,
            "station1".to_string(),
            Bytes::from("arctic_station"),
        );

        // Query near pole should not panic or produce invalid envelopes
        let center = Point3d::new(0.0, 89.5, 1000.0);
        let results = index.query_within_sphere("arctic", &center, 5000.0, 10);

        // Should find the station
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, "station1");
    }

    #[test]
    fn test_exactly_at_pole() {
        let mut index = SpatialIndexManager::new();

        // Insert at exactly 90° (North Pole)
        index.insert_point(
            "pole",
            0.0,
            90.0,
            0.0,
            "north_pole".to_string(),
            Bytes::from("pole_marker"),
        );

        // Query at pole should not panic (latitude is clamped internally)
        let center = Point3d::new(0.0, 90.0, 0.0);
        let results = index.query_within_sphere("pole", &center, 1000.0, 10);

        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_high_latitude_2d_query() {
        let mut index = SpatialIndexManager::new();

        // Insert points at high latitude
        index.insert_point_2d(
            "scandinavia",
            10.0,
            70.0,
            "tromso".to_string(),
            Bytes::from("norway"),
        );

        let center = GeoPoint::new(10.0, 70.0);
        let results = index.query_within_radius_2d("scandinavia", &center, 50_000.0, 10);

        // Should work without panic
        assert_eq!(results.len(), 1);
    }
}
