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
//! let db = Spatio::memory().unwrap();
//! let point = Point3d::new(-74.0, 40.7, 5000.0);
//! db.upsert("aircraft", "id1", point.clone(), serde_json::json!({"data": "data"}), None).unwrap();
//!
//! let center = Point3d::new(-74.0, 40.0, 5000.0);
//! let results = db.query_radius("aircraft", &center, 10000.0, 100).unwrap();
//! ```

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
}

impl IndexedPoint3D {
    pub fn new(x: f64, y: f64, z: f64, key: String) -> Self {
        Self { x, y, z, key }
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

/// Helper struct for heap-based top-k selection (max-heap by distance)
struct QueryCandidate<'a> {
    point: &'a IndexedPoint3D,
    distance: f64,
}

impl PartialEq for QueryCandidate<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.distance == other.distance
    }
}
impl Eq for QueryCandidate<'_> {}
impl PartialOrd for QueryCandidate<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for QueryCandidate<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        // Max-heap: larger distances have higher priority (so the worst can be popped)
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
}

impl SpatialIndexManager {
    pub fn new() -> Self {
        Self {
            indexes: FxHashMap::default(),
        }
    }

    pub fn insert_point(&mut self, prefix: &str, x: f64, y: f64, z: f64, key: String) {
        let point = IndexedPoint3D::new(x, y, z, key);

        // Avoid allocating an owned prefix on the hot path: only the first
        // insert into a namespace needs to create the map entry.
        if let Some(tree) = self.indexes.get_mut(prefix) {
            tree.insert(point);
        } else {
            self.indexes
                .entry(prefix.to_string())
                .or_default()
                .insert(point);
        }
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
    pub fn query_within_sphere(
        &self,
        prefix: &str,
        center: &Point3d,
        radius: f64,
        limit: usize,
    ) -> Vec<(String, f64)> {
        let Some(tree) = self.indexes.get(prefix) else {
            return Vec::new();
        };

        let envelopes = circle_envelopes(
            &center.to_2d(),
            radius,
            center.z() - radius,
            center.z() + radius,
        );
        top_k(
            envelopes
                .iter()
                .flat_map(|e| tree.locate_in_envelope_intersecting(e))
                .map(|p| (p, center.haversine_3d(&Point3d::new(p.x, p.y, p.z))))
                .filter(|(_, d)| *d <= radius),
            limit,
        )
    }

    /// Query points within a 2D bounding box, returning coordinates.
    pub fn query_within_bbox_2d_points(
        &self,
        prefix: &str,
        min_x: f64,
        min_y: f64,
        max_x: f64,
        max_y: f64,
        limit: usize,
    ) -> Vec<(f64, f64, String)> {
        let Some(tree) = self.indexes.get(prefix) else {
            return Vec::new();
        };

        geo_envelopes(min_x, min_y, f64::NEG_INFINITY, max_x, max_y, f64::INFINITY)
            .iter()
            .flat_map(|e| tree.locate_in_envelope(e))
            .take(limit)
            .map(|p| (p.x, p.y, p.key.clone()))
            .collect()
    }

    /// Query points within a 3D bounding box, returning at most `limit` keys.
    ///
    /// - Returns empty result if coordinates are non-finite.
    pub fn query_within_bbox(
        &self,
        prefix: &str,
        query: BBoxQuery,
        limit: usize,
    ) -> Vec<(String,)> {
        let min_x = query.min_x;
        let min_y = query.min_y;
        let min_z = query.min_z;
        let max_x = query.max_x;
        let max_y = query.max_y;
        let max_z = query.max_z;

        // The Z sentinels (±inf) used by the 2D variant are intentionally
        // non-finite, so only validate the finite query corners.
        if ![min_x, min_y, max_x, max_y].iter().all(|v| v.is_finite())
            || min_z.is_nan()
            || max_z.is_nan()
        {
            log::warn!("Rejecting bounding box query with non-finite coordinates");
            return Vec::new();
        }

        let Some(tree) = self.indexes.get(prefix) else {
            return Vec::new();
        };

        geo_envelopes(min_x, min_y, min_z, max_x, max_y, max_z)
            .iter()
            .flat_map(|e| tree.locate_in_envelope_intersecting(e))
            .take(limit)
            .map(|point| (point.key.clone(),))
            .collect()
    }

    /// Query points within a cylindrical volume (altitude-constrained radius query).
    pub fn query_within_cylinder(
        &self,
        prefix: &str,
        query: CylinderQuery,
        limit: usize,
    ) -> Vec<(String, f64)> {
        let center = query.center;
        let min_z = query.min_z;
        let max_z = query.max_z;
        let radius = query.radius;
        let Some(tree) = self.indexes.get(prefix) else {
            return Vec::new();
        };

        let envelopes = circle_envelopes(&center, radius, min_z, max_z);
        top_k(
            envelopes
                .iter()
                .flat_map(|e| tree.locate_in_envelope_intersecting(e))
                .map(|p| (p, center.haversine_distance(&GeoPoint::new(p.x, p.y))))
                .filter(|(_, d)| *d <= radius),
            limit,
        )
    }

    /// Find the k nearest neighbors by [`Point3d::haversine_3d`] distance.
    ///
    /// The tree's raw (lon°, lat°, alt m) metric only seeds a distance bound;
    /// the k nearest are then selected exactly by a sphere query of that radius.
    pub fn knn_3d(&self, prefix: &str, center: &Point3d, k: usize) -> Vec<(String, f64)> {
        let Some(tree) = self.indexes.get(prefix) else {
            return Vec::new();
        };
        if k == 0 {
            return Vec::new();
        }

        let query_point = IndexedPoint3D::new(center.x(), center.y(), center.z(), String::new());
        let bound = tree
            .nearest_neighbor_iter(&query_point)
            .take(k)
            .map(|p| center.haversine_3d(&Point3d::new(p.x, p.y, p.z)))
            .fold(0.0, f64::max);

        self.query_within_sphere(prefix, center, bound, k)
    }

    pub fn remove_entry(
        &mut self,
        prefix: &str,
        key: &str,
        old_coords: Option<(f64, f64, f64)>,
    ) -> bool {
        let Some(tree) = self.indexes.get_mut(prefix) else {
            return false;
        };

        if let Some((x, y, z)) = old_coords {
            // Fast path: O(log N) removal using known coordinates
            let point_to_remove = IndexedPoint3D::new(x, y, z, key.to_string());
            if tree.remove(&point_to_remove).is_some() {
                return true;
            }
        }

        // Slow path: O(N) scan
        let to_remove: Option<IndexedPoint3D> = tree.iter().find(|p| p.key == key).cloned();

        match to_remove {
            Some(point) => tree.remove(&point).is_some(),
            None => false,
        }
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

    /// Get the bounding box of all points in a namespace.
    pub fn namespace_bbox_2d(&self, prefix: &str) -> Option<(f64, f64, f64, f64)> {
        let tree = self.indexes.get(prefix)?;
        if tree.size() == 0 {
            return None;
        }

        let envelope = tree.root().envelope();
        let min = envelope.lower();
        let max = envelope.upper();

        Some((min.x, min.y, max.x, max.y))
    }

    /// Get all points in a namespace (e.g., for convex hull).
    pub fn namespace_points(&self, prefix: &str) -> geo::MultiPoint {
        let Some(tree) = self.indexes.get(prefix) else {
            return geo::MultiPoint::new(Vec::new());
        };

        tree.iter().map(|p| geo::Point::new(p.x, p.y)).collect()
    }

    /// Query points within a polygon (2D).
    ///
    /// Points on the polygon boundary are included, matching bbox queries.
    pub fn query_within_polygon_2d(
        &self,
        prefix: &str,
        polygon: &spatio_types::geo::Polygon,
        limit: usize,
    ) -> Vec<(f64, f64, String)> {
        use geo::{BoundingRect, Intersects};

        let Some(tree) = self.indexes.get(prefix) else {
            return Vec::new();
        };

        let Some(bbox) = polygon.inner().bounding_rect() else {
            return Vec::new();
        };

        let min = bbox.min();
        let max = bbox.max();

        let min_corner = IndexedPoint3D::new(min.x, min.y, f64::NEG_INFINITY, String::new());
        let max_corner = IndexedPoint3D::new(max.x, max.y, f64::INFINITY, String::new());
        let envelope = rstar::AABB::from_corners(min_corner, max_corner);

        tree.locate_in_envelope_intersecting(&envelope)
            .filter(|p| polygon.inner().intersects(&geo::Point::new(p.x, p.y)))
            .take(limit)
            .map(|p| (p.x, p.y, p.key.clone()))
            .collect()
    }

    /// Clear all indexes.
    pub fn clear(&mut self) {
        self.indexes.clear();
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

fn top_k<'a>(
    candidates: impl Iterator<Item = (&'a IndexedPoint3D, f64)>,
    limit: usize,
) -> Vec<(String, f64)> {
    let mut heap = BinaryHeap::new();
    for (point, distance) in candidates {
        if heap.len() < limit {
            heap.push(QueryCandidate { point, distance });
        } else if let Some(worst) = heap.peek()
            && distance < worst.distance
        {
            heap.pop();
            heap.push(QueryCandidate { point, distance });
        }
    }
    heap.into_sorted_vec()
        .into_iter()
        .map(|c| (c.point.key.clone(), c.distance))
        .collect()
}

/// Envelopes for a lon/lat box; `min_x > max_x` denotes a box crossing the antimeridian.
fn geo_envelopes(
    min_x: f64,
    min_y: f64,
    min_z: f64,
    max_x: f64,
    max_y: f64,
    max_z: f64,
) -> Vec<AABB<IndexedPoint3D>> {
    let aabb = |x0, x1| {
        AABB::from_corners(
            IndexedPoint3D::new(x0, min_y, min_z, String::new()),
            IndexedPoint3D::new(x1, max_y, max_z, String::new()),
        )
    };
    if min_x <= max_x {
        vec![aabb(min_x, max_x)]
    } else {
        vec![aabb(min_x, 180.0), aabb(-180.0, max_x)]
    }
}

/// Envelopes covering every point within great-circle `radius` of `center`,
/// wrapping the antimeridian and spanning all longitudes when a pole is enclosed.
fn circle_envelopes(
    center: &GeoPoint,
    radius: f64,
    min_z: f64,
    max_z: f64,
) -> Vec<AABB<IndexedPoint3D>> {
    // Slight inflation absorbs float error at the envelope edge.
    let d = radius / HaversineMeasure::GRS80_MEAN_RADIUS.radius() * (1.0 + 1e-9);
    let (lon, lat) = (center.x(), center.y());
    let (min_y, max_y) = (lat - d.to_degrees(), lat + d.to_degrees());
    if min_y <= -90.0 || max_y >= 90.0 {
        return geo_envelopes(
            -180.0,
            min_y.max(-90.0),
            min_z,
            180.0,
            max_y.min(90.0),
            max_z,
        );
    }
    let dlon = (d.sin() / lat.to_radians().cos())
        .min(1.0)
        .asin()
        .to_degrees();
    let wrap = |x: f64| {
        if x < -180.0 {
            x + 360.0
        } else if x > 180.0 {
            x - 360.0
        } else {
            x
        }
    };
    geo_envelopes(
        wrap(lon - dlon),
        min_y,
        min_z,
        wrap(lon + dlon),
        max_y,
        max_z,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_and_query_3d() {
        let mut index = SpatialIndexManager::new();

        index.insert_point("drones", -74.0, 40.7, 100.0, "drone1".to_string());
        index.insert_point("drones", -74.001, 40.701, 150.0, "drone2".to_string());
        index.insert_point("drones", -74.0, 40.7, 50.0, "drone3".to_string());

        let center = Point3d::new(-74.0, 40.7, 100.0);
        let results = index.query_within_sphere("drones", &center, 1000.0, 10);
        assert!(results.len() >= 2);
    }

    #[test]
    fn test_query_within_bbox_3d() {
        let mut index = SpatialIndexManager::new();

        index.insert_point("aircraft", -74.0, 40.7, 1000.0, "plane1".to_string());
        index.insert_point("aircraft", -74.1, 40.8, 2000.0, "plane2".to_string());
        index.insert_point("aircraft", -74.0, 40.7, 3000.0, "plane3".to_string());

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
            usize::MAX,
        );

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, "plane1");
    }

    #[test]
    fn test_query_within_cylinder() {
        let mut index = SpatialIndexManager::new();

        // Insert points at different altitudes
        index.insert_point("aircraft", -74.0, 40.7, 1000.0, "low".to_string());
        index.insert_point("aircraft", -74.0, 40.7, 5000.0, "mid".to_string());
        index.insert_point("aircraft", -74.0, 40.7, 10000.0, "high".to_string());

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
    fn test_polar_region_query_doesnt_panic() {
        // Test near North Pole
        let mut index = SpatialIndexManager::new();

        // Insert a point near the North Pole
        index.insert_point("arctic", 0.0, 89.5, 1000.0, "station1".to_string());

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
        index.insert_point("pole", 0.0, 90.0, 0.0, "north_pole".to_string());

        // Query at pole should not panic (latitude is clamped internally)
        let center = Point3d::new(0.0, 90.0, 0.0);
        let results = index.query_within_sphere("pole", &center, 1000.0, 10);

        assert_eq!(results.len(), 1);
    }

    fn sphere_keys(index: &SpatialIndexManager, center: Point3d, radius: f64) -> Vec<String> {
        index
            .query_within_sphere("ns", &center, radius, 10)
            .into_iter()
            .map(|(k, _)| k)
            .collect()
    }

    #[test]
    fn test_knn_ranks_by_true_3d_distance() {
        let mut index = SpatialIndexManager::new();
        index.insert_point("ns", 0.0, 0.0, 1000.0, "above".to_string());
        index.insert_point("ns", 5.0, 0.0, 0.0, "far".to_string());
        index.insert_point("ns", 0.0, 75.0, 0.0, "farther".to_string());

        let results = index.knn_3d("ns", &Point3d::new(0.0, 0.0, 0.0), 3);
        let keys: Vec<_> = results.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(keys, ["above", "far", "farther"]);
        assert!((results[0].1 - 1000.0).abs() < 1e-6);
    }

    #[test]
    fn test_radius_wraps_antimeridian() {
        let mut index = SpatialIndexManager::new();
        index.insert_point("ns", -179.99, 0.0, 0.0, "east".to_string());
        index.insert_point("ns", 179.995, 0.0, 0.0, "west".to_string());

        assert_eq!(
            sphere_keys(&index, Point3d::new(179.99, 0.0, 0.0), 10_000.0),
            ["west", "east"]
        );
        assert_eq!(
            sphere_keys(&index, Point3d::new(-179.995, 0.0, 0.0), 10_000.0),
            ["east", "west"]
        );
    }

    #[test]
    fn test_radius_covering_pole_spans_all_longitudes() {
        let mut index = SpatialIndexManager::new();
        index.insert_point("ns", 180.0, 89.6, 0.0, "across_pole".to_string());

        assert_eq!(
            sphere_keys(&index, Point3d::new(0.0, 89.5, 0.0), 150_000.0),
            ["across_pole"]
        );
    }

    #[test]
    fn test_radius_longitude_half_width_at_high_latitude() {
        let mut index = SpatialIndexManager::new();
        index.insert_point("ns", 18.1, 60.5, 0.0, "edge".to_string());

        let center = Point3d::new(0.0, 60.0, 0.0);
        assert!(center.haversine_2d(&Point3d::new(18.1, 60.5, 0.0)) < 1_000_000.0);
        assert_eq!(sphere_keys(&index, center, 1_000_000.0), ["edge"]);
    }

    #[test]
    fn test_polygon_includes_boundary_like_bbox() {
        use geo::polygon;
        let mut index = SpatialIndexManager::new();
        index.insert_point("ns", 1.0, 0.5, 0.0, "edge".to_string());

        let polygon: spatio_types::geo::Polygon = polygon![
            (x: 0.0, y: 0.0),
            (x: 1.0, y: 0.0),
            (x: 1.0, y: 1.0),
            (x: 0.0, y: 1.0),
        ]
        .into();
        assert_eq!(index.query_within_polygon_2d("ns", &polygon, 10).len(), 1);
        assert_eq!(
            index
                .query_within_bbox_2d_points("ns", 0.0, 0.0, 1.0, 1.0, 10)
                .len(),
            1
        );
    }

    #[test]
    fn test_bbox_crossing_antimeridian() {
        let mut index = SpatialIndexManager::new();
        index.insert_point("ns", 179.5, 0.0, 0.0, "west".to_string());
        index.insert_point("ns", -179.5, 0.0, 0.0, "east".to_string());
        index.insert_point("ns", 0.0, 0.0, 0.0, "meridian".to_string());

        let mut keys: Vec<_> = index
            .query_within_bbox_2d_points("ns", 170.0, -10.0, -170.0, 10.0, 10)
            .into_iter()
            .map(|(_, _, k)| k)
            .collect();
        keys.sort();
        assert_eq!(keys, ["east", "west"]);
    }
}
