//! Spatial operations using the geo crate.

use crate::error::{Result, SpatioError};
use geo::{
    BoundingRect, ChamberlainDuquetteArea, Contains, ConvexHull, Distance, GeodesicArea,
    Intersects, Rect, Rhumb,
};
use spatio_types::geo::{Point, Polygon};
use std::cmp::Ordering;
use std::collections::BinaryHeap;

/// Distance metric for spatial calculations.
pub use spatio_types::geo::DistanceMetric;

/// Distance between two points in meters.
pub fn distance_between(point1: &Point, point2: &Point, metric: DistanceMetric) -> f64 {
    match metric {
        DistanceMetric::Haversine => point1.haversine_distance(point2),
        DistanceMetric::Geodesic => point1.geodesic_distance(point2),
        DistanceMetric::Rhumb => Rhumb.distance(*point1.inner(), *point2.inner()),
        DistanceMetric::Euclidean => point1.euclidean_distance(point2),
    }
}

/// Helper struct for KNN heap ordering (max-heap by distance, so largest is popped)
#[derive(Clone)]
struct KnnEntry<'a, T> {
    point: Point,
    distance: f64,
    data: &'a T,
}

impl<'a, T> PartialEq for KnnEntry<'a, T> {
    fn eq(&self, other: &Self) -> bool {
        self.distance == other.distance
    }
}

impl<'a, T> Eq for KnnEntry<'a, T> {}

impl<'a, T> PartialOrd for KnnEntry<'a, T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a, T> Ord for KnnEntry<'a, T> {
    fn cmp(&self, other: &Self) -> Ordering {
        // Max-heap: larger distances have higher priority
        self.distance
            .partial_cmp(&other.distance)
            .unwrap_or(Ordering::Equal)
    }
}

/// K nearest neighbors. Returns (point, distance, data) sorted by distance (ascending).
///
/// Uses a bounded max-heap to efficiently find K nearest neighbors without sorting all points.
/// Complexity: O(n log k) instead of O(n log n), with O(k) clones instead of O(n).
///
/// # Behavior
///
/// - If `k == 0` or `points.is_empty()`, returns an empty vector
/// - If `k > points.len()`, returns all valid points (up to points.len())
/// - Non-finite distances (NaN, Infinity) are automatically filtered out
///
/// # Arguments
///
/// * `center` - The query point to find neighbors around
/// * `points` - Slice of (Point, data) pairs to search through
/// * `k` - Maximum number of neighbors to return
/// * `metric` - Distance calculation method (Haversine, Geodesic, Rhumb, Euclidean)
///
/// # Examples
///
/// ```
/// use spatio::compute::spatial::{knn, DistanceMetric};
/// use spatio::Point;
///
/// let center = Point::new(-74.0, 40.7);
/// let points = vec![
///     (Point::new(-74.1, 40.8), "NYC"),
///     (Point::new(-73.9, 40.6), "Brooklyn"),
///     (Point::new(-75.0, 41.0), "Far"),
/// ];
///
/// let nearest = knn(&center, &points, 2, DistanceMetric::Haversine);
/// assert_eq!(nearest.len(), 2); // Returns 2 closest points
/// ```
pub fn knn<T: Clone>(
    center: &Point,
    points: &[(Point, T)],
    k: usize,
    metric: DistanceMetric,
) -> Vec<(Point, f64, T)> {
    if k == 0 || points.is_empty() {
        return Vec::new();
    }

    // Capacity: min(k, points.len()) to avoid over-allocation
    let mut heap = BinaryHeap::with_capacity(k.min(points.len()));

    for (pt, data) in points.iter() {
        let dist = distance_between(center, pt, metric);

        // Skip non-finite distances (NaN, Infinity)
        // This can occur with degenerate points or extreme coordinates
        if !dist.is_finite() {
            continue;
        }

        if heap.len() < k {
            heap.push(KnnEntry {
                point: *pt,
                distance: dist,
                data,
            });
        } else if let Some(worst) = heap.peek()
            && dist < worst.distance
        {
            heap.pop();
            heap.push(KnnEntry {
                point: *pt,
                distance: dist,
                data,
            });
        }
    }

    // Convert the max-heap to ascending results by popping then reversing.
    let mut results = Vec::with_capacity(heap.len());
    while let Some(entry) = heap.pop() {
        results.push((entry.point, entry.distance, entry.data.clone()));
    }
    results.reverse();
    results
}

pub fn bounding_box(min_lon: f64, min_lat: f64, max_lon: f64, max_lat: f64) -> Result<Rect> {
    if min_lon > max_lon {
        return Err(SpatioError::InvalidInput(format!(
            "min_lon ({}) must be <= max_lon ({})",
            min_lon, max_lon
        )));
    }
    if min_lat > max_lat {
        return Err(SpatioError::InvalidInput(format!(
            "min_lat ({}) must be <= max_lat ({})",
            min_lat, max_lat
        )));
    }

    Ok(Rect::new(
        geo::coord! { x: min_lon, y: min_lat },
        geo::coord! { x: max_lon, y: max_lat },
    ))
}

pub fn point_in_polygon(polygon: &Polygon, point: &Point) -> bool {
    polygon.contains(point)
}

pub fn point_in_bbox(bbox: &Rect, point: &Point) -> bool {
    bbox.contains(point.inner())
}

/// Calculate the planar area of a polygon using Chamberlain-Duquette formula.
///
/// This is a **fast approximation** suitable for small to medium polygons.
/// Uses planar geometry (treats Earth as flat), which introduces errors for large areas.
///
/// # When to use
///
/// - Small areas (< 1000 km²)
/// - Performance is critical
/// - Approximate area is sufficient
/// - Polygons are not near poles
///
/// # When to use `geodesic_polygon_area` instead
///
/// - Large areas (countries, oceans)
/// - High accuracy required
/// - Polygons span multiple UTM zones
/// - Polygons near polar regions
///
/// # Examples
///
/// ```
/// use spatio::compute::spatial::polygon_area;
/// use spatio::Polygon;
/// use geo::polygon;
///
/// let poly = polygon![
///     (x: -1.0, y: -1.0),
///     (x: 1.0, y: -1.0),
///     (x: 1.0, y: 1.0),
///     (x: -1.0, y: 1.0),
/// ];
/// let area = polygon_area(&poly.into());
/// ```
pub fn polygon_area(polygon: &Polygon) -> f64 {
    polygon.inner().chamberlain_duquette_unsigned_area()
}

/// Calculate the geodesic area of a polygon on Earth's surface.
///
/// This uses the **Karney geodesic algorithm** for high accuracy.
/// Accounts for Earth's curvature and ellipsoidal shape (more accurate, slower).
///
/// # When to use
///
/// - Large polygons (countries, regions, oceans)
/// - High-accuracy requirements (land surveys, legal boundaries)
/// - Polygons near poles (> ±60° latitude)
/// - Any polygon where precision matters more than speed
///
/// # Performance note
///
/// ~10-100x slower than `polygon_area()`, but dramatically more accurate
/// for large areas. For a 1000 km² polygon, planar error can be >5%.
///
/// # Examples
///
/// ```
/// use spatio::compute::spatial::geodesic_polygon_area;
/// use spatio::Polygon;
/// use geo::polygon;
///
/// // Large country polygon
/// let france = polygon![
///     (x: -4.0, y: 43.0),  // Southwest
///     (x: 8.0, y: 43.0),   // Southeast  
///     (x: 8.0, y: 51.0),   // Northeast
///     (x: -4.0, y: 51.0),  // Northwest
/// ];
/// let area_m2 = geodesic_polygon_area(&france.into());
/// let area_km2 = area_m2 / 1_000_000.0;
/// ```
pub fn geodesic_polygon_area(polygon: &Polygon) -> f64 {
    polygon.inner().geodesic_area_unsigned()
}

pub fn convex_hull(points: &[Point]) -> Option<Polygon> {
    if points.is_empty() {
        return None;
    }
    let geo_points: Vec<geo::Point> = points.iter().map(|p| (*p).into()).collect();
    let multi_point = geo::MultiPoint::new(geo_points);
    Some(multi_point.convex_hull().into())
}

pub fn bounding_rect_for_points(points: &[Point]) -> Option<Rect> {
    if points.is_empty() {
        return None;
    }

    let geo_points: Vec<geo::Point> = points.iter().map(|p| (*p).into()).collect();
    let multi_point = geo::MultiPoint::new(geo_points);
    multi_point.bounding_rect()
}

pub fn bboxes_intersect(bbox1: &Rect, bbox2: &Rect) -> bool {
    bbox1.intersects(bbox2)
}

/// Expand a bounding box by a distance in all directions.
///
/// Creates a new bounding box that is `distance_meters` larger on all sides.
/// Uses geodesic approximations for expansion calculations.
///
/// # Algorithm
///
/// - Latitude: Simple linear expansion (1° ≈ 111 km everywhere)
/// - Longitude: Cosine-corrected expansion based on latitude
///   - Uses the maximum absolute latitude for conservative (larger) expansion
///   - Clamped at 89.9° to avoid extreme values near poles
///
/// # Limitations
///
/// - **Not recommended for polar regions** (latitude > ±80°)
/// - Longitude expansion near poles can produce very large values
/// - Does NOT handle date line (180°/-180°) wrapping
/// - May produce coordinates outside ±180° longitude range
///
/// # Safety
///
/// - Latitude is automatically clamped to [-90, 90]
/// - Longitude calculation uses max_abs_lat.min(89.9) to prevent division issues
/// - Result longitude MAY exceed ±180° for large expansions or polar regions
///
/// # Examples
///
/// ```
/// use spatio::compute::spatial::{bounding_box, expand_bbox};
///
/// // Create a bbox around NYC
/// let bbox = bounding_box(-74.1, 40.6, -73.9, 40.8).unwrap();
///
/// // Expand by 10 km in all directions
/// let expanded = expand_bbox(&bbox, 10_000.0);
/// ```
///
/// # Panics
///
/// Does not panic. Invalid coordinates are clamped or may wrap.
pub fn expand_bbox(bbox: &Rect, distance_meters: f64) -> Rect {
    // 1 degree of latitude is approximately 111km everywhere
    let lat_offset = distance_meters / 111_000.0;

    // Clamp latitude to valid range [-90, 90]
    let min_y = (bbox.min().y - lat_offset).max(-90.0);
    let max_y = (bbox.max().y + lat_offset).min(90.0);

    // Longitude expansion depends on latitude. Latitude closest to the pole
    // (max absolute latitude) to be conservative (larger expansion).
    let max_abs_lat = bbox.min().y.abs().max(bbox.max().y.abs());

    // Clamp to 89.9° to avoid:
    // - Division by zero at exactly 90°
    // - Extreme expansion near poles (cos(89.9°) ≈ 0.00175, giving ~6.4° per km)
    let calc_lat = max_abs_lat.min(89.9);

    // Calculate longitude offset with cosine correction
    // Note: This can produce large values (>180°) near poles or with large distances
    let lon_offset = distance_meters / (111_000.0 * calc_lat.to_radians().cos());

    // WARNING: No longitude wrapping is performed here.
    Rect::new(
        geo::coord! {
            x: bbox.min().x - lon_offset,
            y: min_y
        },
        geo::coord! {
            x: bbox.max().x + lon_offset,
            y: max_y
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_distance_between() {
        let p1 = Point::new(-74.0060, 40.7128); // NYC
        let p2 = Point::new(-118.2437, 34.0522); // LA

        let dist_haversine = distance_between(&p1, &p2, DistanceMetric::Haversine);
        let dist_geodesic = distance_between(&p1, &p2, DistanceMetric::Geodesic);

        assert!(dist_haversine > 3_900_000.0 && dist_haversine < 4_000_000.0);
        assert!(dist_geodesic > 3_900_000.0 && dist_geodesic < 4_000_000.0);

        let diff = (dist_haversine - dist_geodesic).abs();
        assert!(diff < 10_000.0);
    }

    #[test]
    fn test_knn() {
        let center = Point::new(-74.0060, 40.7128);
        let candidates = vec![
            (Point::new(-73.9442, 40.6782), "Brooklyn"),
            (Point::new(-73.9356, 40.7306), "Queens"),
            (Point::new(-118.2437, 34.0522), "LA"),
            (Point::new(-73.9712, 40.7831), "Upper West Side"),
        ];

        let nearest = knn(&center, &candidates, 2, DistanceMetric::Haversine);

        assert_eq!(nearest.len(), 2);
        assert_ne!(nearest[0].2, "LA");
        assert_ne!(nearest[1].2, "LA");
    }

    #[test]
    fn test_bounding_box() {
        let bbox = bounding_box(-74.0, 40.7, -73.9, 40.8).unwrap();

        assert_eq!(bbox.min().x, -74.0);
        assert_eq!(bbox.min().y, 40.7);
        assert_eq!(bbox.max().x, -73.9);
        assert_eq!(bbox.max().y, 40.8);
    }

    #[test]
    fn test_bounding_box_invalid() {
        let result = bounding_box(-73.9, 40.7, -74.0, 40.8);
        assert!(result.is_err());
    }

    #[test]
    fn test_point_in_bbox() {
        let bbox = bounding_box(-74.0, 40.7, -73.9, 40.8).unwrap();

        assert!(point_in_bbox(&bbox, &Point::new(-73.95, 40.75)));
        assert!(!point_in_bbox(&bbox, &Point::new(-73.85, 40.75)));
    }

    #[test]
    fn test_bboxes_intersect() {
        let bbox1 = bounding_box(-74.0, 40.7, -73.9, 40.8).unwrap();
        let bbox2 = bounding_box(-73.95, 40.75, -73.85, 40.85).unwrap();
        let bbox3 = bounding_box(-73.0, 40.0, -72.9, 40.1).unwrap();

        assert!(bboxes_intersect(&bbox1, &bbox2));
        assert!(!bboxes_intersect(&bbox1, &bbox3));
    }

    #[test]
    fn test_convex_hull() {
        let points = vec![
            Point::new(-74.0, 40.7),
            Point::new(-73.9, 40.7),
            Point::new(-73.95, 40.8),
            Point::new(-73.95, 40.75), // Interior point
        ];

        let hull = convex_hull(&points).unwrap();
        assert_eq!(hull.exterior().0.len(), 4);
    }

    #[test]
    fn test_bounding_rect_for_points() {
        let points = vec![
            Point::new(-74.0, 40.7),
            Point::new(-73.9, 40.8),
            Point::new(-73.95, 40.75),
        ];

        let bbox = bounding_rect_for_points(&points).unwrap();
        assert_eq!(bbox.min().x, -74.0);
        assert_eq!(bbox.min().y, 40.7);
        assert_eq!(bbox.max().x, -73.9);
        assert_eq!(bbox.max().y, 40.8);
    }

    #[test]
    fn test_expand_bbox() {
        let bbox = bounding_box(-74.0, 40.7, -73.9, 40.8).unwrap();
        let expanded = expand_bbox(&bbox, 1000.0);
        assert!(expanded.min().x < bbox.min().x);
        assert!(expanded.min().y < bbox.min().y);
        assert!(expanded.max().x > bbox.max().x);
        assert!(expanded.max().y > bbox.max().y);
    }
}
