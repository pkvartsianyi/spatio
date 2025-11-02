//! Spatial operations and utilities leveraging the geo crate.
//!
//! This module provides high-level spatial operations that wrap and extend
//! the functionality of the `geo` crate, making it easier to work with
//! geographic data in Spatio.

use crate::error::{Result, SpatioError};
use geo::{
    BoundingRect, ChamberlainDuquetteArea, Contains, ConvexHull, Distance, Euclidean, Geodesic,
    GeodesicArea, Haversine, Intersects, Point, Polygon, Rect, Rhumb,
};

/// Distance metrics for spatial calculations.
///
/// Different metrics are appropriate for different use cases:
/// - **Haversine**: Fast spherical distance, good for most lon/lat calculations
/// - **Geodesic**: More accurate ellipsoidal distance (Karney 2013), slower
/// - **Rhumb**: Constant bearing distance, useful for navigation
/// - **Euclidean**: Planar distance, only for projected coordinates
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DistanceMetric {
    /// Haversine formula - assumes spherical Earth, fast and accurate enough for most uses
    #[default]
    Haversine,
    /// Geodesic distance using Karney (2013) - more accurate, accounts for Earth's ellipsoid
    Geodesic,
    /// Rhumb line (loxodrome) - maintains constant bearing
    Rhumb,
    /// Euclidean distance - for planar/projected coordinates only
    Euclidean,
}

/// Calculate the distance between two points using the specified metric.
///
/// # Arguments
///
/// * `point1` - First point
/// * `point2` - Second point
/// * `metric` - Distance metric to use
///
/// # Returns
///
/// Distance in meters
///
/// # Examples
///
/// ```rust
/// use spatio::{Point, spatial::{distance_between, DistanceMetric}};
///
/// let nyc = Point::new(-74.0060, 40.7128);
/// let la = Point::new(-118.2437, 34.0522);
///
/// // Using default Haversine
/// let dist = distance_between(&nyc, &la, DistanceMetric::Haversine);
/// assert!(dist > 3_900_000.0); // ~3,944 km
///
/// // Using more accurate Geodesic
/// let dist_geodesic = distance_between(&nyc, &la, DistanceMetric::Geodesic);
/// assert!(dist_geodesic > 3_900_000.0);
/// ```
pub fn distance_between(point1: &Point, point2: &Point, metric: DistanceMetric) -> f64 {
    match metric {
        DistanceMetric::Haversine => Haversine.distance(*point1, *point2),
        DistanceMetric::Geodesic => Geodesic.distance(*point1, *point2),
        DistanceMetric::Rhumb => Rhumb.distance(*point1, *point2),
        DistanceMetric::Euclidean => Euclidean.distance(*point1, *point2),
    }
}

/// Find the K nearest neighbors from a set of points.
///
/// This is a brute-force implementation that calculates distances to all points
/// and returns the K nearest ones. For large datasets, consider using the
/// spatial index through `DB::query_within_radius` with an appropriate radius.
///
/// # Arguments
///
/// * `center` - The query point
/// * `points` - Collection of candidate points with associated data
/// * `k` - Number of nearest neighbors to return
/// * `metric` - Distance metric to use
///
/// # Returns
///
/// Vector of (Point, distance, data) tuples, sorted by distance (nearest first)
///
/// # Examples
///
/// ```rust
/// use spatio::{Point, spatial::{knn, DistanceMetric}};
///
/// let center = Point::new(-74.0060, 40.7128);
/// let candidates = vec![
///     (Point::new(-73.9442, 40.6782), "Brooklyn"),
///     (Point::new(-73.9356, 40.7306), "Queens"),
///     (Point::new(-118.2437, 34.0522), "LA"),
/// ];
///
/// let nearest = knn(&center, &candidates, 2, DistanceMetric::Haversine);
/// assert_eq!(nearest.len(), 2);
/// // Verify LA is not in the nearest 2
/// assert_ne!(nearest[0].2, "LA");
/// assert_ne!(nearest[1].2, "LA");
/// ```
pub fn knn<T: Clone>(
    center: &Point,
    points: &[(Point, T)],
    k: usize,
    metric: DistanceMetric,
) -> Vec<(Point, f64, T)> {
    let mut distances: Vec<(Point, f64, T)> = points
        .iter()
        .map(|(pt, data)| {
            let dist = distance_between(center, pt, metric);
            (*pt, dist, data.clone())
        })
        .collect();

    // Sort by distance
    distances.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));

    // Take K nearest
    distances.into_iter().take(k).collect()
}

/// Create a bounding box (Rect) from min/max coordinates.
///
/// This is a convenience wrapper around `geo::Rect` that validates
/// the coordinate order and creates the rectangle.
///
/// # Arguments
///
/// * `min_lon` - Minimum longitude (western boundary)
/// * `min_lat` - Minimum latitude (southern boundary)
/// * `max_lon` - Maximum longitude (eastern boundary)
/// * `max_lat` - Maximum latitude (northern boundary)
///
/// # Returns
///
/// A `geo::Rect` representing the bounding box
///
/// # Errors
///
/// Returns an error if min > max for either coordinate
///
/// # Examples
///
/// ```rust
/// use spatio::spatial::bounding_box;
///
/// // Manhattan bounding box
/// let bbox = bounding_box(-74.02, 40.70, -73.93, 40.80).unwrap();
/// ```
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

/// Check if a point is contained within a polygon.
///
/// Uses the `geo::Contains` trait to perform the containment test.
///
/// # Arguments
///
/// * `polygon` - The polygon to test
/// * `point` - The point to test
///
/// # Examples
///
/// ```rust
/// use spatio::{Point, spatial::point_in_polygon};
/// use geo::{polygon, Polygon};
///
/// let poly: Polygon = polygon![
///     (x: -74.0, y: 40.7),
///     (x: -73.9, y: 40.7),
///     (x: -73.9, y: 40.8),
///     (x: -74.0, y: 40.8),
///     (x: -74.0, y: 40.7),
/// ];
///
/// let inside = Point::new(-73.95, 40.75);
/// let outside = Point::new(-73.85, 40.75);
///
/// assert!(point_in_polygon(&poly, &inside));
/// assert!(!point_in_polygon(&poly, &outside));
/// ```
pub fn point_in_polygon(polygon: &Polygon, point: &Point) -> bool {
    polygon.contains(point)
}

/// Check if a point is within a bounding box.
///
/// # Arguments
///
/// * `bbox` - The bounding box
/// * `point` - The point to test
///
/// # Examples
///
/// ```rust
/// use spatio::{Point, spatial::{bounding_box, point_in_bbox}};
///
/// let bbox = bounding_box(-74.0, 40.7, -73.9, 40.8).unwrap();
/// let inside = Point::new(-73.95, 40.75);
/// let outside = Point::new(-73.85, 40.75);
///
/// assert!(point_in_bbox(&bbox, &inside));
/// assert!(!point_in_bbox(&bbox, &outside));
/// ```
pub fn point_in_bbox(bbox: &Rect, point: &Point) -> bool {
    bbox.contains(point)
}

/// Calculate the area of a polygon in square meters.
///
/// For geodesic (lon/lat) coordinates, uses the Chamberlain-Duquette algorithm
/// which assumes a spherical Earth. For more accurate results on an ellipsoid,
/// use `geodesic_area`.
///
/// # Arguments
///
/// * `polygon` - The polygon to measure
///
/// # Returns
///
/// Area in square meters (for geodesic coordinates) or square units (for planar)
///
/// # Examples
///
/// ```rust
/// use spatio::spatial::polygon_area;
/// use geo::{polygon, Polygon};
///
/// let poly: Polygon = polygon![
///     (x: -74.0, y: 40.7),
///     (x: -73.9, y: 40.7),
///     (x: -73.9, y: 40.8),
///     (x: -74.0, y: 40.8),
///     (x: -74.0, y: 40.7),
/// ];
///
/// let area = polygon_area(&poly);
/// assert!(area > 0.0);
/// ```
pub fn polygon_area(polygon: &Polygon) -> f64 {
    polygon.chamberlain_duquette_unsigned_area()
}

/// Calculate the geodesic area of a polygon in square meters.
///
/// Uses Karney (2013) algorithm for accurate area calculation on an ellipsoid.
/// More accurate than `polygon_area` but slower.
///
/// # Arguments
///
/// * `polygon` - The polygon to measure
///
/// # Returns
///
/// Area in square meters
///
/// # Examples
///
/// ```rust
/// use spatio::spatial::geodesic_polygon_area;
/// use geo::{polygon, Polygon};
///
/// let poly: Polygon = polygon![
///     (x: -74.0, y: 40.7),
///     (x: -73.9, y: 40.7),
///     (x: -73.9, y: 40.8),
///     (x: -74.0, y: 40.8),
///     (x: -74.0, y: 40.7),
/// ];
///
/// let area = geodesic_polygon_area(&poly);
/// assert!(area > 0.0);
/// ```
pub fn geodesic_polygon_area(polygon: &Polygon) -> f64 {
    polygon.geodesic_area_unsigned()
}

/// Calculate the convex hull of a set of points.
///
/// The convex hull is the smallest convex polygon that contains all points.
///
/// # Arguments
///
/// * `points` - Collection of points
///
/// # Returns
///
/// A polygon representing the convex hull, or None if there are fewer than 3 points
///
/// # Examples
///
/// ```rust
/// use spatio::{Point, spatial::convex_hull};
///
/// let points = vec![
///     Point::new(-74.0, 40.7),
///     Point::new(-73.9, 40.7),
///     Point::new(-73.95, 40.8),
/// ];
///
/// let hull = convex_hull(&points);
/// assert!(hull.is_some());
/// ```
pub fn convex_hull(points: &[Point]) -> Option<Polygon> {
    if points.is_empty() {
        return None;
    }

    // Convert to MultiPoint for convex hull calculation
    let multi_point = geo::MultiPoint::new(points.to_vec());
    Some(multi_point.convex_hull())
}

/// Calculate the bounding rectangle that encompasses all points.
///
/// # Arguments
///
/// * `points` - Collection of points
///
/// # Returns
///
/// A `Rect` that bounds all points, or None if the collection is empty
///
/// # Examples
///
/// ```rust
/// use spatio::{Point, spatial::bounding_rect_for_points};
///
/// let points = vec![
///     Point::new(-74.0, 40.7),
///     Point::new(-73.9, 40.8),
/// ];
///
/// let bbox = bounding_rect_for_points(&points).unwrap();
/// ```
pub fn bounding_rect_for_points(points: &[Point]) -> Option<Rect> {
    if points.is_empty() {
        return None;
    }

    let multi_point = geo::MultiPoint::new(points.to_vec());
    multi_point.bounding_rect()
}

/// Check if two bounding boxes intersect.
///
/// # Arguments
///
/// * `bbox1` - First bounding box
/// * `bbox2` - Second bounding box
///
/// # Examples
///
/// ```rust
/// use spatio::spatial::{bounding_box, bboxes_intersect};
///
/// let bbox1 = bounding_box(-74.0, 40.7, -73.9, 40.8).unwrap();
/// let bbox2 = bounding_box(-73.95, 40.75, -73.85, 40.85).unwrap();
///
/// assert!(bboxes_intersect(&bbox1, &bbox2));
/// ```
pub fn bboxes_intersect(bbox1: &Rect, bbox2: &Rect) -> bool {
    bbox1.intersects(bbox2)
}

/// Expand a bounding box by a distance in meters (approximation).
///
/// This is an approximation that expands the box by adding/subtracting
/// a degree offset calculated from the distance. For more accurate buffering,
/// consider using geo's buffer operations.
///
/// # Arguments
///
/// * `bbox` - The bounding box to expand
/// * `distance_meters` - Distance to expand by (in meters)
///
/// # Returns
///
/// A new expanded bounding box
///
/// # Examples
///
/// ```rust
/// use spatio::spatial::{bounding_box, expand_bbox};
///
/// let bbox = bounding_box(-74.0, 40.7, -73.9, 40.8).unwrap();
/// let expanded = expand_bbox(&bbox, 1000.0); // Expand by 1km
/// ```
pub fn expand_bbox(bbox: &Rect, distance_meters: f64) -> Rect {
    // Rough approximation: 1 degree ≈ 111km at equator
    let lat_offset = distance_meters / 111_000.0;

    // Longitude offset depends on latitude
    let avg_lat = (bbox.min().y + bbox.max().y) / 2.0;
    let lon_offset = distance_meters / (111_000.0 * avg_lat.to_radians().cos());

    Rect::new(
        geo::coord! {
            x: bbox.min().x - lon_offset,
            y: bbox.min().y - lat_offset
        },
        geo::coord! {
            x: bbox.max().x + lon_offset,
            y: bbox.max().y + lat_offset
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

        // Distance should be approximately 3,944 km
        assert!(dist_haversine > 3_900_000.0 && dist_haversine < 4_000_000.0);
        assert!(dist_geodesic > 3_900_000.0 && dist_geodesic < 4_000_000.0);

        // They should be close but not identical
        let diff = (dist_haversine - dist_geodesic).abs();
        assert!(diff < 10_000.0); // Within 10km difference
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
        // Closest should be Upper West Side or Queens, not LA
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
        assert_eq!(hull.exterior().0.len(), 4); // 3 points + closing point
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

        // Should be larger than original
        assert!(expanded.min().x < bbox.min().x);
        assert!(expanded.min().y < bbox.min().y);
        assert!(expanded.max().x > bbox.max().x);
        assert!(expanded.max().y > bbox.max().y);
    }
}
