//! Spatial operations using the geo crate.

use crate::error::{Result, SpatioError};
use geo::{ConvexHull, Distance, Intersects, Rect, Rhumb};
use spatio_types::geo::{Point, Polygon};

/// Distance metric for spatial calculations.
pub use spatio_types::geo::DistanceMetric;

/// Distance between two points. Haversine/Geodesic/Rhumb return meters;
/// `Euclidean` returns planar coordinate degrees (see [`DistanceMetric`]).
pub fn distance_between(point1: &Point, point2: &Point, metric: DistanceMetric) -> f64 {
    match metric {
        DistanceMetric::Haversine => point1.haversine_distance(point2),
        DistanceMetric::Geodesic => point1.geodesic_distance(point2),
        DistanceMetric::Rhumb => Rhumb.distance(*point1.inner(), *point2.inner()),
        DistanceMetric::Euclidean => point1.euclidean_distance(point2),
    }
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
    polygon.inner().intersects(point.inner())
}

pub fn convex_hull(points: &geo::MultiPoint) -> Option<Polygon> {
    if points.0.is_empty() {
        return None;
    }
    Some(points.convex_hull().into())
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
    fn test_convex_hull() {
        let points = geo::MultiPoint::from(vec![
            (-74.0, 40.7),
            (-73.9, 40.7),
            (-73.95, 40.8),
            (-73.95, 40.75), // Interior point
        ]);

        let hull = convex_hull(&points).unwrap();
        assert_eq!(hull.exterior().0.len(), 4);
    }
}
