//! Spatial object types and representations for the hybrid geohash-rtree index.
//!
//! This module defines the core spatial objects that can be stored and queried
//! in the hybrid index system.

use bytes::Bytes;
use geo::{Distance, Haversine, Point, Polygon, Rect};
use rstar::{AABB, RTreeObject};

/// Type of spatial object stored in the index.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ObjectType {
    /// A point in 2D or 3D space
    Point,
    /// A polygon boundary
    Polygon,
    /// An axis-aligned bounding box
    BoundingBox,
}

/// A spatial object that can be indexed in the geohash-rtree hybrid structure.
///
/// This structure wraps geometric primitives with metadata required for indexing:
/// - Unique identifier (key)
/// - Associated data payload
/// - Bounding envelope for R-tree indexing
/// - Original geometry for exact spatial predicates
#[derive(Debug, Clone, PartialEq)]
pub struct SpatialObject {
    /// Unique identifier for this object
    pub key: String,

    /// Associated data payload
    pub data: Bytes,

    /// Type of spatial object
    pub object_type: ObjectType,

    /// Minimum X coordinate (longitude)
    pub min_x: f64,

    /// Maximum X coordinate (longitude)
    pub max_x: f64,

    /// Minimum Y coordinate (latitude)
    pub min_y: f64,

    /// Maximum Y coordinate (latitude)
    pub max_y: f64,

    /// Minimum Z coordinate (altitude/elevation) - 0.0 for 2D objects
    pub min_z: f64,

    /// Maximum Z coordinate (altitude/elevation) - 0.0 for 2D objects
    pub max_z: f64,
}

impl SpatialObject {
    /// Create a new spatial object from a 2D point.
    ///
    /// # Arguments
    ///
    /// * `key` - Unique identifier for the object
    /// * `point` - The geographic point
    /// * `data` - Associated data payload
    ///
    /// # Returns
    ///
    /// A new `SpatialObject` representing the point.
    pub fn from_point(key: String, point: Point<f64>, data: Bytes) -> Self {
        let x = point.x();
        let y = point.y();

        Self {
            key,
            data,
            object_type: ObjectType::Point,
            min_x: x,
            max_x: x,
            min_y: y,
            max_y: y,
            min_z: 0.0,
            max_z: 0.0,
        }
    }

    /// Create a new spatial object from a 3D point.
    ///
    /// # Arguments
    ///
    /// * `key` - Unique identifier for the object
    /// * `x` - Longitude
    /// * `y` - Latitude
    /// * `z` - Altitude/elevation
    /// * `data` - Associated data payload
    ///
    /// # Returns
    ///
    /// A new `SpatialObject` representing the 3D point.
    pub fn from_point_3d(key: String, x: f64, y: f64, z: f64, data: Bytes) -> Self {
        Self {
            key,
            data,
            object_type: ObjectType::Point,
            min_x: x,
            max_x: x,
            min_y: y,
            max_y: y,
            min_z: z,
            max_z: z,
        }
    }

    /// Create a new spatial object from a polygon.
    ///
    /// # Arguments
    ///
    /// * `key` - Unique identifier for the object
    /// * `polygon` - The polygon geometry
    /// * `data` - Associated data payload
    ///
    /// # Returns
    ///
    /// A new `SpatialObject` representing the polygon's bounding box.
    pub fn from_polygon(key: String, polygon: &Polygon<f64>, data: Bytes) -> Self {
        use geo::BoundingRect;

        let bbox = polygon.bounding_rect().unwrap_or_else(|| {
            // Fallback to point at origin if polygon has no extent
            Rect::new(
                geo::coord! { x: 0.0, y: 0.0 },
                geo::coord! { x: 0.0, y: 0.0 },
            )
        });

        Self {
            key,
            data,
            object_type: ObjectType::Polygon,
            min_x: bbox.min().x,
            max_x: bbox.max().x,
            min_y: bbox.min().y,
            max_y: bbox.max().y,
            min_z: 0.0,
            max_z: 0.0,
        }
    }

    /// Create a new spatial object from a bounding box.
    ///
    /// # Arguments
    ///
    /// * `key` - Unique identifier for the object
    /// * `bbox` - The bounding rectangle
    /// * `data` - Associated data payload
    ///
    /// # Returns
    ///
    /// A new `SpatialObject` representing the bounding box.
    pub fn from_bbox(key: String, bbox: &Rect<f64>, data: Bytes) -> Self {
        Self {
            key,
            data,
            object_type: ObjectType::BoundingBox,
            min_x: bbox.min().x,
            max_x: bbox.max().x,
            min_y: bbox.min().y,
            max_y: bbox.max().y,
            min_z: 0.0,
            max_z: 0.0,
        }
    }

    /// Get the center point of this object.
    ///
    /// # Returns
    ///
    /// A `Point` representing the centroid of the object's bounding box.
    pub fn center(&self) -> Point<f64> {
        Point::new(
            (self.min_x + self.max_x) / 2.0,
            (self.min_y + self.max_y) / 2.0,
        )
    }

    /// Get the center point in 3D space.
    ///
    /// # Returns
    ///
    /// A tuple of (x, y, z) representing the centroid.
    pub fn center_3d(&self) -> (f64, f64, f64) {
        (
            (self.min_x + self.max_x) / 2.0,
            (self.min_y + self.max_y) / 2.0,
            (self.min_z + self.max_z) / 2.0,
        )
    }

    /// Check if this object is a 3D object (has non-zero z extent).
    pub fn is_3d(&self) -> bool {
        self.min_z != 0.0 || self.max_z != 0.0
    }

    /// Calculate the distance from this object to a point using Haversine formula.
    ///
    /// This calculates the great-circle distance on Earth's surface.
    ///
    /// # Arguments
    ///
    /// * `point` - The target point
    ///
    /// # Returns
    ///
    /// Distance in meters.
    pub fn distance_to_point(&self, point: &Point<f64>) -> f64 {
        let center = self.center();
        haversine_distance(center.x(), center.y(), point.x(), point.y())
    }

    /// Check if this object's bounding box intersects with another bounding box.
    pub fn intersects_bbox(&self, bbox: &Rect<f64>) -> bool {
        self.max_x >= bbox.min().x
            && self.min_x <= bbox.max().x
            && self.max_y >= bbox.min().y
            && self.min_y <= bbox.max().y
    }

    /// Check if this object contains a point (bbox containment test).
    pub fn contains_point(&self, point: &Point<f64>) -> bool {
        point.x() >= self.min_x
            && point.x() <= self.max_x
            && point.y() >= self.min_y
            && point.y() <= self.max_y
    }
}

// Implement RTreeObject for spatial indexing with rstar
impl RTreeObject for SpatialObject {
    type Envelope = AABB<[f64; 3]>;

    fn envelope(&self) -> Self::Envelope {
        AABB::from_corners(
            [self.min_x, self.min_y, self.min_z],
            [self.max_x, self.max_y, self.max_z],
        )
    }
}

/// Calculate Haversine distance between two points on Earth's surface.
///
/// # Arguments
///
/// * `lon1` - Longitude of first point in degrees
/// * `lat1` - Latitude of first point in degrees
/// * `lon2` - Longitude of second point in degrees
/// * `lat2` - Latitude of second point in degrees
///
/// # Returns
///
/// Distance in meters.
pub fn haversine_distance(lon1: f64, lat1: f64, lon2: f64, lat2: f64) -> f64 {
    let p1 = Point::new(lon1, lat1);
    let p2 = Point::new(lon2, lat2);
    Haversine.distance(p1, p2)
}

/// Calculate 3D Haversine distance with altitude component.
///
/// This combines great-circle distance with Euclidean altitude difference.
///
/// # Arguments
///
/// * `lon1` - Longitude of first point in degrees
/// * `lat1` - Latitude of first point in degrees
/// * `alt1` - Altitude of first point in meters
/// * `lon2` - Longitude of second point in degrees
/// * `lat2` - Latitude of second point in degrees
/// * `alt2` - Altitude of second point in meters
///
/// # Returns
///
/// 3D distance in meters.
pub fn haversine_distance_3d(
    lon1: f64,
    lat1: f64,
    alt1: f64,
    lon2: f64,
    lat2: f64,
    alt2: f64,
) -> f64 {
    let horizontal = haversine_distance(lon1, lat1, lon2, lat2);
    let vertical = (alt2 - alt1).abs();
    (horizontal.powi(2) + vertical.powi(2)).sqrt()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_spatial_object_from_point() {
        let point = Point::new(-74.0060, 40.7128);
        let obj = SpatialObject::from_point("nyc".to_string(), point, Bytes::from("New York"));

        assert_eq!(obj.key, "nyc");
        assert_eq!(obj.object_type, ObjectType::Point);
        assert_eq!(obj.min_x, -74.0060);
        assert_eq!(obj.max_x, -74.0060);
        assert_eq!(obj.min_y, 40.7128);
        assert_eq!(obj.max_y, 40.7128);
        assert!(!obj.is_3d());
    }

    #[test]
    fn test_spatial_object_from_point_3d() {
        let obj = SpatialObject::from_point_3d(
            "tower".to_string(),
            -74.0060,
            40.7128,
            443.2,
            Bytes::from("Empire State Building"),
        );

        assert_eq!(obj.key, "tower");
        assert_eq!(obj.min_z, 443.2);
        assert_eq!(obj.max_z, 443.2);
        assert!(obj.is_3d());
    }

    #[test]
    fn test_spatial_object_center() {
        let obj = SpatialObject::from_point_3d("test".to_string(), 10.0, 20.0, 30.0, Bytes::new());

        let center = obj.center();
        assert_eq!(center.x(), 10.0);
        assert_eq!(center.y(), 20.0);

        let (x, y, z) = obj.center_3d();
        assert_eq!(x, 10.0);
        assert_eq!(y, 20.0);
        assert_eq!(z, 30.0);
    }

    #[test]
    fn test_haversine_distance() {
        // NYC to SF (approx 4,130 km)
        let nyc = Point::new(-74.0060, 40.7128);
        let sf = Point::new(-122.4194, 37.7749);

        let distance = haversine_distance(nyc.x(), nyc.y(), sf.x(), sf.y());

        // Should be approximately 4,130,000 meters
        assert!(distance > 4_100_000.0 && distance < 4_200_000.0);
    }

    #[test]
    fn test_distance_to_point() {
        let nyc_point = Point::new(-74.0060, 40.7128);
        let nyc_obj = SpatialObject::from_point("nyc".to_string(), nyc_point, Bytes::new());

        let sf = Point::new(-122.4194, 37.7749);
        let distance = nyc_obj.distance_to_point(&sf);

        assert!(distance > 4_100_000.0 && distance < 4_200_000.0);
    }

    #[test]
    fn test_intersects_bbox() {
        let obj = SpatialObject::from_point("test".to_string(), Point::new(0.0, 0.0), Bytes::new());

        let bbox1 = Rect::new(
            geo::coord! { x: -1.0, y: -1.0 },
            geo::coord! { x: 1.0, y: 1.0 },
        );
        assert!(obj.intersects_bbox(&bbox1));

        let bbox2 = Rect::new(
            geo::coord! { x: 10.0, y: 10.0 },
            geo::coord! { x: 20.0, y: 20.0 },
        );
        assert!(!obj.intersects_bbox(&bbox2));
    }

    #[test]
    fn test_contains_point() {
        let bbox = Rect::new(
            geo::coord! { x: -1.0, y: -1.0 },
            geo::coord! { x: 1.0, y: 1.0 },
        );
        let obj = SpatialObject::from_bbox("test".to_string(), &bbox, Bytes::new());

        assert!(obj.contains_point(&Point::new(0.0, 0.0)));
        assert!(obj.contains_point(&Point::new(0.5, 0.5)));
        assert!(!obj.contains_point(&Point::new(2.0, 2.0)));
    }

    #[test]
    fn test_haversine_distance_3d() {
        let distance = haversine_distance_3d(0.0, 0.0, 0.0, 0.0, 0.0, 100.0);

        // Pure vertical distance
        assert_eq!(distance, 100.0);
    }
}
