use geo::Point;
use serde::{Deserialize, Serialize};
use std::time::SystemTime;

/// A 3D geographic point with x, y (longitude/latitude) and z (altitude/elevation).
///
/// This type represents a point in 3D space, typically used for altitude-aware
/// geospatial applications like drone tracking, aviation, or multi-floor buildings.
///
/// # Examples
///
/// ```
/// use spatio_types::point::Point3d;
/// use geo::Point;
///
/// // Create a 3D point for a drone at 100 meters altitude
/// let drone_position = Point3d::new(-74.0060, 40.7128, 100.0);
/// assert_eq!(drone_position.altitude(), 100.0);
///
/// // Calculate 3D distance to another point
/// let other = Point3d::new(-74.0070, 40.7138, 150.0);
/// let distance = drone_position.distance_3d(&other);
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Point3d {
    /// The 2D geographic point (longitude/latitude or x/y)
    pub point: Point<f64>,
    /// The altitude/elevation/z-coordinate (in meters typically)
    pub z: f64,
}

impl Point3d {
    /// Create a new 3D point from x, y, and z coordinates.
    ///
    /// # Arguments
    ///
    /// * `x` - Longitude or x-coordinate
    /// * `y` - Latitude or y-coordinate
    /// * `z` - Altitude/elevation in meters
    ///
    /// # Examples
    ///
    /// ```
    /// use spatio_types::point::Point3d;
    ///
    /// let point = Point3d::new(-74.0060, 40.7128, 100.0);
    /// ```
    pub fn new(x: f64, y: f64, z: f64) -> Self {
        Self {
            point: Point::new(x, y),
            z,
        }
    }

    /// Create a 3D point from a 2D point and altitude.
    pub fn from_point_and_altitude(point: Point<f64>, z: f64) -> Self {
        Self { point, z }
    }

    /// Get the x coordinate (longitude).
    pub fn x(&self) -> f64 {
        self.point.x()
    }

    /// Get the y coordinate (latitude).
    pub fn y(&self) -> f64 {
        self.point.y()
    }

    /// Get the z coordinate (altitude/elevation).
    pub fn z(&self) -> f64 {
        self.z
    }

    /// Get the altitude (alias for z()).
    pub fn altitude(&self) -> f64 {
        self.z
    }

    /// Get a reference to the underlying 2D point.
    pub fn point_2d(&self) -> &Point<f64> {
        &self.point
    }

    /// Project this 3D point to 2D by discarding the z coordinate.
    pub fn to_2d(&self) -> Point<f64> {
        self.point
    }

    /// Calculate the 3D Euclidean distance to another 3D point.
    ///
    /// This calculates the straight-line distance in 3D space using the Pythagorean theorem.
    /// Note: For geographic coordinates, this treats lat/lon as Cartesian coordinates,
    /// which is only accurate for small distances. For large distances, use `haversine_3d`.
    ///
    /// # Examples
    ///
    /// ```
    /// use spatio_types::point::Point3d;
    ///
    /// let p1 = Point3d::new(0.0, 0.0, 0.0);
    /// let p2 = Point3d::new(3.0, 4.0, 12.0);
    /// let distance = p1.distance_3d(&p2);
    /// assert_eq!(distance, 13.0); // 3-4-5 triangle extended to 3D
    /// ```
    pub fn distance_3d(&self, other: &Point3d) -> f64 {
        let dx = self.x() - other.x();
        let dy = self.y() - other.y();
        let dz = self.z - other.z;
        (dx * dx + dy * dy + dz * dz).sqrt()
    }

    /// Calculate all distance components at once (horizontal, altitude, 3D).
    ///
    /// This is more efficient than calling haversine_2d and haversine_3d separately
    /// as it calculates the haversine formula only once.
    ///
    /// # Returns
    ///
    /// Tuple of (horizontal_distance, altitude_difference, distance_3d) in meters.
    ///
    /// # Examples
    ///
    /// ```
    /// use spatio_types::point::Point3d;
    ///
    /// let p1 = Point3d::new(-74.0060, 40.7128, 0.0);
    /// let p2 = Point3d::new(-74.0070, 40.7138, 100.0);
    /// let (h_dist, alt_diff, dist_3d) = p1.haversine_distances(&p2);
    /// ```
    pub fn haversine_distances(&self, other: &Point3d) -> (f64, f64, f64) {
        const EARTH_RADIUS_METERS: f64 = 6_371_000.0;

        let lat1 = self.y().to_radians();
        let lat2 = other.y().to_radians();
        let delta_lat = (other.y() - self.y()).to_radians();
        let delta_lon = (other.x() - self.x()).to_radians();

        let a = (delta_lat / 2.0).sin().powi(2)
            + lat1.cos() * lat2.cos() * (delta_lon / 2.0).sin().powi(2);
        let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());

        let horizontal_distance = EARTH_RADIUS_METERS * c;
        let altitude_diff = (self.z - other.z).abs();
        let distance_3d =
            (horizontal_distance * horizontal_distance + altitude_diff * altitude_diff).sqrt();

        (horizontal_distance, altitude_diff, distance_3d)
    }

    /// Calculate the haversine distance combined with altitude difference.
    ///
    /// This uses the haversine formula for the horizontal distance (considering Earth's curvature)
    /// and combines it with the altitude difference using the Pythagorean theorem.
    ///
    /// # Returns
    ///
    /// Distance in meters.
    ///
    /// # Examples
    ///
    /// ```
    /// use spatio_types::point::Point3d;
    ///
    /// let p1 = Point3d::new(-74.0060, 40.7128, 0.0);    // NYC sea level
    /// let p2 = Point3d::new(-74.0070, 40.7138, 100.0);  // Nearby, 100m up
    /// let distance = p1.haversine_3d(&p2);
    /// ```
    #[inline]
    pub fn haversine_3d(&self, other: &Point3d) -> f64 {
        let (_, _, dist_3d) = self.haversine_distances(other);
        dist_3d
    }

    /// Calculate the haversine distance on the 2D plane (ignoring altitude).
    ///
    /// # Returns
    ///
    /// Distance in meters.
    #[inline]
    pub fn haversine_2d(&self, other: &Point3d) -> f64 {
        const EARTH_RADIUS_METERS: f64 = 6_371_000.0;

        let lat1 = self.y().to_radians();
        let lat2 = other.y().to_radians();
        let delta_lat = (other.y() - self.y()).to_radians();
        let delta_lon = (other.x() - self.x()).to_radians();

        let a = (delta_lat / 2.0).sin().powi(2)
            + lat1.cos() * lat2.cos() * (delta_lon / 2.0).sin().powi(2);
        let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());

        EARTH_RADIUS_METERS * c
    }

    /// Get the altitude difference to another point.
    #[inline]
    pub fn altitude_difference(&self, other: &Point3d) -> f64 {
        (self.z - other.z).abs()
    }
}

/// A geographic point with an associated timestamp.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TemporalPoint {
    pub point: Point<f64>,
    pub timestamp: SystemTime,
}

impl TemporalPoint {
    pub fn new(point: Point<f64>, timestamp: SystemTime) -> Self {
        Self { point, timestamp }
    }

    pub fn point(&self) -> &Point<f64> {
        &self.point
    }

    pub fn timestamp(&self) -> &SystemTime {
        &self.timestamp
    }
}

/// A geographic point with an associated altitude and timestamp.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TemporalPoint3D {
    pub point: Point<f64>,
    pub altitude: f64,
    pub timestamp: SystemTime,
}

impl TemporalPoint3D {
    pub fn new(point: Point<f64>, altitude: f64, timestamp: SystemTime) -> Self {
        Self {
            point,
            altitude,
            timestamp,
        }
    }

    pub fn point(&self) -> &Point<f64> {
        &self.point
    }

    pub fn altitude(&self) -> f64 {
        self.altitude
    }

    pub fn timestamp(&self) -> &SystemTime {
        &self.timestamp
    }

    /// Convert to a 3D point.
    pub fn to_point_3d(&self) -> Point3d {
        Point3d::from_point_and_altitude(self.point, self.altitude)
    }

    /// Calculate 3D haversine distance to another temporal 3D point.
    pub fn distance_to(&self, other: &TemporalPoint3D) -> f64 {
        self.to_point_3d().haversine_3d(&other.to_point_3d())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_point3d_creation() {
        let p = Point3d::new(-74.0, 40.7, 100.0);
        assert_eq!(p.x(), -74.0);
        assert_eq!(p.y(), 40.7);
        assert_eq!(p.z(), 100.0);
        assert_eq!(p.altitude(), 100.0);
    }

    #[test]
    fn test_point3d_distance_3d() {
        let p1 = Point3d::new(0.0, 0.0, 0.0);
        let p2 = Point3d::new(3.0, 4.0, 12.0);
        let distance = p1.distance_3d(&p2);
        assert_eq!(distance, 13.0);
    }

    #[test]
    fn test_point3d_altitude_difference() {
        let p1 = Point3d::new(-74.0, 40.7, 50.0);
        let p2 = Point3d::new(-74.0, 40.7, 150.0);
        assert_eq!(p1.altitude_difference(&p2), 100.0);
    }

    #[test]
    fn test_point3d_to_2d() {
        let p = Point3d::new(-74.0, 40.7, 100.0);
        let p2d = p.to_2d();
        assert_eq!(p2d.x(), -74.0);
        assert_eq!(p2d.y(), 40.7);
    }

    #[test]
    fn test_haversine_3d() {
        // Two points at same location but different altitudes
        let p1 = Point3d::new(-74.0, 40.7, 0.0);
        let p2 = Point3d::new(-74.0, 40.7, 100.0);
        let distance = p1.haversine_3d(&p2);
        // Should be approximately 100 meters (just the altitude difference)
        assert!((distance - 100.0).abs() < 0.1);
    }

    #[test]
    fn test_haversine_distances() {
        let p1 = Point3d::new(-74.0060, 40.7128, 0.0);
        let p2 = Point3d::new(-74.0070, 40.7138, 100.0);
        let (h_dist, alt_diff, dist_3d) = p1.haversine_distances(&p2);

        // Verify altitude difference is correct
        assert_eq!(alt_diff, 100.0);

        // Verify 3D distance is correct
        assert!((dist_3d - (h_dist * h_dist + alt_diff * alt_diff).sqrt()).abs() < 0.1);

        // Verify it matches individual calls
        assert!((h_dist - p1.haversine_2d(&p2)).abs() < 0.1);
        assert!((dist_3d - p1.haversine_3d(&p2)).abs() < 0.1);
    }

    #[test]
    fn test_temporal_point3d_to_point3d() {
        let temporal = TemporalPoint3D::new(Point::new(-74.0, 40.7), 100.0, SystemTime::now());
        let p3d = temporal.to_point_3d();
        assert_eq!(p3d.x(), -74.0);
        assert_eq!(p3d.y(), 40.7);
        assert_eq!(p3d.altitude(), 100.0);
    }
}
