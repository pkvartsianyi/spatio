//! Spatial utilities for Spatio
//!
//! This module provides core spatial functionality for geographic points
//! and basic spatial operations.

use crate::error::{Result, SpatioError};
use geo;
use geohash;
use s2::cellid::CellID;
use serde::{Deserialize, Serialize};
use std::fmt;

/// A geographic point representing a location on Earth's surface.
///
/// `Point` stores latitude and longitude coordinates and provides methods
/// for spatial operations, distance calculations, and spatial indexing.
/// All coordinates use the WGS84 coordinate reference system (EPSG:4326).
///
/// # Examples
///
/// ```rust
/// use spatio::Point;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// // Major world cities
/// let new_york = Point::new(40.7128, -74.0060);
/// let london = Point::new(51.5074, -0.1278);
///
/// // Calculate distance between cities
/// let distance_km = new_york.distance_to(&london) / 1000.0;
/// println!("NYC to London: {:.0} km", distance_km);
///
/// // Generate spatial index keys
/// let geohash = new_york.to_geohash(8)?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Point {
    /// Latitude in decimal degrees (-90.0 to +90.0)
    pub lat: f64,
    /// Longitude in decimal degrees (-180.0 to +180.0)
    pub lon: f64,
}

impl Point {
    /// Creates a new point from latitude and longitude coordinates.
    ///
    /// # Arguments
    ///
    /// * `lat` - Latitude in decimal degrees (-90° to +90°)
    /// * `lon` - Longitude in decimal degrees (-180° to +180°)
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spatio::Point;
    ///
    /// // Empire State Building
    /// let empire_state = Point::new(40.7484, -73.9857);
    ///
    /// // Sydney Opera House
    /// let opera_house = Point::new(-33.8568, 151.2153);
    /// ```
    pub fn new(lat: f64, lon: f64) -> Self {
        Self { lat, lon }
    }

    /// Calculate the distance between two points using the Haversine formula.
    ///
    /// This method calculates the great-circle distance between two points
    /// on Earth's surface, accounting for Earth's curvature. The result
    /// is returned in meters.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spatio::Point;
    ///
    /// let new_york = Point::new(40.7128, -74.0060);
    /// let london = Point::new(51.5074, -0.1278);
    ///
    /// let distance_m = new_york.distance_to(&london);
    /// let distance_km = distance_m / 1000.0;
    /// println!("Distance: {:.0} km", distance_km);
    /// ```
    pub fn distance_to(&self, other: &Point) -> f64 {
        const EARTH_RADIUS_M: f64 = 6_371_000.0;
        const TO_RAD: f64 = std::f64::consts::PI / 180.0;

        let lat1 = self.lat * TO_RAD;
        let lat2 = other.lat * TO_RAD;
        let dlat = (other.lat - self.lat) * TO_RAD;
        let dlon = (other.lon - self.lon) * TO_RAD;

        let half_dlat = dlat * 0.5;
        let half_dlon = dlon * 0.5;
        let sin_half_dlat = half_dlat.sin();
        let sin_half_dlon = half_dlon.sin();

        let a =
            sin_half_dlat * sin_half_dlat + lat1.cos() * lat2.cos() * sin_half_dlon * sin_half_dlon;
        let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());

        EARTH_RADIUS_M * c
    }

    /// Generate a geohash string for this point.
    ///
    /// Geohash is a geocoding system that represents geographic coordinates
    /// as a short string. Higher precision values result in more precise
    /// location encoding but longer strings.
    ///
    /// # Arguments
    ///
    /// * `precision` - Number of characters in the geohash (1-12)
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spatio::Point;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let point = Point::new(40.7128, -74.0060); // NYC
    ///
    /// // Different precision levels for different use cases
    /// let coarse_geohash = point.to_geohash(5)?;  // ~5km precision
    /// let default_geohash = point.to_geohash(8)?; // ~39m precision (default)
    /// let fine_geohash = point.to_geohash(10)?;   // ~61cm precision
    ///
    /// println!("Coarse: {}", coarse_geohash);   // e.g., "dr5re"
    /// println!("Default: {}", default_geohash); // e.g., "dr5regw3"
    /// println!("Fine: {}", fine_geohash);       // e.g., "dr5regw3kg"
    /// # Ok(())
    /// # }
    /// ```
    pub fn to_geohash(&self, precision: usize) -> Result<String> {
        geohash::encode(
            geo::Coord {
                x: self.lon,
                y: self.lat,
            },
            precision,
        )
        .map_err(|_| SpatioError::InvalidGeohash)
    }

    /// Generate an S2 cell ID for this point.
    ///
    /// S2 is Google's library for spherical geometry. It represents
    /// Earth's surface as a hierarchy of cells that can be used for
    /// spatial indexing and proximity queries.
    ///
    /// # Arguments
    ///
    /// * `level` - S2 cell level (0-30, higher = more precise)
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spatio::Point;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let point = Point::new(40.7128, -74.0060);
    /// let s2_cell = point.to_s2_cell(16)?;
    /// println!("S2 cell: {}", s2_cell.0);
    /// # Ok(())
    /// # }
    /// ```
    pub fn to_s2_cell(&self, level: u8) -> Result<CellID> {
        if level > 30 {
            return Err(SpatioError::Other("S2 level must be <= 30".to_string()));
        }

        // Simplified S2 cell ID generation
        // Map lat/lon to cell coordinates and combine with level
        let lat_norm = ((self.lat + 90.0) / 180.0 * ((1u64 << level) as f64)) as u64;
        let lon_norm = ((self.lon + 180.0) / 360.0 * ((1u64 << level) as f64)) as u64;

        let cell_value = (level as u64) << 56 | lat_norm << 28 | lon_norm;
        Ok(CellID(cell_value))
    }

    /// Check if this point is within the given bounding box.
    ///
    /// # Arguments
    ///
    /// * `min_lat` - Minimum latitude of bounding box
    /// * `min_lon` - Minimum longitude of bounding box
    /// * `max_lat` - Maximum latitude of bounding box
    /// * `max_lon` - Maximum longitude of bounding box
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spatio::Point;
    ///
    /// let point = Point::new(40.7128, -74.0060); // NYC
    /// let in_usa = point.within_bounds(24.0, -125.0, 49.0, -66.0);
    /// assert!(in_usa);
    /// ```
    pub fn within_bounds(&self, min_lat: f64, min_lon: f64, max_lat: f64, max_lon: f64) -> bool {
        self.lat >= min_lat && self.lat <= max_lat && self.lon >= min_lon && self.lon <= max_lon
    }

    /// Check if this point is within a circular region defined by center and radius.
    ///
    /// # Arguments
    ///
    /// * `center` - Center point of the circular region
    /// * `radius_meters` - Radius of the circle in meters
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spatio::Point;
    ///
    /// let center = Point::new(40.7128, -74.0060); // NYC
    /// let point = Point::new(40.7150, -74.0040);  // Close to NYC
    ///
    /// let within_1km = point.within_distance(&center, 1000.0);
    /// assert!(within_1km);
    /// ```
    pub fn within_distance(&self, center: &Point, radius_meters: f64) -> bool {
        // For very small distances, use simple approximation to avoid expensive trig
        if radius_meters < 100.0 {
            const TO_RAD: f64 = std::f64::consts::PI / 180.0;
            const EARTH_RADIUS_M: f64 = 6_371_000.0;

            let dlat = (self.lat - center.lat) * TO_RAD;
            let dlon = (self.lon - center.lon) * TO_RAD;
            let avg_lat = (self.lat + center.lat) * 0.5 * TO_RAD;

            let x = dlon * avg_lat.cos() * EARTH_RADIUS_M;
            let y = dlat * EARTH_RADIUS_M;
            let distance_approx = (x * x + y * y).sqrt();

            return distance_approx <= radius_meters;
        }

        self.distance_to(center) <= radius_meters
    }

    /// Check if two bounding boxes intersect.
    ///
    /// This is a convenience method that creates BoundingBox instances and checks intersection.
    ///
    /// # Arguments
    ///
    /// * `min_lat1, min_lon1, max_lat1, max_lon1` - First bounding box
    /// * `min_lat2, min_lon2, max_lat2, max_lon2` - Second bounding box
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spatio::Point;
    ///
    /// // Check if NYC area intersects with general New York state area
    /// let intersects = Point::intersects_bounds(
    ///     40.5, -74.5, 41.0, -73.5,  // NYC area
    ///     40.0, -80.0, 45.0, -71.0   // NY state area
    /// );
    /// assert!(intersects);
    /// ```
    #[allow(clippy::too_many_arguments)]
    pub fn intersects_bounds(
        min_lat1: f64,
        min_lon1: f64,
        max_lat1: f64,
        max_lon1: f64,
        min_lat2: f64,
        min_lon2: f64,
        max_lat2: f64,
        max_lon2: f64,
    ) -> bool {
        let bbox1 = BoundingBox::new(min_lat1, min_lon1, max_lat1, max_lon1);
        let bbox2 = BoundingBox::new(min_lat2, min_lon2, max_lat2, max_lon2);
        bbox1.intersects(&bbox2)
    }

    /// Check if this point contains another point within a specified radius.
    /// This is essentially the same as `within_distance` but with reversed semantics.
    ///
    /// # Arguments
    ///
    /// * `other` - The point to check
    /// * `radius_meters` - Radius in meters for containment
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spatio::Point;
    ///
    /// let center = Point::new(40.7128, -74.0060); // NYC
    /// let point = Point::new(40.7150, -74.0040);  // Close to NYC
    ///
    /// let contains = center.contains_point(&point, 1000.0);
    /// assert!(contains);
    /// ```
    pub fn contains_point(&self, other: &Point, radius_meters: f64) -> bool {
        self.distance_to(other) <= radius_meters
    }
}

/// A bounding box defined by minimum and maximum latitude and longitude coordinates.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BoundingBox {
    pub min_lat: f64,
    pub min_lon: f64,
    pub max_lat: f64,
    pub max_lon: f64,
}

impl BoundingBox {
    /// Create a new bounding box
    pub fn new(min_lat: f64, min_lon: f64, max_lat: f64, max_lon: f64) -> Self {
        Self {
            min_lat,
            min_lon,
            max_lat,
            max_lon,
        }
    }

    /// Check if this bounding box intersects with another bounding box.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spatio::BoundingBox;
    ///
    /// let nyc_area = BoundingBox::new(40.5, -74.5, 41.0, -73.5);
    /// let ny_state = BoundingBox::new(40.0, -80.0, 45.0, -71.0);
    ///
    /// assert!(nyc_area.intersects(&ny_state));
    /// ```
    pub fn intersects(&self, other: &BoundingBox) -> bool {
        !(self.max_lat < other.min_lat
            || self.min_lat > other.max_lat
            || self.max_lon < other.min_lon
            || self.min_lon > other.max_lon)
    }
}

impl fmt::Display for Point {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({:.6}, {:.6})", self.lat, self.lon)
    }
}

/// Spatial key generation utilities for database storage.
///
/// This struct provides methods to generate keys for spatial indexing
/// based on different spatial indexing strategies.
pub struct SpatialKey;

impl SpatialKey {
    /// Generate a geohash-based key for database storage.
    ///
    /// # Arguments
    ///
    /// * `prefix` - Namespace prefix for the key
    /// * `geohash` - The geohash string
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spatio::spatial::SpatialKey;
    ///
    /// let key = SpatialKey::geohash("cities", "dr5regw3");
    /// assert_eq!(key, "cities:gh:dr5regw3");
    /// ```
    pub fn geohash(prefix: &str, geohash: &str) -> String {
        format!("{}:gh:{}", prefix, geohash)
    }

    /// Generate an S2 cell-based key for database storage.
    ///
    /// # Arguments
    ///
    /// * `prefix` - Namespace prefix for the key
    /// * `cell_id` - The S2 cell ID
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spatio::spatial::SpatialKey;
    /// use s2::cellid::CellID;
    ///
    /// let cell_id = CellID(1234567890);
    /// let key = SpatialKey::s2_cell("sensors", cell_id);
    /// assert_eq!(key, "sensors:s2:1234567890");
    /// ```
    pub fn s2_cell(prefix: &str, cell_id: CellID) -> String {
        format!("{}:s2:{}", prefix, cell_id.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_point_creation() {
        let point = Point::new(40.7128, -74.0060);
        assert_eq!(point.lat, 40.7128);
        assert_eq!(point.lon, -74.0060);
    }

    #[test]
    fn test_distance_calculation() {
        let new_york = Point::new(40.7128, -74.0060);
        let london = Point::new(51.5074, -0.1278);

        let distance = new_york.distance_to(&london);
        // Distance should be approximately 5585 km
        assert!((distance - 5_585_000.0).abs() < 50_000.0);
    }

    #[test]
    fn test_geohash_generation() {
        let point = Point::new(40.7128, -74.0060);
        let geohash = point.to_geohash(8).unwrap();
        assert_eq!(geohash.len(), 8);
    }

    #[test]
    fn test_s2_cell_generation() {
        let point = Point::new(40.7128, -74.0060);
        let s2_cell = point.to_s2_cell(16).unwrap();
        assert!(s2_cell.0 > 0);
    }

    #[test]
    fn test_within_bounds() {
        let point = Point::new(40.7128, -74.0060);

        // Should be within USA bounds
        assert!(point.within_bounds(24.0, -125.0, 49.0, -66.0));

        // Should not be within Europe bounds
        assert!(!point.within_bounds(35.0, -10.0, 70.0, 40.0));
    }

    #[test]
    fn test_spatial_key_generation() {
        let geohash_key = SpatialKey::geohash("cities", "dr5regw3");
        assert_eq!(geohash_key, "cities:gh:dr5regw3");

        let s2_key = SpatialKey::s2_cell("sensors", CellID(1234567890));
        assert_eq!(s2_key, "sensors:s2:1234567890");
    }

    #[test]
    fn test_point_display() {
        let point = Point::new(40.7128, -74.0060);
        let display = format!("{}", point);
        assert_eq!(display, "(40.712800, -74.006000)");
    }

    #[test]
    fn test_within_distance() {
        let nyc = Point::new(40.7128, -74.0060);
        let brooklyn = Point::new(40.6782, -73.9442);
        let london = Point::new(51.5074, -0.1278);

        // Brooklyn should be within 20km of NYC
        assert!(brooklyn.within_distance(&nyc, 20_000.0));

        // London should not be within 1000km of NYC
        assert!(!london.within_distance(&nyc, 1_000_000.0));

        // Point should be within 0 distance of itself
        assert!(nyc.within_distance(&nyc, 0.0));
    }

    #[test]
    fn test_contains_point() {
        let nyc = Point::new(40.7128, -74.0060);
        let brooklyn = Point::new(40.6782, -73.9442);
        let london = Point::new(51.5074, -0.1278);

        // NYC should contain Brooklyn within 20km
        assert!(nyc.contains_point(&brooklyn, 20_000.0));

        // NYC should not contain London within 1000km
        assert!(!nyc.contains_point(&london, 1_000_000.0));

        // Point should contain itself within any positive radius
        assert!(nyc.contains_point(&nyc, 1.0));
    }

    #[test]
    fn test_intersects_bounds() {
        // Test overlapping bounds
        assert!(Point::intersects_bounds(
            40.0, -75.0, 41.0, -73.0, // NYC area
            40.5, -74.5, 40.8, -74.0 // Manhattan area (overlaps)
        ));

        // Test non-overlapping bounds
        assert!(!Point::intersects_bounds(
            40.0, -75.0, 41.0, -73.0, // NYC area
            51.0, -1.0, 52.0, 1.0 // London area (no overlap)
        ));

        // Test identical bounds
        assert!(Point::intersects_bounds(
            40.0, -75.0, 41.0, -73.0, 40.0, -75.0, 41.0, -73.0
        ));

        // Test touching bounds (should intersect)
        assert!(Point::intersects_bounds(
            40.0, -75.0, 41.0, -73.0, 41.0, -75.0, 42.0, -73.0
        ));

        // Test completely separate bounds
        assert!(!Point::intersects_bounds(
            40.0, -75.0, 41.0, -73.0, 42.0, -75.0, 43.0, -73.0
        ));
    }

    #[test]
    fn test_bounding_box() {
        let bbox1 = BoundingBox::new(40.0, -75.0, 41.0, -73.0);
        let bbox2 = BoundingBox::new(40.5, -74.5, 40.8, -74.0);
        let bbox3 = BoundingBox::new(51.0, -1.0, 52.0, 1.0);

        // Test overlapping boxes
        assert!(bbox1.intersects(&bbox2));
        assert!(bbox2.intersects(&bbox1));

        // Test non-overlapping boxes
        assert!(!bbox1.intersects(&bbox3));
        assert!(!bbox3.intersects(&bbox1));

        // Test identical boxes
        assert!(bbox1.intersects(&bbox1));
    }
}
