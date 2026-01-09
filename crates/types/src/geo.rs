//! Wrapped geometric types from the `geo` crate with spatio-specific functionality.
//!
//! This module provides wrapper types around `geo` crate primitives with additional
//! methods for GeoJSON serialization, distance calculations, and other spatial operations.

use serde::{Deserialize, Serialize};

/// Error type for GeoJSON conversions.
#[derive(Debug)]
pub enum GeoJsonError {
    /// Serialization failed
    Serialization(String),
    /// Deserialization failed
    Deserialization(String),
    /// Invalid geometry type
    InvalidGeometry(String),
    /// Invalid coordinates
    InvalidCoordinates(String),
}

/// Distance metric for spatial calculations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum DistanceMetric {
    #[default]
    Haversine,
    Geodesic,
    Rhumb,
    Euclidean,
}

impl std::fmt::Display for GeoJsonError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Serialization(msg) => write!(f, "GeoJSON serialization error: {}", msg),
            Self::Deserialization(msg) => write!(f, "GeoJSON deserialization error: {}", msg),
            Self::InvalidGeometry(msg) => write!(f, "Invalid GeoJSON geometry: {}", msg),
            Self::InvalidCoordinates(msg) => write!(f, "Invalid GeoJSON coordinates: {}", msg),
        }
    }
}

impl std::error::Error for GeoJsonError {}

/// A geographic point with longitude/latitude coordinates.
///
/// This wraps `geo::Point` and provides additional functionality for
/// GeoJSON conversion, distance calculations, and other operations.
///
/// # Examples
///
/// ```
/// use spatio_types::geo::Point;
///
/// let nyc = Point::new(-74.0060, 40.7128);
/// assert_eq!(nyc.x(), -74.0060);
/// assert_eq!(nyc.y(), 40.7128);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Point {
    inner: geo::Point<f64>,
}

impl Point {
    /// Create a new point from x (longitude) and y (latitude) coordinates.
    ///
    /// # Arguments
    ///
    /// * `x` - Longitude in degrees (typically -180 to 180)
    /// * `y` - Latitude in degrees (typically -90 to 90)
    ///
    /// # Examples
    ///
    /// ```
    /// use spatio_types::geo::Point;
    ///
    /// let point = Point::new(-74.0060, 40.7128);
    /// ```
    #[inline]
    pub fn new(x: f64, y: f64) -> Self {
        Self {
            inner: geo::Point::new(x, y),
        }
    }

    /// Get the x coordinate (longitude).
    #[inline]
    pub fn x(&self) -> f64 {
        self.inner.x()
    }

    /// Get the y coordinate (latitude).
    #[inline]
    pub fn y(&self) -> f64 {
        self.inner.y()
    }

    /// Get the longitude (alias for x).
    #[inline]
    pub fn lon(&self) -> f64 {
        self.x()
    }

    /// Get the latitude (alias for y).
    #[inline]
    pub fn lat(&self) -> f64 {
        self.y()
    }

    /// Access the inner `geo::Point`.
    #[inline]
    pub fn inner(&self) -> &geo::Point<f64> {
        &self.inner
    }

    /// Convert into the inner `geo::Point`.
    #[inline]
    pub fn into_inner(self) -> geo::Point<f64> {
        self.inner
    }

    /// Calculate haversine distance to another point in meters.
    ///
    /// Uses the haversine formula which accounts for Earth's curvature.
    ///
    /// # Examples
    ///
    /// ```
    /// use spatio_types::geo::Point;
    ///
    /// let nyc = Point::new(-74.0060, 40.7128);
    /// let la = Point::new(-118.2437, 34.0522);
    /// let distance = nyc.haversine_distance(&la);
    /// assert!(distance > 3_900_000.0); // ~3,944 km
    /// ```
    #[inline]
    pub fn haversine_distance(&self, other: &Point) -> f64 {
        use geo::Distance;
        geo::Haversine.distance(self.inner, other.inner)
    }

    /// Calculate geodesic distance to another point in meters.
    ///
    /// Uses the Vincenty formula which is more accurate than haversine
    /// but slightly slower.
    ///
    /// # Examples
    ///
    /// ```
    /// use spatio_types::geo::Point;
    ///
    /// let p1 = Point::new(-74.0060, 40.7128);
    /// let p2 = Point::new(-74.0070, 40.7138);
    /// let distance = p1.geodesic_distance(&p2);
    /// ```
    #[inline]
    pub fn geodesic_distance(&self, other: &Point) -> f64 {
        use geo::Distance;
        geo::Geodesic.distance(self.inner, other.inner)
    }

    /// Calculate euclidean distance to another point.
    ///
    /// This calculates straight-line distance in the coordinate space,
    /// which is only accurate for small distances.
    ///
    /// # Examples
    ///
    /// ```
    /// use spatio_types::geo::Point;
    ///
    /// let p1 = Point::new(0.0, 0.0);
    /// let p2 = Point::new(3.0, 4.0);
    /// let distance = p1.euclidean_distance(&p2);
    /// assert_eq!(distance, 5.0); // 3-4-5 triangle
    /// ```
    #[inline]
    pub fn euclidean_distance(&self, other: &Point) -> f64 {
        use geo::Distance;
        geo::Euclidean.distance(self.inner, other.inner)
    }

    /// Convert to GeoJSON string representation.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "geojson")]
    /// # {
    /// use spatio_types::geo::Point;
    ///
    /// let point = Point::new(-74.0060, 40.7128);
    /// let json = point.to_geojson().unwrap();
    /// assert!(json.contains("Point"));
    /// # }
    /// ```
    #[cfg(feature = "geojson")]
    pub fn to_geojson(&self) -> Result<String, GeoJsonError> {
        use geojson::{Geometry, Value};

        let geom = Geometry::new(Value::Point(vec![self.x(), self.y()]));
        serde_json::to_string(&geom)
            .map_err(|e| GeoJsonError::Serialization(format!("Failed to serialize point: {}", e)))
    }

    /// Parse from GeoJSON string.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "geojson")]
    /// # {
    /// use spatio_types::geo::Point;
    ///
    /// let json = r#"{"type":"Point","coordinates":[-74.006,40.7128]}"#;
    /// let point = Point::from_geojson(json).unwrap();
    /// assert_eq!(point.x(), -74.006);
    /// # }
    /// ```
    #[cfg(feature = "geojson")]
    pub fn from_geojson(geojson: &str) -> Result<Self, GeoJsonError> {
        use geojson::{Geometry, Value};

        let geom: Geometry = serde_json::from_str(geojson).map_err(|e| {
            GeoJsonError::Deserialization(format!("Failed to parse GeoJSON: {}", e))
        })?;

        match geom.value {
            Value::Point(coords) => {
                if coords.len() < 2 {
                    return Err(GeoJsonError::InvalidCoordinates(
                        "Point must have at least 2 coordinates".to_string(),
                    ));
                }
                Ok(Point::new(coords[0], coords[1]))
            }
            _ => Err(GeoJsonError::InvalidGeometry(
                "GeoJSON geometry is not a Point".to_string(),
            )),
        }
    }
}

impl From<geo::Point<f64>> for Point {
    fn from(point: geo::Point<f64>) -> Self {
        Self { inner: point }
    }
}

impl From<Point> for geo::Point<f64> {
    fn from(point: Point) -> Self {
        point.inner
    }
}

impl From<(f64, f64)> for Point {
    fn from((x, y): (f64, f64)) -> Self {
        Self::new(x, y)
    }
}

impl From<Point> for (f64, f64) {
    fn from(point: Point) -> Self {
        (point.x(), point.y())
    }
}

/// A polygon with exterior and optional interior rings.
///
/// This wraps `geo::Polygon` and provides additional functionality for
/// GeoJSON conversion and spatial operations.
///
/// # Examples
///
/// ```
/// use spatio_types::geo::Polygon;
/// use geo::polygon;
///
/// let poly = polygon![
///     (x: -80.0, y: 35.0),
///     (x: -70.0, y: 35.0),
///     (x: -70.0, y: 45.0),
///     (x: -80.0, y: 45.0),
///     (x: -80.0, y: 35.0),
/// ];
/// let wrapped = Polygon::from(poly);
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Polygon {
    inner: geo::Polygon<f64>,
}

impl Polygon {
    /// Create a new polygon from an exterior ring and optional interior rings (holes).
    ///
    /// # Arguments
    ///
    /// * `exterior` - The outer boundary of the polygon
    /// * `interiors` - Optional holes within the polygon
    pub fn new(exterior: geo::LineString<f64>, interiors: Vec<geo::LineString<f64>>) -> Self {
        Self {
            inner: geo::Polygon::new(exterior, interiors),
        }
    }

    /// Create a new polygon from coordinate arrays without requiring `geo::LineString`.
    ///
    /// This is a convenience method that allows creating polygons from raw coordinates
    /// without needing to import types from the `geo` crate.
    ///
    /// # Arguments
    ///
    /// * `exterior` - Coordinates for the outer boundary [(lon, lat), ...]
    /// * `interiors` - Optional holes within the polygon, each as [(lon, lat), ...]
    ///
    /// # Examples
    ///
    /// ```
    /// use spatio_types::geo::Polygon;
    ///
    /// // Create a simple rectangle
    /// let polygon = Polygon::from_coords(
    ///     &[
    ///         (-80.0, 35.0),
    ///         (-70.0, 35.0),
    ///         (-70.0, 45.0),
    ///         (-80.0, 45.0),
    ///         (-80.0, 35.0),  // Close the ring
    ///     ],
    ///     vec![],
    /// );
    ///
    /// // Create a polygon with a hole
    /// let polygon_with_hole = Polygon::from_coords(
    ///     &[
    ///         (-80.0, 35.0),
    ///         (-70.0, 35.0),
    ///         (-70.0, 45.0),
    ///         (-80.0, 45.0),
    ///         (-80.0, 35.0),
    ///     ],
    ///     vec![
    ///         vec![
    ///             (-75.0, 38.0),
    ///             (-74.0, 38.0),
    ///             (-74.0, 40.0),
    ///             (-75.0, 40.0),
    ///             (-75.0, 38.0),
    ///         ]
    ///     ],
    /// );
    /// ```
    pub fn from_coords(exterior: &[(f64, f64)], interiors: Vec<Vec<(f64, f64)>>) -> Self {
        let exterior_coords: Vec<geo::Coord> =
            exterior.iter().map(|&(x, y)| geo::Coord { x, y }).collect();
        let exterior_line = geo::LineString::from(exterior_coords);

        let interior_lines: Vec<geo::LineString<f64>> = interiors
            .into_iter()
            .map(|interior| {
                let coords: Vec<geo::Coord> = interior
                    .into_iter()
                    .map(|(x, y)| geo::Coord { x, y })
                    .collect();
                geo::LineString::from(coords)
            })
            .collect();

        Self::new(exterior_line, interior_lines)
    }

    /// Get a reference to the exterior ring.
    #[inline]
    pub fn exterior(&self) -> &geo::LineString<f64> {
        self.inner.exterior()
    }

    /// Get references to the interior rings (holes).
    #[inline]
    pub fn interiors(&self) -> &[geo::LineString<f64>] {
        self.inner.interiors()
    }

    /// Access the inner `geo::Polygon`.
    #[inline]
    pub fn inner(&self) -> &geo::Polygon<f64> {
        &self.inner
    }

    /// Convert into the inner `geo::Polygon`.
    #[inline]
    pub fn into_inner(self) -> geo::Polygon<f64> {
        self.inner
    }

    /// Check if a point is contained within this polygon.
    ///
    /// # Examples
    ///
    /// ```
    /// use spatio_types::geo::{Point, Polygon};
    /// use geo::polygon;
    ///
    /// let poly = polygon![
    ///     (x: -80.0, y: 35.0),
    ///     (x: -70.0, y: 35.0),
    ///     (x: -70.0, y: 45.0),
    ///     (x: -80.0, y: 45.0),
    ///     (x: -80.0, y: 35.0),
    /// ];
    /// let polygon = Polygon::from(poly);
    /// let point = Point::new(-75.0, 40.0);
    /// assert!(polygon.contains(&point));
    /// ```
    #[inline]
    pub fn contains(&self, point: &Point) -> bool {
        use geo::Contains;
        self.inner.contains(&point.inner)
    }

    /// Convert to GeoJSON string representation.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "geojson")]
    /// # {
    /// use spatio_types::geo::Polygon;
    /// use geo::polygon;
    ///
    /// let poly = polygon![
    ///     (x: -80.0, y: 35.0),
    ///     (x: -70.0, y: 35.0),
    ///     (x: -70.0, y: 45.0),
    ///     (x: -80.0, y: 45.0),
    ///     (x: -80.0, y: 35.0),
    /// ];
    /// let polygon = Polygon::from(poly);
    /// let json = polygon.to_geojson().unwrap();
    /// assert!(json.contains("Polygon"));
    /// # }
    /// ```
    #[cfg(feature = "geojson")]
    pub fn to_geojson(&self) -> Result<String, GeoJsonError> {
        use geojson::{Geometry, Value};

        let mut rings = Vec::new();

        let exterior: Vec<Vec<f64>> = self
            .exterior()
            .coords()
            .map(|coord| vec![coord.x, coord.y])
            .collect();
        rings.push(exterior);

        for interior in self.interiors() {
            let ring: Vec<Vec<f64>> = interior
                .coords()
                .map(|coord| vec![coord.x, coord.y])
                .collect();
            rings.push(ring);
        }

        let geom = Geometry::new(Value::Polygon(rings));

        serde_json::to_string(&geom)
            .map_err(|e| GeoJsonError::Serialization(format!("Failed to serialize polygon: {}", e)))
    }

    /// Parse from GeoJSON string.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "geojson")]
    /// # {
    /// use spatio_types::geo::Polygon;
    ///
    /// let json = r#"{"type":"Polygon","coordinates":[[[-80.0,35.0],[-70.0,35.0],[-70.0,45.0],[-80.0,45.0],[-80.0,35.0]]]}"#;
    /// let polygon = Polygon::from_geojson(json).unwrap();
    /// assert_eq!(polygon.exterior().coords().count(), 5);
    /// # }
    /// ```
    #[cfg(feature = "geojson")]
    pub fn from_geojson(geojson: &str) -> Result<Self, GeoJsonError> {
        use geojson::{Geometry, Value};

        let geom: Geometry = serde_json::from_str(geojson).map_err(|e| {
            GeoJsonError::Deserialization(format!("Failed to parse GeoJSON: {}", e))
        })?;

        match geom.value {
            Value::Polygon(rings) => {
                if rings.is_empty() {
                    return Err(GeoJsonError::InvalidCoordinates(
                        "Polygon must have at least one ring".to_string(),
                    ));
                }

                let exterior: Result<Vec<geo::Coord>, GeoJsonError> = rings[0]
                    .iter()
                    .map(|coords| {
                        if coords.len() < 2 {
                            return Err(GeoJsonError::InvalidCoordinates(
                                "Coordinate must have at least 2 values".to_string(),
                            ));
                        }
                        Ok(geo::Coord {
                            x: coords[0],
                            y: coords[1],
                        })
                    })
                    .collect();

                let exterior_coords = exterior?;
                let exterior_line = geo::LineString::from(exterior_coords);

                let mut interiors = Vec::new();
                for ring in rings.iter().skip(1) {
                    let interior: Result<Vec<geo::Coord>, GeoJsonError> = ring
                        .iter()
                        .map(|coords| {
                            if coords.len() < 2 {
                                return Err(GeoJsonError::InvalidCoordinates(
                                    "Coordinate must have at least 2 values".to_string(),
                                ));
                            }
                            Ok(geo::Coord {
                                x: coords[0],
                                y: coords[1],
                            })
                        })
                        .collect();
                    let interior_coords = interior?;
                    interiors.push(geo::LineString::from(interior_coords));
                }

                Ok(Polygon::new(exterior_line, interiors))
            }
            _ => Err(GeoJsonError::InvalidGeometry(
                "GeoJSON geometry is not a Polygon".to_string(),
            )),
        }
    }
}

impl From<geo::Polygon<f64>> for Polygon {
    fn from(polygon: geo::Polygon<f64>) -> Self {
        Self { inner: polygon }
    }
}

impl From<Polygon> for geo::Polygon<f64> {
    fn from(polygon: Polygon) -> Self {
        polygon.inner
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_point_creation() {
        let point = Point::new(-74.0060, 40.7128);
        assert_eq!(point.x(), -74.0060);
        assert_eq!(point.y(), 40.7128);
        assert_eq!(point.lon(), -74.0060);
        assert_eq!(point.lat(), 40.7128);
    }

    #[test]
    fn test_point_from_tuple() {
        let point: Point = (-74.0060, 40.7128).into();
        assert_eq!(point.x(), -74.0060);
        assert_eq!(point.y(), 40.7128);
    }

    #[test]
    fn test_point_to_tuple() {
        let point = Point::new(-74.0060, 40.7128);
        let (x, y): (f64, f64) = point.into();
        assert_eq!(x, -74.0060);
        assert_eq!(y, 40.7128);
    }

    #[test]
    fn test_point_haversine_distance() {
        let nyc = Point::new(-74.0060, 40.7128);
        let la = Point::new(-118.2437, 34.0522);
        let distance = nyc.haversine_distance(&la);
        // Distance NYC to LA is approximately 3,944 km
        assert!(distance > 3_900_000.0 && distance < 4_000_000.0);
    }

    #[test]
    fn test_point_euclidean_distance() {
        let p1 = Point::new(0.0, 0.0);
        let p2 = Point::new(3.0, 4.0);
        let distance = p1.euclidean_distance(&p2);
        assert_eq!(distance, 5.0);
    }

    #[test]
    fn test_polygon_creation() {
        use geo::polygon;

        let poly = polygon![
            (x: -80.0, y: 35.0),
            (x: -70.0, y: 35.0),
            (x: -70.0, y: 45.0),
            (x: -80.0, y: 45.0),
            (x: -80.0, y: 35.0),
        ];
        let polygon = Polygon::from(poly);
        assert_eq!(polygon.exterior().coords().count(), 5);
        assert_eq!(polygon.interiors().len(), 0);
    }

    #[test]
    fn test_polygon_contains() {
        use geo::polygon;

        let poly = polygon![
            (x: -80.0, y: 35.0),
            (x: -70.0, y: 35.0),
            (x: -70.0, y: 45.0),
            (x: -80.0, y: 45.0),
            (x: -80.0, y: 35.0),
        ];
        let polygon = Polygon::from(poly);

        let inside = Point::new(-75.0, 40.0);
        let outside = Point::new(-85.0, 40.0);

        assert!(polygon.contains(&inside));
        assert!(!polygon.contains(&outside));
    }

    #[cfg(feature = "geojson")]
    #[test]
    fn test_point_geojson_roundtrip() {
        let original = Point::new(-74.0060, 40.7128);
        let json = original.to_geojson().unwrap();
        let parsed = Point::from_geojson(&json).unwrap();

        assert!((original.x() - parsed.x()).abs() < 1e-10);
        assert!((original.y() - parsed.y()).abs() < 1e-10);
    }

    #[cfg(feature = "geojson")]
    #[test]
    fn test_polygon_geojson_roundtrip() {
        use geo::polygon;

        let poly = polygon![
            (x: -80.0, y: 35.0),
            (x: -70.0, y: 35.0),
            (x: -70.0, y: 45.0),
            (x: -80.0, y: 45.0),
            (x: -80.0, y: 35.0),
        ];
        let original = Polygon::from(poly);
        let json = original.to_geojson().unwrap();
        let parsed = Polygon::from_geojson(&json).unwrap();

        assert_eq!(
            original.exterior().coords().count(),
            parsed.exterior().coords().count()
        );
    }
}
