//! Validation for geographic coordinates.

use crate::error::{Result, SpatioError};
use geo::Point;
use spatio_types::point::Point3d;

/// Validates a 2D point has valid longitude and latitude.
///
/// Longitude: [-180.0, 180.0], Latitude: [-90.0, 90.0]
///
/// # Examples
///
/// ```
/// use spatio::compute::validation::validate_geographic_point;
/// use geo::Point;
///
/// // Valid point
/// let nyc = Point::new(-74.0060, 40.7128);
/// assert!(validate_geographic_point(&nyc).is_ok());
///
/// // Invalid longitude
/// let invalid = Point::new(200.0, 40.0);
/// assert!(validate_geographic_point(&invalid).is_err());
///
/// // Invalid latitude
/// let invalid = Point::new(-74.0, 95.0);
/// assert!(validate_geographic_point(&invalid).is_err());
/// ```
pub fn validate_geographic_point(point: &Point) -> Result<()> {
    let (x, y) = (point.x(), point.y());

    if !x.is_finite() {
        return Err(SpatioError::InvalidInput(format!(
            "Longitude must be finite, got: {}",
            x
        )));
    }

    if !y.is_finite() {
        return Err(SpatioError::InvalidInput(format!(
            "Latitude must be finite, got: {}",
            y
        )));
    }

    if !(-180.0..=180.0).contains(&x) {
        return Err(SpatioError::InvalidInput(format!(
            "Longitude out of range [-180.0, 180.0]: {}",
            x
        )));
    }

    if !(-90.0..=90.0).contains(&y) {
        return Err(SpatioError::InvalidInput(format!(
            "Latitude out of range [-90.0, 90.0]: {}",
            y
        )));
    }

    Ok(())
}

/// Validates a 3D point including altitude.
///
/// Altitude range: [-11000, 100000] meters (Mariana Trench to Kármán line)
///
/// # Examples
///
/// ```
/// use spatio::compute::validation::validate_geographic_point_3d;
/// use spatio_types::point::Point3d;
///
/// // Valid 3D point (drone at 100m altitude)
/// let drone = Point3d::new(-74.0060, 40.7128, 100.0);
/// assert!(validate_geographic_point_3d(&drone).is_ok());
///
/// // Invalid altitude (too high)
/// let invalid = Point3d::new(-74.0, 40.7, 200000.0);
/// assert!(validate_geographic_point_3d(&invalid).is_err());
/// ```
pub fn validate_geographic_point_3d(point: &Point3d) -> Result<()> {
    validate_geographic_point(&point.to_2d())?;

    let z = point.z();

    if !z.is_finite() {
        return Err(SpatioError::InvalidInput(format!(
            "Altitude must be finite, got: {}",
            z
        )));
    }

    const MIN_ALTITUDE: f64 = -11000.0;
    const MAX_ALTITUDE: f64 = 100000.0;

    if !(MIN_ALTITUDE..=MAX_ALTITUDE).contains(&z) {
        return Err(SpatioError::InvalidInput(format!(
            "Altitude out of reasonable range [{}, {}] meters: {}",
            MIN_ALTITUDE, MAX_ALTITUDE, z
        )));
    }

    Ok(())
}

/// Validates multiple points.
///
/// # Examples
///
/// ```
/// use spatio::compute::validation::validate_points;
/// use geo::Point;
///
/// let points = vec![
///     Point::new(-74.0, 40.7),
///     Point::new(-73.9, 40.8),
///     Point::new(999.0, 40.0), // Invalid
/// ];
///
/// let result = validate_points(&points);
/// assert!(result.is_err());
/// ```
pub fn validate_points(points: &[Point]) -> Result<()> {
    for (idx, point) in points.iter().enumerate() {
        validate_geographic_point(point)
            .map_err(|e| SpatioError::InvalidInput(format!("Point at index {}: {}", idx, e)))?;
    }
    Ok(())
}

/// Validates multiple 3D points.
pub fn validate_points_3d(points: &[Point3d]) -> Result<()> {
    for (idx, point) in points.iter().enumerate() {
        validate_geographic_point_3d(point)
            .map_err(|e| SpatioError::InvalidInput(format!("Point at index {}: {}", idx, e)))?;
    }
    Ok(())
}

/// Validates all polygon coordinates (exterior and interior rings).
///
/// # Examples
///
/// ```
/// use spatio::compute::validation::validate_polygon;
/// use geo::{polygon, Polygon};
///
/// let poly: Polygon = polygon![
///     (x: -80.0, y: 35.0),
///     (x: -70.0, y: 35.0),
///     (x: -70.0, y: 45.0),
///     (x: -80.0, y: 45.0),
///     (x: -80.0, y: 35.0),
/// ];
///
/// assert!(validate_polygon(&poly).is_ok());
/// ```
pub fn validate_polygon(polygon: &geo::Polygon) -> Result<()> {
    for (idx, coord) in polygon.exterior().coords().enumerate() {
        let point = Point::from(*coord);
        validate_geographic_point(&point).map_err(|e| {
            SpatioError::InvalidInput(format!("Exterior ring point at index {}: {}", idx, e))
        })?;
    }

    for (ring_idx, interior) in polygon.interiors().iter().enumerate() {
        for (idx, coord) in interior.coords().enumerate() {
            let point = Point::from(*coord);
            validate_geographic_point(&point).map_err(|e| {
                SpatioError::InvalidInput(format!(
                    "Interior ring {} point at index {}: {}",
                    ring_idx, idx, e
                ))
            })?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_geographic_point() {
        let nyc = Point::new(-74.0060, 40.7128);
        assert!(validate_geographic_point(&nyc).is_ok());

        let london = Point::new(-0.1278, 51.5074);
        assert!(validate_geographic_point(&london).is_ok());

        let tokyo = Point::new(139.6917, 35.6895);
        assert!(validate_geographic_point(&tokyo).is_ok());

        // Edge cases
        let max_lon = Point::new(180.0, 0.0);
        assert!(validate_geographic_point(&max_lon).is_ok());

        let min_lon = Point::new(-180.0, 0.0);
        assert!(validate_geographic_point(&min_lon).is_ok());

        let max_lat = Point::new(0.0, 90.0);
        assert!(validate_geographic_point(&max_lat).is_ok());

        let min_lat = Point::new(0.0, -90.0);
        assert!(validate_geographic_point(&min_lat).is_ok());
    }

    #[test]
    fn test_invalid_longitude() {
        let invalid = Point::new(200.0, 40.0);
        assert!(validate_geographic_point(&invalid).is_err());

        let invalid = Point::new(-200.0, 40.0);
        assert!(validate_geographic_point(&invalid).is_err());

        let invalid = Point::new(180.1, 40.0);
        assert!(validate_geographic_point(&invalid).is_err());
    }

    #[test]
    fn test_invalid_latitude() {
        let invalid = Point::new(-74.0, 95.0);
        assert!(validate_geographic_point(&invalid).is_err());

        let invalid = Point::new(-74.0, -95.0);
        assert!(validate_geographic_point(&invalid).is_err());

        let invalid = Point::new(-74.0, 90.1);
        assert!(validate_geographic_point(&invalid).is_err());
    }

    #[test]
    fn test_non_finite_coordinates() {
        let nan_lon = Point::new(f64::NAN, 40.0);
        assert!(validate_geographic_point(&nan_lon).is_err());

        let nan_lat = Point::new(-74.0, f64::NAN);
        assert!(validate_geographic_point(&nan_lat).is_err());

        let inf_lon = Point::new(f64::INFINITY, 40.0);
        assert!(validate_geographic_point(&inf_lon).is_err());

        let inf_lat = Point::new(-74.0, f64::INFINITY);
        assert!(validate_geographic_point(&inf_lat).is_err());
    }

    #[test]
    fn test_valid_3d_point() {
        let drone = Point3d::new(-74.0060, 40.7128, 100.0);
        assert!(validate_geographic_point_3d(&drone).is_ok());

        let sea_level = Point3d::new(-74.0, 40.7, 0.0);
        assert!(validate_geographic_point_3d(&sea_level).is_ok());

        let underwater = Point3d::new(-74.0, 40.7, -100.0);
        assert!(validate_geographic_point_3d(&underwater).is_ok());

        let airplane = Point3d::new(-74.0, 40.7, 10000.0);
        assert!(validate_geographic_point_3d(&airplane).is_ok());
    }

    #[test]
    fn test_invalid_altitude() {
        let too_high = Point3d::new(-74.0, 40.7, 200000.0);
        assert!(validate_geographic_point_3d(&too_high).is_err());

        let too_low = Point3d::new(-74.0, 40.7, -20000.0);
        assert!(validate_geographic_point_3d(&too_low).is_err());

        let nan_alt = Point3d::new(-74.0, 40.7, f64::NAN);
        assert!(validate_geographic_point_3d(&nan_alt).is_err());
    }

    #[test]
    fn test_validate_multiple_points() {
        let valid_points = vec![
            Point::new(-74.0, 40.7),
            Point::new(-73.9, 40.8),
            Point::new(-74.1, 40.6),
        ];
        assert!(validate_points(&valid_points).is_ok());

        let invalid_points = vec![
            Point::new(-74.0, 40.7),
            Point::new(999.0, 40.0), // Invalid
            Point::new(-74.1, 40.6),
        ];
        assert!(validate_points(&invalid_points).is_err());
    }

    #[test]
    fn test_validate_polygon() {
        use geo::polygon;

        let valid_poly: geo::Polygon = polygon![
            (x: -80.0, y: 35.0),
            (x: -70.0, y: 35.0),
            (x: -70.0, y: 45.0),
            (x: -80.0, y: 45.0),
            (x: -80.0, y: 35.0),
        ];
        assert!(validate_polygon(&valid_poly).is_ok());

        let invalid_poly: geo::Polygon = polygon![
            (x: -80.0, y: 35.0),
            (x: 999.0, y: 35.0), // Invalid longitude
            (x: -70.0, y: 45.0),
            (x: -80.0, y: 45.0),
            (x: -80.0, y: 35.0),
        ];
        assert!(validate_polygon(&invalid_poly).is_err());
    }
}
