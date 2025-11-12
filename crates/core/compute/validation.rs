//! Validation for geographic coordinates.

use crate::error::{Result, SpatioError};
use spatio_types::geo::Point;
use spatio_types::point::Point3d;

/// Validates a 2D point has valid longitude and latitude.
///
/// Longitude: [-180.0, 180.0], Latitude: [-90.0, 90.0]
///
/// # Examples
///
/// ```
/// use spatio::compute::validation::validate_geographic_point;
/// use spatio_types::geo::Point;
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
/// use spatio_types::geo::Point;
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
/// use spatio::Polygon;
/// use geo::polygon;
///
/// let poly = polygon![
///     (x: -80.0, y: 35.0),
///     (x: -70.0, y: 35.0),
///     (x: -70.0, y: 45.0),
///     (x: -80.0, y: 45.0),
///     (x: -80.0, y: 35.0),
/// ];
/// let poly: Polygon = poly.into();
///
/// assert!(validate_polygon(&poly).is_ok());
/// ```
pub fn validate_polygon(polygon: &spatio_types::geo::Polygon) -> Result<()> {
    for (idx, coord) in polygon.exterior().coords().enumerate() {
        let point = Point::new(coord.x, coord.y);
        validate_geographic_point(&point).map_err(|e| {
            SpatioError::InvalidInput(format!("Exterior ring point at index {}: {}", idx, e))
        })?;
    }

    for (ring_idx, interior) in polygon.interiors().iter().enumerate() {
        for (idx, coord) in interior.coords().enumerate() {
            let point = Point::new(coord.x, coord.y);
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

/// Validates a radius for spatial queries.
///
/// Ensures radius is positive, finite, and not exceeding Earth's circumference.
///
/// # Examples
///
/// ```
/// use spatio::compute::validation::validate_radius;
///
/// assert!(validate_radius(1000.0).is_ok());
/// assert!(validate_radius(0.0).is_err());
/// assert!(validate_radius(-100.0).is_err());
/// assert!(validate_radius(f64::NAN).is_err());
/// ```
pub fn validate_radius(radius: f64) -> Result<()> {
    if !radius.is_finite() {
        return Err(SpatioError::InvalidInput(format!(
            "Radius must be finite, got: {}",
            radius
        )));
    }
    if radius <= 0.0 {
        return Err(SpatioError::InvalidInput(format!(
            "Radius must be positive, got: {}",
            radius
        )));
    }
    const EARTH_CIRCUMFERENCE: f64 = 40_075_000.0; // meters
    if radius > EARTH_CIRCUMFERENCE {
        return Err(SpatioError::InvalidInput(format!(
            "Radius {} exceeds Earth's circumference ({} meters)",
            radius, EARTH_CIRCUMFERENCE
        )));
    }
    Ok(())
}

/// Validates a bounding box.
///
/// Ensures coordinates are valid and min < max for both dimensions.
///
/// # Examples
///
/// ```
/// use spatio::compute::validation::validate_bbox;
///
/// assert!(validate_bbox(-10.0, -10.0, 10.0, 10.0).is_ok());
/// assert!(validate_bbox(10.0, -10.0, -10.0, 10.0).is_err()); // min > max
/// ```
pub fn validate_bbox(min_lon: f64, min_lat: f64, max_lon: f64, max_lat: f64) -> Result<()> {
    // Validate all coordinates
    let min_point = Point::new(min_lon, min_lat);
    let max_point = Point::new(max_lon, max_lat);
    validate_geographic_point(&min_point)?;
    validate_geographic_point(&max_point)?;

    // Ensure min < max
    if min_lon >= max_lon {
        return Err(SpatioError::InvalidInput(format!(
            "min_lon ({}) must be < max_lon ({})",
            min_lon, max_lon
        )));
    }
    if min_lat >= max_lat {
        return Err(SpatioError::InvalidInput(format!(
            "min_lat ({}) must be < max_lat ({})",
            min_lat, max_lat
        )));
    }

    Ok(())
}

/// Validates a 3D bounding box.
pub fn validate_bbox_3d(
    min_lon: f64,
    min_lat: f64,
    min_alt: f64,
    max_lon: f64,
    max_lat: f64,
    max_alt: f64,
) -> Result<()> {
    let min_point = Point3d::new(min_lon, min_lat, min_alt);
    let max_point = Point3d::new(max_lon, max_lat, max_alt);
    validate_geographic_point_3d(&min_point)?;
    validate_geographic_point_3d(&max_point)?;

    if min_lon >= max_lon {
        return Err(SpatioError::InvalidInput(format!(
            "min_lon ({}) must be < max_lon ({})",
            min_lon, max_lon
        )));
    }
    if min_lat >= max_lat {
        return Err(SpatioError::InvalidInput(format!(
            "min_lat ({}) must be < max_lat ({})",
            min_lat, max_lat
        )));
    }
    if min_alt >= max_alt {
        return Err(SpatioError::InvalidInput(format!(
            "min_alt ({}) must be < max_alt ({})",
            min_alt, max_alt
        )));
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
        use spatio_types::geo::Polygon;

        let valid_poly = polygon![
            (x: -80.0, y: 35.0),
            (x: -70.0, y: 35.0),
            (x: -70.0, y: 45.0),
            (x: -80.0, y: 45.0),
            (x: -80.0, y: 35.0),
        ];
        assert!(validate_polygon(&Polygon::from(valid_poly)).is_ok());

        let invalid_poly = polygon![
            (x: -80.0, y: 35.0),
            (x: 200.0, y: 35.0),  // Invalid longitude
            (x: -70.0, y: 45.0),
            (x: -80.0, y: 45.0),
        ];
        assert!(validate_polygon(&Polygon::from(invalid_poly)).is_err());
    }

    #[test]
    fn test_validate_radius() {
        assert!(validate_radius(1000.0).is_ok());
        assert!(validate_radius(0.1).is_ok());
        assert!(validate_radius(1_000_000.0).is_ok());

        assert!(validate_radius(0.0).is_err());
        assert!(validate_radius(-100.0).is_err());
        assert!(validate_radius(f64::NAN).is_err());
        assert!(validate_radius(f64::INFINITY).is_err());
        assert!(validate_radius(50_000_000.0).is_err()); // > Earth circumference
    }

    #[test]
    fn test_validate_bbox() {
        assert!(validate_bbox(-10.0, -10.0, 10.0, 10.0).is_ok());
        assert!(validate_bbox(-180.0, -90.0, 180.0, 90.0).is_ok());

        // min >= max errors
        assert!(validate_bbox(10.0, -10.0, -10.0, 10.0).is_err());
        assert!(validate_bbox(-10.0, 10.0, 10.0, -10.0).is_err());
        assert!(validate_bbox(10.0, 10.0, 10.0, 10.0).is_err());

        // Invalid coordinates
        assert!(validate_bbox(-200.0, -10.0, 10.0, 10.0).is_err());
        assert!(validate_bbox(-10.0, -100.0, 10.0, 10.0).is_err());
    }

    #[test]
    fn test_validate_bbox_3d() {
        assert!(validate_bbox_3d(-10.0, -10.0, 0.0, 10.0, 10.0, 1000.0).is_ok());

        // Altitude validation
        assert!(validate_bbox_3d(-10.0, -10.0, 1000.0, 10.0, 10.0, 0.0).is_err());
        assert!(validate_bbox_3d(-10.0, -10.0, -20000.0, 10.0, 10.0, 0.0).is_err());
    }
}
