//! GeoJSON conversion for Points and Polygons.

use crate::error::{Result, SpatioError};
use geojson::{Feature, FeatureCollection, Geometry, Value};
use serde_json::Map;
use spatio_types::geo::{Point, Polygon};
use spatio_types::point::Point3d;
use std::collections::HashMap;

/// Converts a Point to GeoJSON string.
pub fn point_to_geojson(point: &Point) -> Result<String> {
    let geom = Geometry::new(Value::Point(vec![point.x(), point.y()]));

    serde_json::to_string(&geom).map_err(|e| {
        SpatioError::SerializationErrorWithContext(format!("Failed to serialize point: {}", e))
    })
}

/// Converts a 3D Point to GeoJSON (altitude as third coordinate).
pub fn point_3d_to_geojson(point: &Point3d) -> Result<String> {
    let geom = Geometry::new(Value::Point(vec![point.x(), point.y(), point.z()]));

    serde_json::to_string(&geom).map_err(|e| {
        SpatioError::SerializationErrorWithContext(format!("Failed to serialize 3D point: {}", e))
    })
}

/// Parses GeoJSON into a Point.
pub fn point_from_geojson(geojson: &str) -> Result<Point> {
    let geom: Geometry = serde_json::from_str(geojson)
        .map_err(|e| SpatioError::InvalidInput(format!("Failed to parse GeoJSON: {}", e)))?;

    match geom.value {
        Value::Point(coords) => {
            if coords.len() < 2 {
                return Err(SpatioError::InvalidInput(
                    "Point must have at least 2 coordinates".to_string(),
                ));
            }
            Ok(Point::new(coords[0], coords[1]))
        }
        _ => Err(SpatioError::InvalidInput(
            "GeoJSON geometry is not a Point".to_string(),
        )),
    }
}

/// Parses GeoJSON into a 3D Point (altitude defaults to 0.0 if missing).
pub fn point_3d_from_geojson(geojson: &str) -> Result<Point3d> {
    let geom: Geometry = serde_json::from_str(geojson)
        .map_err(|e| SpatioError::InvalidInput(format!("Failed to parse GeoJSON: {}", e)))?;

    match geom.value {
        Value::Point(coords) => {
            if coords.len() < 2 {
                return Err(SpatioError::InvalidInput(
                    "Point must have at least 2 coordinates".to_string(),
                ));
            }
            let z = coords.get(2).copied().unwrap_or(0.0);
            Ok(Point3d::new(coords[0], coords[1], z))
        }
        _ => Err(SpatioError::InvalidInput(
            "GeoJSON geometry is not a Point".to_string(),
        )),
    }
}

/// Converts a Polygon to GeoJSON.
pub fn polygon_to_geojson(polygon: &Polygon) -> Result<String> {
    let mut rings = Vec::new();

    let exterior: Vec<Vec<f64>> = polygon
        .exterior()
        .coords()
        .map(|coord| vec![coord.x, coord.y])
        .collect();
    rings.push(exterior);

    for interior in polygon.interiors() {
        let ring: Vec<Vec<f64>> = interior
            .coords()
            .map(|coord| vec![coord.x, coord.y])
            .collect();
        rings.push(ring);
    }

    let geom = Geometry::new(Value::Polygon(rings));

    serde_json::to_string(&geom).map_err(|e| {
        SpatioError::SerializationErrorWithContext(format!("Failed to serialize polygon: {}", e))
    })
}

/// Parses GeoJSON into a Polygon.
pub fn polygon_from_geojson(geojson: &str) -> Result<Polygon> {
    let geom: Geometry = serde_json::from_str(geojson)
        .map_err(|e| SpatioError::InvalidInput(format!("Failed to parse GeoJSON: {}", e)))?;

    match geom.value {
        Value::Polygon(rings) => {
            if rings.is_empty() {
                return Err(SpatioError::InvalidInput(
                    "Polygon must have at least one ring".to_string(),
                ));
            }

            let exterior: Vec<geo::Coord> = rings[0]
                .iter()
                .map(|coords| {
                    if coords.len() < 2 {
                        return Err(SpatioError::InvalidInput(
                            "Coordinate must have at least 2 values".to_string(),
                        ));
                    }
                    Ok(geo::Coord {
                        x: coords[0],
                        y: coords[1],
                    })
                })
                .collect::<Result<Vec<_>>>()?;

            let exterior_line = geo::LineString::from(exterior);

            let mut interiors = Vec::new();
            for ring in rings.iter().skip(1) {
                let interior: Vec<geo::Coord> = ring
                    .iter()
                    .map(|coords| {
                        if coords.len() < 2 {
                            return Err(SpatioError::InvalidInput(
                                "Coordinate must have at least 2 values".to_string(),
                            ));
                        }
                        Ok(geo::Coord {
                            x: coords[0],
                            y: coords[1],
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                interiors.push(geo::LineString::from(interior));
            }

            Ok(Polygon::new(exterior_line, interiors))
        }
        _ => Err(SpatioError::InvalidInput(
            "GeoJSON geometry is not a Polygon".to_string(),
        )),
    }
}

/// Converts a Point to a GeoJSON Feature with properties.
pub fn point_to_feature(point: &Point, properties: &HashMap<String, String>) -> Result<String> {
    let geom = Geometry::new(Value::Point(vec![point.x(), point.y()]));

    let props: Map<String, serde_json::Value> = properties
        .iter()
        .map(|(k, v)| (k.clone(), serde_json::Value::String(v.clone())))
        .collect();

    let feature = Feature {
        bbox: None,
        geometry: Some(geom),
        id: None,
        properties: Some(props),
        foreign_members: None,
    };

    serde_json::to_string(&feature).map_err(|e| {
        SpatioError::SerializationErrorWithContext(format!("Failed to serialize feature: {}", e))
    })
}

/// Converts multiple points to a GeoJSON FeatureCollection.
pub fn points_to_feature_collection(points: &[(Point, HashMap<String, String>)]) -> Result<String> {
    let features: Vec<Feature> = points
        .iter()
        .map(|(point, properties)| {
            let geom = Geometry::new(Value::Point(vec![point.x(), point.y()]));

            let props: Map<String, serde_json::Value> = properties
                .iter()
                .map(|(k, v)| (k.clone(), serde_json::Value::String(v.clone())))
                .collect();

            Feature {
                bbox: None,
                geometry: Some(geom),
                id: None,
                properties: Some(props),
                foreign_members: None,
            }
        })
        .collect();

    let collection = FeatureCollection {
        bbox: None,
        features,
        foreign_members: None,
    };

    serde_json::to_string(&collection).map_err(|e| {
        SpatioError::SerializationErrorWithContext(format!(
            "Failed to serialize feature collection: {}",
            e
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_point_to_geojson() {
        let point = Point::new(-74.0060, 40.7128);
        let json = point_to_geojson(&point).unwrap();

        assert!(json.contains("Point"));
        assert!(json.contains("-74.006"));
        assert!(json.contains("40.7128"));
    }

    #[test]
    fn test_point_from_geojson() {
        let json = r#"{"type":"Point","coordinates":[-74.006,40.7128]}"#;
        let point = point_from_geojson(json).unwrap();

        assert_eq!(point.x(), -74.006);
        assert_eq!(point.y(), 40.7128);
    }

    #[test]
    fn test_point_roundtrip() {
        let original = Point::new(-74.0060, 40.7128);
        let json = point_to_geojson(&original).unwrap();
        let parsed = point_from_geojson(&json).unwrap();

        assert!((original.x() - parsed.x()).abs() < 1e-10);
        assert!((original.y() - parsed.y()).abs() < 1e-10);
    }

    #[test]
    fn test_point_3d_to_geojson() {
        let point = Point3d::new(-74.0060, 40.7128, 100.0);
        let json = point_3d_to_geojson(&point).unwrap();

        assert!(json.contains("Point"));
        assert!(json.contains("100"));
    }

    #[test]
    fn test_point_3d_from_geojson() {
        let json = r#"{"type":"Point","coordinates":[-74.006,40.7128,100.0]}"#;
        let point = point_3d_from_geojson(json).unwrap();

        assert_eq!(point.x(), -74.006);
        assert_eq!(point.y(), 40.7128);
        assert_eq!(point.z(), 100.0);
    }

    #[test]
    fn test_point_3d_from_geojson_defaults_z() {
        let json = r#"{"type":"Point","coordinates":[-74.006,40.7128]}"#;
        let point = point_3d_from_geojson(json).unwrap();

        assert_eq!(point.z(), 0.0);
    }

    #[test]
    fn test_polygon_to_geojson() {
        use geo::polygon;

        let poly = polygon![
            (x: -80.0, y: 35.0),
            (x: -70.0, y: 35.0),
            (x: -70.0, y: 45.0),
            (x: -80.0, y: 45.0),
            (x: -80.0, y: 35.0),
        ];

        let json = polygon_to_geojson(&Polygon::from(poly)).unwrap();
        assert!(json.contains("Polygon"));
        assert!(json.contains("-80"));
    }

    #[test]
    fn test_polygon_from_geojson() {
        let json = r#"{"type":"Polygon","coordinates":[[[-80.0,35.0],[-70.0,35.0],[-70.0,45.0],[-80.0,45.0],[-80.0,35.0]]]}"#;
        let polygon = polygon_from_geojson(json).unwrap();

        assert_eq!(polygon.exterior().coords().count(), 5);
    }

    #[test]
    fn test_polygon_roundtrip() {
        use geo::polygon;

        let original = polygon![
            (x: -80.0, y: 35.0),
            (x: -70.0, y: 35.0),
            (x: -70.0, y: 45.0),
            (x: -80.0, y: 45.0),
            (x: -80.0, y: 35.0),
        ];

        let json = polygon_to_geojson(&Polygon::from(original.clone())).unwrap();
        let parsed = polygon_from_geojson(&json).unwrap();

        assert_eq!(
            original.exterior().coords().count(),
            parsed.exterior().coords().count()
        );
    }

    #[test]
    fn test_point_to_feature() {
        let point = Point::new(-74.0060, 40.7128);
        let mut props = HashMap::new();
        props.insert("name".to_string(), "NYC".to_string());
        props.insert("population".to_string(), "8000000".to_string());

        let json = point_to_feature(&point, &props).unwrap();

        assert!(json.contains("Feature"));
        assert!(json.contains("NYC"));
        assert!(json.contains("8000000"));
    }

    #[test]
    fn test_points_to_feature_collection() {
        let mut props1 = HashMap::new();
        props1.insert("name".to_string(), "NYC".to_string());

        let mut props2 = HashMap::new();
        props2.insert("name".to_string(), "LA".to_string());

        let points = vec![
            (Point::new(-74.0060, 40.7128), props1),
            (Point::new(-118.2437, 34.0522), props2),
        ];

        let json = points_to_feature_collection(&points).unwrap();

        assert!(json.contains("FeatureCollection"));
        assert!(json.contains("NYC"));
        assert!(json.contains("LA"));
    }

    #[test]
    fn test_invalid_geojson() {
        let result = point_from_geojson("not valid json");
        assert!(result.is_err());

        let result = point_from_geojson(r#"{"type":"LineString","coordinates":[]}"#);
        assert!(result.is_err());
    }
}
