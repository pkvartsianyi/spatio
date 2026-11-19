//! Stored areas: polygons, circles and corridors.

use crate::geo::{LineString, Point, Polygon};
use serde::{Deserialize, Serialize};

/// An area on the earth's surface.
///
/// Distances are in meters. Polygon edges are straight in lon/lat, as in
/// GeoJSON; circle and corridor distances follow great circles.
///
/// # Examples
///
/// ```
/// use spatio_types::geo::{LineString, Point};
/// use spatio_types::shape::Shape;
///
/// let depot = Shape::circle(Point::new(0.0, 0.0), 500.0);
/// assert!(depot.contains(&Point::new(0.001, 0.0)));
///
/// let route = Shape::corridor(LineString::from_coords(&[(0.0, 0.0), (0.0, 1.0)]), 50.0);
/// assert!(!route.contains(&Point::new(0.001, 0.5)));
/// assert!((route.distance_to(&Point::new(0.001, 0.5)) - 61.2).abs() < 1.0);
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(from = "ShapeRepr", into = "ShapeRepr")]
pub enum Shape {
    Polygon(Polygon),
    Circle {
        center: Point,
        radius_m: f64,
    },
    /// Everything within `half_width_m` of `line`.
    Corridor {
        line: LineString,
        half_width_m: f64,
    },
}

impl Shape {
    pub fn polygon(polygon: Polygon) -> Self {
        Shape::Polygon(polygon)
    }

    pub fn circle(center: Point, radius_m: f64) -> Self {
        Shape::Circle { center, radius_m }
    }

    pub fn corridor(line: LineString, half_width_m: f64) -> Self {
        Shape::Corridor { line, half_width_m }
    }

    /// Whether `point` is inside the shape (on the boundary counts).
    pub fn contains(&self, point: &Point) -> bool {
        match self {
            Shape::Polygon(p) => {
                use geo::Intersects;
                p.inner().intersects(point.inner())
            }
            _ => self.distance_to(point) == 0.0,
        }
    }

    /// Surface distance from `point` to the shape in meters; 0 inside.
    pub fn distance_to(&self, point: &Point) -> f64 {
        match self {
            Shape::Polygon(p) => {
                if self.contains(point) {
                    return 0.0;
                }
                std::iter::once(p.exterior())
                    .chain(p.interiors())
                    .map(|ring| crate::line::path_distance(&ring.0, point))
                    .fold(f64::INFINITY, f64::min)
            }
            Shape::Circle { center, radius_m } => {
                (center.haversine_distance(point) - radius_m).max(0.0)
            }
            Shape::Corridor { line, half_width_m } => {
                (line.distance_to(point) - half_width_m).max(0.0)
            }
        }
    }

    /// Whether any part of the shape lies within `meters` of `point`.
    pub fn is_within(&self, point: &Point, meters: f64) -> bool {
        self.distance_to(point) <= meters
    }

    /// Every vertex (or the centre), for validation and bounds.
    pub fn coords(&self) -> Vec<(f64, f64)> {
        match self {
            Shape::Polygon(p) => std::iter::once(p.exterior())
                .chain(p.interiors())
                .flat_map(|r| r.coords().map(|c| (c.x, c.y)))
                .collect(),
            Shape::Circle { center, .. } => vec![(center.x(), center.y())],
            Shape::Corridor { line, .. } => line.coords(),
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ShapeRepr {
    Polygon {
        exterior: Vec<[f64; 2]>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        interiors: Vec<Vec<[f64; 2]>>,
    },
    Circle {
        center: [f64; 2],
        radius_m: f64,
    },
    Corridor {
        line: Vec<[f64; 2]>,
        half_width_m: f64,
    },
}

fn ring(r: &geo::LineString<f64>) -> Vec<[f64; 2]> {
    r.coords().map(|c| [c.x, c.y]).collect()
}

fn tuples(r: Vec<[f64; 2]>) -> Vec<(f64, f64)> {
    r.into_iter().map(|[x, y]| (x, y)).collect()
}

impl From<Shape> for ShapeRepr {
    fn from(s: Shape) -> Self {
        match s {
            Shape::Polygon(p) => ShapeRepr::Polygon {
                exterior: ring(p.exterior()),
                interiors: p.interiors().iter().map(ring).collect(),
            },
            Shape::Circle { center, radius_m } => ShapeRepr::Circle {
                center: [center.x(), center.y()],
                radius_m,
            },
            Shape::Corridor { line, half_width_m } => ShapeRepr::Corridor {
                line: line.into(),
                half_width_m,
            },
        }
    }
}

impl From<ShapeRepr> for Shape {
    fn from(r: ShapeRepr) -> Self {
        match r {
            ShapeRepr::Polygon {
                exterior,
                interiors,
            } => Shape::Polygon(Polygon::from_coords(
                &tuples(exterior),
                interiors.into_iter().map(tuples).collect(),
            )),
            ShapeRepr::Circle {
                center: [x, y],
                radius_m,
            } => Shape::Circle {
                center: Point::new(x, y),
                radius_m,
            },
            ShapeRepr::Corridor { line, half_width_m } => Shape::Corridor {
                line: line.into(),
                half_width_m,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn square() -> Shape {
        Shape::polygon(Polygon::from_coords(
            &[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0)],
            vec![vec![
                (0.4, 0.4),
                (0.6, 0.4),
                (0.6, 0.6),
                (0.4, 0.6),
                (0.4, 0.4),
            ]],
        ))
    }

    #[test]
    fn polygon_contains_and_distance() {
        let s = square();
        assert!(s.contains(&Point::new(0.2, 0.2)));
        assert!(s.contains(&Point::new(0.0, 0.5)));
        assert!(!s.contains(&Point::new(0.5, 0.5)));
        assert!((s.distance_to(&Point::new(0.5, 0.5)) - 11_119.5).abs() < 20.0);
        assert!((s.distance_to(&Point::new(1.001, 0.5)) - 111.2).abs() < 1.0);
    }

    #[test]
    fn circle() {
        let s = Shape::circle(Point::new(10.0, 50.0), 1000.0);
        assert!(s.contains(&Point::new(10.0, 50.008)));
        assert!(!s.contains(&Point::new(10.0, 50.01)));
        assert!(s.is_within(&Point::new(10.0, 50.01), 200.0));
    }

    #[test]
    fn serde_roundtrip() {
        for s in [
            square(),
            Shape::circle(Point::new(1.0, 2.0), 3.0),
            Shape::corridor(LineString::from_coords(&[(0.0, 0.0), (1.0, 1.0)]), 5.0),
        ] {
            let json = serde_json::to_string(&s).unwrap();
            assert_eq!(serde_json::from_str::<Shape>(&json).unwrap(), s, "{json}");
        }
        assert_eq!(
            serde_json::to_string(&Shape::circle(Point::new(1.0, 2.0), 3.0)).unwrap(),
            r#"{"type":"circle","center":[1.0,2.0],"radius_m":3.0}"#
        );
    }
}
