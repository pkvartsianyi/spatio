//! Lines on the earth's surface.
//!
//! Segments are great-circle arcs, so distances and intersections stay
//! accurate over long lines, unlike planar lon/lat maths.

use crate::geo::Point;
use serde::{Deserialize, Serialize};

/// A path of two or more lon/lat vertices joined by great-circle segments.
///
/// # Examples
///
/// ```
/// use spatio_types::geo::{LineString, Point};
///
/// let road = LineString::from_coords(&[(0.0, 0.0), (0.0, 1.0)]);
/// let d = road.distance_to(&Point::new(0.001, 0.5));
/// assert!((d - 111.2).abs() < 1.0);
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(from = "Vec<[f64; 2]>", into = "Vec<[f64; 2]>")]
pub struct LineString {
    inner: geo::LineString<f64>,
}

impl LineString {
    /// Build a line from `(lon, lat)` vertices.
    pub fn from_coords(coords: &[(f64, f64)]) -> Self {
        Self {
            inner: geo::LineString::from(coords.to_vec()),
        }
    }

    /// The `(lon, lat)` vertices.
    pub fn coords(&self) -> Vec<(f64, f64)> {
        self.inner.coords().map(|c| (c.x, c.y)).collect()
    }

    /// Number of vertices.
    pub fn len(&self) -> usize {
        self.inner.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.0.is_empty()
    }

    #[inline]
    pub fn inner(&self) -> &geo::LineString<f64> {
        &self.inner
    }

    /// Length along the surface, in meters.
    pub fn length(&self) -> f64 {
        use geo::{Haversine, Length};
        Haversine.length(&self.inner)
    }

    /// Shortest surface distance from `point` to the line, in meters.
    pub fn distance_to(&self, point: &Point) -> f64 {
        path_distance(&self.inner.0, point)
    }

    /// Whether any part of the line lies within `meters` of `point`.
    pub fn is_within(&self, point: &Point, meters: f64) -> bool {
        self.distance_to(point) <= meters
    }

    /// Whether the two lines touch or cross.
    pub fn intersects(&self, other: &LineString) -> bool {
        self.segment_pairs(other)
            .any(|(a, b, c, d)| !arc_intersections(a, b, c, d).is_empty())
    }

    /// Points where the two lines touch or cross. Overlapping collinear
    /// stretches report their end points.
    pub fn intersections(&self, other: &LineString) -> Vec<Point> {
        let mut out: Vec<Point> = Vec::new();
        for (a, b, c, d) in self.segment_pairs(other) {
            for v in arc_intersections(a, b, c, d) {
                let p = to_point(v);
                if !out.iter().any(|q| q.haversine_distance(&p) < 1e-3) {
                    out.push(p);
                }
            }
        }
        out
    }

    fn segment_pairs<'a>(
        &'a self,
        other: &'a LineString,
    ) -> impl Iterator<Item = (V3, V3, V3, V3)> + 'a {
        let mine = segments(&self.inner);
        let theirs = segments(&other.inner);
        mine.into_iter()
            .flat_map(move |(a, b)| theirs.clone().into_iter().map(move |(c, d)| (a, b, c, d)))
    }
}

impl From<Vec<[f64; 2]>> for LineString {
    fn from(v: Vec<[f64; 2]>) -> Self {
        Self {
            inner: geo::LineString::from(v.into_iter().map(|[x, y]| (x, y)).collect::<Vec<_>>()),
        }
    }
}

impl From<LineString> for Vec<[f64; 2]> {
    fn from(l: LineString) -> Self {
        l.inner.coords().map(|c| [c.x, c.y]).collect()
    }
}

impl From<geo::LineString<f64>> for LineString {
    fn from(inner: geo::LineString<f64>) -> Self {
        Self { inner }
    }
}

impl From<LineString> for geo::LineString<f64> {
    fn from(l: LineString) -> Self {
        l.inner
    }
}

type V3 = [f64; 3];

/// About 6 mm on the earth's surface.
const EPS: f64 = 1e-9;

/// Mean earth radius, matching `geo::Haversine`.
const EARTH_RADIUS_M: f64 = 6_371_008.8;

fn segments(line: &geo::LineString<f64>) -> Vec<(V3, V3)> {
    match line.0.as_slice() {
        [only] => vec![(to_vec(only.x, only.y), to_vec(only.x, only.y))],
        coords => coords
            .windows(2)
            .map(|w| (to_vec(w[0].x, w[0].y), to_vec(w[1].x, w[1].y)))
            .collect(),
    }
}

fn to_vec(lon: f64, lat: f64) -> V3 {
    let (lon, lat) = (lon.to_radians(), lat.to_radians());
    [lat.cos() * lon.cos(), lat.cos() * lon.sin(), lat.sin()]
}

fn to_point(v: V3) -> Point {
    Point::new(
        v[1].atan2(v[0]).to_degrees(),
        v[2].clamp(-1.0, 1.0).asin().to_degrees(),
    )
}

fn cross(a: V3, b: V3) -> V3 {
    [
        a[1] * b[2] - a[2] * b[1],
        a[2] * b[0] - a[0] * b[2],
        a[0] * b[1] - a[1] * b[0],
    ]
}

fn dot(a: V3, b: V3) -> f64 {
    a[0] * b[0] + a[1] * b[1] + a[2] * b[2]
}

fn norm(a: V3) -> f64 {
    dot(a, a).sqrt()
}

fn scale(a: V3, s: f64) -> V3 {
    [a[0] * s, a[1] * s, a[2] * s]
}

fn angle(a: V3, b: V3) -> f64 {
    norm(cross(a, b)).atan2(dot(a, b))
}

/// Whether `p` lies on the shorter arc from `a` to `b`.
fn on_arc(p: V3, a: V3, b: V3) -> bool {
    (angle(a, p) + angle(p, b) - angle(a, b)).abs() < EPS
}

/// Surface distance in meters from `point` to the path through `coords`.
pub(crate) fn path_distance(coords: &[geo::Coord<f64>], point: &Point) -> f64 {
    let p = to_vec(point.x(), point.y());
    let v = |c: &geo::Coord<f64>| to_vec(c.x, c.y);
    let rad = match coords {
        [] => f64::INFINITY,
        [only] => angle(p, v(only)),
        _ => coords
            .windows(2)
            .map(|w| arc_distance(p, v(&w[0]), v(&w[1])))
            .fold(f64::INFINITY, f64::min),
    };
    rad * EARTH_RADIUS_M
}

/// Angular distance from `p` to the shorter arc from `a` to `b`.
fn arc_distance(p: V3, a: V3, b: V3) -> f64 {
    let ends = angle(p, a).min(angle(p, b));
    let n = cross(a, b);
    let len = norm(n);
    if len < EPS {
        return ends;
    }
    let n = scale(n, 1.0 / len);
    let off = dot(p, n);
    let foot = [p[0] - off * n[0], p[1] - off * n[1], p[2] - off * n[2]];
    let foot_len = norm(foot);
    if foot_len < EPS {
        return ends;
    }
    if on_arc(scale(foot, 1.0 / foot_len), a, b) {
        off.abs().clamp(0.0, 1.0).asin()
    } else {
        ends
    }
}

/// Intersections of great-circle arcs `a`–`b` and `c`–`d`.
fn arc_intersections(a: V3, b: V3, c: V3, d: V3) -> Vec<V3> {
    let (n1, n2) = (cross(a, b), cross(c, d));
    let (l1, l2) = (norm(n1), norm(n2));
    if l1 < EPS || l2 < EPS || norm(cross(scale(n1, 1.0 / l1), scale(n2, 1.0 / l2))) < EPS {
        // A degenerate arc or both arcs on one great circle: the only
        // intersections are end points lying on the other arc.
        let mut hits = Vec::new();
        for (p, (s, e)) in [(a, (c, d)), (b, (c, d)), (c, (a, b)), (d, (a, b))] {
            if on_arc(p, s, e) && !hits.iter().any(|&h| angle(h, p) < EPS) {
                hits.push(p);
            }
        }
        return hits;
    }
    let t = cross(n1, n2);
    let t = scale(t, 1.0 / norm(t));
    [t, scale(t, -1.0)]
        .into_iter()
        .filter(|&p| on_arc(p, a, b) && on_arc(p, c, d))
        .take(1)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn distance_to_long_line_follows_great_circle() {
        // The shorter arc from (-100, 40) to (100, 40) crosses the
        // antimeridian and peaks near 78.3°N, far from the 40th parallel.
        let line = LineString::from_coords(&[(-100.0, 40.0), (100.0, 40.0)]);
        assert!(line.distance_to(&Point::new(180.0, 40.0)) > 4_000_000.0);
        assert!(line.distance_to(&Point::new(180.0, 78.3)) < 5_000.0);
    }

    #[test]
    fn distance_to_short_line() {
        let line = LineString::from_coords(&[(0.0, 0.0), (0.0, 1.0)]);
        assert!((line.distance_to(&Point::new(0.001, 0.5)) - 111.2).abs() < 1.0);
        assert_eq!(line.distance_to(&Point::new(0.0, 0.5)), 0.0);
        assert!((line.distance_to(&Point::new(0.0, 1.001)) - 111.2).abs() < 1.0);
        assert!(line.is_within(&Point::new(0.0005, 0.5), 60.0));
        assert!(!line.is_within(&Point::new(0.0006, 0.5), 60.0));
    }

    #[test]
    fn crossing_segments() {
        let a = LineString::from_coords(&[(-1.0, 0.0), (1.0, 0.0)]);
        let b = LineString::from_coords(&[(0.0, -1.0), (0.0, 1.0)]);
        assert!(a.intersects(&b));
        let hits = a.intersections(&b);
        assert_eq!(hits.len(), 1);
        assert!(hits[0].x().abs() < 1e-9 && hits[0].y().abs() < 1e-9);
    }

    #[test]
    fn disjoint_and_touching_segments() {
        let a = LineString::from_coords(&[(0.0, 0.0), (1.0, 0.0)]);
        let far = LineString::from_coords(&[(2.0, -1.0), (2.0, 1.0)]);
        assert!(!a.intersects(&far));
        let touching = LineString::from_coords(&[(1.0, 0.0), (1.0, 1.0)]);
        assert_eq!(a.intersections(&touching).len(), 1);
    }

    #[test]
    fn collinear_overlap() {
        let a = LineString::from_coords(&[(0.0, 0.0), (2.0, 0.0)]);
        let b = LineString::from_coords(&[(1.0, 0.0), (3.0, 0.0)]);
        assert!(a.intersects(&b));
        assert_eq!(a.intersections(&b).len(), 2);
    }

    #[test]
    fn long_lines_cross_where_planar_maths_says_they_do_not() {
        // In planar lon/lat these miss each other; as great circles the
        // northern arc bulges poleward and crosses the meridian segment.
        let arc = LineString::from_coords(&[(-60.0, 60.0), (60.0, 60.0)]);
        let meridian = LineString::from_coords(&[(0.0, 62.0), (0.0, 75.0)]);
        assert!(arc.intersects(&meridian));
    }

    #[test]
    fn serde_roundtrip() {
        let l = LineString::from_coords(&[(1.0, 2.0), (3.0, 4.0)]);
        let json = serde_json::to_string(&l).unwrap();
        assert_eq!(json, "[[1.0,2.0],[3.0,4.0]]");
        assert_eq!(serde_json::from_str::<LineString>(&json).unwrap(), l);
    }
}
