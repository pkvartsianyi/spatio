//! In-memory index of stored shapes for one namespace.

use rstar::{AABB, RTree, RTreeObject};
use spatio_types::geo::Point;
use spatio_types::shape::Shape;
use std::collections::HashMap;
use std::sync::Arc;

const M_PER_DEG_LAT: f64 = 111_320.0;

/// Lon/lat box around `shape`, padded so circles and corridors are covered.
fn envelope(shape: &Shape) -> AABB<[f64; 2]> {
    let pad_m = match shape {
        Shape::Polygon(_) => 0.0,
        Shape::Circle { radius_m, .. } => *radius_m,
        Shape::Corridor { half_width_m, .. } => *half_width_m,
    };
    let (mut min_x, mut min_y, mut max_x, mut max_y) = (f64::MAX, f64::MAX, f64::MIN, f64::MIN);
    for (x, y) in shape.coords() {
        min_x = min_x.min(x);
        min_y = min_y.min(y);
        max_x = max_x.max(x);
        max_y = max_y.max(y);
    }
    pad(min_x, min_y, max_x, max_y, pad_m)
}

fn pad(min_x: f64, min_y: f64, max_x: f64, max_y: f64, m: f64) -> AABB<[f64; 2]> {
    if m <= 0.0 {
        return AABB::from_corners([min_x, min_y], [max_x, max_y]);
    }
    // Slightly over-pad: the exact test runs on every candidate anyway.
    let dlat = m * 1.01 / M_PER_DEG_LAT;
    let (lo, hi) = (min_y - dlat, max_y + dlat);
    if lo <= -89.0 || hi >= 89.0 {
        return AABB::from_corners([-180.0, lo.max(-90.0)], [180.0, hi.min(90.0)]);
    }
    let dlon = dlat / lo.abs().max(hi.abs()).to_radians().cos();
    if dlon >= 180.0 {
        return AABB::from_corners([-180.0, lo], [180.0, hi]);
    }
    AABB::from_corners([min_x - dlon, lo], [max_x + dlon, hi])
}

struct Entry {
    id: Arc<str>,
    env: AABB<[f64; 2]>,
}

impl RTreeObject for Entry {
    type Envelope = AABB<[f64; 2]>;
    fn envelope(&self) -> Self::Envelope {
        self.env
    }
}

impl PartialEq for Entry {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

#[derive(Default)]
pub(crate) struct ShapeIndex {
    tree: RTree<Entry>,
    shapes: HashMap<Arc<str>, (Shape, AABB<[f64; 2]>)>,
    /// Envelopes reaching past ±180° longitude (padded shapes near the
    /// antimeridian); queries then also probe the wrapped longitude.
    wrapping: usize,
}

fn wraps(env: &AABB<[f64; 2]>) -> bool {
    env.lower()[0] < -180.0 || env.upper()[0] > 180.0
}

impl ShapeIndex {
    pub fn insert(&mut self, id: &str, shape: Shape) {
        self.remove(id);
        let env = envelope(&shape);
        let id: Arc<str> = id.into();
        self.wrapping += wraps(&env) as usize;
        self.tree.insert(Entry {
            id: id.clone(),
            env,
        });
        self.shapes.insert(id, (shape, env));
    }

    pub fn remove(&mut self, id: &str) -> bool {
        let Some((key, (_, env))) = self.shapes.remove_entry(id) else {
            return false;
        };
        self.wrapping -= wraps(&env) as usize;
        self.tree.remove(&Entry { id: key, env });
        true
    }

    pub fn get(&self, id: &str) -> Option<&Shape> {
        self.shapes.get(id).map(|(s, _)| s)
    }

    pub fn len(&self) -> usize {
        self.shapes.len()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, &Shape)> {
        self.shapes.iter().map(|(id, (s, _))| (&**id, s))
    }

    fn candidates(&self, query: AABB<[f64; 2]>) -> Vec<&Entry> {
        let mut out: Vec<&Entry> = self.tree.locate_in_envelope_intersecting(&query).collect();
        if self.wrapping > 0 {
            for shift in [-360.0, 360.0] {
                let (lo, hi) = (query.lower(), query.upper());
                let shifted = AABB::from_corners([lo[0] + shift, lo[1]], [hi[0] + shift, hi[1]]);
                for e in self.tree.locate_in_envelope_intersecting(&shifted) {
                    if !out.iter().any(|o| o.id == e.id) {
                        out.push(e);
                    }
                }
            }
        }
        out
    }

    /// IDs of shapes containing the point, sorted.
    pub fn containing(&self, lon: f64, lat: f64, limit: usize) -> Vec<String> {
        let p = Point::new(lon, lat);
        let mut ids: Vec<String> = self
            .candidates(AABB::from_point([lon, lat]))
            .into_iter()
            .filter(|e| self.shapes[&e.id].0.contains(&p))
            .map(|e| e.id.to_string())
            .collect();
        ids.sort_unstable();
        ids.truncate(limit);
        ids
    }

    /// Shapes within `meters` of the point, nearest first.
    pub fn within(&self, lon: f64, lat: f64, meters: f64, limit: usize) -> Vec<(String, f64)> {
        let p = Point::new(lon, lat);
        let mut hits: Vec<(String, f64)> = self
            .candidates(pad(lon, lat, lon, lat, meters))
            .into_iter()
            .filter_map(|e| {
                let d = self.shapes[&e.id].0.distance_to(&p);
                (d <= meters).then(|| (e.id.to_string(), d))
            })
            .collect();
        hits.sort_by(|a, b| a.1.total_cmp(&b.1).then_with(|| a.0.cmp(&b.0)));
        hits.truncate(limit);
        hits
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn circle_across_antimeridian() {
        let mut idx = ShapeIndex::default();
        idx.insert("c", Shape::circle(Point::new(179.999, 0.0), 1_000.0));
        assert_eq!(idx.containing(-179.999, 0.0, 10), vec!["c"]);
        assert!(idx.containing(-179.98, 0.0, 10).is_empty());
        idx.remove("c");
        assert_eq!(idx.wrapping, 0);
    }

    #[test]
    fn circle_over_pole() {
        let mut idx = ShapeIndex::default();
        idx.insert("p", Shape::circle(Point::new(0.0, 89.9), 50_000.0));
        assert_eq!(idx.containing(180.0, 89.9, 10), vec!["p"]);
    }
}
