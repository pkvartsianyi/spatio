use crate::geo::Point;
use geo::Rect;
use serde::{Deserialize, Serialize};
use std::time::SystemTime;

/// A 2D axis-aligned bounding box.
///
/// Represents a rectangular area defined by minimum and maximum coordinates.
/// This is a wrapper around `geo::Rect` with additional functionality.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BoundingBox2D {
    /// The underlying geometric rectangle
    pub rect: Rect,
}

impl BoundingBox2D {
    /// Create a new bounding box from minimum and maximum coordinates.
    ///
    /// # Arguments
    ///
    /// * `min_x` - Minimum longitude/x coordinate
    /// * `min_y` - Minimum latitude/y coordinate
    /// * `max_x` - Maximum longitude/x coordinate
    /// * `max_y` - Maximum latitude/y coordinate
    ///
    /// # Examples
    ///
    /// ```
    /// use spatio_types::bbox::BoundingBox2D;
    ///
    /// let bbox = BoundingBox2D::new(-74.0, 40.7, -73.9, 40.8);
    /// ```
    pub fn new(min_x: f64, min_y: f64, max_x: f64, max_y: f64) -> Self {
        Self {
            rect: Rect::new(
                geo::coord! { x: min_x, y: min_y },
                geo::coord! { x: max_x, y: max_y },
            ),
        }
    }

    /// Create a bounding box from a `geo::Rect`.
    pub fn from_rect(rect: Rect) -> Self {
        Self { rect }
    }

    /// Get the minimum x coordinate.
    pub fn min_x(&self) -> f64 {
        self.rect.min().x
    }

    /// Get the minimum y coordinate.
    pub fn min_y(&self) -> f64 {
        self.rect.min().y
    }

    /// Get the maximum x coordinate.
    pub fn max_x(&self) -> f64 {
        self.rect.max().x
    }

    /// Get the maximum y coordinate.
    pub fn max_y(&self) -> f64 {
        self.rect.max().y
    }

    /// Get the center point of the bounding box.
    pub fn center(&self) -> Point {
        Point::new(
            (self.min_x() + self.max_x()) / 2.0,
            (self.min_y() + self.max_y()) / 2.0,
        )
    }

    /// Get the width of the bounding box.
    pub fn width(&self) -> f64 {
        self.max_x() - self.min_x()
    }

    /// Get the height of the bounding box.
    pub fn height(&self) -> f64 {
        self.max_y() - self.min_y()
    }

    /// Check if a point is contained within this bounding box.
    pub fn contains_point(&self, point: &Point) -> bool {
        point.x() >= self.min_x()
            && point.x() <= self.max_x()
            && point.y() >= self.min_y()
            && point.y() <= self.max_y()
    }

    /// Check if this bounding box intersects with another.
    pub fn intersects(&self, other: &BoundingBox2D) -> bool {
        !(self.max_x() < other.min_x()
            || self.min_x() > other.max_x()
            || self.max_y() < other.min_y()
            || self.min_y() > other.max_y())
    }

    /// Expand the bounding box by a given amount in all directions.
    pub fn expand(&self, amount: f64) -> Self {
        Self::new(
            self.min_x() - amount,
            self.min_y() - amount,
            self.max_x() + amount,
            self.max_y() + amount,
        )
    }
}

/// A 3D axis-aligned bounding box.
///
/// Represents a rectangular volume defined by minimum and maximum coordinates
/// in three dimensions (x, y, z).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BoundingBox3D {
    /// Minimum x coordinate
    pub min_x: f64,
    /// Minimum y coordinate
    pub min_y: f64,
    /// Minimum z coordinate (altitude/elevation)
    pub min_z: f64,
    /// Maximum x coordinate
    pub max_x: f64,
    /// Maximum y coordinate
    pub max_y: f64,
    /// Maximum z coordinate (altitude/elevation)
    pub max_z: f64,
}

impl BoundingBox3D {
    /// Create a new 3D bounding box from minimum and maximum coordinates.
    ///
    /// # Arguments
    ///
    /// * `min_x` - Minimum x coordinate
    /// * `min_y` - Minimum y coordinate
    /// * `min_z` - Minimum z coordinate (altitude/elevation)
    /// * `max_x` - Maximum x coordinate
    /// * `max_y` - Maximum y coordinate
    /// * `max_z` - Maximum z coordinate (altitude/elevation)
    ///
    /// # Examples
    ///
    /// ```
    /// use spatio_types::bbox::BoundingBox3D;
    ///
    /// let bbox = BoundingBox3D::new(-74.0, 40.7, 0.0, -73.9, 40.8, 100.0);
    /// ```
    pub fn new(min_x: f64, min_y: f64, min_z: f64, max_x: f64, max_y: f64, max_z: f64) -> Self {
        Self {
            min_x,
            min_y,
            min_z,
            max_x,
            max_y,
            max_z,
        }
    }

    /// Get the center point of the bounding box.
    pub fn center(&self) -> (f64, f64, f64) {
        (
            (self.min_x + self.max_x) / 2.0,
            (self.min_y + self.max_y) / 2.0,
            (self.min_z + self.max_z) / 2.0,
        )
    }

    /// Get the width (x dimension) of the bounding box.
    pub fn width(&self) -> f64 {
        self.max_x - self.min_x
    }

    /// Get the height (y dimension) of the bounding box.
    pub fn height(&self) -> f64 {
        self.max_y - self.min_y
    }

    /// Get the depth (z dimension) of the bounding box.
    pub fn depth(&self) -> f64 {
        self.max_z - self.min_z
    }

    /// Get the volume of the bounding box.
    pub fn volume(&self) -> f64 {
        self.width() * self.height() * self.depth()
    }

    /// Check if a 3D point is contained within this bounding box.
    pub fn contains_point(&self, x: f64, y: f64, z: f64) -> bool {
        x >= self.min_x
            && x <= self.max_x
            && y >= self.min_y
            && y <= self.max_y
            && z >= self.min_z
            && z <= self.max_z
    }

    /// Check if this bounding box intersects with another 3D bounding box.
    pub fn intersects(&self, other: &BoundingBox3D) -> bool {
        !(self.max_x < other.min_x
            || self.min_x > other.max_x
            || self.max_y < other.min_y
            || self.min_y > other.max_y
            || self.max_z < other.min_z
            || self.min_z > other.max_z)
    }

    /// Expand the bounding box by a given amount in all directions.
    pub fn expand(&self, amount: f64) -> Self {
        Self::new(
            self.min_x - amount,
            self.min_y - amount,
            self.min_z - amount,
            self.max_x + amount,
            self.max_y + amount,
            self.max_z + amount,
        )
    }

    /// Project the 3D bounding box to a 2D bounding box (discarding z).
    pub fn to_2d(&self) -> BoundingBox2D {
        BoundingBox2D::new(self.min_x, self.min_y, self.max_x, self.max_y)
    }
}

/// A 2D bounding box with an associated timestamp.
///
/// Useful for tracking how spatial bounds change over time.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TemporalBoundingBox2D {
    /// The bounding box
    pub bbox: BoundingBox2D,
    /// The timestamp when this bounding box was valid
    pub timestamp: SystemTime,
}

impl TemporalBoundingBox2D {
    /// Create a new temporal bounding box.
    pub fn new(bbox: BoundingBox2D, timestamp: SystemTime) -> Self {
        Self { bbox, timestamp }
    }

    /// Get a reference to the bounding box.
    pub fn bbox(&self) -> &BoundingBox2D {
        &self.bbox
    }

    /// Get a reference to the timestamp.
    pub fn timestamp(&self) -> &SystemTime {
        &self.timestamp
    }
}

/// A 3D bounding box with an associated timestamp.
///
/// Useful for tracking how 3D spatial bounds change over time.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TemporalBoundingBox3D {
    /// The 3D bounding box
    pub bbox: BoundingBox3D,
    /// The timestamp when this bounding box was valid
    pub timestamp: SystemTime,
}

impl TemporalBoundingBox3D {
    /// Create a new temporal 3D bounding box.
    pub fn new(bbox: BoundingBox3D, timestamp: SystemTime) -> Self {
        Self { bbox, timestamp }
    }

    /// Get a reference to the bounding box.
    pub fn bbox(&self) -> &BoundingBox3D {
        &self.bbox
    }

    /// Get a reference to the timestamp.
    pub fn timestamp(&self) -> &SystemTime {
        &self.timestamp
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bbox2d_creation() {
        let bbox = BoundingBox2D::new(-74.0, 40.7, -73.9, 40.8);
        assert_eq!(bbox.min_x(), -74.0);
        assert_eq!(bbox.min_y(), 40.7);
        assert_eq!(bbox.max_x(), -73.9);
        assert_eq!(bbox.max_y(), 40.8);
    }

    #[test]
    fn test_bbox2d_dimensions() {
        let bbox = BoundingBox2D::new(0.0, 0.0, 10.0, 5.0);
        assert_eq!(bbox.width(), 10.0);
        assert_eq!(bbox.height(), 5.0);
    }

    #[test]
    fn test_bbox2d_center() {
        let bbox = BoundingBox2D::new(0.0, 0.0, 10.0, 10.0);
        let center = bbox.center();
        assert_eq!(center.x(), 5.0);
        assert_eq!(center.y(), 5.0);
    }

    #[test]
    fn test_bbox2d_contains() {
        let bbox = BoundingBox2D::new(0.0, 0.0, 10.0, 10.0);
        assert!(bbox.contains_point(&Point::new(5.0, 5.0)));
        assert!(bbox.contains_point(&Point::new(0.0, 0.0)));
        assert!(bbox.contains_point(&Point::new(10.0, 10.0)));
        assert!(!bbox.contains_point(&Point::new(-1.0, 5.0)));
        assert!(!bbox.contains_point(&Point::new(11.0, 5.0)));
    }

    #[test]
    fn test_bbox2d_intersects() {
        let bbox1 = BoundingBox2D::new(0.0, 0.0, 10.0, 10.0);
        let bbox2 = BoundingBox2D::new(5.0, 5.0, 15.0, 15.0);
        let bbox3 = BoundingBox2D::new(20.0, 20.0, 30.0, 30.0);

        assert!(bbox1.intersects(&bbox2));
        assert!(bbox2.intersects(&bbox1));
        assert!(!bbox1.intersects(&bbox3));
        assert!(!bbox3.intersects(&bbox1));
    }

    #[test]
    fn test_bbox2d_expand() {
        let bbox = BoundingBox2D::new(0.0, 0.0, 10.0, 10.0);
        let expanded = bbox.expand(5.0);
        assert_eq!(expanded.min_x(), -5.0);
        assert_eq!(expanded.min_y(), -5.0);
        assert_eq!(expanded.max_x(), 15.0);
        assert_eq!(expanded.max_y(), 15.0);
    }

    #[test]
    fn test_bbox3d_creation() {
        let bbox = BoundingBox3D::new(0.0, 0.0, 0.0, 10.0, 10.0, 10.0);
        assert_eq!(bbox.min_x, 0.0);
        assert_eq!(bbox.min_y, 0.0);
        assert_eq!(bbox.min_z, 0.0);
        assert_eq!(bbox.max_x, 10.0);
        assert_eq!(bbox.max_y, 10.0);
        assert_eq!(bbox.max_z, 10.0);
    }

    #[test]
    fn test_bbox3d_dimensions() {
        let bbox = BoundingBox3D::new(0.0, 0.0, 0.0, 10.0, 5.0, 3.0);
        assert_eq!(bbox.width(), 10.0);
        assert_eq!(bbox.height(), 5.0);
        assert_eq!(bbox.depth(), 3.0);
        assert_eq!(bbox.volume(), 150.0);
    }

    #[test]
    fn test_bbox3d_center() {
        let bbox = BoundingBox3D::new(0.0, 0.0, 0.0, 10.0, 10.0, 10.0);
        let (x, y, z) = bbox.center();
        assert_eq!(x, 5.0);
        assert_eq!(y, 5.0);
        assert_eq!(z, 5.0);
    }

    #[test]
    fn test_bbox3d_contains() {
        let bbox = BoundingBox3D::new(0.0, 0.0, 0.0, 10.0, 10.0, 10.0);
        assert!(bbox.contains_point(5.0, 5.0, 5.0));
        assert!(bbox.contains_point(0.0, 0.0, 0.0));
        assert!(bbox.contains_point(10.0, 10.0, 10.0));
        assert!(!bbox.contains_point(-1.0, 5.0, 5.0));
        assert!(!bbox.contains_point(5.0, 5.0, 11.0));
    }

    #[test]
    fn test_bbox3d_intersects() {
        let bbox1 = BoundingBox3D::new(0.0, 0.0, 0.0, 10.0, 10.0, 10.0);
        let bbox2 = BoundingBox3D::new(5.0, 5.0, 5.0, 15.0, 15.0, 15.0);
        let bbox3 = BoundingBox3D::new(20.0, 20.0, 20.0, 30.0, 30.0, 30.0);

        assert!(bbox1.intersects(&bbox2));
        assert!(bbox2.intersects(&bbox1));
        assert!(!bbox1.intersects(&bbox3));
        assert!(!bbox3.intersects(&bbox1));
    }

    #[test]
    fn test_bbox3d_to_2d() {
        let bbox3d = BoundingBox3D::new(0.0, 0.0, 5.0, 10.0, 10.0, 15.0);
        let bbox2d = bbox3d.to_2d();
        assert_eq!(bbox2d.min_x(), 0.0);
        assert_eq!(bbox2d.min_y(), 0.0);
        assert_eq!(bbox2d.max_x(), 10.0);
        assert_eq!(bbox2d.max_y(), 10.0);
    }

    #[test]
    fn test_temporal_bbox2d() {
        let bbox = BoundingBox2D::new(0.0, 0.0, 10.0, 10.0);
        let timestamp = SystemTime::now();
        let temporal_bbox = TemporalBoundingBox2D::new(bbox.clone(), timestamp);

        assert_eq!(temporal_bbox.bbox(), &bbox);
        assert_eq!(temporal_bbox.timestamp(), &timestamp);
    }

    #[test]
    fn test_temporal_bbox3d() {
        let bbox = BoundingBox3D::new(0.0, 0.0, 0.0, 10.0, 10.0, 10.0);
        let timestamp = SystemTime::now();
        let temporal_bbox = TemporalBoundingBox3D::new(bbox.clone(), timestamp);

        assert_eq!(temporal_bbox.bbox(), &bbox);
        assert_eq!(temporal_bbox.timestamp(), &timestamp);
    }
}
