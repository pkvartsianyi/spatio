use crate::geo::Point;
use geo::{Intersects, Rect};
use serde::{Deserialize, Serialize};

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

    /// Check if a point lies within this bounding box (edges inclusive).
    pub fn contains_point(&self, point: &Point) -> bool {
        self.rect.intersects(point.inner())
    }

    /// Check if this bounding box intersects with another.
    pub fn intersects(&self, other: &BoundingBox2D) -> bool {
        self.rect.intersects(&other.rect)
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
}
