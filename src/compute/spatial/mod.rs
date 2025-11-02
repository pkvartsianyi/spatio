//! Spatial computation and query processing.
//!
//! This module provides spatial algorithms, indexing, and query execution:
//! - `algorithms`: Core spatial algorithms (distance, KNN, bounding box, convex hull)
//! - `index`: R-tree based spatial indexing for 2D and 3D data
//! - `queries_2d`: 2D spatial query operations
//! - `queries_3d`: 3D spatial query operations

pub mod algorithms;
pub mod index;
pub mod queries_2d;
pub mod queries_3d;

pub use algorithms::{
    DistanceMetric, bounding_box, bounding_rect_for_points, convex_hull, distance_between, knn,
    point_in_polygon,
};
pub use index::{BBoxQuery, CylinderQuery, IndexedPoint3D, SpatialIndexManager};
