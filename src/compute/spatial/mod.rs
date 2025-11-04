pub mod algorithms;
pub use algorithms::{
    DistanceMetric, bounding_box, bounding_rect_for_points, convex_hull, distance_between, knn,
    point_in_polygon,
};

pub mod rtree;
pub use rtree::rtree::SpatialIndexManager;
