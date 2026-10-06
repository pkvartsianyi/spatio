pub mod algorithms;
pub use algorithms::{
    DistanceMetric, bounding_box, convex_hull, distance_between, point_in_polygon,
};

pub mod rtree;
pub use rtree::{BBoxQuery, CylinderQuery, SpatialIndexManager};
