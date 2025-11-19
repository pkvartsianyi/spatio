pub mod algorithms;
pub use algorithms::{
    DistanceMetric, bounding_box, bounding_rect_for_points, convex_hull, distance_between,
    expand_bbox, geodesic_polygon_area, knn, point_in_polygon, polygon_area,
};

pub mod queries;

pub mod rtree;
pub use rtree::{BBoxQuery, CylinderQuery, SpatialIndexManager};
