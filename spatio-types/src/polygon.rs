use std::time::SystemTime;
use geo::Polygon;
use serde::{Serialize, Deserialize};
use crate::point::{Point3d};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Polygon3D {
    points: Vec<Point3d>,
}

impl Polygon3D {
    pub fn new(points: Vec<Point3d>) -> Self {
        Self { points }
    }

    pub fn points(&self) -> &Vec<Point3d> {
        &self.points
    }

}


#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PolygonDynamic {
    pub polygon: Polygon,
    pub timestamp: SystemTime,
}

impl PolygonDynamic {
    pub fn new(polygon: Polygon, timestamp: SystemTime) -> Self {
        Self { polygon, timestamp }
    }

    pub fn polygon(&self) -> &Polygon {
        &self.polygon
    }

    pub fn timestamp(&self) -> &SystemTime {
        &self.timestamp
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PolygonDynamic3D {
    pub polygon: Polygon3D,
    pub timestamp: SystemTime,
}

impl PolygonDynamic3D {
    pub fn new(polygon: Polygon3D, timestamp: SystemTime) -> Self {
        Self { polygon, timestamp }
    }

    pub fn polygon(&self) -> &Polygon3D {
        &self.polygon
    }

    pub fn timestamp(&self) -> &SystemTime {
        &self.timestamp
    }
}
