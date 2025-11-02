use geo::Point;
use serde::{Deserialize, Serialize};
use std::time::SystemTime;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Point3d {
    pub point: Point<f64>,
    pub z: f64,
}

/// A geographic point with an associated timestamp.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TemporalPoint {
    pub point: Point<f64>,
    pub timestamp: SystemTime,
}

impl TemporalPoint {
    pub fn new(point: Point<f64>, timestamp: SystemTime) -> Self {
        Self { point, timestamp }
    }

    pub fn point(&self) -> &Point<f64> {
        &self.point
    }

    pub fn timestamp(&self) -> &SystemTime {
        &self.timestamp
    }
}

/// A geographic point with an associated altitude and timestamp.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TemporalPoint3D {
    pub point: Point<f64>,
    pub altitude: f64,
    pub timestamp: SystemTime,
}

impl TemporalPoint3D {
    pub fn new(point: Point<f64>, altitude: f64, timestamp: SystemTime) -> Self {
        Self {
            point,
            altitude,
            timestamp,
        }
    }

    pub fn point(&self) -> &Point<f64> {
        &self.point
    }

    pub fn altitude(&self) -> f64 {
        self.altitude
    }

    pub fn timestamp(&self) -> &SystemTime {
        &self.timestamp
    }
}
