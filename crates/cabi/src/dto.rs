//! Flat, JSON-friendly data-transfer structs sent across the C ABI.
//!
//! Core types (`CurrentLocation`, `LocationUpdate`) serialize to a nested shape
//! (`Point3d` wraps a `geo::Point`), which is awkward for binding authors. These
//! DTOs flatten coordinates to `x`/`y`/`z` scalars and timestamps to unix
//! seconds (`f64`), matching the Python binding's result tuples.

use serde::Serialize;
use serde_json::Value;
use spatio::db::{CurrentLocation, LocationUpdate};
use std::time::{SystemTime, UNIX_EPOCH};

/// Seconds since the unix epoch (matches the timestamp convention used at the
/// boundary and by the Python bindings).
fn unix_secs(t: SystemTime) -> f64 {
    t.duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

/// A current location, flattened for the wire.
#[derive(Serialize)]
pub struct LocationDto {
    pub object_id: String,
    pub namespace: String,
    pub x: f64,
    pub y: f64,
    pub z: f64,
    pub metadata: Value,
    pub timestamp: f64,
}

impl From<&CurrentLocation> for LocationDto {
    fn from(loc: &CurrentLocation) -> Self {
        Self {
            object_id: loc.object_id.clone(),
            namespace: loc.namespace.clone(),
            x: loc.position.x(),
            y: loc.position.y(),
            z: loc.position.z(),
            metadata: loc.metadata.clone(),
            timestamp: unix_secs(loc.timestamp),
        }
    }
}

/// A location paired with a distance (radius/knn/cylinder queries).
#[derive(Serialize)]
pub struct NeighborDto {
    #[serde(flatten)]
    pub location: LocationDto,
    pub distance: f64,
}

impl NeighborDto {
    pub fn new(loc: &CurrentLocation, distance: f64) -> Self {
        Self {
            location: LocationDto::from(loc),
            distance,
        }
    }
}

/// A single historical trajectory sample.
#[derive(Serialize)]
pub struct TrajectoryPointDto {
    pub x: f64,
    pub y: f64,
    pub timestamp: f64,
    pub metadata: Value,
}

impl From<&LocationUpdate> for TrajectoryPointDto {
    fn from(u: &LocationUpdate) -> Self {
        Self {
            x: u.position.x(),
            y: u.position.y(),
            timestamp: unix_secs(u.timestamp),
            metadata: u.metadata.clone(),
        }
    }
}
