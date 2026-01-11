//! Protocol definitions for Spatio RPC
//!
//! This module contains the service trait and types used for RPC communication.

#![allow(clippy::too_many_arguments)]

use serde::{Deserialize, Serialize};
use spatio_types::geo::{DistanceMetric, Point, Polygon};
use spatio_types::point::Point3d;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocationUpdate {
    pub timestamp: f64,
    pub position: Point3d,
    pub metadata: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Stats {
    pub object_count: usize,
    pub memory_usage_bytes: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CurrentLocation {
    pub object_id: String,
    pub position: Point3d,
    pub metadata: Vec<u8>,
}

#[allow(clippy::too_many_arguments)]
#[tarpc::service]
pub trait SpatioService {
    async fn upsert(
        namespace: String,
        id: String,
        point: Point3d,
        metadata: serde_json::Value,
    ) -> Result<(), String>;

    async fn get(namespace: String, id: String) -> Result<Option<CurrentLocation>, String>;

    async fn delete(namespace: String, id: String) -> Result<(), String>;

    async fn query_radius(
        namespace: String,
        center: Point3d,
        radius: f64,
        limit: usize,
    ) -> Result<Vec<(CurrentLocation, f64)>, String>;

    async fn knn(
        namespace: String,
        center: Point3d,
        k: usize,
    ) -> Result<Vec<(CurrentLocation, f64)>, String>;

    async fn query_bbox(
        namespace: String,
        min_x: f64,
        min_y: f64,
        max_x: f64,
        max_y: f64,
        limit: usize,
    ) -> Result<Vec<CurrentLocation>, String>;

    async fn query_cylinder(
        namespace: String,
        center: Point,
        min_z: f64,
        max_z: f64,
        radius: f64,
        limit: usize,
    ) -> Result<Vec<(CurrentLocation, f64)>, String>;

    async fn query_trajectory(
        namespace: String,
        id: String,
        start_time: Option<f64>,
        end_time: Option<f64>,
        limit: usize,
    ) -> Result<Vec<LocationUpdate>, String>;

    async fn insert_trajectory(
        namespace: String,
        id: String,
        trajectory: Vec<(f64, Point3d, serde_json::Value)>,
    ) -> Result<(), String>;

    async fn query_bbox_3d(
        namespace: String,
        min_x: f64,
        min_y: f64,
        min_z: f64,
        max_x: f64,
        max_y: f64,
        max_z: f64,
        limit: usize,
    ) -> Result<Vec<CurrentLocation>, String>;

    async fn query_near(
        namespace: String,
        id: String,
        radius: f64,
        limit: usize,
    ) -> Result<Vec<(CurrentLocation, f64)>, String>;

    async fn contains(
        namespace: String,
        polygon: Polygon,
        limit: usize,
    ) -> Result<Vec<CurrentLocation>, String>;

    async fn distance(
        namespace: String,
        id1: String,
        id2: String,
        metric: Option<DistanceMetric>,
    ) -> Result<Option<f64>, String>;

    async fn distance_to(
        namespace: String,
        id: String,
        point: Point,
        metric: Option<DistanceMetric>,
    ) -> Result<Option<f64>, String>;

    async fn convex_hull(namespace: String) -> Result<Option<Polygon>, String>;

    async fn bounding_box(
        namespace: String,
    ) -> Result<Option<spatio_types::bbox::BoundingBox2D>, String>;

    async fn stats() -> Stats;
}
