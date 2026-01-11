//! Handler implementation for Spatio RPC service

use crate::protocol::{CurrentLocation, LocationUpdate, SpatioService, Stats};
use crate::reader::Reader;
use crate::writer::WriteOp;
use spatio::Spatio;
use spatio_types::geo::{DistanceMetric, Point, Polygon};
use spatio_types::point::Point3d;
use std::sync::Arc;
use tarpc::context;
use tokio::sync::mpsc;

#[derive(Clone)]
pub struct Handler {
    write_tx: mpsc::Sender<WriteOp>,
    reader: Reader,
}

impl Handler {
    pub fn new(db: Arc<Spatio>, write_tx: mpsc::Sender<WriteOp>) -> Self {
        let reader = Reader::new(db);
        Self { write_tx, reader }
    }
}

impl SpatioService for Handler {
    async fn upsert(
        self,
        _: context::Context,
        namespace: String,
        id: String,
        point: Point3d,
        metadata: serde_json::Value,
    ) -> Result<(), String> {
        let op = WriteOp::Upsert {
            namespace,
            id,
            point,
            metadata,
        };

        self.write_tx
            .send(op)
            .await
            .map_err(|_| "Server storage is overwhelmed or shutting down".to_string())
    }

    async fn get(
        self,
        _: context::Context,
        namespace: String,
        id: String,
    ) -> Result<Option<CurrentLocation>, String> {
        self.reader.get(&namespace, &id)
    }

    async fn delete(
        self,
        _: context::Context,
        namespace: String,
        id: String,
    ) -> Result<(), String> {
        let op = WriteOp::Delete { namespace, id };
        self.write_tx
            .send(op)
            .await
            .map_err(|_| "Server storage is overwhelmed or shutting down".to_string())
    }

    async fn query_radius(
        self,
        _: context::Context,
        namespace: String,
        center: Point3d,
        radius: f64,
        limit: usize,
    ) -> Result<Vec<(CurrentLocation, f64)>, String> {
        self.reader.query_radius(&namespace, &center, radius, limit)
    }

    async fn knn(
        self,
        _: context::Context,
        namespace: String,
        center: Point3d,
        k: usize,
    ) -> Result<Vec<(CurrentLocation, f64)>, String> {
        self.reader.knn(&namespace, &center, k)
    }

    async fn query_bbox(
        self,
        _: context::Context,
        namespace: String,
        min_x: f64,
        min_y: f64,
        max_x: f64,
        max_y: f64,
        limit: usize,
    ) -> Result<Vec<CurrentLocation>, String> {
        self.reader
            .query_bbox(&namespace, min_x, min_y, max_x, max_y, limit)
    }

    async fn query_cylinder(
        self,
        _: context::Context,
        namespace: String,
        center: Point,
        min_z: f64,
        max_z: f64,
        radius: f64,
        limit: usize,
    ) -> Result<Vec<(CurrentLocation, f64)>, String> {
        self.reader
            .query_cylinder(&namespace, center, min_z, max_z, radius, limit)
    }

    async fn query_trajectory(
        self,
        _: context::Context,
        namespace: String,
        id: String,
        start_time: Option<f64>,
        end_time: Option<f64>,
        limit: usize,
    ) -> Result<Vec<LocationUpdate>, String> {
        let reader = self.reader.clone();
        tokio::task::spawn_blocking(move || {
            reader.query_trajectory(&namespace, &id, start_time, end_time, limit)
        })
        .await
        .map_err(|e| format!("Internal error: {}", e))?
    }

    async fn insert_trajectory(
        self,
        _: context::Context,
        namespace: String,
        id: String,
        trajectory: Vec<(f64, Point3d, serde_json::Value)>,
    ) -> Result<(), String> {
        let op = WriteOp::InsertTrajectory {
            namespace,
            id,
            trajectory,
        };
        self.write_tx
            .send(op)
            .await
            .map_err(|_| "Server storage is overwhelmed or shutting down".to_string())
    }

    async fn query_bbox_3d(
        self,
        _: context::Context,
        namespace: String,
        min_x: f64,
        min_y: f64,
        min_z: f64,
        max_x: f64,
        max_y: f64,
        max_z: f64,
        limit: usize,
    ) -> Result<Vec<CurrentLocation>, String> {
        self.reader
            .query_bbox_3d(&namespace, min_x, min_y, min_z, max_x, max_y, max_z, limit)
    }

    async fn query_near(
        self,
        _: context::Context,
        namespace: String,
        id: String,
        radius: f64,
        limit: usize,
    ) -> Result<Vec<(CurrentLocation, f64)>, String> {
        self.reader.query_near(&namespace, &id, radius, limit)
    }

    async fn contains(
        self,
        _: context::Context,
        namespace: String,
        polygon: Polygon,
        limit: usize,
    ) -> Result<Vec<CurrentLocation>, String> {
        self.reader.contains(&namespace, &polygon, limit)
    }

    async fn distance(
        self,
        _: context::Context,
        namespace: String,
        id1: String,
        id2: String,
        metric: Option<DistanceMetric>,
    ) -> Result<Option<f64>, String> {
        self.reader.distance(&namespace, &id1, &id2, metric)
    }

    async fn distance_to(
        self,
        _: context::Context,
        namespace: String,
        id: String,
        point: Point,
        metric: Option<DistanceMetric>,
    ) -> Result<Option<f64>, String> {
        self.reader.distance_to(&namespace, &id, &point, metric)
    }

    async fn convex_hull(
        self,
        _: context::Context,
        namespace: String,
    ) -> Result<Option<Polygon>, String> {
        self.reader.convex_hull(&namespace)
    }

    async fn bounding_box(
        self,
        _: context::Context,
        namespace: String,
    ) -> Result<Option<spatio_types::bbox::BoundingBox2D>, String> {
        self.reader.bounding_box(&namespace)
    }

    async fn stats(self, _: context::Context) -> Stats {
        self.reader.stats()
    }
}
