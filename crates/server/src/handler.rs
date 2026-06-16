//! Handler implementation for Spatio RPC service

use crate::protocol::{CurrentLocation, LocationUpdate, SpatioService, Stats};
use crate::reader::Reader;
use crate::writer::WriteOp;
use spatio::Spatio;
use spatio_types::geo::{DistanceMetric, Point, Polygon};
use spatio_types::point::Point3d;
use std::sync::Arc;
use tarpc::context;
use tokio::sync::{mpsc, oneshot};

/// Upper bound on result/neighbour counts accepted from the wire, so a single
/// request can't drive an unbounded allocation.
const MAX_QUERY_LIMIT: usize = 100_000;

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

    /// Enqueue a write and await its actual completion on the writer thread.
    async fn submit_write(
        &self,
        make_op: impl FnOnce(oneshot::Sender<Result<(), String>>) -> WriteOp,
    ) -> Result<(), String> {
        let (ack_tx, ack_rx) = oneshot::channel();
        self.write_tx
            .send(make_op(ack_tx))
            .await
            .map_err(|_| "Server storage is overwhelmed or shutting down".to_string())?;
        ack_rx
            .await
            .map_err(|_| "Write was dropped before completion".to_string())?
    }
}

/// Run a blocking reader call on the blocking pool so it can't stall the async
/// runtime, mapping a join failure to an error string.
async fn blocking<T, F>(f: F) -> Result<T, String>
where
    F: FnOnce() -> Result<T, String> + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| format!("Internal error: {e}"))?
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
        self.submit_write(|ack| WriteOp::Upsert {
            namespace,
            id,
            point,
            metadata,
            ack,
        })
        .await
    }

    async fn get(
        self,
        _: context::Context,
        namespace: String,
        id: String,
    ) -> Result<Option<CurrentLocation>, String> {
        let reader = self.reader;
        blocking(move || reader.get(&namespace, &id)).await
    }

    async fn delete(
        self,
        _: context::Context,
        namespace: String,
        id: String,
    ) -> Result<(), String> {
        self.submit_write(|ack| WriteOp::Delete { namespace, id, ack })
            .await
    }

    async fn query_radius(
        self,
        _: context::Context,
        namespace: String,
        center: Point3d,
        radius: f64,
        limit: usize,
    ) -> Result<Vec<(CurrentLocation, f64)>, String> {
        let reader = self.reader;
        let limit = limit.min(MAX_QUERY_LIMIT);
        blocking(move || reader.query_radius(&namespace, &center, radius, limit)).await
    }

    async fn knn(
        self,
        _: context::Context,
        namespace: String,
        center: Point3d,
        k: usize,
    ) -> Result<Vec<(CurrentLocation, f64)>, String> {
        let reader = self.reader;
        let k = k.min(MAX_QUERY_LIMIT);
        blocking(move || reader.knn(&namespace, &center, k)).await
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
        let reader = self.reader;
        let limit = limit.min(MAX_QUERY_LIMIT);
        blocking(move || reader.query_bbox(&namespace, min_x, min_y, max_x, max_y, limit)).await
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
        let reader = self.reader;
        let limit = limit.min(MAX_QUERY_LIMIT);
        blocking(move || reader.query_cylinder(&namespace, center, min_z, max_z, radius, limit))
            .await
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
        let reader = self.reader;
        let limit = limit.min(MAX_QUERY_LIMIT);
        blocking(move || reader.query_trajectory(&namespace, &id, start_time, end_time, limit))
            .await
    }

    async fn insert_trajectory(
        self,
        _: context::Context,
        namespace: String,
        id: String,
        trajectory: Vec<(f64, Point3d, serde_json::Value)>,
    ) -> Result<(), String> {
        self.submit_write(|ack| WriteOp::InsertTrajectory {
            namespace,
            id,
            trajectory,
            ack,
        })
        .await
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
        let reader = self.reader;
        let limit = limit.min(MAX_QUERY_LIMIT);
        blocking(move || {
            reader.query_bbox_3d(&namespace, min_x, min_y, min_z, max_x, max_y, max_z, limit)
        })
        .await
    }

    async fn query_near(
        self,
        _: context::Context,
        namespace: String,
        id: String,
        radius: f64,
        limit: usize,
    ) -> Result<Vec<(CurrentLocation, f64)>, String> {
        let reader = self.reader;
        let limit = limit.min(MAX_QUERY_LIMIT);
        blocking(move || reader.query_near(&namespace, &id, radius, limit)).await
    }

    async fn contains(
        self,
        _: context::Context,
        namespace: String,
        polygon: Polygon,
        limit: usize,
    ) -> Result<Vec<CurrentLocation>, String> {
        let reader = self.reader;
        let limit = limit.min(MAX_QUERY_LIMIT);
        blocking(move || reader.contains(&namespace, &polygon, limit)).await
    }

    async fn distance(
        self,
        _: context::Context,
        namespace: String,
        id1: String,
        id2: String,
        metric: Option<DistanceMetric>,
    ) -> Result<Option<f64>, String> {
        let reader = self.reader;
        blocking(move || reader.distance(&namespace, &id1, &id2, metric)).await
    }

    async fn distance_to(
        self,
        _: context::Context,
        namespace: String,
        id: String,
        point: Point,
        metric: Option<DistanceMetric>,
    ) -> Result<Option<f64>, String> {
        let reader = self.reader;
        blocking(move || reader.distance_to(&namespace, &id, &point, metric)).await
    }

    async fn convex_hull(
        self,
        _: context::Context,
        namespace: String,
    ) -> Result<Option<Polygon>, String> {
        let reader = self.reader;
        blocking(move || reader.convex_hull(&namespace)).await
    }

    async fn bounding_box(
        self,
        _: context::Context,
        namespace: String,
    ) -> Result<Option<spatio_types::bbox::BoundingBox2D>, String> {
        let reader = self.reader;
        blocking(move || reader.bounding_box(&namespace)).await
    }

    async fn stats(self, _: context::Context) -> Stats {
        self.reader.stats()
    }
}
