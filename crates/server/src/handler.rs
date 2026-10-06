//! Handler implementation for Spatio RPC service

use crate::protocol::{CurrentLocation, LocationUpdate, SpatioService, Stats};
use crate::reader::Reader;
use spatio::Spatio;
use spatio_types::geo::{DistanceMetric, Point, Polygon};
use spatio_types::point::Point3d;
use spatio_types::time::system_time_from_secs;
use std::sync::Arc;
use tarpc::context;
use tokio::sync::Semaphore;

/// Upper bound on result/neighbour counts accepted from the wire, so a reply
/// stays well under the transport frame limit.
pub const MAX_QUERY_LIMIT: usize = 10_000;

#[derive(Clone)]
pub struct Handler {
    db: Arc<Spatio>,
    reader: Reader,
    permits: Arc<Semaphore>,
}

impl Handler {
    pub fn new(db: Arc<Spatio>, permits: Arc<Semaphore>) -> Self {
        let reader = Reader::new(db.clone());
        Self {
            db,
            reader,
            permits,
        }
    }

    /// Run a blocking DB call on the blocking pool, bounded by the shared
    /// permit pool so requests can't pile up unbounded blocking work.
    async fn blocking<T, F>(&self, f: F) -> Result<T, String>
    where
        F: FnOnce() -> Result<T, String> + Send + 'static,
        T: Send + 'static,
    {
        let permit = self
            .permits
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| "Server is shutting down".to_string())?;
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            f()
        })
        .await
        .map_err(|e| format!("Internal error: {e}"))?
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
        let db = self.db.clone();
        self.blocking(move || {
            db.upsert(&namespace, &id, point, metadata, None)
                .map_err(|e| e.to_string())
        })
        .await
    }

    async fn get(
        self,
        _: context::Context,
        namespace: String,
        id: String,
    ) -> Result<Option<CurrentLocation>, String> {
        let reader = self.reader.clone();
        self.blocking(move || reader.get(&namespace, &id)).await
    }

    async fn delete(
        self,
        _: context::Context,
        namespace: String,
        id: String,
    ) -> Result<(), String> {
        let db = self.db.clone();
        self.blocking(move || db.delete(&namespace, &id).map_err(|e| e.to_string()))
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
        let reader = self.reader.clone();
        let limit = limit.min(MAX_QUERY_LIMIT);
        self.blocking(move || reader.query_radius(&namespace, &center, radius, limit))
            .await
    }

    async fn knn(
        self,
        _: context::Context,
        namespace: String,
        center: Point3d,
        k: usize,
    ) -> Result<Vec<(CurrentLocation, f64)>, String> {
        let reader = self.reader.clone();
        let k = k.min(MAX_QUERY_LIMIT);
        self.blocking(move || reader.knn(&namespace, &center, k))
            .await
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
        let reader = self.reader.clone();
        let limit = limit.min(MAX_QUERY_LIMIT);
        self.blocking(move || reader.query_bbox(&namespace, min_x, min_y, max_x, max_y, limit))
            .await
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
        let reader = self.reader.clone();
        let limit = limit.min(MAX_QUERY_LIMIT);
        self.blocking(move || {
            reader.query_cylinder(&namespace, center, min_z, max_z, radius, limit)
        })
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
        let reader = self.reader.clone();
        let limit = limit.min(MAX_QUERY_LIMIT);
        self.blocking(move || reader.query_trajectory(&namespace, &id, start_time, end_time, limit))
            .await
    }

    async fn insert_trajectory(
        self,
        _: context::Context,
        namespace: String,
        id: String,
        trajectory: Vec<(f64, Point)>,
    ) -> Result<(), String> {
        let db = self.db.clone();
        self.blocking(move || {
            let points = trajectory
                .into_iter()
                .map(|(ts, p)| Ok(spatio::TemporalPoint::new(p, system_time_from_secs(ts)?)))
                .collect::<Result<Vec<_>, String>>()?;
            db.insert_trajectory(&namespace, &id, &points)
                .map_err(|e| e.to_string())
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
        let reader = self.reader.clone();
        let limit = limit.min(MAX_QUERY_LIMIT);
        self.blocking(move || {
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
        let reader = self.reader.clone();
        let limit = limit.min(MAX_QUERY_LIMIT);
        self.blocking(move || reader.query_near(&namespace, &id, radius, limit))
            .await
    }

    async fn contains(
        self,
        _: context::Context,
        namespace: String,
        polygon: Polygon,
        limit: usize,
    ) -> Result<Vec<CurrentLocation>, String> {
        let reader = self.reader.clone();
        let limit = limit.min(MAX_QUERY_LIMIT);
        self.blocking(move || reader.contains(&namespace, &polygon, limit))
            .await
    }

    async fn distance(
        self,
        _: context::Context,
        namespace: String,
        id1: String,
        id2: String,
        metric: Option<DistanceMetric>,
    ) -> Result<Option<f64>, String> {
        let reader = self.reader.clone();
        self.blocking(move || reader.distance(&namespace, &id1, &id2, metric))
            .await
    }

    async fn distance_to(
        self,
        _: context::Context,
        namespace: String,
        id: String,
        point: Point,
        metric: Option<DistanceMetric>,
    ) -> Result<Option<f64>, String> {
        let reader = self.reader.clone();
        self.blocking(move || reader.distance_to(&namespace, &id, &point, metric))
            .await
    }

    async fn convex_hull(
        self,
        _: context::Context,
        namespace: String,
    ) -> Result<Option<Polygon>, String> {
        let reader = self.reader.clone();
        self.blocking(move || reader.convex_hull(&namespace)).await
    }

    async fn bounding_box(
        self,
        _: context::Context,
        namespace: String,
    ) -> Result<Option<spatio_types::bbox::BoundingBox2D>, String> {
        let reader = self.reader.clone();
        self.blocking(move || reader.bounding_box(&namespace)).await
    }

    async fn stats(self, _: context::Context) -> Result<Stats, String> {
        let reader = self.reader.clone();
        self.blocking(move || Ok(reader.stats())).await
    }
}
