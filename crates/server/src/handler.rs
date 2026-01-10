use crate::protocol::{CurrentLocation, LocationUpdate, SpatioService, Stats, UpsertOptions};
use spatio::Spatio;
use spatio_types::geo::{DistanceMetric, Point, Polygon};
use spatio_types::point::Point3d;
use std::sync::Arc;
use tarpc::context;

#[derive(Clone)]
pub struct Handler {
    db: Arc<Spatio>,
}

impl Handler {
    pub fn new(db: Arc<Spatio>) -> Self {
        Self { db }
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
        opts: Option<UpsertOptions>,
    ) -> Result<(), String> {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            let db_opts = opts.map(|o| spatio::config::SetOptions {
                ttl: Some(o.ttl),
                ..Default::default()
            });

            db.upsert(&namespace, &id, point, metadata, db_opts)
                .map_err(|e| e.to_string())
        })
        .await
        .map_err(|e| format!("Internal error: {}", e))?
    }

    async fn get(
        self,
        _: context::Context,
        namespace: String,
        id: String,
    ) -> Result<Option<CurrentLocation>, String> {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || match db.get(&namespace, &id) {
            Ok(Some(loc)) => Ok(Some(CurrentLocation {
                object_id: loc.object_id,
                position: loc.position,
                metadata: serde_json::to_vec(&loc.metadata).unwrap_or_default(),
            })),
            Ok(None) => Ok(None),
            Err(e) => Err(e.to_string()),
        })
        .await
        .map_err(|e| format!("Internal error: {}", e))?
    }

    async fn delete(
        self,
        _: context::Context,
        namespace: String,
        id: String,
    ) -> Result<(), String> {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || db.delete(&namespace, &id).map_err(|e| e.to_string()))
            .await
            .map_err(|e| format!("Internal error: {}", e))?
    }

    async fn query_radius(
        self,
        _: context::Context,
        namespace: String,
        center: Point3d,
        radius: f64,
        limit: usize,
    ) -> Result<Vec<(CurrentLocation, f64)>, String> {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            db.query_radius(&namespace, &center, radius, limit)
                .map(|results| {
                    results
                        .into_iter()
                        .map(|(loc, dist)| {
                            (
                                CurrentLocation {
                                    object_id: loc.object_id,
                                    position: loc.position,
                                    metadata: serde_json::to_vec(&loc.metadata).unwrap_or_default(),
                                },
                                dist,
                            )
                        })
                        .collect()
                })
                .map_err(|e| e.to_string())
        })
        .await
        .map_err(|e| format!("Internal error: {}", e))?
    }

    async fn knn(
        self,
        _: context::Context,
        namespace: String,
        center: Point3d,
        k: usize,
    ) -> Result<Vec<(CurrentLocation, f64)>, String> {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            db.knn(&namespace, &center, k)
                .map(|results| {
                    results
                        .into_iter()
                        .map(|(loc, dist)| {
                            (
                                CurrentLocation {
                                    object_id: loc.object_id,
                                    position: loc.position,
                                    metadata: serde_json::to_vec(&loc.metadata).unwrap_or_default(),
                                },
                                dist,
                            )
                        })
                        .collect()
                })
                .map_err(|e| e.to_string())
        })
        .await
        .map_err(|e| format!("Internal error: {}", e))?
    }

    async fn stats(self, _: context::Context) -> Stats {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            let s = db.stats();
            Stats {
                object_count: s.hot_state_objects,
                memory_usage_bytes: s.memory_usage_bytes,
            }
        })
        .await
        .unwrap()
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
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            db.query_bbox(&namespace, min_x, min_y, max_x, max_y, limit)
                .map(|results| {
                    results
                        .into_iter()
                        .map(|loc| CurrentLocation {
                            object_id: loc.object_id,
                            position: loc.position,
                            metadata: serde_json::to_vec(&loc.metadata).unwrap_or_default(),
                        })
                        .collect()
                })
                .map_err(|e| e.to_string())
        })
        .await
        .map_err(|e| format!("Internal error: {}", e))?
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
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            db.query_within_cylinder(&namespace, center, min_z, max_z, radius, limit)
                .map(|results| {
                    results
                        .into_iter()
                        .map(|(loc, dist)| {
                            (
                                CurrentLocation {
                                    object_id: loc.object_id,
                                    position: loc.position,
                                    metadata: serde_json::to_vec(&loc.metadata).unwrap_or_default(),
                                },
                                dist,
                            )
                        })
                        .collect()
                })
                .map_err(|e| e.to_string())
        })
        .await
        .map_err(|e| format!("Internal error: {}", e))?
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
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            let start = start_time
                .map(|t| std::time::UNIX_EPOCH + std::time::Duration::from_secs_f64(t))
                .unwrap_or(std::time::UNIX_EPOCH);
            let end = end_time
                .map(|t| std::time::UNIX_EPOCH + std::time::Duration::from_secs_f64(t))
                .unwrap_or_else(std::time::SystemTime::now);

            db.query_trajectory(&namespace, &id, start, end, limit)
                .map(|results| {
                    results
                        .into_iter()
                        .map(|upd| {
                            let timestamp = upd
                                .timestamp
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_secs_f64();

                            LocationUpdate {
                                timestamp,
                                position: upd.position,
                                metadata: serde_json::to_vec(&upd.metadata).unwrap_or_default(),
                            }
                        })
                        .collect()
                })
                .map_err(|e| e.to_string())
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
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            let updates: Vec<spatio::config::TemporalPoint> = trajectory
                .into_iter()
                .map(|(ts, p, _meta)| {
                    // TODO: Current DB insert_trajectory uses TemporalPoint (2D) and drops Z/metadata
                    // Spatio-core needs update to support 3D points and metadata in trajectory storage
                    let timestamp = std::time::UNIX_EPOCH + std::time::Duration::from_secs_f64(ts);
                    spatio::config::TemporalPoint::new(*p.point_2d(), timestamp)
                })
                .collect();

            db.insert_trajectory(&namespace, &id, &updates)
                .map_err(|e| e.to_string())
        })
        .await
        .map_err(|e| format!("Internal error: {}", e))?
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
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            db.query_within_bbox_3d(&namespace, min_x, min_y, min_z, max_x, max_y, max_z, limit)
                .map(|results| {
                    results
                        .into_iter()
                        .map(|loc| CurrentLocation {
                            object_id: loc.object_id,
                            position: loc.position,
                            metadata: serde_json::to_vec(&loc.metadata).unwrap_or_default(),
                        })
                        .collect()
                })
                .map_err(|e| e.to_string())
        })
        .await
        .map_err(|e| format!("Internal error: {}", e))?
    }

    async fn query_near(
        self,
        _: context::Context,
        namespace: String,
        id: String,
        radius: f64,
        limit: usize,
    ) -> Result<Vec<(CurrentLocation, f64)>, String> {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            db.query_near(&namespace, &id, radius, limit)
                .map(|results| {
                    results
                        .into_iter()
                        .map(|(loc, dist)| {
                            (
                                CurrentLocation {
                                    object_id: loc.object_id,
                                    position: loc.position,
                                    metadata: serde_json::to_vec(&loc.metadata).unwrap_or_default(),
                                },
                                dist,
                            )
                        })
                        .collect()
                })
                .map_err(|e| e.to_string())
        })
        .await
        .map_err(|e| format!("Internal error: {}", e))?
    }

    async fn contains(
        self,
        _: context::Context,
        namespace: String,
        polygon: Polygon,
        limit: usize,
    ) -> Result<Vec<CurrentLocation>, String> {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            db.query_polygon(&namespace, &polygon, limit)
                .map(|results| {
                    results
                        .into_iter()
                        .map(|loc| CurrentLocation {
                            object_id: loc.object_id,
                            position: loc.position,
                            metadata: serde_json::to_vec(&loc.metadata).unwrap_or_default(),
                        })
                        .collect()
                })
                .map_err(|e| e.to_string())
        })
        .await
        .map_err(|e| format!("Internal error: {}", e))?
    }

    async fn distance(
        self,
        _: context::Context,
        namespace: String,
        id1: String,
        id2: String,
        metric: Option<DistanceMetric>,
    ) -> Result<Option<f64>, String> {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            db.distance_between(&namespace, &id1, &id2, metric.unwrap_or_default())
                .map_err(|e| e.to_string())
        })
        .await
        .map_err(|e| format!("Internal error: {}", e))?
    }

    async fn distance_to(
        self,
        _: context::Context,
        namespace: String,
        id: String,
        point: Point,
        metric: Option<DistanceMetric>,
    ) -> Result<Option<f64>, String> {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            db.distance_to(&namespace, &id, &point, metric.unwrap_or_default())
                .map_err(|e| e.to_string())
        })
        .await
        .map_err(|e| format!("Internal error: {}", e))?
    }

    async fn convex_hull(
        self,
        _: context::Context,
        namespace: String,
    ) -> Result<Option<Polygon>, String> {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || db.convex_hull(&namespace).map_err(|e| e.to_string()))
            .await
            .map_err(|e| format!("Internal error: {}", e))?
    }

    async fn bounding_box(
        self,
        _: context::Context,
        namespace: String,
    ) -> Result<Option<spatio_types::bbox::BoundingBox2D>, String> {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            db.bounding_box(&namespace)
                .map(|opt| opt.map(spatio_types::bbox::BoundingBox2D::from_rect))
                .map_err(|e| e.to_string())
        })
        .await
        .map_err(|e| format!("Internal error: {}", e))?
    }
}
