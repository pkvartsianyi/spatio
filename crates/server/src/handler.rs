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

// tarpc 0.34+ removed #[tarpc::server] macro - traits use async fn directly
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
        // Convert RPC options to internal options if necessary, or pass through
        // Here we assume internal API options match or we ignore for now,
        // but `upsert` in DB takes `Option<UpsertOptions>`?
        // Let's check DB signature. The DB.upsert takes `Option<UpsertOptions>`.
        // We need to make sure the types align or convert.
        // `protocol` module UpsertOptions struct is defined there.
        // `spatio-core` has its own UpsertOptions? Let's check.
        // Assuming we need to convert or if they are compatible.
        // For now, let's map the fields manually to be safe if types differ, or use serde.

        let db_opts = opts.map(|o| spatio::config::SetOptions {
            ttl: Some(o.ttl),
            ..Default::default()
        });

        self.db
            .upsert(&namespace, &id, point, metadata, db_opts)
            .map_err(|e| e.to_string())
    }

    async fn get(
        self,
        _: context::Context,
        namespace: String,
        id: String,
    ) -> Result<Option<CurrentLocation>, String> {
        match self.db.get(&namespace, &id) {
            Ok(Some(loc)) => Ok(Some(CurrentLocation {
                object_id: loc.object_id,
                position: loc.position,
                metadata: serde_json::to_vec(&loc.metadata).unwrap_or_default(),
            })),
            Ok(None) => Ok(None),
            Err(e) => Err(e.to_string()),
        }
    }

    async fn delete(
        self,
        _: context::Context,
        namespace: String,
        id: String,
    ) -> Result<(), String> {
        self.db.delete(&namespace, &id).map_err(|e| e.to_string())
    }

    async fn query_radius(
        self,
        _: context::Context,
        namespace: String,
        center: Point3d,
        radius: f64,
        limit: usize,
    ) -> Result<Vec<(CurrentLocation, f64)>, String> {
        self.db
            .query_radius(&namespace, &center, radius, limit)
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
    }

    async fn knn(
        self,
        _: context::Context,
        namespace: String,
        center: Point3d,
        k: usize,
    ) -> Result<Vec<(CurrentLocation, f64)>, String> {
        self.db
            .knn(&namespace, &center, k)
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
    }

    async fn stats(self, _: context::Context) -> Stats {
        let s = self.db.stats();
        Stats {
            object_count: s.hot_state_objects,
            memory_usage_bytes: s.memory_usage_bytes,
        }
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
        self.db
            .query_bbox(&namespace, min_x, min_y, max_x, max_y, limit)
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
        self.db
            .query_within_cylinder(&namespace, center, min_z, max_z, radius, limit)
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
        let start = start_time
            .map(|t| std::time::UNIX_EPOCH + std::time::Duration::from_secs_f64(t))
            .unwrap_or(std::time::UNIX_EPOCH);
        let end = end_time
            .map(|t| std::time::UNIX_EPOCH + std::time::Duration::from_secs_f64(t))
            .unwrap_or_else(std::time::SystemTime::now);

        self.db
            .query_trajectory(&namespace, &id, start, end, limit)
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
    }

    async fn insert_trajectory(
        self,
        _: context::Context,
        namespace: String,
        id: String,
        trajectory: Vec<(f64, Point3d, serde_json::Value)>,
    ) -> Result<(), String> {
        let updates: Vec<spatio::config::TemporalPoint> = trajectory
            .into_iter()
            .map(|(ts, p, _meta)| {
                // Note: Current DB insert_trajectory uses TemporalPoint (2D) and drops Z/metadata
                let timestamp = std::time::UNIX_EPOCH + std::time::Duration::from_secs_f64(ts);
                spatio::config::TemporalPoint::new(*p.point_2d(), timestamp)
            })
            .collect();

        self.db
            .insert_trajectory(&namespace, &id, &updates)
            .map_err(|e| e.to_string())
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
        self.db
            .query_within_bbox_3d(&namespace, min_x, min_y, min_z, max_x, max_y, max_z, limit)
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
    }

    async fn query_near(
        self,
        _: context::Context,
        namespace: String,
        id: String,
        radius: f64,
        limit: usize,
    ) -> Result<Vec<(CurrentLocation, f64)>, String> {
        self.db
            .query_near(&namespace, &id, radius, limit)
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
    }

    async fn contains(
        self,
        _: context::Context,
        namespace: String,
        polygon: Polygon,
        limit: usize,
    ) -> Result<Vec<CurrentLocation>, String> {
        self.db
            .query_polygon(&namespace, &polygon, limit)
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
    }

    async fn distance(
        self,
        _: context::Context,
        namespace: String,
        id1: String,
        id2: String,
        metric: Option<DistanceMetric>,
    ) -> Result<Option<f64>, String> {
        self.db
            .distance_between(&namespace, &id1, &id2, metric.unwrap_or_default())
            .map_err(|e| e.to_string())
    }

    async fn distance_to(
        self,
        _: context::Context,
        namespace: String,
        id: String,
        point: Point,
        metric: Option<DistanceMetric>,
    ) -> Result<Option<f64>, String> {
        self.db
            .distance_to(&namespace, &id, &point, metric.unwrap_or_default())
            .map_err(|e| e.to_string())
    }

    async fn convex_hull(
        self,
        _: context::Context,
        namespace: String,
    ) -> Result<Option<Polygon>, String> {
        self.db.convex_hull(&namespace).map_err(|e| e.to_string())
    }

    async fn bounding_box(
        self,
        _: context::Context,
        namespace: String,
    ) -> Result<Option<spatio_types::bbox::BoundingBox2D>, String> {
        self.db
            .bounding_box(&namespace)
            .map(|opt| opt.map(spatio_types::bbox::BoundingBox2D::from_rect))
            .map_err(|e| e.to_string())
    }
}
