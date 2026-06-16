use crate::protocol::{CurrentLocation, LocationUpdate, Stats};
use crate::writer::system_time_from_secs;
use spatio::Spatio;
use spatio_types::geo::{DistanceMetric, Point, Polygon};
use spatio_types::point::Point3d;
use std::sync::Arc;

#[derive(Clone)]
pub struct Reader {
    db: Arc<Spatio>,
}

/// Serialize object metadata for the wire, surfacing serialization failures
/// instead of silently substituting an empty (invalid-JSON) byte vector.
fn encode_metadata(metadata: &serde_json::Value) -> Result<Vec<u8>, String> {
    serde_json::to_vec(metadata).map_err(|e| format!("Failed to serialize metadata: {e}"))
}

impl Reader {
    pub fn new(db: Arc<Spatio>) -> Self {
        Self { db }
    }

    pub fn get(&self, namespace: &str, id: &str) -> Result<Option<CurrentLocation>, String> {
        match self.db.get(namespace, id).map_err(|e| e.to_string())? {
            Some(loc) => Ok(Some(CurrentLocation {
                object_id: loc.object_id.clone(),
                position: loc.position.clone(),
                metadata: encode_metadata(&loc.metadata)?,
            })),
            None => Ok(None),
        }
    }

    pub fn query_radius(
        &self,
        namespace: &str,
        center: &Point3d,
        radius: f64,
        limit: usize,
    ) -> Result<Vec<(CurrentLocation, f64)>, String> {
        let results = self
            .db
            .query_radius(namespace, center, radius, limit)
            .map_err(|e| format!("Internal error: {e}"))?;
        results
            .into_iter()
            .map(|(loc, dist)| {
                Ok((
                    CurrentLocation {
                        object_id: loc.object_id.clone(),
                        position: loc.position.clone(),
                        metadata: encode_metadata(&loc.metadata)?,
                    },
                    dist,
                ))
            })
            .collect()
    }

    pub fn knn(
        &self,
        namespace: &str,
        center: &Point3d,
        k: usize,
    ) -> Result<Vec<(CurrentLocation, f64)>, String> {
        let results = self
            .db
            .knn(namespace, center, k)
            .map_err(|e| format!("Internal error: {e}"))?;
        results
            .into_iter()
            .map(|(loc, dist)| {
                Ok((
                    CurrentLocation {
                        object_id: loc.object_id.clone(),
                        position: loc.position.clone(),
                        metadata: encode_metadata(&loc.metadata)?,
                    },
                    dist,
                ))
            })
            .collect()
    }

    pub fn stats(&self) -> Stats {
        let s = self.db.stats();
        Stats {
            object_count: s.hot_state_objects,
            memory_usage_bytes: s.memory_usage_bytes,
        }
    }

    pub fn query_bbox(
        &self,
        namespace: &str,
        min_x: f64,
        min_y: f64,
        max_x: f64,
        max_y: f64,
        limit: usize,
    ) -> Result<Vec<CurrentLocation>, String> {
        let results = self
            .db
            .query_bbox(namespace, min_x, min_y, max_x, max_y, limit)
            .map_err(|e| format!("Internal error: {e}"))?;
        results
            .into_iter()
            .map(|loc| {
                Ok(CurrentLocation {
                    object_id: loc.object_id.clone(),
                    position: loc.position.clone(),
                    metadata: encode_metadata(&loc.metadata)?,
                })
            })
            .collect()
    }

    pub fn query_cylinder(
        &self,
        namespace: &str,
        center: Point,
        min_z: f64,
        max_z: f64,
        radius: f64,
        limit: usize,
    ) -> Result<Vec<(CurrentLocation, f64)>, String> {
        let results = self
            .db
            .query_within_cylinder(namespace, center, min_z, max_z, radius, limit)
            .map_err(|e| format!("Internal error: {e}"))?;
        results
            .into_iter()
            .map(|(loc, dist)| {
                Ok((
                    CurrentLocation {
                        object_id: loc.object_id.clone(),
                        position: loc.position.clone(),
                        metadata: encode_metadata(&loc.metadata)?,
                    },
                    dist,
                ))
            })
            .collect()
    }

    pub fn query_trajectory(
        &self,
        namespace: &str,
        id: &str,
        start_time: Option<f64>,
        end_time: Option<f64>,
        limit: usize,
    ) -> Result<Vec<LocationUpdate>, String> {
        let start = match start_time {
            Some(t) => system_time_from_secs(t)?,
            None => std::time::UNIX_EPOCH,
        };
        let end = match end_time {
            Some(t) => system_time_from_secs(t)?,
            None => std::time::SystemTime::now(),
        };

        let results = self
            .db
            .query_trajectory(namespace, id, start, end, limit)
            .map_err(|e| e.to_string())?;
        results
            .into_iter()
            .map(|upd| {
                let timestamp = upd
                    .timestamp
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs_f64();
                Ok(LocationUpdate {
                    timestamp,
                    position: upd.position,
                    metadata: encode_metadata(&upd.metadata)?,
                })
            })
            .collect()
    }

    #[allow(clippy::too_many_arguments)]
    pub fn query_bbox_3d(
        &self,
        namespace: &str,
        min_x: f64,
        min_y: f64,
        min_z: f64,
        max_x: f64,
        max_y: f64,
        max_z: f64,
        limit: usize,
    ) -> Result<Vec<CurrentLocation>, String> {
        let results = self
            .db
            .query_within_bbox_3d(namespace, min_x, min_y, min_z, max_x, max_y, max_z, limit)
            .map_err(|e| format!("Internal error: {e}"))?;
        results
            .into_iter()
            .map(|loc| {
                Ok(CurrentLocation {
                    object_id: loc.object_id.clone(),
                    position: loc.position.clone(),
                    metadata: encode_metadata(&loc.metadata)?,
                })
            })
            .collect()
    }

    pub fn query_near(
        &self,
        namespace: &str,
        id: &str,
        radius: f64,
        limit: usize,
    ) -> Result<Vec<(CurrentLocation, f64)>, String> {
        let results = self
            .db
            .query_near(namespace, id, radius, limit)
            .map_err(|e| format!("Internal error: {e}"))?;
        results
            .into_iter()
            .map(|(loc, dist)| {
                Ok((
                    CurrentLocation {
                        object_id: loc.object_id.clone(),
                        position: loc.position.clone(),
                        metadata: encode_metadata(&loc.metadata)?,
                    },
                    dist,
                ))
            })
            .collect()
    }

    pub fn contains(
        &self,
        namespace: &str,
        polygon: &Polygon,
        limit: usize,
    ) -> Result<Vec<CurrentLocation>, String> {
        let results = self
            .db
            .query_polygon(namespace, polygon, limit)
            .map_err(|e| format!("Internal error: {e}"))?;
        results
            .into_iter()
            .map(|loc| {
                Ok(CurrentLocation {
                    object_id: loc.object_id.clone(),
                    position: loc.position.clone(),
                    metadata: encode_metadata(&loc.metadata)?,
                })
            })
            .collect()
    }

    pub fn distance(
        &self,
        namespace: &str,
        id1: &str,
        id2: &str,
        metric: Option<DistanceMetric>,
    ) -> Result<Option<f64>, String> {
        self.db
            .distance_between(namespace, id1, id2, metric.unwrap_or_default())
            .map_err(|e| format!("Internal error: {e}"))
    }

    pub fn distance_to(
        &self,
        namespace: &str,
        id: &str,
        point: &Point,
        metric: Option<DistanceMetric>,
    ) -> Result<Option<f64>, String> {
        self.db
            .distance_to(namespace, id, point, metric.unwrap_or_default())
            .map_err(|e| format!("Internal error: {e}"))
    }

    pub fn convex_hull(&self, namespace: &str) -> Result<Option<Polygon>, String> {
        self.db
            .convex_hull(namespace)
            .map_err(|e| format!("Internal error: {e}"))
    }

    pub fn bounding_box(
        &self,
        namespace: &str,
    ) -> Result<Option<spatio_types::bbox::BoundingBox2D>, String> {
        self.db
            .bounding_box(namespace)
            .map(|opt| opt.map(spatio_types::bbox::BoundingBox2D::from_rect))
            .map_err(|e| format!("Internal error: {e}"))
    }
}
