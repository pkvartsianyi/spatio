use crate::rpc::{Command, ResponsePayload, ResponseStatus};
use spatio::Spatio;
use std::sync::Arc;

pub struct Handler {
    db: Arc<Spatio>,
}

impl Handler {
    pub fn new(db: Arc<Spatio>) -> Self {
        Self { db }
    }

    pub async fn handle(&self, cmd: Command) -> (ResponseStatus, ResponsePayload) {
        let db = self.db.clone();
        let result = tokio::task::spawn_blocking(move || match cmd {
            Command::Upsert {
                namespace,
                id,
                point,
                metadata,
                opts,
            } => {
                let metadata_json =
                    serde_json::from_slice(&metadata).unwrap_or(serde_json::Value::Null);
                match db.upsert(&namespace, &id, point, metadata_json, opts) {
                    Ok(_) => (ResponseStatus::Ok, ResponsePayload::Ok),
                    Err(e) => (ResponseStatus::Error, ResponsePayload::Error(e.to_string())),
                }
            }
            Command::Get { namespace, id } => match db.get(&namespace, &id) {
                Ok(Some(loc)) => (
                    ResponseStatus::Ok,
                    ResponsePayload::Object {
                        id: loc.object_id,
                        point: loc.position,
                        metadata: serde_json::to_vec(&loc.metadata).unwrap_or_default(),
                    },
                ),
                Ok(None) => (
                    ResponseStatus::Error,
                    ResponsePayload::Error("Not found".into()),
                ),
                Err(e) => (ResponseStatus::Error, ResponsePayload::Error(e.to_string())),
            },
            Command::QueryRadius {
                namespace,
                center,
                radius,
                limit,
            } => match db.query_radius(&namespace, &center, radius, limit) {
                Ok(results) => {
                    let formatted = results
                        .into_iter()
                        .map(|(loc, dist)| {
                            (
                                loc.object_id,
                                loc.position,
                                serde_json::to_vec(&loc.metadata).unwrap_or_default(),
                                dist,
                            )
                        })
                        .collect();
                    (ResponseStatus::Ok, ResponsePayload::Objects(formatted))
                }
                Err(e) => (ResponseStatus::Error, ResponsePayload::Error(e.to_string())),
            },
            Command::Knn {
                namespace,
                center,
                k,
            } => match db.knn(&namespace, &center, k) {
                Ok(results) => {
                    let formatted = results
                        .into_iter()
                        .map(|(loc, dist)| {
                            (
                                loc.object_id,
                                loc.position,
                                serde_json::to_vec(&loc.metadata).unwrap_or_default(),
                                dist,
                            )
                        })
                        .collect();
                    (ResponseStatus::Ok, ResponsePayload::Objects(formatted))
                }
                Err(e) => (ResponseStatus::Error, ResponsePayload::Error(e.to_string())),
            },
            Command::Stats => {
                let stats = db.stats();
                (ResponseStatus::Ok, ResponsePayload::Stats(stats))
            }
            Command::Close => match db.close() {
                Ok(_) => (ResponseStatus::Ok, ResponsePayload::Ok),
                Err(e) => (ResponseStatus::Error, ResponsePayload::Error(e.to_string())),
            },
            Command::Delete { namespace, id } => match db.delete(&namespace, &id) {
                Ok(_) => (ResponseStatus::Ok, ResponsePayload::Ok),
                Err(e) => (ResponseStatus::Error, ResponsePayload::Error(e.to_string())),
            },
            Command::QueryBbox {
                namespace,
                min_x,
                min_y,
                max_x,
                max_y,
                limit,
            } => match db.query_bbox(&namespace, min_x, min_y, max_x, max_y, limit) {
                Ok(results) => {
                    let formatted = results
                        .into_iter()
                        .map(|loc| {
                            (
                                loc.object_id,
                                loc.position,
                                serde_json::to_vec(&loc.metadata).unwrap_or_default(),
                            )
                        })
                        .collect();
                    (ResponseStatus::Ok, ResponsePayload::ObjectList(formatted))
                }
                Err(e) => (ResponseStatus::Error, ResponsePayload::Error(e.to_string())),
            },
            Command::QueryCylinder {
                namespace,
                center_x,
                center_y,
                min_z,
                max_z,
                radius,
                limit,
            } => {
                let center = spatio_types::geo::Point::new(center_x, center_y);
                match db.query_within_cylinder(&namespace, center, min_z, max_z, radius, limit) {
                    Ok(results) => {
                        let formatted = results
                            .into_iter()
                            .map(|(loc, dist)| {
                                (
                                    loc.object_id,
                                    loc.position,
                                    serde_json::to_vec(&loc.metadata).unwrap_or_default(),
                                    dist,
                                )
                            })
                            .collect();
                        (ResponseStatus::Ok, ResponsePayload::Objects(formatted))
                    }
                    Err(e) => (ResponseStatus::Error, ResponsePayload::Error(e.to_string())),
                }
            }
            Command::QueryTrajectory {
                namespace,
                id,
                start_time,
                end_time,
                limit,
            } => match db.query_trajectory(&namespace, &id, start_time, end_time, limit) {
                Ok(updates) => {
                    let mut formatted = Vec::with_capacity(updates.len());
                    let mut error = None;
                    for upd in updates {
                        match serde_json::to_vec(&upd.metadata) {
                            Ok(metadata_bytes) => {
                                formatted.push(crate::rpc::LocationUpdate {
                                    timestamp: upd.timestamp,
                                    position: upd.position,
                                    metadata: metadata_bytes,
                                });
                            }
                            Err(e) => {
                                error = Some(e);
                                break;
                            }
                        }
                    }
                    if let Some(e) = error {
                        (
                            ResponseStatus::Error,
                            ResponsePayload::Error(format!(
                                "Failed to serialize trajectory metadata: {}",
                                e
                            )),
                        )
                    } else {
                        (ResponseStatus::Ok, ResponsePayload::Trajectory(formatted))
                    }
                }
                Err(e) => (ResponseStatus::Error, ResponsePayload::Error(e.to_string())),
            },
            Command::InsertTrajectory {
                namespace,
                id,
                trajectory,
            } => match db.insert_trajectory(&namespace, &id, &trajectory) {
                Ok(_) => (ResponseStatus::Ok, ResponsePayload::Ok),
                Err(e) => (ResponseStatus::Error, ResponsePayload::Error(e.to_string())),
            },
            Command::QueryBbox3d {
                namespace,
                min_x,
                min_y,
                min_z,
                max_x,
                max_y,
                max_z,
                limit,
            } => match db
                .query_within_bbox_3d(&namespace, min_x, min_y, min_z, max_x, max_y, max_z, limit)
            {
                Ok(results) => {
                    let formatted = results
                        .into_iter()
                        .map(|loc| {
                            (
                                loc.object_id,
                                loc.position,
                                serde_json::to_vec(&loc.metadata).unwrap_or_default(),
                            )
                        })
                        .collect();
                    (ResponseStatus::Ok, ResponsePayload::ObjectList(formatted))
                }
                Err(e) => (ResponseStatus::Error, ResponsePayload::Error(e.to_string())),
            },
            Command::QueryNear {
                namespace,
                id,
                radius,
                limit,
            } => match db.query_near(&namespace, &id, radius, limit) {
                Ok(results) => {
                    let formatted = results
                        .into_iter()
                        .map(|(loc, dist)| {
                            (
                                loc.object_id,
                                loc.position,
                                serde_json::to_vec(&loc.metadata).unwrap_or_default(),
                                dist,
                            )
                        })
                        .collect();
                    (ResponseStatus::Ok, ResponsePayload::Objects(formatted))
                }
                Err(e) => (ResponseStatus::Error, ResponsePayload::Error(e.to_string())),
            },
        })
        .await;

        match result {
            Ok(response) => response,
            Err(e) => (
                ResponseStatus::Error,
                ResponsePayload::Error(format!("Task failed: {}", e)),
            ),
        }
    }
}
