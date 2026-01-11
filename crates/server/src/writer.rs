use spatio::Spatio;
use spatio_types::point::Point3d;
use std::sync::Arc;
use tokio::sync::mpsc;

/// Write operation to be buffered and executed by background worker
#[derive(Debug)]
pub enum WriteOp {
    Upsert {
        namespace: String,
        id: String,
        point: Point3d,
        metadata: serde_json::Value,
    },
    Delete {
        namespace: String,
        id: String,
    },
    InsertTrajectory {
        namespace: String,
        id: String,
        trajectory: Vec<(f64, Point3d, serde_json::Value)>,
    },
}

/// Returns the sender channel to be used by the handler
pub fn spawn_background_writer(db: Arc<Spatio>, buffer_size: usize) -> mpsc::Sender<WriteOp> {
    let (tx, mut rx) = mpsc::channel(buffer_size);

    // Spawn a dedicated thread for writing to ensure we don't block tokio runtime
    std::thread::spawn(move || {
        while let Some(op) = rx.blocking_recv() {
            match op {
                WriteOp::Upsert {
                    namespace,
                    id,
                    point,
                    metadata,
                } => {
                    if let Err(e) = db.upsert(&namespace, &id, point, metadata, None) {
                        tracing::error!("Background write failed (upsert): {}", e);
                    }
                }
                WriteOp::Delete { namespace, id } => {
                    if let Err(e) = db.delete(&namespace, &id) {
                        tracing::error!("Background write failed (delete): {}", e);
                    }
                }
                WriteOp::InsertTrajectory {
                    namespace,
                    id,
                    trajectory,
                } => {
                    let updates: Vec<spatio::config::TemporalPoint> = trajectory
                        .into_iter()
                        .map(|(ts, p, _meta)| {
                            let timestamp =
                                std::time::UNIX_EPOCH + std::time::Duration::from_secs_f64(ts);
                            spatio::config::TemporalPoint::new(*p.point_2d(), timestamp)
                        })
                        .collect();

                    if let Err(e) = db.insert_trajectory(&namespace, &id, &updates) {
                        tracing::error!("Background write failed (insert_trajectory): {}", e);
                    }
                }
            }
        }
        tracing::info!("Background writer shutting down");
    });

    tx
}
