use spatio::Spatio;
use spatio_types::point::Point3d;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, oneshot};

/// Convert a client-supplied `f64` seconds-since-epoch into a [`SystemTime`].
///
/// Returns an error instead of panicking on non-finite, negative, or
/// out-of-range values — `Duration::from_secs_f64` panics on those, and the
/// input here comes straight off the wire.
pub(crate) fn system_time_from_secs(secs: f64) -> Result<SystemTime, String> {
    // try_from_secs_f64 rejects negative, NaN, infinite and overflowing values
    // (Duration::from_secs_f64 would panic on all of those).
    let dur =
        Duration::try_from_secs_f64(secs).map_err(|e| format!("invalid timestamp {secs}: {e}"))?;
    UNIX_EPOCH
        .checked_add(dur)
        .ok_or_else(|| format!("timestamp out of range: {secs}"))
}

/// Acknowledgement channel a write operation uses to report its result.
type Ack = oneshot::Sender<Result<(), String>>;

/// Write operation to be executed by the background writer thread.
///
/// Each variant carries an [`Ack`] so the handler can await the *actual* write
/// result rather than reporting success the moment the op is enqueued.
#[derive(Debug)]
pub enum WriteOp {
    Upsert {
        namespace: String,
        id: String,
        point: Point3d,
        metadata: serde_json::Value,
        ack: Ack,
    },
    Delete {
        namespace: String,
        id: String,
        ack: Ack,
    },
    InsertTrajectory {
        namespace: String,
        id: String,
        trajectory: Vec<(f64, Point3d, serde_json::Value)>,
        ack: Ack,
    },
}

/// Spawn the dedicated writer thread.
///
/// Returns the sender used by the handler and the thread's [`JoinHandle`] so
/// the caller can wait for buffered writes to drain on shutdown.
pub fn spawn_background_writer(
    db: Arc<Spatio>,
    buffer_size: usize,
) -> (mpsc::Sender<WriteOp>, std::thread::JoinHandle<()>) {
    let (tx, mut rx) = mpsc::channel(buffer_size);

    // A dedicated OS thread keeps the blocking DB writes off the tokio runtime.
    let handle = std::thread::spawn(move || {
        while let Some(op) = rx.blocking_recv() {
            match op {
                WriteOp::Upsert {
                    namespace,
                    id,
                    point,
                    metadata,
                    ack,
                } => {
                    let result = db
                        .upsert(&namespace, &id, point, metadata, None)
                        .map_err(|e| e.to_string());
                    let _ = ack.send(result);
                }
                WriteOp::Delete { namespace, id, ack } => {
                    let result = db.delete(&namespace, &id).map_err(|e| e.to_string());
                    let _ = ack.send(result);
                }
                WriteOp::InsertTrajectory {
                    namespace,
                    id,
                    trajectory,
                    ack,
                } => {
                    let result = build_trajectory(trajectory).and_then(|updates| {
                        db.insert_trajectory(&namespace, &id, &updates)
                            .map_err(|e| e.to_string())
                    });
                    let _ = ack.send(result);
                }
            }
        }
        tracing::info!("Background writer shutting down");
    });

    (tx, handle)
}

fn build_trajectory(
    trajectory: Vec<(f64, Point3d, serde_json::Value)>,
) -> Result<Vec<spatio::config::TemporalPoint>, String> {
    trajectory
        .into_iter()
        .map(|(ts, p, _meta)| {
            let timestamp = system_time_from_secs(ts)?;
            Ok(spatio::config::TemporalPoint::new(*p.point_2d(), timestamp))
        })
        .collect()
}
