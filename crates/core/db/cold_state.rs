//! Cold state: historical trajectories of tracked objects
//!
//! This module manages the historical data of moving objects, optimized for
//! append-only writes and time-range queries. It uses a persistent log for
//! durability and a memory buffer for recent history access.

use bytes::Bytes;
use dashmap::DashMap;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use spatio_types::point::Point3d;
use std::collections::VecDeque;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::error::{Result, SpatioError};

/// Single location update in history
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocationUpdate {
    pub timestamp: SystemTime,
    pub position: Point3d,
    pub metadata: Bytes,
}

/// Cold state: historical trajectories
pub struct ColdState {
    /// Append-only log file
    trajectory_log: Mutex<TrajectoryLog>,

    /// Recent history buffer for fast access
    /// Maps "namespace::object_id" -> recent updates
    recent_buffer: DashMap<String, VecDeque<LocationUpdate>>,

    /// Buffer size per object (e.g., last 100 updates)
    buffer_capacity: usize,
}

impl ColdState {
    /// Create a new cold state
    pub fn new(log_path: &Path, buffer_capacity: usize) -> Result<Self> {
        // Ensure directory exists
        if let Some(parent) = log_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        Ok(Self {
            trajectory_log: Mutex::new(TrajectoryLog::open(log_path)?),
            recent_buffer: DashMap::new(),
            buffer_capacity,
        })
    }

    /// Create a composite key from namespace and object ID
    #[inline]
    fn make_key(namespace: &str, object_id: &str) -> String {
        format!("{}::{}", namespace, object_id)
    }

    /// Append location update to persistent log + buffer
    pub fn append_update(
        &self,
        namespace: &str,
        object_id: &str,
        position: Point3d,
        metadata: Bytes,
        timestamp: SystemTime,
    ) -> Result<()> {
        let update = LocationUpdate {
            timestamp,
            position,
            metadata: metadata.clone(),
        };

        // 1. Write to persistent log (serialized via Mutex)
        {
            let mut log = self.trajectory_log.lock();
            log.append(namespace, object_id, &update)?;
        }

        // 2. Add to recent buffer (concurrent via DashMap)
        let full_key = Self::make_key(namespace, object_id);
        let mut buffer = self.recent_buffer.entry(full_key).or_default();

        buffer.push_back(update);

        // Keep only last N updates
        while buffer.len() > self.buffer_capacity {
            buffer.pop_front();
        }

        Ok(())
    }

    /// Query trajectory history
    pub fn query_trajectory(
        &self,
        namespace: &str,
        object_id: &str,
        start_time: SystemTime,
        end_time: SystemTime,
        limit: usize,
    ) -> Result<Vec<LocationUpdate>> {
        let full_key = Self::make_key(namespace, object_id);

        // Try buffer first (fast path)
        if let Some(buffer) = self.recent_buffer.get(&full_key) {
            let filtered: Vec<_> = buffer
                .iter()
                .filter(|u| u.timestamp >= start_time && u.timestamp <= end_time)
                .rev() // Newest first
                .take(limit)
                .cloned()
                .collect();

            // If we found enough data in buffer covering the requested range end, return it
            // Note: This is a simplification. Ideally we'd check if the buffer covers the *entire* requested range.
            // For now, if we have data in buffer, we return it.
            if !filtered.is_empty() {
                return Ok(filtered);
            }
        }

        // Fallback to disk (slow path)
        // TODO: Implement efficient file scanning/indexing
        // For now, we just return empty if not in buffer, as full file scan is expensive
        // and we haven't implemented the index yet.
        Ok(Vec::new())
    }
}

/// Trajectory log format management
struct TrajectoryLog {
    writer: BufWriter<File>,
}

impl TrajectoryLog {
    fn open(path: &Path) -> Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;

        Ok(Self {
            writer: BufWriter::new(file),
        })
    }

    fn append(&mut self, namespace: &str, object_id: &str, update: &LocationUpdate) -> Result<()> {
        // Simple text format for now:
        // timestamp_micros|namespace|object_id|lat|lon|alt|metadata_len|base64_metadata
        let micros = update
            .timestamp
            .duration_since(UNIX_EPOCH)
            .map_err(|_| SpatioError::InvalidTimestamp)?
            .as_micros();

        // We use a simple pipe-separated format
        // Note: This assumes namespace and object_id don't contain pipes.
        // In a real impl, we'd escape or use binary format.
        writeln!(
            self.writer,
            "{}|{}|{}|{:.6}|{:.6}|{:.6}|{}|{}",
            micros,
            namespace,
            object_id,
            update.position.y(), // lat
            update.position.x(), // lon
            update.position.z(), // alt
            update.metadata.len(),
            base64_encode(&update.metadata),
        )?;

        // Flush periodically or on every write?
        // For durability, we flush on every write (slower but safer)
        // In production, we might want to buffer more.
        self.writer.flush()?;

        Ok(())
    }
}

// Simple base64 encoder to avoid another dependency if possible,
// but better to use the crate if we added it.
// Since we didn't add base64 crate yet, let's use a placeholder or hex.
// Hex is standard in Rust std lib (via fmt).
fn base64_encode(data: &[u8]) -> String {
    // Using hex encoding for simplicity without extra deps
    data.iter().map(|b| format!("{:02x}", b)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tempfile::tempdir;

    #[test]
    fn test_append_and_query_buffer() {
        let dir = tempdir().unwrap();
        let log_path = dir.path().join("traj.log");
        let cold = ColdState::new(&log_path, 10).unwrap();

        let pos1 = Point3d::new(-74.0, 40.7, 0.0);
        let pos2 = Point3d::new(-74.1, 40.8, 0.0);

        let t1 = UNIX_EPOCH + Duration::from_secs(1000);
        let t2 = UNIX_EPOCH + Duration::from_secs(2000);

        cold.append_update("vehicles", "truck_001", pos1, Bytes::from("m1"), t1)
            .unwrap();
        cold.append_update("vehicles", "truck_001", pos2, Bytes::from("m2"), t2)
            .unwrap();

        let history = cold
            .query_trajectory(
                "vehicles",
                "truck_001",
                t1,
                t2 + Duration::from_secs(1),
                100,
            )
            .unwrap();

        assert_eq!(history.len(), 2);
        assert_eq!(history[0].position.x(), -74.1); // Newest first
        assert_eq!(history[1].position.x(), -74.0);
    }

    #[test]
    fn test_buffer_capacity() {
        let dir = tempdir().unwrap();
        let log_path = dir.path().join("traj.log");
        let cold = ColdState::new(&log_path, 2).unwrap(); // Capacity 2

        let pos = Point3d::new(0.0, 0.0, 0.0);

        for i in 0..5 {
            let t = UNIX_EPOCH + Duration::from_secs(i);
            cold.append_update("v", "o", pos.clone(), Bytes::from(vec![i as u8]), t)
                .unwrap();
        }

        let history = cold
            .query_trajectory(
                "v",
                "o",
                UNIX_EPOCH,
                UNIX_EPOCH + Duration::from_secs(10),
                100,
            )
            .unwrap();

        assert_eq!(history.len(), 2); // Should only have last 2
        assert_eq!(history[0].timestamp, UNIX_EPOCH + Duration::from_secs(4));
        assert_eq!(history[1].timestamp, UNIX_EPOCH + Duration::from_secs(3));
    }
}
