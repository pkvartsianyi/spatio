//! Cold state: historical trajectories of tracked objects
//!
//! This module manages the historical data of moving objects, optimized for
//! append-only writes and time-range queries. It uses a persistent log for
//! durability and a memory buffer for recent history access.

use dashmap::DashMap;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use spatio_types::point::Point3d;
use std::collections::VecDeque;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::config::PersistenceConfig;
use crate::error::Result;

/// Single location update in history
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocationUpdate {
    pub timestamp: SystemTime,
    pub position: Point3d,
    pub metadata: serde_json::Value,
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

    #[allow(dead_code)] // Used for debug/inspection
    config: PersistenceConfig,
}

impl ColdState {
    /// Create a new cold state
    pub fn new(log_path: &Path, buffer_capacity: usize, config: PersistenceConfig) -> Result<Self> {
        // Ensure directory exists
        if let Some(parent) = log_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        Ok(Self {
            trajectory_log: Mutex::new(TrajectoryLog::open(log_path, config.buffer_size)?),
            recent_buffer: DashMap::new(),
            buffer_capacity,
            config,
        })
    }

    /// Create a composite key from namespace and object ID
    #[inline]
    fn make_key(namespace: &str, object_id: &str) -> String {
        format!("{}::{}", namespace, object_id)
    }

    /// Get detailed statistics about cold state
    pub fn stats(&self) -> (usize, usize) {
        let trajectory_count = self.recent_buffer.len();

        // Estimate buffer size: sum of all trajectory lengths * ~100 bytes per point
        let buffer_bytes = self
            .recent_buffer
            .iter()
            .map(|entry| entry.value().len() * 100)
            .sum();

        (trajectory_count, buffer_bytes)
    }

    /// Append location update to persistent log + buffer
    pub fn append_update(
        &self,
        namespace: &str,
        object_id: &str,
        position: Point3d,
        metadata: serde_json::Value,
        timestamp: SystemTime,
    ) -> Result<()> {
        // Truncate timestamp to microseconds to match disk storage precision
        // This prevents duplicates when merging buffer and disk results
        let micros = timestamp
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros();
        let timestamp_truncated = UNIX_EPOCH + std::time::Duration::from_micros(micros as u64);

        let update = LocationUpdate {
            timestamp: timestamp_truncated,
            position,
            metadata,
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

    /// Force flush of the trajectory log to disk
    pub fn flush(&self) -> Result<()> {
        let mut log = self.trajectory_log.lock();
        log.flush()
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
        let mut from_buffer = Vec::new();
        if let Some(buffer) = self.recent_buffer.get(&full_key) {
            from_buffer = buffer
                .iter()
                .filter(|u| u.timestamp >= start_time && u.timestamp <= end_time)
                .rev() // Newest first
                .cloned()
                .collect();

            // If buffer has enough results, return immediately
            if from_buffer.len() >= limit {
                from_buffer.truncate(limit);
                return Ok(from_buffer);
            }
        }

        // Fallback to disk (slow path) - scan entire log file
        let log = self.trajectory_log.lock();
        let path = log.path();

        if !path.exists() {
            return Ok(from_buffer);
        }

        let file = std::fs::File::open(path)?;
        let reader = std::io::BufReader::new(file);

        // Use a set to track timestamps already retrieved from buffer
        let buffer_timestamps: std::collections::HashSet<SystemTime> =
            from_buffer.iter().map(|u| u.timestamp).collect();

        let mut from_disk: Vec<LocationUpdate> = Vec::new();

        for line_result in std::io::BufRead::lines(reader) {
            let line = match line_result {
                Ok(l) => l,
                Err(_) => continue,
            };

            // Parse format: timestamp_micros|namespace|object_id|lat|lon|alt|metadata_len|hex_metadata
            let parts: Vec<&str> = line.split('|').collect();
            if parts.len() != 8 {
                continue;
            }

            // Check namespace and object_id match
            if parts[1] != namespace || parts[2] != object_id {
                continue;
            }

            // Parse timestamp
            let timestamp_micros: u128 = match parts[0].parse() {
                Ok(t) => t,
                Err(_) => continue,
            };
            let timestamp = UNIX_EPOCH + std::time::Duration::from_micros(timestamp_micros as u64);

            // Skip if already in buffer
            if buffer_timestamps.contains(&timestamp) {
                continue;
            }

            // Check if in time range
            if timestamp < start_time || timestamp > end_time {
                continue;
            }

            // Parse position
            let lat: f64 = match parts[3].parse() {
                Ok(v) => v,
                Err(_) => continue,
            };
            let lon: f64 = match parts[4].parse() {
                Ok(v) => v,
                Err(_) => continue,
            };
            let alt: f64 = match parts[5].parse() {
                Ok(v) => v,
                Err(_) => continue,
            };

            // Parse JSON metadata
            let metadata: serde_json::Value =
                serde_json::from_str(parts[7]).unwrap_or(serde_json::Value::Null);

            from_disk.push(LocationUpdate {
                timestamp,
                position: Point3d::new(lon, lat, alt),
                metadata,
            });
        }

        // Merge buffer and disk results, sort by timestamp (newest first), limit
        from_buffer.extend(from_disk);
        from_buffer.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));
        from_buffer.truncate(limit);

        Ok(from_buffer)
    }

    /// Recover current locations by scanning the trajectory log
    ///
    /// Returns a map of "namespace::object_id" → LocationUpdate with the latest
    /// timestamp for each object. Used during DB startup to rebuild HotState.
    pub fn recover_current_locations(
        &self,
    ) -> Result<std::collections::HashMap<String, LocationUpdate>> {
        use std::collections::HashMap;
        use std::io::{BufRead, BufReader};

        let log = self.trajectory_log.lock();
        let path = log.path();

        // If file doesn't exist or is empty, return empty map
        if !path.exists() {
            return Ok(HashMap::new());
        }

        let file = std::fs::File::open(path)?;
        let reader = BufReader::new(file);

        let mut latest_positions: HashMap<String, LocationUpdate> = HashMap::new();

        for (line_num, line_result) in reader.lines().enumerate() {
            let line = match line_result {
                Ok(l) => l,
                Err(e) => {
                    log::warn!(
                        "Failed to read line {} in trajectory log: {}",
                        line_num + 1,
                        e
                    );
                    continue;
                }
            };

            // Parse format: timestamp_micros|namespace|object_id|lat|lon|alt|json_len|json_metadata
            let parts: Vec<&str> = line.split('|').collect();
            if parts.len() != 8 {
                log::warn!(
                    "Malformed log line {} (expected 8 fields, got {})",
                    line_num + 1,
                    parts.len()
                );
                continue;
            }

            // Parse timestamp
            let timestamp_micros: u128 = match parts[0].parse() {
                Ok(t) => t,
                Err(e) => {
                    log::warn!("Invalid timestamp on line {}: {}", line_num + 1, e);
                    continue;
                }
            };
            let timestamp = UNIX_EPOCH + std::time::Duration::from_micros(timestamp_micros as u64);

            let namespace = parts[1];
            let object_id = parts[2];

            // Parse position
            let lat: f64 = match parts[3].parse() {
                Ok(v) => v,
                Err(_) => {
                    log::warn!("Invalid latitude on line {}", line_num + 1);
                    continue;
                }
            };
            let lon: f64 = match parts[4].parse() {
                Ok(v) => v,
                Err(_) => {
                    log::warn!("Invalid longitude on line {}", line_num + 1);
                    continue;
                }
            };
            let alt: f64 = match parts[5].parse() {
                Ok(v) => v,
                Err(_) => {
                    log::warn!("Invalid altitude on line {}", line_num + 1);
                    continue;
                }
            };

            // Parse JSON metadata
            let metadata: serde_json::Value = serde_json::from_str(parts[7]).unwrap_or_else(|e| {
                log::warn!("Invalid metadata on line {}: {}", line_num + 1, e);
                serde_json::Value::Null
            });

            let full_key = format!("{}::{}", namespace, object_id);
            let update = LocationUpdate {
                timestamp,
                position: Point3d::new(lon, lat, alt),
                metadata,
            };

            // Keep only the latest update for each object
            latest_positions
                .entry(full_key)
                .and_modify(|existing| {
                    if update.timestamp > existing.timestamp {
                        *existing = update.clone();
                    }
                })
                .or_insert(update);
        }

        Ok(latest_positions)
    }
}

/// Trajectory log format management
struct TrajectoryLog {
    writer: BufWriter<File>,
    path: std::path::PathBuf,
    pending_writes: usize,
    buffer_limit: usize,
}

impl TrajectoryLog {
    fn open(path: &Path, buffer_limit: usize) -> Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;

        Ok(Self {
            writer: BufWriter::new(file),
            path: path.to_path_buf(),
            pending_writes: 0,
            buffer_limit,
        })
    }

    fn path(&self) -> &std::path::Path {
        &self.path
    }

    fn append(&mut self, namespace: &str, object_id: &str, update: &LocationUpdate) -> Result<()> {
        // Simple text format for now:
        // timestamp_micros|namespace|object_id|lat|lon|alt|metadata_len|base64_metadata
        let micros = update
            .timestamp
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros();

        // Serialize metadata to JSON string
        let json_str =
            serde_json::to_string(&update.metadata).unwrap_or_else(|_| "null".to_string());

        // Use a simple pipe-separated format
        // Note: This assumes namespace and object_id don't contain pipes.
        // In a real impl, specialized escaping or binary format should be used.
        writeln!(
            self.writer,
            "{}|{}|{}|{:.6}|{:.6}|{:.6}|{}|{}",
            micros,
            namespace,
            object_id,
            update.position.y(), // lat
            update.position.x(), // lon
            update.position.z(), // alt
            json_str.len(),
            json_str,
        )?;

        self.pending_writes += 1;
        if self.pending_writes >= self.buffer_limit {
            self.writer.flush()?;
            self.pending_writes = 0;
        }

        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        self.writer.flush()?;
        self.pending_writes = 0;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::PersistenceConfig;
    use std::time::Duration;
    use tempfile::tempdir;

    #[test]
    fn test_append_and_query_buffer() {
        let dir = tempdir().unwrap();
        let log_path = dir.path().join("traj.log");
        let cold = ColdState::new(&log_path, 10, PersistenceConfig::default()).unwrap();

        let pos1 = Point3d::new(-74.0, 40.7, 0.0);
        let pos2 = Point3d::new(-74.1, 40.8, 0.0);

        let t1 = UNIX_EPOCH + Duration::from_secs(1000);
        let t2 = UNIX_EPOCH + Duration::from_secs(2000);

        cold.append_update(
            "vehicles",
            "truck_001",
            pos1,
            serde_json::json!({"msg": "m1"}),
            t1,
        )
        .unwrap();
        cold.append_update(
            "vehicles",
            "truck_001",
            pos2,
            serde_json::json!({"msg": "m2"}),
            t2,
        )
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

        let cold = ColdState::new(&log_path, 2, PersistenceConfig { buffer_size: 0 }).unwrap(); // Capacity 2

        let pos = Point3d::new(0.0, 0.0, 0.0);

        for i in 0..5 {
            let t = UNIX_EPOCH + Duration::from_secs(i);
            cold.append_update("v", "o", pos.clone(), serde_json::json!({"id": i}), t)
                .unwrap();
        }

        // Check buffer directly - should only have last 2
        let key = ColdState::make_key("v", "o");
        let buffer = cold.recent_buffer.get(&key).unwrap();
        assert_eq!(buffer.len(), 2);
        assert_eq!(buffer[0].timestamp, UNIX_EPOCH + Duration::from_secs(3));
        assert_eq!(buffer[1].timestamp, UNIX_EPOCH + Duration::from_secs(4));

        // But query_trajectory should return all from disk
        let history = cold
            .query_trajectory(
                "v",
                "o",
                UNIX_EPOCH,
                UNIX_EPOCH + Duration::from_secs(10),
                100,
            )
            .unwrap();

        assert_eq!(history.len(), 5); // All 5 from disk scan
        assert_eq!(history[0].timestamp, UNIX_EPOCH + Duration::from_secs(4));
        assert_eq!(history[1].timestamp, UNIX_EPOCH + Duration::from_secs(3));
    }

    #[test]
    fn test_recover_current_locations() {
        let dir = tempdir().unwrap();
        let log_path = dir.path().join("traj.log");

        let cold = ColdState::new(&log_path, 10, PersistenceConfig { buffer_size: 0 }).unwrap();

        let t1 = UNIX_EPOCH + Duration::from_secs(1000);
        let t2 = UNIX_EPOCH + Duration::from_secs(2000);
        let t3 = UNIX_EPOCH + Duration::from_secs(3000);

        // Add multiple updates for same object (should keep latest)
        cold.append_update(
            "vehicles",
            "truck_001",
            Point3d::new(-74.0, 40.0, 100.0),
            serde_json::json!({"data": "old"}),
            t1,
        )
        .unwrap();

        cold.append_update(
            "vehicles",
            "truck_001",
            Point3d::new(-74.1, 40.1, 200.0),
            serde_json::json!({"data": "new"}),
            t2,
        )
        .unwrap();

        // Add different object
        cold.append_update(
            "aircraft",
            "plane_001",
            Point3d::new(-75.0, 41.0, 5000.0),
            serde_json::json!({"type": "flight"}),
            t3,
        )
        .unwrap();

        // Recover
        let recovered = cold.recover_current_locations().unwrap();

        assert_eq!(recovered.len(), 2);

        // Check truck - should have latest position
        let truck_key = "vehicles::truck_001";
        let truck = recovered.get(truck_key).unwrap();
        assert_eq!(truck.position.x(), -74.1);
        assert_eq!(truck.position.y(), 40.1);
        assert_eq!(truck.timestamp, t2);
        assert_eq!(truck.metadata, serde_json::json!({"data": "new"}));

        // Check plane
        let plane_key = "aircraft::plane_001";
        let plane = recovered.get(plane_key).unwrap();
        assert_eq!(plane.position.x(), -75.0);
        assert_eq!(plane.position.z(), 5000.0);
        assert_eq!(plane.timestamp, t3);
    }

    #[test]
    fn test_disk_based_trajectory_query() {
        let dir = tempdir().unwrap();
        let log_path = dir.path().join("traj.log");

        let cold = ColdState::new(&log_path, 2, PersistenceConfig { buffer_size: 0 }).unwrap(); // Small buffer to force disk scan

        let t1 = UNIX_EPOCH + Duration::from_secs(1000);
        let t2 = UNIX_EPOCH + Duration::from_secs(2000);
        let t3 = UNIX_EPOCH + Duration::from_secs(3000);
        let t4 = UNIX_EPOCH + Duration::from_secs(4000);
        let t5 = UNIX_EPOCH + Duration::from_secs(5000);

        // Add 5 updates - buffer only keeps last 2
        for (i, t) in [t1, t2, t3, t4, t5].iter().enumerate() {
            cold.append_update(
                "vehicles",
                "truck_001",
                Point3d::new(-74.0 + i as f64 * 0.1, 40.0, 0.0),
                serde_json::json!({"data": format!("data_{}", i)}),
                *t,
            )
            .unwrap();
        }

        // Query entire range - should scan disk for older entries
        let trajectory = cold
            .query_trajectory("vehicles", "truck_001", t1, t5, 10)
            .unwrap();

        // Should get all 5 results
        assert_eq!(trajectory.len(), 5);

        // Should be sorted newest first
        assert_eq!(trajectory[0].timestamp, t5);
        assert_eq!(trajectory[1].timestamp, t4);
        assert_eq!(trajectory[2].timestamp, t3);
        assert_eq!(trajectory[3].timestamp, t2);
        assert_eq!(trajectory[4].timestamp, t1);

        // Query with limit
        let limited = cold
            .query_trajectory("vehicles", "truck_001", t1, t5, 3)
            .unwrap();
        assert_eq!(limited.len(), 3);
        assert_eq!(limited[0].timestamp, t5);
    }
}
