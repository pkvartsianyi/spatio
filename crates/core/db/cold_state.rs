//! Cold state: historical trajectories of tracked objects
//!
//! This module manages the historical data of moving objects, optimized for
//! append-only writes and time-range queries. It uses a persistent log for
//! durability and a memory buffer for recent history access.

use dashmap::DashMap;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use spatio_types::config::{SyncMode, SyncPolicy};
use spatio_types::point::Point3d;
use std::collections::VecDeque;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::Path;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::config::PersistenceConfig;
use crate::error::Result;

/// Durability settings governing when buffered writes are flushed to the OS
/// and synced to stable storage.
#[derive(Debug, Clone, Copy)]
pub struct SyncSettings {
    pub policy: SyncPolicy,
    pub mode: SyncMode,
    pub batch_size: usize,
}

impl Default for SyncSettings {
    fn default() -> Self {
        Self {
            policy: SyncPolicy::default(),
            mode: SyncMode::default(),
            batch_size: 1,
        }
    }
}

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
}

impl ColdState {
    /// Create a new cold state
    pub fn new(
        log_path: &Path,
        buffer_capacity: usize,
        config: PersistenceConfig,
        sync: SyncSettings,
    ) -> Result<Self> {
        // Ensure directory exists
        if let Some(parent) = log_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        Ok(Self {
            trajectory_log: Mutex::new(TrajectoryLog::open_file(log_path, config.buffer_size, sync)?),
            recent_buffer: DashMap::new(),
            buffer_capacity,
        })
    }

    /// Create a purely in-memory cold state.
    ///
    /// Used by `:memory:` databases: no file is created and no temp directory
    /// is touched. Trajectory history lives in an in-memory append log, so
    /// `query_trajectory` returns the same results a file-backed DB would,
    /// without paying for text serialization, `BufWriter` flushes, or `fsync`.
    pub fn new_memory(buffer_capacity: usize) -> Self {
        Self {
            trajectory_log: Mutex::new(TrajectoryLog::open_memory()),
            recent_buffer: DashMap::new(),
            buffer_capacity,
        }
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

        // Keep only the last N updates. Each call appends exactly one record to
        // a buffer that already held <= capacity, so at most one eviction is
        // ever needed — no loop required.
        if buffer.len() > self.buffer_capacity {
            buffer.pop_front();
        }

        Ok(())
    }

    pub fn append_tombstone(&self, namespace: &str, object_id: &str) -> Result<()> {
        let micros = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros();
        let mut log = self.trajectory_log.lock();
        log.append_tombstone(micros, namespace, object_id)
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
            // Below capacity the buffer holds this key's complete history; at or
            // above it, some records (including newer ones, under out-of-order
            // timestamps) live only on disk, so fall through to the disk merge.
            let buffer_is_complete = buffer.len() < self.buffer_capacity;

            from_buffer = buffer
                .iter()
                .filter(|u| u.timestamp >= start_time && u.timestamp <= end_time)
                .cloned()
                .collect();

            if buffer_is_complete {
                // Sort by timestamp (newest first) rather than trusting insertion
                // order, which can differ from time order under custom timestamps.
                from_buffer.sort_by_key(|u| std::cmp::Reverse(u.timestamp));
                from_buffer.truncate(limit);
                return Ok(from_buffer);
            }
        }

        // Fallback to the append log (slow path): scan the durable file log
        // or the in-memory log, depending on backend.
        let buffer_timestamps: std::collections::HashSet<SystemTime> =
            from_buffer.iter().map(|u| u.timestamp).collect();

        let from_disk = {
            let log = self.trajectory_log.lock();
            log.scan_trajectory(namespace, object_id, start_time, end_time, &buffer_timestamps)?
        };

        // Merge buffer and log results, sort by timestamp (newest first), limit
        from_buffer.extend(from_disk);
        from_buffer.sort_by_key(|b| std::cmp::Reverse(b.timestamp));
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
        let log = self.trajectory_log.lock();
        log.recover_latest()
    }
}

/// A single record in the in-memory trajectory log (memory-mode DBs).
#[derive(Clone)]
enum MemRecord {
    Update {
        namespace: String,
        object_id: String,
        update: LocationUpdate,
    },
    Tombstone {
        namespace: String,
        object_id: String,
    },
}

/// Storage backend for the trajectory log.
///
/// File-backed databases serialize records to a durable append-only text log;
/// `:memory:` databases keep parsed records in memory and never touch the
/// filesystem.
enum LogBackend {
    File {
        writer: BufWriter<File>,
        path: std::path::PathBuf,
        /// Records buffered in `writer` that have not yet been pushed to the OS.
        pending_writes: usize,
        /// Records written since the last `fsync`.
        writes_since_sync: usize,
        /// Wall-clock instant of the last `fsync`, used by [`SyncPolicy::EverySecond`].
        last_sync: Instant,
        buffer_limit: usize,
        sync: SyncSettings,
    },
    Memory {
        records: Vec<MemRecord>,
    },
}

/// Trajectory log: durable file-backed log or an in-memory log.
struct TrajectoryLog {
    backend: LogBackend,
}

impl TrajectoryLog {
    fn open_file(path: &Path, buffer_limit: usize, sync: SyncSettings) -> Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;

        Ok(Self {
            backend: LogBackend::File {
                writer: BufWriter::new(file),
                path: path.to_path_buf(),
                pending_writes: 0,
                writes_since_sync: 0,
                last_sync: Instant::now(),
                buffer_limit,
                sync,
            },
        })
    }

    fn open_memory() -> Self {
        Self {
            backend: LogBackend::Memory {
                records: Vec::new(),
            },
        }
    }

    /// Flush the in-memory write buffer to the OS and, depending on the
    /// configured [`SyncPolicy`], `fsync` it to stable storage.
    ///
    /// `force` is set on explicit flush/close/drop: it triggers an `fsync`
    /// regardless of batch/interval thresholds (unless the policy is
    /// [`SyncPolicy::Never`], which never syncs). A no-op for memory logs.
    fn maybe_sync(&mut self, force: bool) -> Result<()> {
        let LogBackend::File {
            writer,
            pending_writes,
            writes_since_sync,
            last_sync,
            buffer_limit,
            sync,
            ..
        } = &mut self.backend
        else {
            return Ok(());
        };

        let fsync = match sync.policy {
            SyncPolicy::Never => false,
            SyncPolicy::Always => force || *writes_since_sync >= sync.batch_size,
            SyncPolicy::EverySecond => force || last_sync.elapsed() >= Duration::from_secs(1),
        };

        if fsync {
            writer.flush()?;
            match sync.mode {
                SyncMode::All => writer.get_ref().sync_all()?,
                SyncMode::Data => writer.get_ref().sync_data()?,
            }
            *pending_writes = 0;
            *writes_since_sync = 0;
            *last_sync = Instant::now();
        } else if force || *pending_writes >= *buffer_limit {
            // Push buffered bytes to the OS even when not syncing, so a clean
            // process exit doesn't lose writes still sitting in the BufWriter.
            writer.flush()?;
            *pending_writes = 0;
        }

        Ok(())
    }

    fn append(&mut self, namespace: &str, object_id: &str, update: &LocationUpdate) -> Result<()> {
        match &mut self.backend {
            // Log format (pipe-separated, 8 fields per line):
            //   timestamp_micros|namespace|object_id|lat|lon|alt|json_len|json_metadata
            //
            // Coordinates are written to 6 decimal places (~0.1 m precision).
            // Namespace and object_id must not contain the `|` character.
            LogBackend::File {
                writer,
                pending_writes,
                writes_since_sync,
                ..
            } => {
                let micros = update
                    .timestamp
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_micros();

                let json_str =
                    serde_json::to_string(&update.metadata).unwrap_or_else(|_| "null".to_string());

                writeln!(
                    writer,
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

                *pending_writes += 1;
                *writes_since_sync += 1;
            }
            LogBackend::Memory { records } => {
                records.push(MemRecord::Update {
                    namespace: namespace.to_string(),
                    object_id: object_id.to_string(),
                    update: update.clone(),
                });
                return Ok(());
            }
        }
        self.maybe_sync(false)
    }

    fn append_tombstone(&mut self, micros: u128, namespace: &str, object_id: &str) -> Result<()> {
        match &mut self.backend {
            LogBackend::File {
                writer,
                pending_writes,
                writes_since_sync,
                ..
            } => {
                writeln!(writer, "TOMBSTONE|{}|{}|{}", micros, namespace, object_id)?;
                *pending_writes += 1;
                *writes_since_sync += 1;
            }
            LogBackend::Memory { records } => {
                // The tombstone's own timestamp is irrelevant to recovery, which
                // resolves the latest state by append order, so we don't store it.
                let _ = micros;
                records.push(MemRecord::Tombstone {
                    namespace: namespace.to_string(),
                    object_id: object_id.to_string(),
                });
                return Ok(());
            }
        }
        self.maybe_sync(false)
    }

    fn flush(&mut self) -> Result<()> {
        self.maybe_sync(true)
    }

    /// Scan the log for an object's updates within `[start, end]`, skipping any
    /// timestamps already present in `exclude` (i.e. served from the buffer).
    fn scan_trajectory(
        &self,
        namespace: &str,
        object_id: &str,
        start_time: SystemTime,
        end_time: SystemTime,
        exclude: &std::collections::HashSet<SystemTime>,
    ) -> Result<Vec<LocationUpdate>> {
        let mut out: Vec<LocationUpdate> = Vec::new();

        match &self.backend {
            LogBackend::File { path, .. } => {
                if !path.exists() {
                    return Ok(out);
                }
                let file = std::fs::File::open(path)?;
                let reader = std::io::BufReader::new(file);

                for line_result in std::io::BufRead::lines(reader) {
                    let line = match line_result {
                        Ok(l) => l,
                        Err(_) => continue,
                    };

                    // Parse: timestamp_micros|namespace|object_id|lat|lon|alt|json_len|json_metadata
                    let parts: Vec<&str> = line.split('|').collect();
                    if parts.len() != 8 {
                        continue;
                    }
                    if parts[1] != namespace || parts[2] != object_id {
                        continue;
                    }

                    let timestamp_micros: u128 = match parts[0].parse() {
                        Ok(t) => t,
                        Err(_) => continue,
                    };
                    let timestamp = UNIX_EPOCH + Duration::from_micros(timestamp_micros as u64);

                    if exclude.contains(&timestamp) {
                        continue;
                    }
                    if timestamp < start_time || timestamp > end_time {
                        continue;
                    }

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

                    let metadata: serde_json::Value =
                        serde_json::from_str(parts[7]).unwrap_or(serde_json::Value::Null);

                    out.push(LocationUpdate {
                        timestamp,
                        position: Point3d::new(lon, lat, alt),
                        metadata,
                    });
                }
            }
            LogBackend::Memory { records } => {
                for rec in records {
                    let MemRecord::Update {
                        namespace: ns,
                        object_id: id,
                        update,
                    } = rec
                    else {
                        continue;
                    };
                    if ns != namespace || id != object_id {
                        continue;
                    }
                    if exclude.contains(&update.timestamp) {
                        continue;
                    }
                    if update.timestamp < start_time || update.timestamp > end_time {
                        continue;
                    }
                    out.push(update.clone());
                }
            }
        }

        Ok(out)
    }

    /// Replay the whole log and return the latest surviving update per object
    /// (tombstones clear an object; a later update revives it). Used on startup.
    fn recover_latest(&self) -> Result<std::collections::HashMap<String, LocationUpdate>> {
        use std::collections::HashMap;

        // `None` slot means the key was tombstoned (or not yet seen with a value).
        let mut entries: HashMap<String, Option<LocationUpdate>> = HashMap::new();

        // Keep an update if the slot is empty/tombstoned, or strictly newer.
        fn merge(slot: &mut Option<LocationUpdate>, update: LocationUpdate) {
            match slot {
                None => *slot = Some(update),
                Some(existing) if update.timestamp > existing.timestamp => *slot = Some(update),
                _ => {}
            }
        }

        match &self.backend {
            LogBackend::File { path, .. } => {
                use std::io::{BufRead, BufReader};
                if !path.exists() {
                    return Ok(HashMap::new());
                }
                let file = std::fs::File::open(path)?;
                let reader = BufReader::new(file);

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

                    let parts: Vec<&str> = line.split('|').collect();

                    // Tombstone: TOMBSTONE|timestamp_micros|namespace|object_id
                    if parts.first() == Some(&"TOMBSTONE") {
                        if parts.len() != 4 {
                            log::warn!("Malformed tombstone on line {}", line_num + 1);
                            continue;
                        }
                        entries.insert(format!("{}::{}", parts[2], parts[3]), None);
                        continue;
                    }

                    if parts.len() != 8 {
                        log::warn!(
                            "Malformed log line {} (expected 8 fields, got {})",
                            line_num + 1,
                            parts.len()
                        );
                        continue;
                    }

                    let timestamp_micros: u128 = match parts[0].parse() {
                        Ok(t) => t,
                        Err(e) => {
                            log::warn!("Invalid timestamp on line {}: {}", line_num + 1, e);
                            continue;
                        }
                    };
                    let micros_u64 = u64::try_from(timestamp_micros).unwrap_or(u64::MAX);
                    let timestamp = UNIX_EPOCH + Duration::from_micros(micros_u64);

                    let namespace = parts[1];
                    let object_id = parts[2];

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

                    let metadata: serde_json::Value =
                        serde_json::from_str(parts[7]).unwrap_or_else(|e| {
                            log::warn!("Invalid metadata on line {}: {}", line_num + 1, e);
                            serde_json::Value::Null
                        });

                    let slot = entries.entry(format!("{}::{}", namespace, object_id)).or_insert(None);
                    merge(
                        slot,
                        LocationUpdate {
                            timestamp,
                            position: Point3d::new(lon, lat, alt),
                            metadata,
                        },
                    );
                }
            }
            LogBackend::Memory { records } => {
                for rec in records {
                    match rec {
                        MemRecord::Update {
                            namespace,
                            object_id,
                            update,
                        } => {
                            let slot = entries
                                .entry(format!("{}::{}", namespace, object_id))
                                .or_insert(None);
                            merge(slot, update.clone());
                        }
                        MemRecord::Tombstone {
                            namespace,
                            object_id,
                        } => {
                            entries.insert(format!("{}::{}", namespace, object_id), None);
                        }
                    }
                }
            }
        }

        Ok(entries
            .into_iter()
            .filter_map(|(key, slot)| slot.map(|u| (key, u)))
            .collect())
    }
}

impl Drop for TrajectoryLog {
    fn drop(&mut self) {
        if let Err(e) = self.maybe_sync(true) {
            log::warn!("Failed to flush trajectory log on drop: {}", e);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::PersistenceConfig;
    use std::time::Duration;
    use tempfile::tempdir;

    /// With `SyncPolicy::Always` and a large write buffer, a single append must
    /// already be on disk (flushed past the BufWriter) without any explicit flush.
    #[test]
    fn test_sync_policy_always_persists_immediately() {
        let dir = tempdir().unwrap();
        let log_path = dir.path().join("traj.log");
        let cold = ColdState::new(
            &log_path,
            10,
            // Large buffer: without fsync, one write would not reach disk.
            PersistenceConfig {
                buffer_size: 10_000,
            },
            SyncSettings {
                policy: SyncPolicy::Always,
                mode: SyncMode::Data,
                batch_size: 1,
            },
        )
        .unwrap();

        cold.append_update(
            "v",
            "o",
            Point3d::new(1.0, 2.0, 3.0),
            serde_json::json!({"k": "v"}),
            UNIX_EPOCH + Duration::from_secs(1),
        )
        .unwrap();

        // Read the raw file directly (a separate handle): the bytes must be there.
        let contents = std::fs::read_to_string(&log_path).unwrap();
        assert!(
            contents.contains("|v|o|"),
            "Always policy must fsync the record to disk immediately, got: {contents:?}"
        );
    }

    /// `flush()`/close must push buffered writes to disk even under
    /// `SyncPolicy::Never` (which otherwise never syncs).
    #[test]
    fn test_flush_persists_under_never_policy() {
        let dir = tempdir().unwrap();
        let log_path = dir.path().join("traj.log");
        let cold = ColdState::new(
            &log_path,
            10,
            PersistenceConfig {
                buffer_size: 10_000,
            },
            SyncSettings {
                policy: SyncPolicy::Never,
                mode: SyncMode::All,
                batch_size: 1,
            },
        )
        .unwrap();

        cold.append_update(
            "v",
            "o",
            Point3d::new(1.0, 2.0, 3.0),
            serde_json::json!({}),
            UNIX_EPOCH + Duration::from_secs(1),
        )
        .unwrap();

        // Not yet flushed: still buffered in the BufWriter.
        assert!(std::fs::read_to_string(&log_path).unwrap().is_empty());

        cold.flush().unwrap();
        assert!(
            std::fs::read_to_string(&log_path)
                .unwrap()
                .contains("|v|o|")
        );
    }

    #[test]
    fn test_append_and_query_buffer() {
        let dir = tempdir().unwrap();
        let log_path = dir.path().join("traj.log");
        let cold = ColdState::new(
            &log_path,
            10,
            PersistenceConfig::default(),
            SyncSettings::default(),
        )
        .unwrap();

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

        let cold = ColdState::new(
            &log_path,
            2,
            PersistenceConfig { buffer_size: 0 },
            SyncSettings::default(),
        )
        .unwrap(); // Capacity 2

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

        let cold = ColdState::new(
            &log_path,
            10,
            PersistenceConfig { buffer_size: 0 },
            SyncSettings::default(),
        )
        .unwrap();

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
    fn test_tombstone_beats_future_timestamp_on_recovery() {
        // An object inserted with a future SetOptions timestamp must still stay deleted
        // after a tombstone is written, even though its timestamp is > current time.
        let dir = tempdir().unwrap();
        let log_path = dir.path().join("traj.log");
        let cold = ColdState::new(
            &log_path,
            10,
            PersistenceConfig { buffer_size: 0 },
            SyncSettings::default(),
        )
        .unwrap();

        // Insert with a timestamp far in the future.
        let future_ts = UNIX_EPOCH + Duration::from_secs(99_999_999_999);
        cold.append_update(
            "ns",
            "obj",
            Point3d::new(1.0, 2.0, 0.0),
            serde_json::json!({}),
            future_ts,
        )
        .unwrap();
        // Tombstone written after the insert (later in log order).
        cold.append_tombstone("ns", "obj").unwrap();

        let recovered = cold.recover_current_locations().unwrap();
        assert!(
            !recovered.contains_key("ns::obj"),
            "future-timestamped object must not reappear after tombstone"
        );
    }

    #[test]
    fn test_tombstone_excludes_deleted_object_on_recovery() {
        let dir = tempdir().unwrap();
        let log_path = dir.path().join("traj.log");
        let cold = ColdState::new(
            &log_path,
            10,
            PersistenceConfig { buffer_size: 0 },
            SyncSettings::default(),
        )
        .unwrap();

        let t1 = UNIX_EPOCH + Duration::from_secs(1000);
        let t2 = UNIX_EPOCH + Duration::from_secs(2000);

        cold.append_update(
            "ns",
            "obj_keep",
            Point3d::new(1.0, 2.0, 0.0),
            serde_json::json!({}),
            t1,
        )
        .unwrap();
        cold.append_update(
            "ns",
            "obj_del",
            Point3d::new(3.0, 4.0, 0.0),
            serde_json::json!({}),
            t1,
        )
        .unwrap();
        cold.append_tombstone("ns", "obj_del").unwrap();
        cold.append_update(
            "ns",
            "obj_keep",
            Point3d::new(1.1, 2.1, 0.0),
            serde_json::json!({}),
            t2,
        )
        .unwrap();

        let recovered = cold.recover_current_locations().unwrap();
        assert!(
            recovered.contains_key("ns::obj_keep"),
            "kept object should be recovered"
        );
        assert!(
            !recovered.contains_key("ns::obj_del"),
            "deleted object must not be recovered"
        );
    }

    #[test]
    fn test_tombstone_then_reinsert_recovers_object() {
        let dir = tempdir().unwrap();
        let log_path = dir.path().join("traj.log");
        let cold = ColdState::new(
            &log_path,
            10,
            PersistenceConfig { buffer_size: 0 },
            SyncSettings::default(),
        )
        .unwrap();

        let t1 = UNIX_EPOCH + Duration::from_secs(1000);
        cold.append_update(
            "ns",
            "obj",
            Point3d::new(1.0, 2.0, 0.0),
            serde_json::json!({}),
            t1,
        )
        .unwrap();
        cold.append_tombstone("ns", "obj").unwrap();

        // Re-insert with a timestamp guaranteed to be after the tombstone (now + margin).
        let t2 = SystemTime::now() + Duration::from_secs(1);
        cold.append_update(
            "ns",
            "obj",
            Point3d::new(5.0, 6.0, 0.0),
            serde_json::json!({}),
            t2,
        )
        .unwrap();

        let recovered = cold.recover_current_locations().unwrap();
        assert!(
            recovered.contains_key("ns::obj"),
            "re-inserted object should survive recovery"
        );
        assert_eq!(recovered["ns::obj"].position.x(), 5.0);
    }

    /// With a full buffer and out-of-order timestamps, the newest record may
    /// have been evicted to disk. The query must still return it rather than
    /// short-circuiting on the buffer's contents.
    #[test]
    fn test_trajectory_query_out_of_order_timestamps() {
        let dir = tempdir().unwrap();
        let log_path = dir.path().join("traj.log");
        // Capacity 2: only the two most recently *inserted* records stay buffered.
        let cold = ColdState::new(
            &log_path,
            2,
            PersistenceConfig { buffer_size: 0 },
            SyncSettings::default(),
        )
        .unwrap();

        let mk = |secs| UNIX_EPOCH + Duration::from_secs(secs);
        // Insert the newest-timestamped record FIRST so it gets evicted to disk.
        cold.append_update(
            "v",
            "o",
            Point3d::new(5.0, 0.0, 0.0),
            serde_json::json!({}),
            mk(5000),
        )
        .unwrap();
        cold.append_update(
            "v",
            "o",
            Point3d::new(1.0, 0.0, 0.0),
            serde_json::json!({}),
            mk(1000),
        )
        .unwrap();
        cold.append_update(
            "v",
            "o",
            Point3d::new(2.0, 0.0, 0.0),
            serde_json::json!({}),
            mk(2000),
        )
        .unwrap();
        cold.append_update(
            "v",
            "o",
            Point3d::new(3.0, 0.0, 0.0),
            serde_json::json!({}),
            mk(3000),
        )
        .unwrap();

        // Buffer now holds only t=2000 and t=3000; t=5000 lives on disk.
        let newest = cold
            .query_trajectory("v", "o", UNIX_EPOCH, mk(6000), 1)
            .unwrap();
        assert_eq!(newest.len(), 1);
        assert_eq!(
            newest[0].timestamp,
            mk(5000),
            "must return the globally-newest record even when it was evicted from the buffer"
        );
    }

    #[test]
    fn test_disk_based_trajectory_query() {
        let dir = tempdir().unwrap();
        let log_path = dir.path().join("traj.log");

        let cold = ColdState::new(
            &log_path,
            2,
            PersistenceConfig { buffer_size: 0 },
            SyncSettings::default(),
        )
        .unwrap(); // Small buffer to force disk scan

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
