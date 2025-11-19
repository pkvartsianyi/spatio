//! Temporal operations for time-based queries and TTL management.
//!
//! ## Trajectory Storage Format
//!
//! Trajectories are stored using a **binary key format** for efficient storage and fast queries:
//!
//! ```text
//! [object_id_bytes][0x00][timestamp_be:u64][sequence_be:u32]
//! ```
//!
//! ### Format Components
//!
//! - **`object_id_bytes`** - UTF-8 bytes of the object identifier (variable length)
//! - **`0x00`** - Null byte separator
//! - **`timestamp_be`** - Unix timestamp in seconds (8 bytes, big-endian)
//! - **`sequence_be`** - Index within trajectory array (4 bytes, big-endian)
//!
//! ###Examples
//!
//! For `object_id="vehicle:truck001"`, `timestamp=1640995200`, `sequence=0`:
//!
//! ```text
//! [76 65 68 69 63 6c 65 3a 74 72 75 63 6b 30 30 31] // "vehicle:truck001"
//! [00]                                                 // null separator
//! [00 00 00 00 61 df 8e 80]                          // timestamp (big-endian)
//! [00 00 00 00]                                        // sequence (big-endian)

use crate::config::{SetOptions, TemporalPoint};
use crate::db::{DB, DBInner};
use crate::error::{Result, SpatioError};
use bytes::Bytes;
use std::time::{Duration, SystemTime};

#[cfg(feature = "time-index")]
use crate::config::HistoryEntry;

/// Encode a trajectory key in binary format for efficient storage.
///
/// Format: `[object_id_bytes][0x00][timestamp_be:u64][sequence_be:u32]`
///
/// - object_id: UTF-8 bytes of the object identifier
/// - 0x00: Null byte separator
/// - timestamp: Unix timestamp in seconds (big-endian u64)
/// - sequence: Index in trajectory array (big-endian u32)
///
/// Big-endian encoding ensures lexicographic byte order matches numeric order,
/// which is critical for BTreeMap range queries.
fn encode_trajectory_key(object_id: &str, timestamp: u64, sequence: u32) -> Bytes {
    let mut key = Vec::with_capacity(object_id.len() + 1 + 8 + 4);

    // Object ID (variable length)
    key.extend_from_slice(object_id.as_bytes());

    // Null byte separator
    key.push(0);

    // Timestamp (8 bytes, big-endian for lexicographic ordering)
    key.extend_from_slice(&timestamp.to_be_bytes());

    // Sequence (4 bytes, big-endian for lexicographic ordering)
    key.extend_from_slice(&sequence.to_be_bytes());

    Bytes::from(key)
}

/// Create start key for trajectory range query.
fn make_trajectory_start_key(object_id: &str, start_time: u64) -> Bytes {
    encode_trajectory_key(object_id, start_time, 0)
}

/// Create end key for trajectory range query.
fn make_trajectory_end_key(object_id: &str, end_time: u64) -> Bytes {
    encode_trajectory_key(object_id, end_time, u32::MAX)
}

///Decode a trajectory key to extract object_id, timestamp, and sequence.
///
/// Returns None if the key format is invalid.
fn decode_trajectory_key(key: &[u8]) -> Option<(&str, u64, u32)> {
    // Find null byte separator
    let null_pos = key.iter().position(|&b| b == 0)?;

    // Extract object_id
    let object_id = std::str::from_utf8(&key[..null_pos]).ok()?;

    // Need at least 12 bytes after null byte (8 for timestamp + 4 for sequence)
    if key.len() < null_pos + 1 + 12 {
        return None;
    }

    // Extract timestamp (big-endian u64)
    let timestamp_bytes = &key[null_pos + 1..null_pos + 1 + 8];
    let timestamp = u64::from_be_bytes(timestamp_bytes.try_into().ok()?);

    // Extract sequence (big-endian u32)
    let sequence_bytes = &key[null_pos + 1 + 8..null_pos + 1 + 8 + 4];
    let sequence = u32::from_be_bytes(sequence_bytes.try_into().ok()?);

    Some((object_id, timestamp, sequence))
}

impl DB {
    pub fn count_expired(&self) -> usize {
        let now = SystemTime::now();
        self.inner
            .keys
            .values()
            .filter(|item| item.is_expired_at(now))
            .count()
    }

    pub fn cleanup_expired(&mut self) -> Result<usize> {
        let now = SystemTime::now();

        // Collect all expired keys
        let expired_keys: Vec<Bytes> = self
            .inner
            .keys
            .iter()
            .filter_map(|(key, item)| {
                if item.is_expired_at(now) {
                    Some(key.clone())
                } else {
                    None
                }
            })
            .collect();

        let mut removed = 0;
        for key in expired_keys {
            if self.inner.remove_item(&key).is_some() {
                self.inner.write_delete_to_aof_if_needed(&key)?;
                removed += 1;
            }
        }

        Ok(removed)
    }

    #[cfg(feature = "time-index")]
    /// Return keys whose last update occurred within the given duration.
    pub fn keys_created_since(&self, duration: Duration) -> Result<Vec<Bytes>> {
        let end = SystemTime::now();
        let start = end.checked_sub(duration).unwrap_or(SystemTime::UNIX_EPOCH);

        Ok(self.inner.collect_keys_created_between(start, end))
    }

    #[cfg(feature = "time-index")]
    /// Return keys whose last update timestamp falls within the specified interval.
    pub fn keys_created_between(&self, start: SystemTime, end: SystemTime) -> Result<Vec<Bytes>> {
        let (start, end) = if start <= end {
            (start, end)
        } else {
            (end, start)
        };
        Ok(self.inner.collect_keys_created_between(start, end))
    }

    #[cfg(feature = "time-index")]
    /// Retrieve the recent history of mutations for a specific key.
    pub fn history(&self, key: impl AsRef<[u8]>) -> Result<Vec<HistoryEntry>> {
        if let Some(ref tracker) = self.inner.history {
            let key_bytes = Bytes::copy_from_slice(key.as_ref());
            Ok(tracker.history_for(&key_bytes).unwrap_or_default())
        } else {
            Ok(Vec::new())
        }
    }

    /// Insert a trajectory (sequence of points over time).
    ///
    /// Trajectories represent the movement of objects over time. Each
    /// point in the trajectory has a timestamp for temporal queries.
    ///
    /// # Arguments
    ///
    /// * `object_id` - Unique identifier for the moving object
    /// * `trajectory` - Sequence of (Point, timestamp) pairs
    /// * `opts` - Optional settings like TTL for the entire trajectory
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spatio::{Spatio, Point, TemporalPoint};
    /// use std::time::{Duration, SystemTime, UNIX_EPOCH};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut db = Spatio::memory()?;
    ///
    /// let trajectory = vec![
    ///     TemporalPoint { point: Point::new(-74.0060, 40.7128), timestamp: UNIX_EPOCH + Duration::from_secs(1640995200) }, // Start
    ///     TemporalPoint { point: Point::new(-74.0040, 40.7150), timestamp: UNIX_EPOCH + Duration::from_secs(1640995260) }, // 1 min later
    ///     TemporalPoint { point: Point::new(-74.0020, 40.7172), timestamp: UNIX_EPOCH + Duration::from_secs(1640995320) }, // 2 min later
    /// ];
    ///
    /// db.insert_trajectory("vehicle:truck001", &trajectory, None)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn insert_trajectory(
        &mut self,
        object_id: &str,
        trajectory: &[TemporalPoint],
        opts: Option<SetOptions>,
    ) -> Result<()> {
        for (i, temporal_point) in trajectory.iter().enumerate() {
            let timestamp = temporal_point
                .timestamp
                .duration_since(SystemTime::UNIX_EPOCH)
                .map_err(|_| SpatioError::InvalidTimestamp)?
                .as_secs();

            let key = encode_trajectory_key(object_id, timestamp, i as u32);

            let point_data =
                bincode::serde::encode_to_vec(temporal_point, bincode::config::standard())
                    .map_err(|e| {
                        SpatioError::SerializationErrorWithContext(format!(
                            "Failed to serialize trajectory point for object '{}': {}",
                            object_id, e
                        ))
                    })?;

            self.insert(&key, &point_data, opts.clone())?;
        }
        Ok(())
    }

    /// Query trajectory between timestamps.
    ///
    /// Returns all trajectory points for an object within the specified
    /// time range, ordered by timestamp.
    ///
    /// # Arguments
    ///
    /// * `object_id` - The object to query
    /// * `start_time` - Start of time range (unix timestamp)
    /// * `end_time` - End of time range (unix timestamp)
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spatio::{Spatio, TemporalPoint};
    /// use std::time::{Duration, SystemTime, UNIX_EPOCH};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut db = Spatio::memory()?;
    ///
    /// // Query trajectory for first hour
    /// let path = db.query_trajectory("vehicle:truck001", 1640995200, 1640998800)?;
    /// println!("Found {} trajectory points", path.len());
    /// # Ok(())
    /// # }
    /// ```
    pub fn query_trajectory(
        &self,
        object_id: &str,
        start_time: u64,
        end_time: u64,
    ) -> Result<Vec<TemporalPoint>> {
        let mut results = Vec::new();

        let start_key = make_trajectory_start_key(object_id, start_time);
        let end_key = make_trajectory_end_key(object_id, end_time);

        for (key, item) in self.inner.keys.range(start_key..=end_key) {
            // Validate key format and check if it matches expected object_id
            if let Some((key_object_id, timestamp, _sequence)) = decode_trajectory_key(key) {
                // Ensure key belongs to the requested object
                if key_object_id != object_id {
                    break;
                }

                // Additional timestamp validation (range query handles most of this)
                if timestamp < start_time || timestamp > end_time {
                    continue;
                }
            } else {
                // Invalid key format, skip
                continue;
            }

            if item.is_expired() {
                continue;
            }

            match bincode::serde::decode_from_slice::<TemporalPoint, _>(
                &item.value,
                bincode::config::standard(),
            ) {
                Ok((temporal_point, _)) => results.push(temporal_point),
                Err(e) => {
                    log::warn!(
                        "Failed to deserialize trajectory point for object '{}': {}. Skipping corrupted point.",
                        object_id,
                        e
                    );
                }
            }
        }
        Ok(results)
    }
}

#[cfg(feature = "time-index")]
impl DBInner {
    pub(super) fn collect_keys_created_between(
        &self,
        start: SystemTime,
        end: SystemTime,
    ) -> Vec<Bytes> {
        let mut results = Vec::new();
        let now = SystemTime::now();
        for (_timestamp, keys) in self.created_index.range(start..=end) {
            for key in keys {
                if let Some(item) = self.keys.get(key)
                    && !item.is_expired_at(now)
                {
                    results.push(key.clone());
                }
            }
        }
        results
    }
}
