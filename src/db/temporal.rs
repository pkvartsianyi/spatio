//! Temporal operations for time-based queries and TTL management.

use super::{DB, DBInner};
use crate::config::{SetOptions, TemporalPoint};
use crate::error::{Result, SpatioError};
use bytes::Bytes;
use std::time::{Duration, SystemTime};

#[cfg(feature = "time-index")]
use crate::config::HistoryEntry;

impl DB {
    /// Remove all expired keys and compact indexes.
    pub fn cleanup_expired(&self) -> Result<usize> {
        let mut inner = self.write_checked()?;

        let now = SystemTime::now();
        let expired_times: Vec<SystemTime> =
            inner.expirations.range(..=now).map(|(&ts, _)| ts).collect();

        let mut removed = 0;
        for ts in expired_times {
            if let Some(keys) = inner.expirations.remove(&ts) {
                for key in keys {
                    if let Some(_item) = inner.remove_item(&key) {
                        inner.write_delete_to_aof_if_needed(&key)?;
                        removed += 1;
                    }
                }
            }
        }

        Ok(removed)
    }

    #[cfg(feature = "time-index")]
    /// Return keys whose last update occurred within the given duration.
    pub fn keys_created_since(&self, duration: Duration) -> Result<Vec<Bytes>> {
        let inner = self.read_checked()?;

        let end = SystemTime::now();
        let start = end.checked_sub(duration).unwrap_or(SystemTime::UNIX_EPOCH);

        Ok(inner.collect_keys_created_between(start, end))
    }

    #[cfg(feature = "time-index")]
    /// Return keys whose last update timestamp falls within the specified interval.
    pub fn keys_created_between(&self, start: SystemTime, end: SystemTime) -> Result<Vec<Bytes>> {
        let inner = self.read_checked()?;

        let (start, end) = if start <= end {
            (start, end)
        } else {
            (end, start)
        };
        Ok(inner.collect_keys_created_between(start, end))
    }

    #[cfg(feature = "time-index")]
    /// Retrieve the recent history of mutations for a specific key.
    pub fn history(&self, key: impl AsRef<[u8]>) -> Result<Vec<HistoryEntry>> {
        let inner = self.read_checked()?;

        if let Some(ref tracker) = inner.history {
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
    /// let db = Spatio::memory()?;
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
        &self,
        object_id: &str,
        trajectory: &[TemporalPoint],
        opts: Option<SetOptions>,
    ) -> Result<()> {
        for (i, temporal_point) in trajectory.iter().enumerate() {
            let key = format!(
                "traj:{}:{:010}:{:06}",
                object_id,
                temporal_point
                    .timestamp
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .map_err(|_| SpatioError::InvalidTimestamp)?
                    .as_secs(),
                i
            );
            let point_data = bincode::serialize(&temporal_point).map_err(|e| {
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
    /// let db = Spatio::memory()?;
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

        let start_key = format!("traj:{}:{:010}:000000", object_id, start_time);
        let end_key = format!("traj:{}:{:010}:999999", object_id, end_time);

        let inner = self.read()?;

        for (key, item) in inner
            .keys
            .range(Bytes::from(start_key)..=Bytes::from(end_key))
        {
            if let Ok(key_str) = std::str::from_utf8(key) {
                if !key_str.starts_with(&format!("traj:{}:", object_id)) {
                    break;
                }

                let parts: Vec<&str> = key_str.split(':').collect();
                if parts.len() >= 4
                    && let Ok(ts) = parts[parts.len() - 2].parse::<u64>()
                    && (ts < start_time || ts > end_time)
                {
                    continue;
                }
            }

            if item.is_expired() {
                continue;
            }

            match bincode::deserialize::<TemporalPoint>(&item.value) {
                Ok(temporal_point) => results.push(temporal_point),
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
