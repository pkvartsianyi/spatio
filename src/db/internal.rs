//! Internal database operations and state management.

use super::{DBInner, HistoryTracker};
use crate::config::{Config, DbItem, DbStats, SetOptions};
use crate::error::{Result, SpatioError};
use crate::persistence::{AOFCommand, AOFFile};
use crate::spatial_index::SpatialIndexManager;
use bytes::Bytes;
use std::collections::BTreeMap;
use std::time::{Duration, SystemTime};

impl DBInner {
    /// Maximum allowed timestamp drift into the future (1 day)
    const MAX_FUTURE_TIMESTAMP: Duration = Duration::from_secs(86400);

    /// Validate that a timestamp is reasonable (not too far in the future)
    pub(super) fn validate_timestamp(created_at: SystemTime) -> Result<()> {
        let now = SystemTime::now();
        if created_at > now + Self::MAX_FUTURE_TIMESTAMP {
            return Err(SpatioError::InvalidTimestamp);
        }
        Ok(())
    }

    /// Generate a spatial key with encoded coordinates for AOF replay
    /// Format: "prefix:lat_hex:lon_hex:z_hex:timestamp_nanos:uuid"
    pub(super) fn generate_spatial_key(
        prefix: &str,
        x: f64,
        y: f64,
        z: f64,
        created_at: SystemTime,
    ) -> Result<String> {
        let timestamp_nanos = created_at
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_err(|_| SpatioError::InvalidTimestamp)?
            .as_nanos();

        Ok(format!(
            "{}:{:x}:{:x}:{:x}:{:x}:{}",
            prefix,
            y.to_bits(),
            x.to_bits(),
            z.to_bits(),
            timestamp_nanos,
            uuid::Uuid::new_v4()
        ))
    }

    pub(crate) fn new_with_config(config: &Config) -> Self {
        Self {
            keys: BTreeMap::new(),
            expirations: BTreeMap::new(),
            spatial_index: SpatialIndexManager::new(),
            aof_file: None,
            closed: false,
            stats: DbStats::default(),
            config: config.clone(),
            sync_ops_since_flush: 0,
            #[cfg(feature = "time-index")]
            created_index: BTreeMap::new(),
            #[cfg(feature = "time-index")]
            history: config.history_capacity.map(HistoryTracker::new),
        }
    }

    pub(super) fn add_expiration(&mut self, key: &Bytes, expires_at: Option<SystemTime>) {
        if let Some(exp) = expires_at {
            let keys_at_time = self.expirations.entry(exp).or_default();
            keys_at_time.push(key.clone());

            const EXPIRATION_VEC_WARN_THRESHOLD: usize = 10_000;
            if keys_at_time.len() == EXPIRATION_VEC_WARN_THRESHOLD {
                log::warn!(
                    "Large expiration cluster detected: {} keys expire at {:?}. \
                     Consider spreading TTL values to avoid cleanup spikes.",
                    keys_at_time.len(),
                    exp
                );
            }
        }
    }

    pub(super) fn remove_expiration_entry(&mut self, key: &Bytes, item: &DbItem) {
        if let Some(exp) = item.expires_at
            && let Some(keys) = self.expirations.get_mut(&exp)
        {
            keys.retain(|k| k != key);
            if keys.is_empty() {
                self.expirations.remove(&exp);
            }
        }
    }

    #[cfg(feature = "time-index")]
    pub(super) fn add_created_index(&mut self, key: &Bytes, created_at: SystemTime) {
        self.created_index
            .entry(created_at)
            .or_default()
            .insert(key.clone());
    }

    #[cfg(feature = "time-index")]
    pub(super) fn remove_created_index(&mut self, key: &Bytes, item: &DbItem) {
        if let Some(keys) = self.created_index.get_mut(&item.created_at) {
            keys.remove(key);
            if keys.is_empty() {
                self.created_index.remove(&item.created_at);
            }
        }
    }

    /// Insert an item into the database
    pub fn insert_item(&mut self, key: Bytes, item: DbItem) -> Option<DbItem> {
        let expires_at = item.expires_at;
        #[cfg(feature = "time-index")]
        let created_at = item.created_at;
        #[cfg(feature = "time-index")]
        let history_value = self.history.as_ref().map(|_| item.value.clone());

        let old_item = self.keys.insert(key.clone(), item);
        if let Some(ref old) = old_item {
            self.remove_expiration_entry(&key, old);
            #[cfg(feature = "time-index")]
            self.remove_created_index(&key, old);
        }

        self.add_expiration(&key, expires_at);
        #[cfg(feature = "time-index")]
        self.add_created_index(&key, created_at);

        #[cfg(feature = "time-index")]
        if let Some(history) = self.history.as_mut()
            && let Some(value) = history_value
        {
            history.record_set(&key, value, created_at, expires_at);
        }

        self.stats.key_count = self.keys.len();
        old_item
    }

    /// Remove an item from the database
    pub fn remove_item(&mut self, key: &Bytes) -> Option<DbItem> {
        if let Some(item) = self.keys.remove(key) {
            #[cfg(feature = "time-index")]
            let history_value = self.history.as_ref().map(|_| item.value.clone());
            self.remove_expiration_entry(key, &item);
            #[cfg(feature = "time-index")]
            self.remove_created_index(key, &item);

            if let Ok(key_str) = std::str::from_utf8(key)
                && let Some(prefix) = key_str.split(':').next()
            {
                let _ = self.spatial_index.remove_entry(prefix, key_str);
            }

            #[cfg(feature = "time-index")]
            if let Some(history) = self.history.as_mut() {
                history.record_delete(key, SystemTime::now(), history_value);
            }

            self.stats.key_count = self.keys.len();
            Some(item)
        } else {
            None
        }
    }

    pub(super) fn amortized_cleanup(&mut self, max_items: usize) -> Result<usize> {
        if max_items == 0 {
            return Ok(0);
        }

        let now = SystemTime::now();
        let mut removed = 0;
        let timestamps_to_check: Vec<SystemTime> =
            self.expirations.range(..=now).map(|(ts, _)| *ts).collect();

        for ts in timestamps_to_check {
            if removed >= max_items {
                break;
            }

            let Some(mut keys) = self.expirations.remove(&ts) else {
                continue;
            };

            let to_process = (max_items - removed).min(keys.len());

            for _ in 0..to_process {
                if let Some(key) = keys.pop()
                    && self.remove_item(&key).is_some()
                {
                    self.write_delete_to_aof_if_needed(&key)?;
                    removed += 1;
                }
            }

            if !keys.is_empty() {
                self.expirations.insert(ts, keys);
            }
        }

        Ok(removed)
    }

    /// Get an item from the database
    pub fn get_item(&self, key: &Bytes) -> Option<&DbItem> {
        self.keys.get(key)
    }

    /// Load database state from the AOF file (startup replay).
    ///
    /// This method replays all commands from the append-only file to restore
    /// the database to its previous state. It's called automatically during
    /// database initialization.
    ///
    /// The replay process:
    /// 1. Reads all commands from the AOF sequentially
    /// 2. Applies each SET and DELETE command to rebuild state
    /// 3. Reconstructs spatial indexes from geographic data
    /// 4. Updates statistics (key counts, etc.)
    ///
    /// # Error Handling
    ///
    /// If the AOF is corrupted or unreadable, this method returns an error
    /// and the database will not open. To recover from corruption:
    /// - Restore from backup if available
    /// - Or delete the AOF file to start fresh (data loss)
    pub fn load_from_aof(&mut self, aof_file: &mut AOFFile) -> Result<()> {
        for command in aof_file.replay()? {
            match command {
                AOFCommand::Set {
                    key,
                    value,
                    created_at,
                    expires_at,
                } => {
                    self.apply_set_from_aof(key, value, created_at, expires_at)?;
                }
                AOFCommand::Delete { key } => {
                    self.apply_delete_from_aof(key)?;
                }
            }
        }

        self.stats.key_count = self.keys.len();
        Ok(())
    }

    fn apply_set_from_aof(
        &mut self,
        key: Bytes,
        value: Bytes,
        created_at: SystemTime,
        expires_at: Option<SystemTime>,
    ) -> Result<()> {
        let item = DbItem {
            value: value.clone(),
            created_at,
            expires_at,
        };

        if let Some(old) = self.keys.insert(key.clone(), item) {
            self.remove_expiration_entry(&key, &old);
            #[cfg(feature = "time-index")]
            self.remove_created_index(&key, &old);
        }

        self.add_expiration(&key, expires_at);
        #[cfg(feature = "time-index")]
        self.add_created_index(&key, created_at);

        #[cfg(feature = "time-index")]
        if let Some(history) = self.history.as_mut() {
            history.record_set(&key, value.clone(), created_at, expires_at);
        }

        self.rebuild_spatial_index(&key, &value);
        Ok(())
    }

    fn apply_delete_from_aof(&mut self, key: Bytes) -> Result<()> {
        if let Some(item) = self.keys.remove(&key) {
            #[cfg(feature = "time-index")]
            let deleted_value = item.value.clone();
            self.remove_expiration_entry(&key, &item);
            #[cfg(feature = "time-index")]
            self.remove_created_index(&key, &item);

            #[cfg(feature = "time-index")]
            if let Some(history) = self.history.as_mut() {
                history.record_delete(&key, SystemTime::now(), Some(deleted_value));
            }
        }

        self.remove_from_spatial_index(&key);
        Ok(())
    }

    fn rebuild_spatial_index(&mut self, key: &Bytes, value: &Bytes) {
        if let Ok(key_str) = std::str::from_utf8(key) {
            let parts: Vec<&str> = key_str.split(':').collect();

            if parts.len() < 6 {
                return;
            }

            if let (Ok(lat_bits), Ok(lon_bits), Ok(z_bits)) = (
                u64::from_str_radix(parts[1], 16),
                u64::from_str_radix(parts[2], 16),
                u64::from_str_radix(parts[3], 16),
            ) {
                let prefix = parts[0];
                let lat = f64::from_bits(lat_bits);
                let lon = f64::from_bits(lon_bits);
                let z = f64::from_bits(z_bits);

                if !lat.is_finite() || !lon.is_finite() || !z.is_finite() {
                    log::warn!(
                        "Skipping AOF entry with invalid coordinates: key='{}', lat={}, lon={}, z={}",
                        key_str,
                        lat,
                        lon,
                        z
                    );
                    return;
                }

                if !(-90.0..=90.0).contains(&lat) || !(-180.0..=180.0).contains(&lon) {
                    log::warn!(
                        "Skipping AOF entry with coordinates out of valid geographic range: key='{}', lat={}, lon={}",
                        key_str,
                        lat,
                        lon
                    );
                    return;
                }

                if z == 0.0 {
                    self.spatial_index.insert_point_2d(
                        prefix,
                        lon,
                        lat,
                        key_str.to_string(),
                        value.clone(),
                    );
                } else {
                    self.spatial_index.insert_point(
                        prefix,
                        lon,
                        lat,
                        z,
                        key_str.to_string(),
                        value.clone(),
                    );
                }
            }
        }
    }

    fn remove_from_spatial_index(&mut self, key: &Bytes) {
        if !key.contains(&b':') {
            return;
        }

        if let Ok(key_str) = std::str::from_utf8(key)
            && let Some(colon_pos) = key_str.find(':')
        {
            let prefix = &key_str[..colon_pos];
            let _ = self.spatial_index.remove_entry(prefix, key_str);
        }
    }

    /// Write to AOF file if needed
    pub fn write_to_aof_if_needed(
        &mut self,
        key: &Bytes,
        value: &[u8],
        options: Option<&SetOptions>,
        created_at: SystemTime,
    ) -> Result<()> {
        let Some(aof_file) = self.aof_file.as_mut() else {
            return Ok(());
        };

        let sync_policy = self.config.sync_policy;
        let sync_mode = self.config.sync_mode;
        let batch_size = self.config.sync_batch_size;
        let value_bytes = Bytes::copy_from_slice(value);

        aof_file.write_set(key, &value_bytes, options, created_at)?;
        self.maybe_flush_or_sync(sync_policy, sync_mode, batch_size)?;
        Ok(())
    }

    /// Write delete operation to AOF if needed
    pub fn write_delete_to_aof_if_needed(&mut self, key: &Bytes) -> Result<()> {
        let Some(aof_file) = self.aof_file.as_mut() else {
            return Ok(());
        };

        let sync_policy = self.config.sync_policy;
        let sync_mode = self.config.sync_mode;
        let batch_size = self.config.sync_batch_size;

        aof_file.write_delete(key)?;
        self.maybe_flush_or_sync(sync_policy, sync_mode, batch_size)?;
        Ok(())
    }

    fn maybe_flush_or_sync(
        &mut self,
        policy: crate::config::SyncPolicy,
        mode: crate::config::SyncMode,
        batch_size: usize,
    ) -> Result<()> {
        use crate::config::SyncPolicy;

        let Some(aof_file) = self.aof_file.as_mut() else {
            return Ok(());
        };

        match policy {
            SyncPolicy::Always => {
                self.sync_ops_since_flush += 1;
                if self.sync_ops_since_flush >= batch_size {
                    aof_file.sync_with_mode(mode)?;
                    self.sync_ops_since_flush = 0;
                } else {
                    aof_file.flush()?;
                }
            }
            SyncPolicy::EverySecond => {
                aof_file.flush()?;
            }
            SyncPolicy::Never => {}
        }

        Ok(())
    }
}
