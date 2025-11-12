//! Internal database operations and state management.

use super::{DBInner, HistoryTracker};
use crate::compute::spatial::SpatialIndexManager;
use crate::config::{Config, DbItem, DbStats, SetOptions};
use crate::error::{Result, SpatioError};
#[cfg(feature = "aof")]
use crate::storage::{AOFCommand, PersistenceLog};
use bytes::Bytes;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime};

/// Global counter for spatial key generation (fast, non-cryptographic uniqueness).
///
/// This counter wraps around at u64::MAX (18,446,744,073,709,551,615). Even at
/// 1 million inserts/second, this would take ~584,942 years to wrap around.
///
/// The key format combines: prefix:lat:lon:z:timestamp_nanos:counter
/// Even if the counter wraps, the nanosecond timestamp ensures uniqueness across
/// different time periods, making collisions effectively impossible in practice.
static SPATIAL_KEY_COUNTER: AtomicU64 = AtomicU64::new(0);

impl DBInner {
    const MAX_FUTURE_TIMESTAMP: Duration = Duration::from_secs(86400);

    pub(crate) fn validate_timestamp(created_at: SystemTime) -> Result<()> {
        let now = SystemTime::now();
        if created_at > now + Self::MAX_FUTURE_TIMESTAMP {
            return Err(SpatioError::InvalidTimestamp);
        }
        Ok(())
    }

    pub(crate) fn generate_spatial_key(
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

        // fetch_add wraps around at u64::MAX (after ~584,942 years at 1M ops/sec).
        // The nanosecond timestamp ensures uniqueness even after theoretical wrap-around.
        let counter = SPATIAL_KEY_COUNTER.fetch_add(1, Ordering::Relaxed);

        Ok(format!(
            "{}:{:x}:{:x}:{:x}:{:x}:{:x}",
            prefix,
            y.to_bits(),
            x.to_bits(),
            z.to_bits(),
            timestamp_nanos,
            counter
        ))
    }

    pub(crate) fn new_with_config(config: &Config) -> Self {
        Self {
            keys: BTreeMap::new(),
            spatial_index: SpatialIndexManager::new(),
            #[cfg(feature = "aof")]
            aof_file: None,
            #[cfg(feature = "snapshot")]
            snapshot_file: None,
            closed: false,
            stats: DbStats::default(),
            config: config.clone(),
            #[cfg(feature = "aof")]
            sync_ops_since_flush: 0,
            #[cfg(feature = "time-index")]
            created_index: BTreeMap::new(),
            #[cfg(feature = "time-index")]
            history: config.history_capacity.map(HistoryTracker::new),
        }
    }

    #[cfg(feature = "snapshot")]
    pub fn load_from_snapshot(
        &mut self,
        snapshot_file: &crate::storage::SnapshotFile,
    ) -> Result<()> {
        let keys = snapshot_file.load()?;
        self.keys = keys;
        self.stats.key_count = self.keys.len();
        Ok(())
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

    pub fn insert_item(&mut self, key: Bytes, item: DbItem) -> Option<DbItem> {
        #[cfg(feature = "time-index")]
        let created_at = item.created_at;
        #[cfg(feature = "time-index")]
        let expires_at = item.expires_at;
        #[cfg(feature = "time-index")]
        let history_value = self.history.as_ref().map(|_| item.value.clone());

        let old_item = self.keys.insert(key.clone(), item);
        if let Some(ref old) = old_item {
            #[cfg(feature = "time-index")]
            self.remove_created_index(&key, old);
        }

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

    pub fn remove_item(&mut self, key: &Bytes) -> Option<DbItem> {
        if let Some(item) = self.keys.remove(key) {
            #[cfg(feature = "time-index")]
            let history_value = self.history.as_ref().map(|_| item.value.clone());
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

    pub fn get_item(&self, key: &Bytes) -> Option<&DbItem> {
        self.keys.get(key)
    }

    #[cfg(feature = "aof")]
    pub fn load_from_aof(&mut self, aof_file: &mut dyn PersistenceLog) -> Result<()> {
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

    #[cfg(feature = "aof")]
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
            #[cfg(feature = "time-index")]
            self.remove_created_index(&key, &old);
        }

        #[cfg(feature = "time-index")]
        self.add_created_index(&key, created_at);

        #[cfg(feature = "time-index")]
        if let Some(history) = self.history.as_mut() {
            history.record_set(&key, value.clone(), created_at, expires_at);
        }

        self.rebuild_spatial_index(&key, &value);
        Ok(())
    }

    #[cfg(feature = "aof")]
    fn apply_delete_from_aof(&mut self, key: Bytes) -> Result<()> {
        if let Some(item) = self.keys.remove(&key) {
            #[cfg(feature = "time-index")]
            let deleted_value = item.value.clone();
            #[cfg(feature = "time-index")]
            self.remove_created_index(&key, &item);

            #[cfg(feature = "time-index")]
            if let Some(history) = self.history.as_mut() {
                history.record_delete(&key, SystemTime::now(), Some(deleted_value));
            }
        }

        Ok(())
    }

    #[cfg(feature = "snapshot")]
    pub(super) fn maybe_auto_snapshot(&mut self) -> Result<()> {
        if let Some(snapshot_file) = self.snapshot_file.as_mut() {
            snapshot_file.record_operation();
            if snapshot_file.should_snapshot() {
                snapshot_file.save(&self.keys)?;
            }
        }
        Ok(())
    }

    #[cfg(feature = "snapshot")]
    pub(super) fn save_snapshot(&mut self) -> Result<()> {
        if let Some(snapshot_file) = self.snapshot_file.as_mut() {
            snapshot_file.save(&self.keys)?;
        }
        Ok(())
    }

    #[cfg(feature = "aof")]
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

    #[cfg(feature = "aof")]
    pub fn write_to_aof_if_needed(
        &mut self,
        key: &Bytes,
        value: &[u8],
        options: Option<&SetOptions>,
        created_at: SystemTime,
    ) -> Result<()> {
        let Some(aof_file) = self.aof_file.as_ref() else {
            return Ok(());
        };

        let sync_policy = self.config.sync_policy;
        let sync_mode = self.config.sync_mode;
        let batch_size = self.config.sync_batch_size;
        let value_bytes = Bytes::copy_from_slice(value);

        {
            let mut log = aof_file.lock();
            log.write_set(key, &value_bytes, options, created_at)?;
        }
        self.maybe_flush_or_sync(sync_policy, sync_mode, batch_size)?;
        Ok(())
    }

    #[cfg(not(feature = "aof"))]
    pub fn write_to_aof_if_needed(
        &mut self,
        _key: &Bytes,
        _value: &[u8],
        _options: Option<&SetOptions>,
        _created_at: SystemTime,
    ) -> Result<()> {
        Ok(())
    }

    #[cfg(feature = "aof")]
    pub fn write_delete_to_aof_if_needed(&mut self, key: &Bytes) -> Result<()> {
        let Some(aof_file) = self.aof_file.as_ref() else {
            return Ok(());
        };

        let sync_policy = self.config.sync_policy;
        let sync_mode = self.config.sync_mode;
        let batch_size = self.config.sync_batch_size;

        {
            let mut log = aof_file.lock();
            log.write_delete(key)?;
        }
        self.maybe_flush_or_sync(sync_policy, sync_mode, batch_size)?;
        Ok(())
    }

    #[cfg(not(feature = "aof"))]
    pub fn write_delete_to_aof_if_needed(&mut self, _key: &Bytes) -> Result<()> {
        Ok(())
    }

    #[cfg(feature = "aof")]
    pub fn write_batch_to_aof(
        &mut self,
        operations: &[(Bytes, Bytes, Option<SetOptions>, SystemTime, bool)], // (key, value, opts, created_at, is_delete)
    ) -> Result<()> {
        let Some(aof_file) = self.aof_file.as_ref() else {
            return Ok(());
        };

        let sync_policy = self.config.sync_policy;
        let sync_mode = self.config.sync_mode;
        let batch_size = self.config.sync_batch_size;

        {
            let mut log = aof_file.lock();
            for (key, value, opts, created_at, is_delete) in operations {
                if *is_delete {
                    log.write_delete(key)?;
                } else {
                    log.write_set(key, value, opts.as_ref(), *created_at)?;
                }
            }
        }

        self.sync_ops_since_flush += operations.len();
        self.maybe_flush_or_sync(sync_policy, sync_mode, batch_size)?;

        Ok(())
    }

    #[cfg(not(feature = "aof"))]
    pub fn write_batch_to_aof(
        &mut self,
        _operations: &[(Bytes, Bytes, Option<SetOptions>, SystemTime, bool)],
    ) -> Result<()> {
        Ok(())
    }

    #[cfg(feature = "aof")]
    fn maybe_flush_or_sync(
        &mut self,
        policy: crate::config::SyncPolicy,
        mode: crate::config::SyncMode,
        batch_size: usize,
    ) -> Result<()> {
        use crate::config::SyncPolicy;

        let Some(aof_file) = self.aof_file.as_ref() else {
            return Ok(());
        };

        match policy {
            SyncPolicy::Always => {
                self.sync_ops_since_flush += 1;
                if self.sync_ops_since_flush >= batch_size {
                    let mut log = aof_file.lock();
                    log.sync_with_mode(mode)?;
                    self.sync_ops_since_flush = 0;
                } else {
                    let mut log = aof_file.lock();
                    log.flush()?;
                }
            }
            SyncPolicy::EverySecond => {
                let mut log = aof_file.lock();
                log.flush()?;
            }
            SyncPolicy::Never => {}
        }

        Ok(())
    }
}
