//! Thread-safe wrapper for concurrent database access.
//!
//! This module provides `SyncDB`, a thread-safe wrapper around `DB`.
//! Since `DB` is now inherently thread-safe (using DashMap and internal locking),
//! `SyncDB` is just a lightweight wrapper for API compatibility.

use crate::config::{Config, DbStats};
use crate::db::{CurrentLocation, DB, LocationUpdate};
use crate::error::Result;
use std::path::Path;
use std::time::SystemTime;

/// Thread-safe wrapper around `DB`.
#[derive(Clone)]
pub struct SyncDB {
    inner: DB,
}

impl SyncDB {
    /// Open a database with default configuration.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        Ok(Self {
            inner: DB::open(path)?,
        })
    }

    /// Open a database with custom configuration.
    pub fn open_with_config<P: AsRef<Path>>(path: P, config: Config) -> Result<Self> {
        Ok(Self {
            inner: DB::open_with_config(path, config)?,
        })
    }

    /// Create an in-memory database.
    pub fn memory() -> Result<Self> {
        Ok(Self {
            inner: DB::memory()?,
        })
    }

    /// Create an in-memory database with custom configuration.
    pub fn memory_with_config(config: Config) -> Result<Self> {
        Ok(Self {
            inner: DB::memory_with_config(config)?,
        })
    }

    /// Get database statistics.
    pub fn stats(&self) -> DbStats {
        self.inner.stats()
    }

    /// Update object's current location
    pub fn update_location(
        &self,
        namespace: &str,
        object_id: &str,
        position: spatio_types::point::Point3d,
        metadata: serde_json::Value,
    ) -> Result<()> {
        self.inner
            .update_location(namespace, object_id, position, metadata)
    }

    /// Query current locations within radius
    pub fn query_current_within_radius(
        &self,
        namespace: &str,
        center: &spatio_types::point::Point3d,
        radius: f64,
        limit: usize,
    ) -> Result<Vec<CurrentLocation>> {
        self.inner
            .query_current_within_radius(namespace, center, radius, limit)
    }

    /// Query objects near another object
    pub fn query_near_object(
        &self,
        namespace: &str,
        object_id: &str,
        radius: f64,
        limit: usize,
    ) -> Result<Vec<CurrentLocation>> {
        self.inner
            .query_near_object(namespace, object_id, radius, limit)
    }

    /// Query historical trajectory
    pub fn query_trajectory(
        &self,
        namespace: &str,
        object_id: &str,
        start_time: SystemTime,
        end_time: SystemTime,
        limit: usize,
    ) -> Result<Vec<LocationUpdate>> {
        self.inner
            .query_trajectory(namespace, object_id, start_time, end_time, limit)
    }

    /// Close the database.
    pub fn close(&self) -> Result<()> {
        self.inner.close()
    }
}
