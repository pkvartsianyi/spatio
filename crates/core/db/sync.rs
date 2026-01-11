//! Thread-safe wrapper for concurrent database access.
//!
//! This module provides `SyncDB`, a thread-safe wrapper around `DB`.
//! Since `DB` is now inherently thread-safe (using DashMap and internal locking),
//! `SyncDB` is just a lightweight wrapper for API compatibility.

use crate::config::{Config, DbStats, SetOptions};
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

    /// Upsert an object's location.
    pub fn upsert(
        &self,
        namespace: &str,
        object_id: &str,
        position: spatio_types::point::Point3d,
        metadata: serde_json::Value,
        opts: Option<SetOptions>,
    ) -> Result<()> {
        self.inner
            .upsert(namespace, object_id, position, metadata, opts)
    }

    /// Get current location of an object.
    pub fn get(
        &self,
        namespace: &str,
        object_id: &str,
    ) -> Result<Option<std::sync::Arc<CurrentLocation>>> {
        self.inner.get(namespace, object_id)
    }

    /// Delete an object from the database.
    pub fn delete(&self, namespace: &str, object_id: &str) -> Result<()> {
        self.inner.delete(namespace, object_id)
    }

    /// Query objects within radius (returns location and distance)
    pub fn query_radius(
        &self,
        namespace: &str,
        center: &spatio_types::point::Point3d,
        radius: f64,
        limit: usize,
    ) -> Result<Vec<(std::sync::Arc<CurrentLocation>, f64)>> {
        self.inner.query_radius(namespace, center, radius, limit)
    }

    /// Query objects near another object (returns location and distance)
    pub fn query_near(
        &self,
        namespace: &str,
        object_id: &str,
        radius: f64,
        limit: usize,
    ) -> Result<Vec<(std::sync::Arc<CurrentLocation>, f64)>> {
        self.inner.query_near(namespace, object_id, radius, limit)
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
