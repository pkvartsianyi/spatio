//! Storage utilities for Spatio
//!
//! This module provides shared types for historical and current state storage.

use crate::config::Point3d;
use std::time::SystemTime;

/// Single location update in history
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct LocationUpdate {
    pub timestamp: SystemTime,
    pub position: Point3d,
    pub metadata: serde_json::Value,
}

/// Storage backend statistics
#[derive(Debug, Clone, Default)]
pub struct StorageStats {
    /// Total number of keys
    pub key_count: usize,
    /// Number of expired keys cleaned up
    pub expired_count: usize,
    /// Storage size in bytes (approximate)
    pub size_bytes: usize,
    /// Number of operations performed
    pub operations_count: u64,
}
