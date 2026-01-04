use serde::{Deserialize, Serialize};

/// Database statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DbStats {
    /// Number of items that have expired
    pub expired_count: u64,
    /// Total number of operations performed
    pub operations_count: u64,
    /// Total size in bytes (approximate)
    pub size_bytes: usize,
    /// Total number of objects currently tracked in hot state
    pub hot_state_objects: usize,
    /// Number of trajectories stored in cold state
    pub cold_state_trajectories: usize,
    /// Bytes used in cold state buffer
    pub cold_state_buffer_bytes: usize,
    /// Approximate total memory usage in bytes
    pub memory_usage_bytes: usize,
}

impl DbStats {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_operation(&mut self) {
        self.operations_count += 1;
    }

    pub fn record_expired(&mut self, count: u64) {
        self.expired_count += count;
    }

    pub fn set_size_bytes(&mut self, bytes: usize) {
        self.size_bytes = bytes;
    }
}
