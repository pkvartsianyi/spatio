use serde::{Deserialize, Serialize};

/// Database statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DbStats {
    /// Total number of operations performed
    pub operations_count: u64,
    /// Total number of objects currently tracked in hot state
    pub hot_state_objects: usize,
    /// Number of trajectories stored in cold state
    pub cold_state_trajectories: usize,
    /// Bytes used in cold state buffer
    pub cold_state_buffer_bytes: usize,
    /// Approximate total memory usage in bytes
    pub memory_usage_bytes: usize,
}
