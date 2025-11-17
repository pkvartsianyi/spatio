//! Storage backend abstraction for Spatio
//!
//! This module provides a trait-based abstraction for storage backends,
//! allowing different storage implementations while maintaining a consistent API.

use crate::config::DbItem;
use crate::error::Result;
use bytes::Bytes;
use std::collections::BTreeMap;
use std::time::SystemTime;

pub mod backends;
/// Persistence interface to allow pluggable backends
pub mod persistence;
#[cfg(feature = "snapshot")]
mod snapshot;

pub use backends::MemoryBackend;

pub use persistence::{AOFCommand, PersistenceLog};
#[cfg(feature = "snapshot")]
pub use snapshot::{SnapshotConfig, SnapshotFile};

/// Trait for storage backend implementations
///
/// This trait abstracts the storage layer, allowing for different backends
/// such as in-memory, persistent file-based storage, or external databases.
pub trait StorageBackend: Send + Sync {
    /// Insert or update a key-value pair
    fn put(&mut self, key: &[u8], item: &DbItem) -> Result<()>;

    /// Get a value by key
    fn get(&self, key: &[u8]) -> Result<Option<DbItem>>;

    /// Delete a key and return the old value if it existed
    fn delete(&mut self, key: &[u8]) -> Result<Option<DbItem>>;

    /// Check if a key exists
    fn contains_key(&self, key: &[u8]) -> Result<bool>;

    /// Get all keys with a given prefix
    fn keys_with_prefix(&self, prefix: &[u8]) -> Result<Vec<Bytes>>;

    /// Returns all key-value pairs with keys matching the given prefix.
    fn scan_prefix(&self, prefix: &[u8]) -> Result<BTreeMap<Bytes, DbItem>>;

    /// Get the total number of keys
    fn len(&self) -> Result<usize>;

    /// Check if the storage is empty
    fn is_empty(&self) -> Result<bool>;

    /// Flush any pending writes to persistent storage
    fn sync(&mut self) -> Result<()>;

    /// Close the storage backend
    fn close(&mut self) -> Result<()>;

    /// Get storage statistics
    fn stats(&self) -> Result<StorageStats>;

    /// Batch operation support
    fn batch(&mut self, ops: &[StorageOp]) -> Result<()>;

    /// Iterator over all key-value pairs
    fn iter(&self) -> Result<Box<dyn Iterator<Item = (Bytes, DbItem)> + '_>>;

    /// Cleanup expired items (for TTL support)
    fn cleanup_expired(&mut self, now: SystemTime) -> Result<usize>;
}

/// Storage operation for batch processing
#[derive(Debug, Clone)]
pub enum StorageOp {
    /// Put a key-value pair
    Put { key: Bytes, item: DbItem },
    /// Delete a key
    Delete { key: Bytes },
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

/// Computes the upper bound for a prefix scan.
pub(crate) fn calculate_prefix_end(prefix: &[u8]) -> Vec<u8> {
    let mut prefix_end = prefix.to_vec();

    while let Some(last_byte) = prefix_end.pop() {
        if last_byte < 255 {
            prefix_end.push(last_byte + 1);
            break;
        }
    }
    prefix_end
}
