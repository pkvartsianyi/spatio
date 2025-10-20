//! Storage backend abstraction for Spatio
//!
//! This module provides a trait-based abstraction for storage backends,
//! allowing different storage implementations while maintaining a consistent API.

use crate::error::Result;
use crate::types::{DbItem, SetOptions};
use bytes::Bytes;
use std::collections::BTreeMap;
use std::time::SystemTime;

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

    /// Get all key-value pairs with a given prefix
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

/// In-memory storage backend using BTreeMap
pub struct MemoryBackend {
    data: BTreeMap<Bytes, DbItem>,
    stats: StorageStats,
}

impl MemoryBackend {
    /// Create a new in-memory storage backend
    pub fn new() -> Self {
        Self {
            data: BTreeMap::new(),
            stats: StorageStats::default(),
        }
    }

    /// Create with initial capacity hint
    pub fn with_capacity(capacity: usize) -> Self {
        // BTreeMap doesn't have with_capacity, but we can still track the hint
        let mut backend = Self::new();
        backend.stats.size_bytes = capacity * 64; // Rough estimate
        backend
    }
}

impl Default for MemoryBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl StorageBackend for MemoryBackend {
    fn put(&mut self, key: &[u8], item: &DbItem) -> Result<()> {
        let key_bytes = Bytes::copy_from_slice(key);
        let old_item = self.data.insert(key_bytes, item.clone());

        if old_item.is_none() {
            self.stats.key_count += 1;
        }
        self.stats.operations_count += 1;

        Ok(())
    }

    fn get(&self, key: &[u8]) -> Result<Option<DbItem>> {
        let key_bytes = Bytes::copy_from_slice(key);
        Ok(self.data.get(&key_bytes).cloned())
    }

    fn delete(&mut self, key: &[u8]) -> Result<Option<DbItem>> {
        let key_bytes = Bytes::copy_from_slice(key);
        let old_item = self.data.remove(&key_bytes);

        if old_item.is_some() {
            self.stats.key_count = self.stats.key_count.saturating_sub(1);
        }
        self.stats.operations_count += 1;

        Ok(old_item)
    }

    fn contains_key(&self, key: &[u8]) -> Result<bool> {
        let key_bytes = Bytes::copy_from_slice(key);
        Ok(self.data.contains_key(&key_bytes))
    }

    fn keys_with_prefix(&self, prefix: &[u8]) -> Result<Vec<Bytes>> {
        let mut keys = Vec::new();
        for key in self.data.keys() {
            if key.starts_with(prefix) {
                keys.push(key.clone());
            }
        }
        Ok(keys)
    }

    fn scan_prefix(&self, prefix: &[u8]) -> Result<BTreeMap<Bytes, DbItem>> {
        let mut result = BTreeMap::new();
        for (key, item) in &self.data {
            if key.starts_with(prefix) {
                result.insert(key.clone(), item.clone());
            }
        }
        Ok(result)
    }

    fn len(&self) -> Result<usize> {
        Ok(self.data.len())
    }

    fn is_empty(&self) -> Result<bool> {
        Ok(self.data.is_empty())
    }

    fn sync(&mut self) -> Result<()> {
        // No-op for in-memory storage
        Ok(())
    }

    fn close(&mut self) -> Result<()> {
        self.data.clear();
        self.stats = StorageStats::default();
        Ok(())
    }

    fn stats(&self) -> Result<StorageStats> {
        let mut stats = self.stats.clone();
        stats.key_count = self.data.len();
        stats.size_bytes = self.data.iter().map(|(k, v)| k.len() + v.value.len()).sum();
        Ok(stats)
    }

    fn batch(&mut self, ops: &[StorageOp]) -> Result<()> {
        for op in ops {
            match op {
                StorageOp::Put { key, item } => {
                    self.put(key, item)?;
                }
                StorageOp::Delete { key } => {
                    self.delete(key)?;
                }
            }
        }
        Ok(())
    }

    fn iter(&self) -> Result<Box<dyn Iterator<Item = (Bytes, DbItem)> + '_>> {
        Ok(Box::new(
            self.data.iter().map(|(k, v)| (k.clone(), v.clone())),
        ))
    }

    fn cleanup_expired(&mut self, now: SystemTime) -> Result<usize> {
        let mut expired_keys = Vec::new();

        for (key, item) in &self.data {
            if let Some(expires_at) = item.expires_at {
                if expires_at <= now {
                    expired_keys.push(key.clone());
                }
            }
        }

        let count = expired_keys.len();
        for key in expired_keys {
            self.data.remove(&key);
        }

        self.stats.key_count = self.data.len();
        self.stats.expired_count += count;

        Ok(count)
    }
}

/// Persistent storage backend using AOF (Append-Only File)
#[cfg(feature = "aof")]
pub struct AOFBackend {
    memory: MemoryBackend,
    aof_writer: crate::persistence::AOFFile,
}

#[cfg(feature = "aof")]
impl AOFBackend {
    /// Create a new AOF storage backend
    pub fn new<P: AsRef<std::path::Path>>(path: P) -> Result<Self> {
        let aof_writer = crate::persistence::AOFFile::open(path)?;
        let memory = MemoryBackend::new();

        Ok(Self { memory, aof_writer })
    }

    /// Load existing data from AOF file
    pub fn load_from_aof(&mut self) -> Result<()> {
        // Implementation would replay AOF file to restore state
        // This is a placeholder for the actual implementation
        Ok(())
    }
}

#[cfg(feature = "aof")]
impl StorageBackend for AOFBackend {
    fn put(&mut self, key: &[u8], item: &DbItem) -> Result<()> {
        // Write to AOF first for durability
        let opts = item.expires_at.map(SetOptions::with_expiration);
        self.aof_writer
            .write_set(&Bytes::copy_from_slice(key), &item.value, opts.as_ref())?;

        // Then update in-memory state
        self.memory.put(key, item)
    }

    fn get(&self, key: &[u8]) -> Result<Option<DbItem>> {
        self.memory.get(key)
    }

    fn delete(&mut self, key: &[u8]) -> Result<Option<DbItem>> {
        // Write deletion to AOF
        self.aof_writer.write_delete(&Bytes::copy_from_slice(key))?;

        // Update in-memory state
        self.memory.delete(key)
    }

    fn contains_key(&self, key: &[u8]) -> Result<bool> {
        self.memory.contains_key(key)
    }

    fn keys_with_prefix(&self, prefix: &[u8]) -> Result<Vec<Bytes>> {
        self.memory.keys_with_prefix(prefix)
    }

    fn scan_prefix(&self, prefix: &[u8]) -> Result<BTreeMap<Bytes, DbItem>> {
        self.memory.scan_prefix(prefix)
    }

    fn len(&self) -> Result<usize> {
        self.memory.len()
    }

    fn is_empty(&self) -> Result<bool> {
        self.memory.is_empty()
    }

    fn sync(&mut self) -> Result<()> {
        self.aof_writer.sync()
    }

    fn close(&mut self) -> Result<()> {
        self.aof_writer.sync()?;
        self.memory.close()
    }

    fn stats(&self) -> Result<StorageStats> {
        self.memory.stats()
    }

    fn batch(&mut self, ops: &[StorageOp]) -> Result<()> {
        // Write all operations to AOF first
        for op in ops {
            match op {
                StorageOp::Put { key, item } => {
                    let opts = item.expires_at.map(SetOptions::with_expiration);
                    self.aof_writer.write_set(key, &item.value, opts.as_ref())?;
                }
                StorageOp::Delete { key } => {
                    self.aof_writer.write_delete(key)?;
                }
            }
        }

        // Then apply to memory
        self.memory.batch(ops)
    }

    fn iter(&self) -> Result<Box<dyn Iterator<Item = (Bytes, DbItem)> + '_>> {
        self.memory.iter()
    }

    fn cleanup_expired(&mut self, now: SystemTime) -> Result<usize> {
        // For AOF backend, we might want to write deletions to AOF
        let expired_keys = {
            let mut keys = Vec::new();
            for (key, item) in self.memory.iter()? {
                if let Some(expires_at) = item.expires_at {
                    if expires_at <= now {
                        keys.push(key);
                    }
                }
            }
            keys
        };

        // Write deletions to AOF
        for key in &expired_keys {
            self.aof_writer.write_delete(key)?;
        }

        self.memory.cleanup_expired(now)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::DbItem;
    use std::time::{Duration, SystemTime};

    #[test]
    fn test_memory_backend_basic_ops() {
        let mut backend = MemoryBackend::new();

        let key = b"test_key";
        let item = DbItem {
            value: b"test_value".to_vec().into(),
            expires_at: None,
        };

        // Test put and get
        backend.put(key, &item).unwrap();
        let retrieved = backend.get(key).unwrap().unwrap();
        assert_eq!(retrieved.value, item.value);

        // Test contains
        assert!(backend.contains_key(key).unwrap());
        assert!(!backend.contains_key(b"nonexistent").unwrap());

        // Test delete
        let deleted = backend.delete(key).unwrap().unwrap();
        assert_eq!(deleted.value, item.value);
        assert!(!backend.contains_key(key).unwrap());
    }

    #[test]
    fn test_memory_backend_prefix_scan() {
        let mut backend = MemoryBackend::new();

        let item = DbItem {
            value: b"value".to_vec().into(),
            expires_at: None,
        };

        backend.put(b"prefix:key1", &item).unwrap();
        backend.put(b"prefix:key2", &item).unwrap();
        backend.put(b"other:key", &item).unwrap();

        let keys = backend.keys_with_prefix(b"prefix:").unwrap();
        assert_eq!(keys.len(), 2);

        let scan_result = backend.scan_prefix(b"prefix:").unwrap();
        assert_eq!(scan_result.len(), 2);
    }

    #[test]
    fn test_memory_backend_ttl_cleanup() {
        let mut backend = MemoryBackend::new();

        let now = SystemTime::now();
        let past = now - Duration::from_secs(60);
        let future = now + Duration::from_secs(60);

        let expired_item = DbItem {
            value: b"expired".to_vec().into(),
            expires_at: Some(past),
        };

        let valid_item = DbItem {
            value: b"valid".to_vec().into(),
            expires_at: Some(future),
        };

        backend.put(b"expired_key", &expired_item).unwrap();
        backend.put(b"valid_key", &valid_item).unwrap();

        let cleaned = backend.cleanup_expired(now).unwrap();
        assert_eq!(cleaned, 1);
        assert!(!backend.contains_key(b"expired_key").unwrap());
        assert!(backend.contains_key(b"valid_key").unwrap());
    }

    #[test]
    fn test_storage_batch_operations() {
        let mut backend = MemoryBackend::new();

        let ops = vec![
            StorageOp::Put {
                key: b"key1".to_vec().into(),
                item: DbItem {
                    value: b"value1".to_vec().into(),
                    expires_at: None,
                },
            },
            StorageOp::Put {
                key: b"key2".to_vec().into(),
                item: DbItem {
                    value: b"value2".to_vec().into(),
                    expires_at: None,
                },
            },
            StorageOp::Delete {
                key: b"key1".to_vec().into(),
            },
        ];

        backend.batch(&ops).unwrap();

        assert!(!backend.contains_key(b"key1").unwrap());
        assert!(backend.contains_key(b"key2").unwrap());
    }
}
