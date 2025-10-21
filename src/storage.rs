//! Storage backend abstraction for Spatio
//!
//! This module provides a trait-based abstraction for storage backends,
//! allowing different storage implementations while maintaining a consistent API.

use crate::error::Result;
use crate::types::DbItem;
#[cfg(feature = "aof")]
use crate::types::SetOptions;
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
    ///
    /// This operation should be implemented efficiently using range scans
    /// rather than linear iteration for optimal performance.
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
fn calculate_prefix_end(prefix: &[u8]) -> Vec<u8> {
    let mut prefix_end = prefix.to_vec();

    // Find the last non-0xFF byte and increment it.
    // This creates the smallest key that is lexicographically greater than
    // any key that could start with the given prefix.
    while let Some(last_byte) = prefix_end.pop() {
        if last_byte < 255 {
            prefix_end.push(last_byte + 1);
            break;
        }
    }
    prefix_end
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

        if prefix.is_empty() {
            // If prefix is empty, return all keys
            for key in self.data.keys() {
                keys.push(key.clone());
            }
            return Ok(keys);
        }

        // Compute the upper bound for the range scan
        let prefix_end = calculate_prefix_end(prefix);

        // Use BTreeMap's range() for efficient iteration over only matching keys
        let range = if prefix_end.len() < prefix.len() {
            // All trailing bytes were 0xFF, scan from prefix to end
            self.data.range(Bytes::from(prefix.to_vec())..)
        } else {
            // Normal case: scan from prefix to computed upper bound
            self.data
                .range(Bytes::from(prefix.to_vec())..Bytes::from(prefix_end))
        };

        for (key, _) in range {
            // Defensive check: should always be true with correct range bounds
            if key.starts_with(prefix) {
                keys.push(key.clone());
            }
        }

        Ok(keys)
    }

    /// Efficiently scan for all key-value pairs with a given prefix using BTreeMap range operations.
    ///
    /// This implementation uses O(log n + k) complexity where n is the total number of keys
    /// and k is the number of matching keys, rather than O(n) linear scan.
    ///
    /// The algorithm works by:
    /// 1. Computing an upper bound key (prefix with last byte incremented)
    /// 2. Using BTreeMap's range() method to iterate only over the relevant key range
    /// 3. Handling edge cases like prefixes ending in 0xFF bytes
    ///
    /// Examples:
    /// - prefix "abc" -> range ["abc", "abd")
    /// - prefix "test\xFF" -> range ["test\xFF", "tesu")
    /// - prefix "abc\xFF\xFF" -> range ["abc\xFF\xFF", "abd")
    fn scan_prefix(&self, prefix: &[u8]) -> Result<BTreeMap<Bytes, DbItem>> {
        let mut result = BTreeMap::new();

        if prefix.is_empty() {
            // If prefix is empty, return all keys
            for (key, item) in &self.data {
                result.insert(key.clone(), item.clone());
            }
            return Ok(result);
        }

        // Compute the upper bound for the range scan
        let prefix_end = calculate_prefix_end(prefix);

        // Use BTreeMap's range() for efficient iteration over only matching keys
        let range = if prefix_end.len() < prefix.len() {
            // All trailing bytes were 0xFF, scan from prefix to end of map
            // Example: prefix "\xFF\xFF\xFF" scans to end
            self.data.range(Bytes::from(prefix.to_vec())..)
        } else {
            // Normal case: scan from prefix to computed upper bound (exclusive)
            // Example: prefix "abc" scans range ["abc", "abd")
            self.data
                .range(Bytes::from(prefix.to_vec())..Bytes::from(prefix_end))
        };

        for (key, item) in range {
            // Defensive check: should always be true with correct range bounds
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
            if let Some(expires_at) = item.expires_at
                && expires_at <= now
            {
                expired_keys.push(key.clone());
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
                if let Some(expires_at) = item.expires_at
                    && expires_at <= now
                {
                    keys.push(key);
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
    fn test_prefix_scan_edge_cases() {
        let mut backend = MemoryBackend::new();
        let item = DbItem {
            value: b"value".to_vec().into(),
            expires_at: None,
        };

        // Test empty prefix
        backend.put(b"a", &item).unwrap();
        backend.put(b"b", &item).unwrap();
        let all_keys = backend.scan_prefix(b"").unwrap();
        assert_eq!(all_keys.len(), 2);

        // Test prefix with 0xFF bytes
        backend.put(b"test\xff\xffa", &item).unwrap();
        backend.put(b"test\xff\xffb", &item).unwrap();
        backend.put(b"test\xff\xff\xff", &item).unwrap();
        backend.put(b"testb", &item).unwrap();

        let xff_prefix_scan = backend.scan_prefix(b"test\xff\xff").unwrap();
        assert_eq!(xff_prefix_scan.len(), 3); // Should match all test\xff\xff* keys

        // Test exact prefix boundaries
        backend.put(b"abc", &item).unwrap();
        backend.put(b"abcd", &item).unwrap();
        backend.put(b"abd", &item).unwrap();

        let abc_scan = backend.scan_prefix(b"abc").unwrap();
        assert_eq!(abc_scan.len(), 2); // abc and abcd, not abd

        // Test prefix that matches no keys
        let no_match_scan = backend.scan_prefix(b"nonexistent").unwrap();
        assert_eq!(no_match_scan.len(), 0);

        // Test single character prefix
        let a_prefix_scan = backend.scan_prefix(b"a").unwrap();
        assert_eq!(a_prefix_scan.len(), 4); // abc, abcd, abd, and original "a"
    }

    #[test]
    fn test_prefix_scan_ordering() {
        let mut backend = MemoryBackend::new();
        let item = DbItem {
            value: b"value".to_vec().into(),
            expires_at: None,
        };

        // Insert keys in non-sorted order
        backend.put(b"prefix:z", &item).unwrap();
        backend.put(b"prefix:a", &item).unwrap();
        backend.put(b"prefix:m", &item).unwrap();
        backend.put(b"different:key", &item).unwrap();

        let scan_result = backend.scan_prefix(b"prefix:").unwrap();
        assert_eq!(scan_result.len(), 3);

        // Verify keys are returned in sorted order (BTreeMap property)
        let keys: Vec<_> = scan_result.keys().collect();
        assert_eq!(keys[0].as_ref(), b"prefix:a");
        assert_eq!(keys[1].as_ref(), b"prefix:m");
        assert_eq!(keys[2].as_ref(), b"prefix:z");
    }

    #[test]
    fn test_prefix_scan_performance_demo() {
        let mut backend = MemoryBackend::new();
        let item = DbItem {
            value: b"value".to_vec().into(),
            expires_at: None,
        };

        // Insert a large number of keys with different prefixes
        // This demonstrates the efficiency of range-based prefix scanning
        for i in 0..1000 {
            // Keys with "target:" prefix (what we'll search for)
            if i < 10 {
                let key = format!("target:key_{:03}", i);
                backend.put(key.as_bytes(), &item).unwrap();
            }

            // Many keys with other prefixes (noise)
            let noise_key = format!("noise_{:03}:data", i);
            backend.put(noise_key.as_bytes(), &item).unwrap();

            let other_key = format!("zzz_other_{:03}", i);
            backend.put(other_key.as_bytes(), &item).unwrap();
        }

        // The optimized prefix scan should efficiently find only the 10 target keys
        // without scanning through all 2010 keys in the database
        let target_scan = backend.scan_prefix(b"target:").unwrap();
        assert_eq!(target_scan.len(), 10);

        let target_keys = backend.keys_with_prefix(b"target:").unwrap();
        assert_eq!(target_keys.len(), 10);

        // Verify we can find keys efficiently even with a large dataset
        assert_eq!(backend.data.len(), 2010); // Total keys in database
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
