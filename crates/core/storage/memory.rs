//! In-memory storage backend implementation.

use super::{StorageBackend, StorageOp, StorageStats};
use crate::config::DbItem;
use crate::error::Result;
use bytes::Bytes;
use rustc_hash::FxHashMap;
use std::collections::BTreeMap;
use std::time::SystemTime;

/// In-memory storage backend using HashMap
pub struct MemoryBackend {
    data: FxHashMap<Vec<u8>, DbItem>,
    stats: StorageStats,
}

impl MemoryBackend {
    /// Create a new in-memory storage backend
    pub fn new() -> Self {
        Self {
            data: FxHashMap::default(),
            stats: StorageStats::default(),
        }
    }

    /// Create with initial capacity hint
    pub fn with_capacity(capacity: usize) -> Self {
        let mut backend = Self::new();
        backend.data.reserve(capacity);
        backend.stats.size_bytes = capacity * 64; // Rough estimate
        backend
    }

    pub fn iter_ref(&self) -> impl Iterator<Item = (&[u8], &DbItem)> {
        self.data.iter().map(|(k, v)| (k.as_slice(), v))
    }
}

impl Default for MemoryBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl StorageBackend for MemoryBackend {
    fn put(&mut self, key: &[u8], item: &DbItem) -> Result<()> {
        let old_item = self.data.insert(key.to_vec(), item.clone());

        if old_item.is_none() {
            self.stats.key_count += 1;
        }
        self.stats.operations_count += 1;

        Ok(())
    }

    fn get(&self, key: &[u8]) -> Result<Option<DbItem>> {
        Ok(self.data.get(key).cloned())
    }

    fn delete(&mut self, key: &[u8]) -> Result<Option<DbItem>> {
        let old_item = self.data.remove(key);

        if old_item.is_some() {
            self.stats.key_count = self.stats.key_count.saturating_sub(1);
        }
        self.stats.operations_count += 1;

        Ok(old_item)
    }

    fn contains_key(&self, key: &[u8]) -> Result<bool> {
        Ok(self.data.contains_key(key))
    }

    fn keys_with_prefix(&self, prefix: &[u8]) -> Result<Vec<Bytes>> {
        let mut keys = Vec::new();

        if prefix.is_empty() {
            for key in self.data.keys() {
                keys.push(Bytes::copy_from_slice(key));
            }
        } else {
            for key in self.data.keys() {
                if key.starts_with(prefix) {
                    keys.push(Bytes::copy_from_slice(key));
                }
            }
        }

        // Sort keys to maintain consistent order expected by some tests/consumers
        keys.sort();

        Ok(keys)
    }

    fn scan_prefix(&self, prefix: &[u8]) -> Result<BTreeMap<Bytes, DbItem>> {
        let mut result = BTreeMap::new();

        if prefix.is_empty() {
            for (key, item) in &self.data {
                result.insert(Bytes::copy_from_slice(key), item.clone());
            }
        } else {
            for (key, item) in &self.data {
                if key.starts_with(prefix) {
                    result.insert(Bytes::copy_from_slice(key), item.clone());
                }
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
            self.iter_ref()
                .map(|(k, v)| (Bytes::copy_from_slice(k), v.clone())),
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_memory_backend_basic_ops() {
        let mut backend = MemoryBackend::new();

        let key = b"test_key";
        let item = DbItem {
            value: b"test_value".to_vec().into(),
            created_at: SystemTime::now(),
            expires_at: None,
        };

        backend.put(key, &item).unwrap();
        let retrieved = backend.get(key).unwrap().unwrap();
        assert_eq!(retrieved.value, item.value);

        assert!(backend.contains_key(key).unwrap());
        assert!(!backend.contains_key(b"nonexistent").unwrap());

        let deleted = backend.delete(key).unwrap().unwrap();
        assert_eq!(deleted.value, item.value);
        assert!(!backend.contains_key(key).unwrap());
    }

    #[test]
    fn test_memory_backend_prefix_scan() {
        let mut backend = MemoryBackend::new();

        let item = DbItem {
            value: b"value".to_vec().into(),
            created_at: SystemTime::now(),
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
            created_at: SystemTime::now(),
            expires_at: None,
        };

        backend.put(b"a", &item).unwrap();
        backend.put(b"b", &item).unwrap();
        let all_keys = backend.scan_prefix(b"").unwrap();
        assert_eq!(all_keys.len(), 2);

        backend.put(b"test\xff\xffa", &item).unwrap();
        backend.put(b"test\xff\xffb", &item).unwrap();
        backend.put(b"test\xff\xff\xff", &item).unwrap();
        backend.put(b"testb", &item).unwrap();

        let xff_prefix_scan = backend.scan_prefix(b"test\xff\xff").unwrap();
        assert_eq!(xff_prefix_scan.len(), 3);

        backend.put(b"abc", &item).unwrap();
        backend.put(b"abcd", &item).unwrap();
        backend.put(b"abd", &item).unwrap();

        let abc_scan = backend.scan_prefix(b"abc").unwrap();
        assert_eq!(abc_scan.len(), 2);

        let no_match_scan = backend.scan_prefix(b"nonexistent").unwrap();
        assert_eq!(no_match_scan.len(), 0);

        let a_prefix_scan = backend.scan_prefix(b"a").unwrap();
        assert_eq!(a_prefix_scan.len(), 4);
    }

    #[test]
    fn test_prefix_scan_ordering() {
        let mut backend = MemoryBackend::new();
        let item = DbItem {
            value: b"value".to_vec().into(),
            created_at: SystemTime::now(),
            expires_at: None,
        };

        backend.put(b"prefix:z", &item).unwrap();
        backend.put(b"prefix:a", &item).unwrap();
        backend.put(b"prefix:m", &item).unwrap();
        backend.put(b"different:key", &item).unwrap();

        let scan_result = backend.scan_prefix(b"prefix:").unwrap();
        assert_eq!(scan_result.len(), 3);

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
            created_at: SystemTime::now(),
            expires_at: None,
        };

        for i in 0..1000 {
            if i < 10 {
                let key = format!("target:key_{:03}", i);
                backend.put(key.as_bytes(), &item).unwrap();
            }

            let noise_key = format!("noise_{:03}:data", i);
            backend.put(noise_key.as_bytes(), &item).unwrap();

            let other_key = format!("zzz_other_{:03}", i);
            backend.put(other_key.as_bytes(), &item).unwrap();
        }

        let target_scan = backend.scan_prefix(b"target:").unwrap();
        assert_eq!(target_scan.len(), 10);

        let target_keys = backend.keys_with_prefix(b"target:").unwrap();
        assert_eq!(target_keys.len(), 10);

        assert_eq!(backend.data.len(), 2010);
    }

    #[test]
    fn test_memory_backend_ttl_cleanup() {
        let mut backend = MemoryBackend::new();

        let now = SystemTime::now();
        let past = now - Duration::from_secs(60);
        let future = now + Duration::from_secs(60);

        let expired_item = DbItem {
            value: b"expired".to_vec().into(),
            created_at: SystemTime::now(),
            expires_at: Some(past),
        };

        let valid_item = DbItem {
            value: b"valid".to_vec().into(),
            created_at: SystemTime::now(),
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
                    created_at: SystemTime::now(),
                    expires_at: None,
                },
            },
            StorageOp::Put {
                key: b"key2".to_vec().into(),
                item: DbItem {
                    value: b"value2".to_vec().into(),
                    created_at: SystemTime::now(),
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
