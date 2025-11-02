//! Persistent storage backend using AOF (Append-Only File).

use super::{MemoryBackend, StorageBackend, StorageOp, StorageStats};
use crate::config::{DbItem, SetOptions};
use crate::error::Result;
use crate::persistence::AOFCommand;
use bytes::Bytes;
use std::collections::BTreeMap;
use std::time::SystemTime;

/// Persistent storage backend using AOF (Append-Only File)
pub struct AOFBackend {
    memory: MemoryBackend,
    aof_writer: crate::persistence::AOFFile,
}

impl AOFBackend {
    /// Create a new AOF storage backend
    pub fn new<P: AsRef<std::path::Path>>(path: P) -> Result<Self> {
        let aof_writer = crate::persistence::AOFFile::open(path)?;
        let memory = MemoryBackend::new();

        Ok(Self { memory, aof_writer })
    }

    /// Load existing data from AOF file
    pub fn load_from_aof(&mut self) -> Result<()> {
        let commands = self.aof_writer.replay()?;
        self.memory = MemoryBackend::new();

        for command in commands {
            match command {
                AOFCommand::Set {
                    key,
                    value,
                    created_at,
                    expires_at,
                } => {
                    let item = DbItem {
                        value,
                        created_at,
                        expires_at,
                    };
                    self.memory.put(key.as_ref(), &item)?;
                }
                AOFCommand::Delete { key } => {
                    let _ = self.memory.delete(key.as_ref())?;
                }
            }
        }

        Ok(())
    }
}

impl StorageBackend for AOFBackend {
    fn put(&mut self, key: &[u8], item: &DbItem) -> Result<()> {
        // Write to AOF first for durability
        let opts = item.expires_at.map(SetOptions::with_expiration);
        self.aof_writer.write_set(
            &Bytes::copy_from_slice(key),
            &item.value,
            opts.as_ref(),
            item.created_at,
        )?;

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
                    self.aof_writer
                        .write_set(key, &item.value, opts.as_ref(), item.created_at)?;
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
