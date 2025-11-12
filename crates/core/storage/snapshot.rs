//! Snapshot-based persistence for Spatio.
//!
//! Provides point-in-time snapshot storage of the entire database state.
//! Snapshots are written synchronously and atomically replace the previous snapshot.

use crate::config::DbItem;
use crate::error::{Result, SpatioError};
use bytes::Bytes;
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

const SNAPSHOT_MAGIC: &[u8] = b"SPATIO_SNAPSHOT";
const SNAPSHOT_VERSION: u8 = 1;

#[derive(Debug, Clone, Default)]
pub struct SnapshotConfig {
    pub auto_snapshot_ops: Option<usize>,
}

pub struct SnapshotFile {
    path: PathBuf,
    config: SnapshotConfig,
    ops_since_snapshot: usize,
}

impl SnapshotFile {
    pub fn new<P: AsRef<Path>>(path: P, config: SnapshotConfig) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
            config,
            ops_since_snapshot: 0,
        }
    }

    pub fn exists(&self) -> bool {
        self.path.exists()
    }

    pub fn load(&self) -> Result<BTreeMap<Bytes, DbItem>> {
        if !self.exists() {
            return Ok(BTreeMap::new());
        }

        let file = File::open(&self.path)?;
        let metadata = file.metadata()?;
        if metadata.len() == 0 {
            return Ok(BTreeMap::new());
        }

        let mut reader = BufReader::new(file);

        let mut magic = vec![0u8; SNAPSHOT_MAGIC.len()];
        reader.read_exact(&mut magic)?;
        if magic != SNAPSHOT_MAGIC {
            return Err(SpatioError::InvalidFormat);
        }

        let mut version = [0u8; 1];
        reader.read_exact(&mut version)?;
        if version[0] != SNAPSHOT_VERSION {
            return Err(SpatioError::InvalidFormat);
        }

        let mut timestamp_bytes = [0u8; 16];
        reader.read_exact(&mut timestamp_bytes)?;

        let entry_count = read_u64(&mut reader)?;
        let mut keys = BTreeMap::new();

        for _ in 0..entry_count {
            let key_len = read_u64(&mut reader)? as usize;
            let mut key_buf = vec![0u8; key_len];
            reader.read_exact(&mut key_buf)?;
            let key = Bytes::from(key_buf);

            let value_len = read_u64(&mut reader)? as usize;
            let mut value_buf = vec![0u8; value_len];
            reader.read_exact(&mut value_buf)?;
            let value = Bytes::from(value_buf);

            let created_secs = read_u64(&mut reader)?;
            let created_nanos = read_u32(&mut reader)?;
            let created_at = UNIX_EPOCH + std::time::Duration::new(created_secs, created_nanos);

            let has_expiration = read_u8(&mut reader)?;
            let expires_at = if has_expiration == 1 {
                let expires_secs = read_u64(&mut reader)?;
                let expires_nanos = read_u32(&mut reader)?;
                Some(UNIX_EPOCH + std::time::Duration::new(expires_secs, expires_nanos))
            } else {
                None
            };

            keys.insert(
                key,
                DbItem {
                    value,
                    created_at,
                    expires_at,
                },
            );
        }

        Ok(keys)
    }

    pub fn save(&mut self, keys: &BTreeMap<Bytes, DbItem>) -> Result<()> {
        let temp_path = self.temp_path();

        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&temp_path)?;

        let mut writer = BufWriter::new(file);

        writer.write_all(SNAPSHOT_MAGIC)?;
        writer.write_all(&[SNAPSHOT_VERSION])?;

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| SpatioError::InvalidTimestamp)?;
        let mut timestamp_buf = [0u8; 16];
        timestamp_buf[0..8].copy_from_slice(&timestamp.as_secs().to_le_bytes());
        timestamp_buf[8..12].copy_from_slice(&timestamp.subsec_nanos().to_le_bytes());
        writer.write_all(&timestamp_buf)?;

        write_u64(&mut writer, keys.len() as u64)?;

        for (key, item) in keys {
            write_u64(&mut writer, key.len() as u64)?;
            writer.write_all(key)?;

            write_u64(&mut writer, item.value.len() as u64)?;
            writer.write_all(&item.value)?;

            let created_duration = item
                .created_at
                .duration_since(UNIX_EPOCH)
                .map_err(|_| SpatioError::InvalidTimestamp)?;
            write_u64(&mut writer, created_duration.as_secs())?;
            write_u32(&mut writer, created_duration.subsec_nanos())?;

            if let Some(expires_at) = item.expires_at {
                write_u8(&mut writer, 1)?;
                let expires_duration = expires_at
                    .duration_since(UNIX_EPOCH)
                    .map_err(|_| SpatioError::InvalidTimestamp)?;
                write_u64(&mut writer, expires_duration.as_secs())?;
                write_u32(&mut writer, expires_duration.subsec_nanos())?;
            } else {
                write_u8(&mut writer, 0)?;
            }
        }

        writer.flush()?;
        let file = writer.into_inner().map_err(|e| e.into_error())?;
        file.sync_all()?;
        drop(file);

        std::fs::rename(&temp_path, &self.path)?;
        self.sync_parent_dir()?;

        self.ops_since_snapshot = 0;

        Ok(())
    }

    pub fn record_operation(&mut self) {
        self.ops_since_snapshot += 1;
    }

    pub fn should_snapshot(&self) -> bool {
        if let Some(threshold) = self.config.auto_snapshot_ops {
            self.ops_since_snapshot >= threshold
        } else {
            false
        }
    }

    fn temp_path(&self) -> PathBuf {
        let mut temp = self.path.clone();
        if let Some(name) = temp.file_name() {
            let mut new_name = name.to_string_lossy().into_owned();
            new_name.push_str(".tmp");
            temp.set_file_name(new_name);
        }
        temp
    }

    fn sync_parent_dir(&self) -> Result<()> {
        if let Some(parent) = self.path.parent() {
            let dir = File::open(parent)?;
            dir.sync_all()?;
        }
        Ok(())
    }
}

fn write_u8<W: Write>(writer: &mut W, value: u8) -> Result<()> {
    writer.write_all(&[value])?;
    Ok(())
}

fn write_u32<W: Write>(writer: &mut W, value: u32) -> Result<()> {
    writer.write_all(&value.to_le_bytes())?;
    Ok(())
}

fn write_u64<W: Write>(writer: &mut W, value: u64) -> Result<()> {
    writer.write_all(&value.to_le_bytes())?;
    Ok(())
}

fn read_u8<R: Read>(reader: &mut R) -> Result<u8> {
    let mut buf = [0u8; 1];
    reader.read_exact(&mut buf)?;
    Ok(buf[0])
}

fn read_u32<R: Read>(reader: &mut R) -> Result<u32> {
    let mut buf = [0u8; 4];
    reader.read_exact(&mut buf)?;
    Ok(u32::from_le_bytes(buf))
}

fn read_u64<R: Read>(reader: &mut R) -> Result<u64> {
    let mut buf = [0u8; 8];
    reader.read_exact(&mut buf)?;
    Ok(u64::from_le_bytes(buf))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_snapshot_roundtrip() {
        let temp = NamedTempFile::new().unwrap();
        let path = temp.path();

        let mut snapshot = SnapshotFile::new(path, SnapshotConfig::default());

        let mut data = BTreeMap::new();
        data.insert(
            Bytes::from("key1"),
            DbItem {
                value: Bytes::from("value1"),
                created_at: SystemTime::now(),
                expires_at: None,
            },
        );
        data.insert(
            Bytes::from("key2"),
            DbItem {
                value: Bytes::from("value2"),
                created_at: SystemTime::now(),
                expires_at: Some(SystemTime::now()),
            },
        );

        snapshot.save(&data).unwrap();

        let loaded = snapshot.load().unwrap();
        assert_eq!(loaded.len(), 2);
        assert_eq!(
            loaded.get(&Bytes::from("key1")).unwrap().value,
            Bytes::from("value1")
        );
        assert_eq!(
            loaded.get(&Bytes::from("key2")).unwrap().value,
            Bytes::from("value2")
        );
    }

    #[test]
    fn test_auto_snapshot_threshold() {
        let temp = NamedTempFile::new().unwrap();
        let config = SnapshotConfig {
            auto_snapshot_ops: Some(10),
        };
        let mut snapshot = SnapshotFile::new(temp.path(), config);

        assert!(!snapshot.should_snapshot());

        for _ in 0..9 {
            snapshot.record_operation();
            assert!(!snapshot.should_snapshot());
        }

        snapshot.record_operation();
        assert!(snapshot.should_snapshot());

        let data = BTreeMap::new();
        snapshot.save(&data).unwrap();
        assert!(!snapshot.should_snapshot());
    }

    #[test]
    fn test_load_nonexistent() {
        let temp = NamedTempFile::new().unwrap();
        let path = temp.path().with_extension("nonexistent");
        let snapshot = SnapshotFile::new(&path, SnapshotConfig::default());

        let loaded = snapshot.load().unwrap();
        assert!(loaded.is_empty());
    }

    #[test]
    fn test_invalid_magic() {
        let temp = NamedTempFile::new().unwrap();
        let path = temp.path();

        let mut file = File::create(path).unwrap();
        file.write_all(b"INVALID_MAGIC").unwrap();
        file.sync_all().unwrap();
        drop(file);

        let snapshot = SnapshotFile::new(path, SnapshotConfig::default());
        assert!(snapshot.load().is_err());
    }
}
