use crate::error::{Result, SpatioError};
use crate::types::SetOptions;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// AOF configuration for background rewriting
#[derive(Debug, Clone)]
pub struct AOFConfig {
    /// Trigger rewrite when file size exceeds this many bytes
    pub rewrite_size_threshold: u64,
    /// Trigger rewrite when file size grows by this percentage
    pub rewrite_growth_percentage: f64,
    /// Enable background rewriting
    pub background_rewrite: bool,
}

impl Default for AOFConfig {
    fn default() -> Self {
        Self {
            rewrite_size_threshold: 64 * 1024 * 1024, // 64MB
            rewrite_growth_percentage: 100.0,         // 100%
            background_rewrite: true,
        }
    }
}

/// AOF (Append-Only File) for persistence with background rewriting
pub struct AOFFile {
    file: File,
    writer: BufWriter<File>,
    path: std::path::PathBuf,
    size: u64,
    config: AOFConfig,
    rewrite_in_progress: Arc<RwLock<bool>>,
    last_rewrite_size: u64,
}

impl Clone for AOFFile {
    fn clone(&self) -> Self {
        let file = self.file.try_clone().expect("Failed to clone file handle");
        let writer_file = file
            .try_clone()
            .expect("Failed to clone file handle for writer");
        let writer = BufWriter::new(writer_file);

        Self {
            file,
            writer,
            path: self.path.clone(),
            size: self.size,
            config: self.config.clone(),
            rewrite_in_progress: self.rewrite_in_progress.clone(),
            last_rewrite_size: self.last_rewrite_size,
        }
    }
}

/// AOF command types
#[derive(Debug, Clone)]
pub enum AOFCommand {
    Set {
        key: Bytes,
        value: Bytes,
        expires_at: Option<SystemTime>,
    },
    Delete {
        key: Bytes,
    },
    Expire {
        key: Bytes,
        expires_at: SystemTime,
    },
}

impl AOFFile {
    /// Open an AOF file at the given path with default config
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::open_with_config(path, AOFConfig::default())
    }

    /// Open an AOF file with custom configuration
    pub fn open_with_config<P: AsRef<Path>>(path: P, config: AOFConfig) -> Result<Self> {
        let path = path.as_ref().to_path_buf();

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)?;

        let size = file.metadata()?.len();

        // Clone file handle for writer
        let writer_file = file.try_clone()?;
        let writer = BufWriter::new(writer_file);

        Ok(Self {
            file,
            writer,
            path,
            size,
            config,
            rewrite_in_progress: Arc::new(RwLock::new(false)),
            last_rewrite_size: size,
        })
    }

    /// Get the current size of the AOF file
    pub fn size(&self) -> Result<u64> {
        Ok(self.size)
    }

    /// Write a SET command to the AOF
    pub fn write_set(
        &mut self,
        key: &Bytes,
        value: &Bytes,
        opts: Option<&SetOptions>,
    ) -> Result<()> {
        let expires_at = if let Some(opts) = opts {
            if let Some(ttl) = opts.ttl {
                Some(SystemTime::now() + ttl)
            } else {
                opts.expires_at
            }
        } else {
            None
        };

        let command = AOFCommand::Set {
            key: key.clone(),
            value: value.clone(),
            expires_at,
        };

        self.write_command(&command)
    }

    /// Write a DELETE command to the AOF
    pub fn write_delete(&mut self, key: &Bytes) -> Result<()> {
        let command = AOFCommand::Delete { key: key.clone() };
        self.write_command(&command)
    }

    /// Write an EXPIRE command to the AOF
    pub fn write_expire(&mut self, key: &Bytes, expires_at: SystemTime) -> Result<()> {
        let command = AOFCommand::Expire {
            key: key.clone(),
            expires_at,
        };
        self.write_command(&command)
    }

    /// Write a command to the AOF file
    fn write_command(&mut self, command: &AOFCommand) -> Result<()> {
        let serialized = self.serialize_command(command)?;

        self.writer.write_all(&serialized)?;
        self.size += serialized.len() as u64;

        // Check if we should trigger a background rewrite
        if self.config.background_rewrite && self.should_rewrite() {
            self.maybe_trigger_background_rewrite()?;
        }

        Ok(())
    }

    /// Check if AOF should be rewritten based on size thresholds
    fn should_rewrite(&self) -> bool {
        // Don't rewrite if one is already in progress
        if *self.rewrite_in_progress.read().unwrap() {
            return false;
        }

        // Check size threshold
        if self.size > self.config.rewrite_size_threshold {
            return true;
        }

        // Check growth percentage
        if self.last_rewrite_size > 0 {
            let growth = (self.size as f64 - self.last_rewrite_size as f64)
                / self.last_rewrite_size as f64
                * 100.0;
            if growth > self.config.rewrite_growth_percentage {
                return true;
            }
        }

        false
    }

    /// Trigger background AOF rewrite if conditions are met
    fn maybe_trigger_background_rewrite(&mut self) -> Result<()> {
        // Set rewrite in progress flag
        {
            let mut in_progress = self.rewrite_in_progress.write().unwrap();
            if *in_progress {
                return Ok(()); // Already in progress
            }
            *in_progress = true;
        }

        // Clone necessary data for background thread
        let aof_clone = self.clone();

        // Spawn background rewrite thread
        thread::spawn(move || {
            if let Err(e) = Self::perform_background_rewrite(aof_clone) {
                eprintln!("Background AOF rewrite failed: {}", e);
            }
        });

        Ok(())
    }

    /// Perform the actual background rewrite
    fn perform_background_rewrite(mut aof: AOFFile) -> Result<()> {
        // Create a temporary rewrite file
        let rewrite_path = aof.path.with_extension("aof.rewrite");
        let mut rewrite_file = AOFFile::open_with_config(&rewrite_path, aof.config.clone())?;

        // Read current data and write compacted version
        // This is a simplified version - in practice, you'd want to:
        // 1. Take a snapshot of current state
        // 2. Write only the latest value for each key
        // 3. Handle concurrent writes properly

        // For now, just copy the existing file (this could be optimized)
        aof.file.seek(SeekFrom::Start(0))?;
        let mut buffer = Vec::new();
        aof.file.read_to_end(&mut buffer)?;

        rewrite_file.writer.write_all(&buffer)?;
        rewrite_file.flush()?;

        // Atomically replace the old file
        std::fs::rename(&rewrite_path, &aof.path)?;

        // Update last rewrite size
        // Note: In a real implementation, you'd need to communicate this back
        // to the main AOF instance through shared state

        // Clear the rewrite in progress flag
        {
            let mut in_progress = aof.rewrite_in_progress.write().unwrap();
            *in_progress = false;
        }

        Ok(())
    }

    /// Manually trigger an AOF rewrite
    pub fn rewrite(&mut self) -> Result<()> {
        // Force a rewrite regardless of thresholds
        let original_config = self.config.clone();
        self.config.rewrite_size_threshold = 0;

        let result = self.maybe_trigger_background_rewrite();

        // Restore original config
        self.config = original_config;

        result
    }

    /// Get AOF configuration
    pub fn config(&self) -> &AOFConfig {
        &self.config
    }

    /// Update AOF configuration
    pub fn set_config(&mut self, config: AOFConfig) {
        self.config = config;
    }

    /// Check if a rewrite is currently in progress
    pub fn is_rewrite_in_progress(&self) -> bool {
        *self.rewrite_in_progress.read().unwrap()
    }

    /// Serialize a command to bytes using a simple binary format
    fn serialize_command(&self, command: &AOFCommand) -> Result<Vec<u8>> {
        let mut buf = BytesMut::new();

        match command {
            AOFCommand::Set {
                key,
                value,
                expires_at,
            } => {
                // Command type: 1 = SET
                buf.put_u8(1);

                // Key length and data
                buf.put_u32(key.len() as u32);
                buf.put(key.as_ref());

                // Value length and data
                buf.put_u32(value.len() as u32);
                buf.put(value.as_ref());

                // Expiration (0 = no expiration, otherwise timestamp)
                if let Some(expires_at) = expires_at {
                    let timestamp = expires_at
                        .duration_since(UNIX_EPOCH)
                        .map_err(|_| SpatioError::SerializationError)?
                        .as_secs();
                    buf.put_u64(timestamp);
                } else {
                    buf.put_u64(0);
                }
            }
            AOFCommand::Delete { key } => {
                // Command type: 2 = DELETE
                buf.put_u8(2);

                // Key length and data
                buf.put_u32(key.len() as u32);
                buf.put(key.as_ref());
            }
            AOFCommand::Expire { key, expires_at } => {
                // Command type: 3 = EXPIRE
                buf.put_u8(3);

                // Key length and data
                buf.put_u32(key.len() as u32);
                buf.put(key.as_ref());

                // Expiration timestamp
                let timestamp = expires_at
                    .duration_since(UNIX_EPOCH)
                    .map_err(|_| SpatioError::SerializationError)?
                    .as_secs();
                buf.put_u64(timestamp);
            }
        }

        Ok(buf.to_vec())
    }

    /// Read and replay all commands from the AOF file
    pub fn replay<F>(&mut self, mut callback: F) -> Result<()>
    where
        F: FnMut(AOFCommand) -> Result<()>,
    {
        // Seek to beginning of file
        self.file.seek(SeekFrom::Start(0))?;

        let mut reader = BufReader::new(&mut self.file);
        let mut buffer = Vec::new();

        // Read entire file
        reader.read_to_end(&mut buffer)?;

        if buffer.is_empty() {
            return Ok(());
        }

        let mut buf = Bytes::from(buffer);

        while buf.remaining() > 0 {
            let command = self.deserialize_command(&mut buf)?;
            callback(command)?;
        }

        Ok(())
    }

    /// Deserialize a command from bytes
    fn deserialize_command(&self, buf: &mut Bytes) -> Result<AOFCommand> {
        if buf.remaining() < 1 {
            return Err(SpatioError::SerializationError);
        }

        let cmd_type = buf.get_u8();

        match cmd_type {
            1 => {
                // SET command
                if buf.remaining() < 8 {
                    return Err(SpatioError::SerializationError);
                }

                let key_len = buf.get_u32() as usize;
                if buf.remaining() < key_len {
                    return Err(SpatioError::SerializationError);
                }
                let key = buf.copy_to_bytes(key_len);

                if buf.remaining() < 4 {
                    return Err(SpatioError::SerializationError);
                }
                let value_len = buf.get_u32() as usize;
                if buf.remaining() < value_len {
                    return Err(SpatioError::SerializationError);
                }
                let value = buf.copy_to_bytes(value_len);

                if buf.remaining() < 8 {
                    return Err(SpatioError::SerializationError);
                }
                let expires_timestamp = buf.get_u64();
                let expires_at = if expires_timestamp == 0 {
                    None
                } else {
                    Some(UNIX_EPOCH + Duration::from_secs(expires_timestamp))
                };

                Ok(AOFCommand::Set {
                    key,
                    value,
                    expires_at,
                })
            }
            2 => {
                // DELETE command
                if buf.remaining() < 4 {
                    return Err(SpatioError::SerializationError);
                }

                let key_len = buf.get_u32() as usize;
                if buf.remaining() < key_len {
                    return Err(SpatioError::SerializationError);
                }
                let key = buf.copy_to_bytes(key_len);

                Ok(AOFCommand::Delete { key })
            }
            3 => {
                // EXPIRE command
                if buf.remaining() < 4 {
                    return Err(SpatioError::SerializationError);
                }

                let key_len = buf.get_u32() as usize;
                if buf.remaining() < key_len {
                    return Err(SpatioError::SerializationError);
                }
                let key = buf.copy_to_bytes(key_len);

                if buf.remaining() < 8 {
                    return Err(SpatioError::SerializationError);
                }
                let expires_timestamp = buf.get_u64();
                let expires_at = UNIX_EPOCH + Duration::from_secs(expires_timestamp);

                Ok(AOFCommand::Expire { key, expires_at })
            }
            _ => Err(SpatioError::SerializationError),
        }
    }

    /// Flush the write buffer to disk
    pub fn flush(&mut self) -> Result<()> {
        self.writer.flush()?;
        Ok(())
    }

    /// Sync data to disk
    pub fn sync(&mut self) -> Result<()> {
        self.flush()?;
        self.file.sync_all()?;
        Ok(())
    }

    /// Create a new AOF file for shrinking (compaction)
    pub fn create_shrink_file(&self) -> Result<AOFFile> {
        let shrink_path = self.path.with_extension("aof.shrink");
        AOFFile::open(shrink_path)
    }

    /// Replace the current AOF file with the shrunk version
    pub fn replace_with_shrink(&mut self) -> Result<()> {
        let shrink_path = self.path.with_extension("aof.shrink");

        // Flush and close current file
        self.flush()?;

        // Replace file
        std::fs::rename(&shrink_path, &self.path)?;

        // Reopen the file
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&self.path)?;

        let size = file.metadata()?.len();
        let writer_file = file.try_clone()?;
        let writer = BufWriter::new(writer_file);

        self.file = file;
        self.writer = writer;
        self.size = size;
        self.last_rewrite_size = size;

        Ok(())
    }

    /// Get the path of the AOF file
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for AOFFile {
    fn drop(&mut self) {
        let _ = self.flush();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_aof_creation() {
        let temp_file = NamedTempFile::new().unwrap();
        let aof = AOFFile::open(temp_file.path()).unwrap();
        assert_eq!(aof.size().unwrap(), 0);
    }

    #[test]
    fn test_set_command_serialization() {
        let temp_file = NamedTempFile::new().unwrap();
        let mut aof = AOFFile::open(temp_file.path()).unwrap();

        let key = Bytes::from("test_key");
        let value = Bytes::from("test_value");

        aof.write_set(&key, &value, None).unwrap();
        aof.flush().unwrap();

        assert!(aof.size().unwrap() > 0);
    }

    #[test]
    fn test_command_replay() {
        let temp_file = NamedTempFile::new().unwrap();
        let mut aof = AOFFile::open(temp_file.path()).unwrap();

        // Write some commands
        let key1 = Bytes::from("key1");
        let value1 = Bytes::from("value1");
        let key2 = Bytes::from("key2");

        aof.write_set(&key1, &value1, None).unwrap();
        aof.write_delete(&key2).unwrap();
        aof.flush().unwrap();

        // Replay commands
        let mut commands = Vec::new();
        aof.replay(|cmd| {
            commands.push(cmd);
            Ok(())
        })
        .unwrap();

        assert_eq!(commands.len(), 2);

        match &commands[0] {
            AOFCommand::Set {
                key,
                value,
                expires_at,
            } => {
                assert_eq!(key, &key1);
                assert_eq!(value, &value1);
                assert!(expires_at.is_none());
            }
            _ => panic!("Expected SET command"),
        }

        match &commands[1] {
            AOFCommand::Delete { key } => {
                assert_eq!(key, &key2);
            }
            _ => panic!("Expected DELETE command"),
        }
    }

    #[test]
    fn test_expiration_serialization() {
        let temp_file = NamedTempFile::new().unwrap();
        let mut aof = AOFFile::open(temp_file.path()).unwrap();

        let key = Bytes::from("test_key");
        let value = Bytes::from("test_value");
        let opts = SetOptions::with_ttl(Duration::from_secs(60));

        aof.write_set(&key, &value, Some(&opts)).unwrap();
        aof.flush().unwrap();

        // Replay and verify expiration is set
        let mut commands = Vec::new();
        aof.replay(|cmd| {
            commands.push(cmd);
            Ok(())
        })
        .unwrap();

        assert_eq!(commands.len(), 1);

        match &commands[0] {
            AOFCommand::Set {
                key: _,
                value: _,
                expires_at,
            } => {
                assert!(expires_at.is_some());
            }
            _ => panic!("Expected SET command with expiration"),
        }
    }
}
