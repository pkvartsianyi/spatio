use crate::error::{Result, SpatioError};
use crate::types::SetOptions;
use bytes::{BufMut, Bytes, BytesMut};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// AOF configuration for rewriting
#[derive(Debug, Clone)]
pub struct AOFConfig {
    /// Trigger rewrite when file size exceeds this many bytes
    pub rewrite_size_threshold: u64,
}

impl Default for AOFConfig {
    fn default() -> Self {
        Self {
            rewrite_size_threshold: 64 * 1024 * 1024, // 64MB
        }
    }
}

/// Simplified AOF (Append-Only File) for embedded database persistence
pub struct AOFFile {
    file: File,
    writer: BufWriter<File>,
    path: PathBuf,
    size: u64,
    config: AOFConfig,
    last_rewrite_size: u64,
    rewrite_in_progress: bool,
}

#[derive(Debug)]
pub enum AOFCommand {
    Set {
        key: Bytes,
        value: Bytes,
        expires_at: Option<SystemTime>,
    },
    Delete {
        key: Bytes,
    },
}

impl AOFFile {
    /// Open AOF file with default configuration
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::open_with_config(path, AOFConfig::default())
    }

    /// Open AOF file with custom configuration
    pub fn open_with_config<P: AsRef<Path>>(path: P, config: AOFConfig) -> Result<Self> {
        let path = path.as_ref().to_path_buf();

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)?;

        let size = file.metadata()?.len();
        let writer_file = file.try_clone()?;
        let writer = BufWriter::new(writer_file);

        Ok(AOFFile {
            file,
            writer,
            path,
            size,
            config,
            last_rewrite_size: size,
            rewrite_in_progress: false,
        })
    }

    /// Get current file size
    pub fn size(&self) -> u64 {
        self.size
    }

    /// Write a SET command to the AOF
    pub fn write_set(
        &mut self,
        key: &[u8],
        value: &[u8],
        options: Option<&SetOptions>,
    ) -> Result<()> {
        let expires_at = match options {
            Some(opts) => opts.expires_at,
            None => None,
        };

        let command = AOFCommand::Set {
            key: Bytes::copy_from_slice(key),
            value: Bytes::copy_from_slice(value),
            expires_at,
        };

        self.write_command(&command)
    }

    /// Write a DELETE command to the AOF
    pub fn write_delete(&mut self, key: &[u8]) -> Result<()> {
        let command = AOFCommand::Delete {
            key: Bytes::copy_from_slice(key),
        };
        self.write_command(&command)
    }

    /// Write a command to the AOF file
    fn write_command(&mut self, command: &AOFCommand) -> Result<()> {
        if self.rewrite_in_progress {
            return Err(SpatioError::RewriteInProgress);
        }

        let serialized = self.serialize_command(command)?;
        self.writer.write_all(&serialized)?;
        self.size += serialized.len() as u64;

        // Check if we should trigger a rewrite
        if self.should_rewrite() {
            self.maybe_trigger_rewrite()?;
        }

        Ok(())
    }

    /// Check if AOF should be rewritten based on size threshold
    fn should_rewrite(&self) -> bool {
        !self.rewrite_in_progress && self.size >= self.config.rewrite_size_threshold
    }

    /// Trigger AOF rewrite if conditions are met
    fn maybe_trigger_rewrite(&mut self) -> Result<()> {
        if self.rewrite_in_progress {
            return Ok(());
        }

        // Always perform synchronous rewrite for embedded database
        // Background rewrite would require thread coordination which we avoid
        self.perform_rewrite()
    }

    /// Perform the actual AOF rewrite operation
    fn perform_rewrite(&mut self) -> Result<()> {
        if self.rewrite_in_progress {
            return Err(SpatioError::RewriteInProgress);
        }

        self.rewrite_in_progress = true;

        // Perform the rewrite and always clear the flag, even on error
        let result = (|| {
            // Flush current writer to ensure all data is persisted
            self.writer.flush()?;
            self.file.sync_all()?;

            // Create temporary rewrite file
            let rewrite_path = self.path.with_extension("aof.rewrite");
            let mut rewrite_file = Self::open_with_config(&rewrite_path, self.config.clone())?;

            // For simplicity, just copy the existing file
            // In a real implementation, you'd want to compact by removing deleted keys
            // and only keeping the latest value for each key
            self.file.seek(SeekFrom::Start(0))?;
            let mut buffer = Vec::new();
            self.file.read_to_end(&mut buffer)?;

            rewrite_file.writer.write_all(&buffer)?;
            rewrite_file.flush()?;

            // CRITICAL: Sync rewritten file to disk before rename to guarantee durability
            rewrite_file.sync()?;

            // Atomically replace the old file
            std::fs::rename(&rewrite_path, &self.path)?;

            // Reopen the file with new handles
            let new_file = OpenOptions::new()
                .create(true)
                .append(true)
                .read(true)
                .open(&self.path)?;

            let new_size = new_file.metadata()?.len();
            let writer_file = new_file.try_clone()?;
            let new_writer = BufWriter::new(writer_file);

            // Update file handles
            self.file = new_file;
            self.writer = new_writer;
            self.size = new_size;
            self.last_rewrite_size = new_size;

            Ok(())
        })();

        self.rewrite_in_progress = false;

        result
    }

    /// Serialize a command to bytes
    fn serialize_command(&self, command: &AOFCommand) -> Result<Vec<u8>> {
        let mut buf = BytesMut::new();

        match command {
            AOFCommand::Set {
                key,
                value,
                expires_at,
            } => {
                buf.put_u8(0); // Command type: SET

                // Key length and data
                buf.put_u32(key.len() as u32);
                buf.put(key.as_ref());

                // Value length and data
                buf.put_u32(value.len() as u32);
                buf.put(value.as_ref());

                // Expiration
                match expires_at {
                    Some(exp) => {
                        buf.put_u8(1); // Has expiration
                        let timestamp = exp
                            .duration_since(UNIX_EPOCH)
                            .map_err(|_| SpatioError::InvalidTimestamp)?
                            .as_secs();
                        buf.put_u64(timestamp);
                    }
                    None => {
                        buf.put_u8(0); // No expiration
                    }
                }
            }
            AOFCommand::Delete { key } => {
                buf.put_u8(1); // Command type: DELETE

                // Key length and data
                buf.put_u32(key.len() as u32);
                buf.put(key.as_ref());
            }
        }

        Ok(buf.to_vec())
    }

    /// Replay AOF commands and return them
    pub fn replay(&mut self) -> Result<Vec<AOFCommand>> {
        self.file.seek(SeekFrom::Start(0))?;
        let mut reader = BufReader::new(&mut self.file);
        let mut commands = Vec::new();

        loop {
            match Self::deserialize_command_static(&mut reader) {
                Ok(command) => commands.push(command),
                Err(SpatioError::UnexpectedEof) => break, // End of file
                Err(e) => return Err(e),
            }
        }

        Ok(commands)
    }

    /// Deserialize a command from the reader
    fn deserialize_command_static(reader: &mut BufReader<&mut File>) -> Result<AOFCommand> {
        let mut cmd_type_buf = [0u8; 1];
        if reader.read_exact(&mut cmd_type_buf).is_err() {
            return Err(SpatioError::UnexpectedEof);
        }
        let cmd_type = cmd_type_buf[0];

        match cmd_type {
            0 => {
                // SET command
                let key = Self::read_bytes(reader)?;
                let value = Self::read_bytes(reader)?;

                let mut has_exp_buf = [0u8; 1];
                reader.read_exact(&mut has_exp_buf)?;
                let has_expiration = has_exp_buf[0] != 0;

                let expires_at = if has_expiration {
                    let mut timestamp_buf = [0u8; 8];
                    reader.read_exact(&mut timestamp_buf)?;
                    let timestamp = u64::from_be_bytes(timestamp_buf);
                    Some(UNIX_EPOCH + Duration::from_secs(timestamp))
                } else {
                    None
                };

                Ok(AOFCommand::Set {
                    key,
                    value,
                    expires_at,
                })
            }
            1 => {
                // DELETE command
                let key = Self::read_bytes(reader)?;
                Ok(AOFCommand::Delete { key })
            }
            _ => Err(SpatioError::InvalidFormat),
        }
    }

    /// Helper to read length-prefixed bytes
    fn read_bytes(reader: &mut BufReader<&mut File>) -> Result<Bytes> {
        let mut len_buf = [0u8; 4];
        reader.read_exact(&mut len_buf)?;
        let len = u32::from_be_bytes(len_buf) as usize;

        let mut buf = vec![0u8; len];
        reader.read_exact(&mut buf)?;

        Ok(Bytes::from(buf))
    }

    /// Flush buffered writes to disk
    pub fn flush(&mut self) -> Result<()> {
        self.writer.flush()?;
        Ok(())
    }

    /// Flush and sync to disk
    pub fn sync(&mut self) -> Result<()> {
        self.writer.flush()?;
        self.file.sync_all()?;
        Ok(())
    }

    /// Get the file path
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for AOFFile {
    fn drop(&mut self) {
        // Best effort flush on drop, ignore errors
        let _ = self.writer.flush();
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
        assert_eq!(aof.size(), 0);
    }

    #[test]
    fn test_set_command_serialization() {
        let temp_file = NamedTempFile::new().unwrap();
        let mut aof = AOFFile::open(temp_file.path()).unwrap();

        aof.write_set(b"key1", b"value1", None).unwrap();
        assert!(aof.size() > 0);
    }

    #[test]
    fn test_command_replay() {
        let temp_file = NamedTempFile::new().unwrap();
        let mut aof = AOFFile::open(temp_file.path()).unwrap();

        // Write some commands
        aof.write_set(b"key1", b"value1", None).unwrap();
        aof.write_delete(b"key2").unwrap();
        aof.flush().unwrap();

        // Replay commands
        let commands = aof.replay().unwrap();
        assert_eq!(commands.len(), 2);

        match &commands[0] {
            AOFCommand::Set {
                key,
                value,
                expires_at,
            } => {
                assert_eq!(key.as_ref(), b"key1");
                assert_eq!(value.as_ref(), b"value1");
                assert!(expires_at.is_none());
            }
            _ => panic!("Expected SET command"),
        }

        match &commands[1] {
            AOFCommand::Delete { key } => {
                assert_eq!(key.as_ref(), b"key2");
            }
            _ => panic!("Expected DELETE command"),
        }
    }

    #[test]
    fn test_expiration_serialization() {
        let temp_file = NamedTempFile::new().unwrap();
        let mut aof = AOFFile::open(temp_file.path()).unwrap();

        let expires_at = SystemTime::now() + Duration::from_secs(3600);
        let options = SetOptions {
            ttl: None,
            expires_at: Some(expires_at),
        };

        aof.write_set(b"key1", b"value1", Some(&options)).unwrap();
        aof.flush().unwrap();

        let commands = aof.replay().unwrap();
        assert_eq!(commands.len(), 1);

        match &commands[0] {
            AOFCommand::Set {
                expires_at: exp, ..
            } => {
                assert!(exp.is_some());
                // Allow for small timing differences
                let diff = expires_at
                    .duration_since(exp.unwrap())
                    .unwrap_or_else(|_| exp.unwrap().duration_since(expires_at).unwrap());
                assert!(diff.as_secs() < 2);
            }
            _ => panic!("Expected SET command with expiration"),
        }
    }

    #[test]
    fn test_synchronous_rewrite() {
        let temp_file = NamedTempFile::new().unwrap();
        let config = AOFConfig {
            rewrite_size_threshold: 100, // Small threshold
        };

        let mut aof = AOFFile::open_with_config(temp_file.path(), config).unwrap();

        // Write enough data to trigger rewrite
        for i in 0..50 {
            let key = format!("key{}", i);
            let value = format!("value{}", i);
            aof.write_set(key.as_bytes(), value.as_bytes(), None)
                .unwrap();
        }

        // Rewrite should have been triggered automatically (synchronous)
    }
}
