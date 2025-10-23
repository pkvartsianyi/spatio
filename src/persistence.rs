use crate::error::{Result, SpatioError};
use crate::types::{SetOptions, SyncMode};
use bytes::{BufMut, Bytes, BytesMut};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
#[cfg(feature = "bench-prof")]
use std::time::Instant;
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
    scratch: BytesMut,
    #[cfg(feature = "bench-prof")]
    profile: AOFProfile,
}

const SCRATCH_INITIAL_CAPACITY: usize = 8 * 1024;
const SCRATCH_SHRINK_THRESHOLD: usize = 1 << 20;

#[cfg(feature = "bench-prof")]
#[derive(Default)]
struct AOFProfile {
    serialize_ns: u128,
    write_ns: u128,
    sync_ns: u128,
    commands: u64,
    syncs: u64,
}

#[derive(Debug)]
pub enum AOFCommand {
    Set {
        key: Bytes,
        value: Bytes,
        created_at: SystemTime,
        expires_at: Option<SystemTime>,
    },
    Delete {
        key: Bytes,
    },
}

impl AOFFile {
    const FLAG_HAS_EXPIRATION: u8 = 0b0000_0001;
    const FLAG_HAS_CREATED_AT: u8 = 0b0000_0010;

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
            scratch: BytesMut::with_capacity(SCRATCH_INITIAL_CAPACITY),
            #[cfg(feature = "bench-prof")]
            profile: AOFProfile::default(),
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
        created_at: SystemTime,
    ) -> Result<()> {
        let expires_at = match options {
            Some(opts) => opts.expires_at,
            None => None,
        };

        let command = AOFCommand::Set {
            key: Bytes::copy_from_slice(key),
            value: Bytes::copy_from_slice(value),
            created_at,
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

        #[cfg(feature = "bench-prof")]
        let serialize_start = Instant::now();
        let written_len = self.serialize_command(command)?;
        #[cfg(feature = "bench-prof")]
        {
            let elapsed = serialize_start.elapsed();
            self.profile.serialize_ns += elapsed.as_nanos();
            self.profile.commands += 1;
        }
        #[cfg(feature = "bench-prof")]
        let write_start = Instant::now();
        self.writer.write_all(&self.scratch[..written_len])?;
        #[cfg(feature = "bench-prof")]
        {
            let elapsed = write_start.elapsed();
            self.profile.write_ns += elapsed.as_nanos();
        }
        self.size += written_len as u64;

        if self.scratch.capacity() > SCRATCH_SHRINK_THRESHOLD
            && written_len <= SCRATCH_INITIAL_CAPACITY
        {
            self.scratch = BytesMut::with_capacity(SCRATCH_INITIAL_CAPACITY);
        }

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

            // Current rewrite strategy copies the entire AOF.
            // TODO: replace with compaction that keeps only the latest value per key.
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

    /// Serialize a command into the reusable scratch buffer.
    fn serialize_command(&mut self, command: &AOFCommand) -> Result<usize> {
        match command {
            AOFCommand::Set {
                key,
                value,
                created_at,
                expires_at,
            } => {
                let capacity =
                    Self::calc_aof_capacity(key.len(), value.len(), expires_at.is_some());
                self.scratch.clear();
                if self.scratch.capacity() < capacity {
                    self.scratch.reserve(capacity - self.scratch.capacity());
                }
                let buf = &mut self.scratch;

                buf.put_u8(0); // Command type: SET

                // Key length and data
                buf.put_u32(key.len() as u32);
                buf.put(key.as_ref());

                // Value length and data
                buf.put_u32(value.len() as u32);
                buf.put(value.as_ref());

                let mut flags = Self::FLAG_HAS_CREATED_AT;
                if expires_at.is_some() {
                    flags |= Self::FLAG_HAS_EXPIRATION;
                }
                buf.put_u8(flags);

                let created_ts = created_at
                    .duration_since(UNIX_EPOCH)
                    .map_err(|_| SpatioError::InvalidTimestamp)?
                    .as_secs();
                buf.put_u64(created_ts);

                if let Some(exp) = expires_at {
                    let timestamp = exp
                        .duration_since(UNIX_EPOCH)
                        .map_err(|_| SpatioError::InvalidTimestamp)?
                        .as_secs();
                    buf.put_u64(timestamp);
                }

                Ok(buf.len())
            }
            AOFCommand::Delete { key } => {
                self.scratch.clear();
                let needed = 1 + 4 + key.len();
                if self.scratch.capacity() < needed {
                    self.scratch.reserve(needed - self.scratch.capacity());
                }
                let buf = &mut self.scratch;
                buf.put_u8(1); // Command type: DELETE

                // Key length and data
                buf.put_u32(key.len() as u32);
                buf.put(key.as_ref());

                Ok(buf.len())
            }
        }
    }

    fn calc_aof_capacity(key_len: usize, value_len: usize, has_expiration: bool) -> usize {
        const CMD_TYPE_LEN: usize = 1;
        const LEN_FIELD_LEN: usize = 4;
        const FLAGS_LEN: usize = 1;
        const TIMESTAMP_LEN: usize = 8;

        let mut capacity = CMD_TYPE_LEN
            + LEN_FIELD_LEN
            + key_len
            + LEN_FIELD_LEN
            + value_len
            + FLAGS_LEN
            + TIMESTAMP_LEN; // created_at timestamp

        if has_expiration {
            capacity += TIMESTAMP_LEN;
        }

        capacity
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

                let mut flags_buf = [0u8; 1];
                if let Err(err) = reader.read_exact(&mut flags_buf) {
                    return match err.kind() {
                        std::io::ErrorKind::UnexpectedEof => Err(SpatioError::UnexpectedEof),
                        _ => Err(SpatioError::from(err)),
                    };
                }

                let flags = flags_buf[0];
                let has_expiration = (flags & Self::FLAG_HAS_EXPIRATION) != 0;
                let has_created_at = (flags & Self::FLAG_HAS_CREATED_AT) != 0;

                if !has_created_at {
                    return Err(SpatioError::InvalidFormat);
                }

                let mut ts_buf = [0u8; 8];
                reader.read_exact(&mut ts_buf)?;
                let timestamp = u64::from_be_bytes(ts_buf);
                let created_at = UNIX_EPOCH + Duration::from_secs(timestamp);

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
                    created_at,
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
        self.sync_with_mode(SyncMode::All)
    }

    /// Flush and sync using the provided mode.
    pub fn sync_with_mode(&mut self, mode: SyncMode) -> Result<()> {
        #[cfg(feature = "bench-prof")]
        let sync_start = Instant::now();
        self.writer.flush()?;
        match mode {
            SyncMode::All => self.file.sync_all()?,
            SyncMode::Data => self.file.sync_data()?,
        }
        #[cfg(feature = "bench-prof")]
        {
            let elapsed = sync_start.elapsed();
            self.profile.sync_ns += elapsed.as_nanos();
            self.profile.syncs += 1;
        }
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

        #[cfg(feature = "bench-prof")]
        {
            if std::thread::panicking() {
                return;
            }

            if self.profile.commands == 0 {
                return;
            }

            let avg_serialize = self.profile.serialize_ns / self.profile.commands as u128;
            let avg_write = self.profile.write_ns / self.profile.commands as u128;
            let avg_sync = if self.profile.syncs > 0 {
                self.profile.sync_ns / self.profile.syncs as u128
            } else {
                0
            };

            eprintln!(
                "[bench-prof] AOF stats: serialize avg = {} ns, write avg = {} ns over {} commands; sync avg = {} ns over {} calls",
                avg_serialize, avg_write, self.profile.commands, avg_sync, self.profile.syncs
            );
        }
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

        aof.write_set(b"key1", b"value1", None, SystemTime::now())
            .unwrap();
        assert!(aof.size() > 0);
    }

    #[test]
    fn test_command_replay() {
        let temp_file = NamedTempFile::new().unwrap();
        let mut aof = AOFFile::open(temp_file.path()).unwrap();

        let created_at = SystemTime::now();

        // Write some commands
        aof.write_set(b"key1", b"value1", None, created_at).unwrap();
        aof.write_delete(b"key2").unwrap();
        aof.flush().unwrap();

        // Replay commands
        let commands = aof.replay().unwrap();
        assert_eq!(commands.len(), 2);

        match &commands[0] {
            AOFCommand::Set {
                key,
                value,
                created_at: stored_created,
                expires_at,
            } => {
                assert_eq!(key.as_ref(), b"key1");
                assert_eq!(value.as_ref(), b"value1");
                assert!(expires_at.is_none());
                let delta = stored_created
                    .duration_since(created_at)
                    .unwrap_or_else(|_| created_at.duration_since(*stored_created).unwrap());
                assert!(delta.as_secs() < 2);
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

        aof.write_set(b"key1", b"value1", Some(&options), SystemTime::now())
            .unwrap();
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
            aof.write_set(key.as_bytes(), value.as_bytes(), None, SystemTime::now())
                .unwrap();
        }

        // Rewrite should have been triggered automatically (synchronous)
    }
}
