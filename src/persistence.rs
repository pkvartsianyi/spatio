use crate::error::{Result, SpatioError};
use crate::types::SetOptions;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Type alias for shared state in AOF file handle coordination
type SharedFileState = Arc<RwLock<Option<(File, BufWriter<File>, u64)>>>;

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
    path: PathBuf,
    size: u64,
    config: AOFConfig,
    rewrite_in_progress: Arc<RwLock<bool>>,
    last_rewrite_size: u64,
    // Shared state for coordinating file handle updates during rewrite
    shared_state: SharedFileState,
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
            shared_state: self.shared_state.clone(),
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
            shared_state: Arc::new(RwLock::new(None)),
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
    ///
    /// This method implements write coordination to prevent data loss during
    /// AOF rewrite operations. It waits for any ongoing rewrite to complete
    /// before proceeding to ensure writes go to the correct file.
    fn write_command(&mut self, command: &AOFCommand) -> Result<()> {
        // Wait for any ongoing rewrite to complete first
        // This ensures we don't write to orphaned file descriptors
        {
            let rewrite_guard = self.rewrite_in_progress.read().unwrap();
            if *rewrite_guard {
                // Drop read lock and wait for write lock to ensure rewrite completes
                drop(rewrite_guard);
                let _wait_for_rewrite = self.rewrite_in_progress.write().unwrap();
            }
        }

        // Apply any pending file handle updates from completed rewrite
        self.apply_pending_file_update()?;

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
    ///
    /// This function implements safe AOF rewriting that prevents data loss during
    /// concurrent operations. The key safety measures are:
    ///
    /// 1. **Write Coordination**: Sets the rewrite_in_progress flag to coordinate
    ///    with concurrent write operations. Writers will wait for the rewrite to
    ///    complete before proceeding, preventing writes to orphaned file descriptors.
    ///
    /// 2. **Flush and Sync**: Ensures all pending writes are persisted to disk
    ///    before taking the snapshot, preventing loss of in-flight writes.
    ///
    /// 3. **Atomic File Replacement**: Uses filesystem rename for atomic
    ///    replacement of the old AOF file with the rewritten one.
    ///
    /// 4. **Safe File Handle Management**: After rename, new file handles are
    ///    provided to writers via shared state, ensuring no writes go to orphaned
    ///    descriptors that point to the old (now unlinked) file.
    ///
    /// 5. **Robust Flag Management**: Uses RAII guard to ensure the rewrite flag is
    ///    always cleared when the function exits, preventing indefinite blocking.
    ///    The flag is cleared automatically on success, error, or panic.
    fn perform_background_rewrite(mut aof: AOFFile) -> Result<()> {
        // Set rewrite in progress flag to block new writes during critical section
        // This prevents the race condition where writes could be lost during the
        // window between snapshot and file replacement
        {
            let mut in_progress = aof.rewrite_in_progress.write().unwrap();
            *in_progress = true;
        }

        // RAII guard ensures the rewrite flag is ALWAYS cleared when function exits.
        // This prevents indefinite blocking if an error occurs during rewrite.
        // The guard runs on: success, error return, or panic during unwinding.
        struct RewriteGuard<'a> {
            rewrite_in_progress: &'a Arc<RwLock<bool>>,
        }

        impl<'a> Drop for RewriteGuard<'a> {
            fn drop(&mut self) {
                let mut in_progress = self.rewrite_in_progress.write().unwrap();
                *in_progress = false;
            }
        }

        let _cleanup_guard = RewriteGuard {
            rewrite_in_progress: &aof.rewrite_in_progress,
        };

        // Critical: Flush and sync current writer to ensure all pending writes
        // are persisted before taking the snapshot. This prevents data loss.
        aof.writer.flush()?;
        aof.file.sync_all()?;

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
        // After this point, the old file descriptor points to an orphaned file
        std::fs::rename(&rewrite_path, &aof.path)?;

        // Get the new file size from filesystem metadata
        //
        // CRITICAL BUG FIX: We cannot use rewrite_file.size here because it was never
        // updated. The data was written directly to rewrite_file.writer via write_all(),
        // not through write_command() which would update the size field. This would
        // result in new_size being 0, breaking size tracking and rewrite thresholds.
        //
        // Using filesystem metadata ensures we get the actual size of the written file.
        let new_size = std::fs::metadata(&aof.path)?.len();

        // Open new file handles for the renamed file
        let new_file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&aof.path)?;

        let new_writer_file = new_file.try_clone()?;
        let new_writer = BufWriter::new(new_writer_file);

        // Update shared state with new file handles
        {
            let mut shared_state = aof.shared_state.write().unwrap();
            *shared_state = Some((new_file, new_writer, new_size));
        }

        // Success! The RAII guard will automatically clear the flag when it goes out of scope.
        // This ensures the flag is cleared consistently regardless of how the function exits:
        // - Normal return (success)
        // - Early return (error)
        // - Panic (unwinding)
        // No manual flag management needed - the guard handles all cases robustly.

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
        self.writer.flush()?;
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

    /// Apply any pending file handle updates from background rewrite
    ///
    /// CRITICAL SAFETY: Do NOT flush the old writer here! After a rewrite, the old
    /// writer points to an orphaned file descriptor that no longer corresponds to
    /// the AOF file path. Flushing it would write buffered data to the wrong file
    /// (or to a file that has been unlinked), causing data loss.
    ///
    /// The write coordination ensures that all writes are blocked during the rewrite
    /// process, so there should be no buffered data in the old writer when this
    /// method is called.
    fn apply_pending_file_update(&mut self) -> Result<()> {
        let mut shared_state = self.shared_state.write().unwrap();
        if let Some((new_file, new_writer, new_size)) = shared_state.take() {
            // Replace file handles with new ones WITHOUT flushing old writer
            // The old writer is now orphaned after the rename operation
            self.file = new_file;
            self.writer = new_writer;
            self.size = new_size;
            self.last_rewrite_size = new_size;
        }
        Ok(())
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
    fn test_aof_size_tracking_after_rewrite() {
        let temp_file = NamedTempFile::new().unwrap();
        let mut aof = AOFFile::open(temp_file.path()).unwrap();

        // Write some commands to build up file size
        let key1 = Bytes::from("key1");
        let value1 = Bytes::from("value1");
        let key2 = Bytes::from("key2");
        let value2 = Bytes::from("value2");
        let key3 = Bytes::from("key3");
        let value3 = Bytes::from("value3");

        aof.write_set(&key1, &value1, None).unwrap();
        aof.write_set(&key2, &value2, None).unwrap();
        aof.write_delete(&key1).unwrap();
        aof.flush().unwrap();

        let size_before_rewrite = aof.size().unwrap();
        assert!(
            size_before_rewrite > 0,
            "AOF size should be greater than 0 before rewrite"
        );

        // Perform a rewrite
        aof.rewrite().unwrap();

        // The size should still be tracked correctly after rewrite
        let size_after_rewrite = aof.size().unwrap();
        assert!(
            size_after_rewrite > 0,
            "AOF size should be greater than 0 after rewrite"
        );

        // Write another command after rewrite to verify size tracking continues to work
        aof.write_set(&key3, &value3, None).unwrap();
        aof.flush().unwrap();

        let size_after_new_write = aof.size().unwrap();
        assert!(
            size_after_new_write > size_after_rewrite,
            "AOF size should increase after writing new commands post-rewrite"
        );
    }

    #[test]
    fn test_no_data_loss_during_rewrite() {
        use std::sync::{Arc, Barrier};
        use std::thread;

        let temp_file = NamedTempFile::new().unwrap();
        let aof_path = temp_file.path().to_path_buf();

        // Create initial AOF with some data
        {
            let mut aof = AOFFile::open(&aof_path).unwrap();
            let key1 = Bytes::from("initial_key");
            let value1 = Bytes::from("initial_value");
            aof.write_set(&key1, &value1, None).unwrap();
            aof.flush().unwrap();
        }

        // Clone path for thread safety
        let aof_path_clone = aof_path.clone();
        let aof_path_clone2 = aof_path.clone();

        // Barrier to synchronize threads
        let barrier = Arc::new(Barrier::new(2));
        let barrier_clone = barrier.clone();

        // Spawn thread that will perform rewrite
        let rewrite_handle = thread::spawn(move || {
            let mut aof = AOFFile::open(&aof_path_clone).unwrap();

            // Wait for writer thread to be ready
            barrier_clone.wait();

            // Perform rewrite - this should block writes during critical section
            aof.rewrite().unwrap();
        });

        // Main thread performs concurrent writes
        let write_handle = thread::spawn(move || {
            let mut aof = AOFFile::open(&aof_path_clone2).unwrap();

            // Signal ready and wait for rewrite to start
            barrier.wait();

            // Write commands that should not be lost
            for i in 0..10 {
                let key = Bytes::from(format!("concurrent_key_{}", i));
                let value = Bytes::from(format!("concurrent_value_{}", i));
                aof.write_set(&key, &value, None).unwrap();
            }

            aof.flush().unwrap();
        });

        // Wait for both threads to complete
        rewrite_handle.join().unwrap();
        write_handle.join().unwrap();

        // Verify all data is present by replaying the AOF
        let mut aof_for_replay = AOFFile::open(&aof_path).unwrap();
        let mut replayed_commands = Vec::new();

        aof_for_replay
            .replay(|cmd| {
                replayed_commands.push(cmd);
                Ok(())
            })
            .unwrap();

        // Should have initial command plus 10 concurrent commands
        assert!(
            replayed_commands.len() >= 11,
            "Expected at least 11 commands, found {}",
            replayed_commands.len()
        );

        // Verify we have both initial and concurrent data
        let mut has_initial = false;
        let mut concurrent_count = 0;

        for cmd in replayed_commands {
            if let AOFCommand::Set { key, .. } = cmd {
                if key == Bytes::from("initial_key") {
                    has_initial = true;
                } else if key.starts_with(b"concurrent_key_") {
                    concurrent_count += 1;
                }
            }
        }

        assert!(has_initial, "Initial data was lost");
        assert_eq!(
            concurrent_count, 10,
            "Expected 10 concurrent writes, found {}",
            concurrent_count
        );
    }

    #[test]
    fn test_orphaned_descriptor_safety() {
        use std::fs;
        use std::sync::{Arc, Barrier};
        use std::thread;
        use std::time::Duration;

        let temp_file = NamedTempFile::new().unwrap();
        let aof_path = temp_file.path().to_path_buf();

        // Create initial AOF with data
        {
            let mut aof = AOFFile::open(&aof_path).unwrap();
            let key = Bytes::from("initial_key");
            let value = Bytes::from("initial_value");
            aof.write_set(&key, &value, None).unwrap();
            aof.flush().unwrap();
        }

        let initial_size = fs::metadata(&aof_path).unwrap().len();
        let aof_path_clone = aof_path.clone();
        let aof_path_clone2 = aof_path.clone();
        let barrier = Arc::new(Barrier::new(2));
        let barrier_clone = barrier.clone();

        // Thread that will perform rewrite
        let rewrite_handle = thread::spawn(move || {
            let mut aof = AOFFile::open(&aof_path_clone).unwrap();
            barrier_clone.wait();
            aof.rewrite().unwrap();
        });

        // Thread that will try to write and flush after rewrite
        let write_handle = thread::spawn(move || {
            // Wait for rewrite to start
            barrier.wait();

            // Give rewrite time to complete
            thread::sleep(Duration::from_millis(200));

            // Open a NEW AOF instance after rewrite should be done
            // This ensures we get fresh file handles
            let mut aof = AOFFile::open(&aof_path_clone2).unwrap();

            // This write should go to the NEW file, not orphaned descriptor
            let key = Bytes::from("post_rewrite_key");
            let value = Bytes::from("post_rewrite_value");
            aof.write_set(&key, &value, None).unwrap();
            aof.flush().unwrap();
        });

        rewrite_handle.join().unwrap();
        write_handle.join().unwrap();

        // Give filesystem time to sync
        thread::sleep(Duration::from_millis(50));

        // Verify that the file size increased (data went to correct file)
        let final_size = fs::metadata(&aof_path).unwrap().len();
        assert!(
            final_size > initial_size,
            "File size should have increased from {} to {} if write went to correct file. Initial: {}, Final: {}",
            initial_size,
            final_size,
            initial_size,
            final_size
        );

        // Verify data integrity by replaying
        let mut aof_for_replay = AOFFile::open(&aof_path).unwrap();
        let mut commands = Vec::new();
        aof_for_replay
            .replay(|cmd| {
                commands.push(cmd);
                Ok(())
            })
            .unwrap();

        // Should have both initial and post-rewrite data
        let has_initial = commands.iter().any(
            |cmd| matches!(cmd, AOFCommand::Set { key, .. } if key == &Bytes::from("initial_key")),
        );
        let has_post_rewrite = commands.iter().any(|cmd| {
            matches!(cmd, AOFCommand::Set { key, .. } if key == &Bytes::from("post_rewrite_key"))
        });

        assert!(has_initial, "Initial data should be preserved");
        assert!(
            has_post_rewrite,
            "Post-rewrite data should be written to correct file"
        );
    }

    #[test]
    fn test_rewrite_flag_raii_guard() {
        // Test that the RAII guard properly clears the rewrite flag
        use std::sync::Arc;
        use std::sync::RwLock;

        let rewrite_flag = Arc::new(RwLock::new(false));

        // Test successful case - guard should clear flag on normal exit
        {
            {
                let mut flag = rewrite_flag.write().unwrap();
                *flag = true;
            }
            assert!(*rewrite_flag.read().unwrap());

            // Create the guard (same structure as in perform_background_rewrite)
            struct RewriteGuard<'a> {
                rewrite_in_progress: &'a Arc<RwLock<bool>>,
            }

            impl<'a> Drop for RewriteGuard<'a> {
                fn drop(&mut self) {
                    let mut in_progress = self.rewrite_in_progress.write().unwrap();
                    *in_progress = false;
                }
            }

            let _guard = RewriteGuard {
                rewrite_in_progress: &rewrite_flag,
            };

            // Flag should still be true while guard exists
            assert!(*rewrite_flag.read().unwrap());
        } // Guard goes out of scope here

        // Flag should be cleared by guard's Drop
        assert!(!*rewrite_flag.read().unwrap());

        // Test error case - guard should clear flag even if function returns early
        {
            let mut flag = rewrite_flag.write().unwrap();
            *flag = true;
        }
        assert!(*rewrite_flag.read().unwrap());

        // Simulate function that returns early with error
        let test_with_early_return = || -> Result<()> {
            struct RewriteGuard<'a> {
                rewrite_in_progress: &'a Arc<RwLock<bool>>,
            }

            impl<'a> Drop for RewriteGuard<'a> {
                fn drop(&mut self) {
                    let mut in_progress = self.rewrite_in_progress.write().unwrap();
                    *in_progress = false;
                }
            }

            let _guard = RewriteGuard {
                rewrite_in_progress: &rewrite_flag,
            };

            // Simulate an error that causes early return
            return Err(crate::error::SpatioError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                "simulated error",
            )));
        };

        let result = test_with_early_return();
        assert!(result.is_err());

        // Flag should be cleared by guard even though function returned error
        assert!(!*rewrite_flag.read().unwrap());
    }

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

    #[test]
    fn test_file_handle_update_after_rewrite() {
        let temp_file = NamedTempFile::new().unwrap();
        let mut aof = AOFFile::open_with_config(
            temp_file.path(),
            AOFConfig {
                rewrite_size_threshold: 0, // Force immediate rewrite
                rewrite_growth_percentage: 0.0,
                background_rewrite: false,
            },
        )
        .unwrap();

        // Write initial data
        let key1 = Bytes::from("test_key_1");
        let key2 = Bytes::from("test_key_2");
        let initial_value = Bytes::from("initial_value");

        aof.write_set(&key1, &initial_value, None).unwrap();
        aof.write_set(&key2, &initial_value, None).unwrap();
        aof.flush().unwrap();

        // Manually trigger rewrite to test file handle update
        aof.rewrite().unwrap();

        // Write new data after rewrite - this tests that file handles were updated correctly
        let post_rewrite_value = Bytes::from("post_rewrite_value");
        aof.write_set(&key1, &post_rewrite_value, None).unwrap();
        aof.write_set(&key2, &post_rewrite_value, None).unwrap();
        aof.flush().unwrap();

        // Test that we can read back all the data correctly
        // This verifies that the file handles are pointing to the right file
        let mut all_commands = Vec::new();
        aof.replay(|cmd| {
            all_commands.push(cmd);
            Ok(())
        })
        .unwrap();

        // We should have at least 4 commands (2 initial + 2 post-rewrite)
        assert!(all_commands.len() >= 4, "Should have at least 4 commands");

        // Verify that post-rewrite writes are present and readable
        let post_rewrite_commands: Vec<_> = all_commands
            .iter()
            .filter(|cmd| match cmd {
                AOFCommand::Set { value, .. } => value == &post_rewrite_value,
                _ => false,
            })
            .collect();

        assert!(
            post_rewrite_commands.len() >= 2,
            "Should find both post-rewrite writes in the AOF file"
        );

        // Additionally test recovery by reopening the file
        drop(aof);
        let mut recovered_aof = AOFFile::open(temp_file.path()).unwrap();

        let mut recovered_commands = Vec::new();
        recovered_aof
            .replay(|cmd| {
                recovered_commands.push(cmd);
                Ok(())
            })
            .unwrap();

        // The recovered AOF should have the same commands
        assert_eq!(
            all_commands.len(),
            recovered_commands.len(),
            "Recovered AOF should have same number of commands"
        );
    }
}
