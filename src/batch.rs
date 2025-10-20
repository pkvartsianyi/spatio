use crate::error::Result;
use crate::types::SetOptions;
use crate::DB;
use bytes::Bytes;

/// Atomic batch for grouping multiple operations together.
///
/// All operations in a batch are applied atomically - either all succeed
/// or all fail. This ensures data consistency when performing multiple
/// related operations.
///
/// # Examples
///
/// ```rust
/// use spatio::Spatio;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let db = Spatio::memory()?;
///
/// // All operations succeed or all fail
/// db.atomic(|batch| {
///     batch.insert("user:123", b"John Doe", None)?;
///     batch.insert("email:john@example.com", b"user:123", None)?;
///     batch.insert("session:abc", b"user:123", None)?;
///     Ok(())
/// })?;
/// # Ok(())
/// # }
/// ```
pub struct AtomicBatch {
    db: DB,
    operations: Vec<BatchOperation>,
}

#[derive(Debug, Clone)]
enum BatchOperation {
    Insert {
        key: Bytes,
        value: Bytes,
        opts: Option<SetOptions>,
    },
    Delete {
        key: Bytes,
    },
}

impl AtomicBatch {
    pub(crate) fn new(db: DB) -> Self {
        Self {
            db,
            operations: Vec::new(),
        }
    }

    /// Insert a key-value pair in this batch.
    ///
    /// The operation will be queued and executed when the batch is committed.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to insert
    /// * `value` - The value to associate with the key
    /// * `opts` - Optional settings like TTL
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spatio::{Spatio, SetOptions};
    /// use std::time::Duration;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let db = Spatio::memory()?;
    ///
    /// db.atomic(|batch| {
    ///     batch.insert("key1", b"value1", None)?;
    ///
    ///     let opts = SetOptions::with_ttl(Duration::from_secs(300));
    ///     batch.insert("key2", b"value2", Some(opts))?;
    ///     Ok(())
    /// })?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn insert(
        &mut self,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
        opts: Option<SetOptions>,
    ) -> Result<()> {
        let op = BatchOperation::Insert {
            key: Bytes::copy_from_slice(key.as_ref()),
            value: Bytes::copy_from_slice(value.as_ref()),
            opts,
        };
        self.operations.push(op);
        Ok(())
    }

    /// Delete a key in this batch.
    ///
    /// The operation will be queued and executed when the batch is committed.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to delete
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spatio::Spatio;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let db = Spatio::memory()?;
    ///
    /// // First insert some data
    /// db.insert("temp_key", b"temp_value", None)?;
    ///
    /// // Then delete it in a batch
    /// db.atomic(|batch| {
    ///     batch.delete("temp_key")?;
    ///     batch.insert("new_key", b"new_value", None)?;
    ///     Ok(())
    /// })?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn delete(&mut self, key: impl AsRef<[u8]>) -> Result<()> {
        let op = BatchOperation::Delete {
            key: Bytes::copy_from_slice(key.as_ref()),
        };
        self.operations.push(op);
        Ok(())
    }

    /// Commit all operations in this batch atomically.
    ///
    /// This is called automatically when the batch closure returns successfully.
    /// All operations are applied in the order they were added to the batch.
    pub(crate) fn commit(self) -> Result<()> {
        // Apply all operations atomically
        let mut inner = self.db.write()?;

        for operation in &self.operations {
            match operation {
                BatchOperation::Insert { key, value, opts } => {
                    let item = match opts {
                        Some(SetOptions { ttl: Some(ttl), .. }) => {
                            crate::types::DbItem::with_ttl(value.clone(), *ttl)
                        }
                        Some(SetOptions {
                            expires_at: Some(expires_at),
                            ..
                        }) => crate::types::DbItem::with_expiration(value.clone(), *expires_at),
                        _ => crate::types::DbItem::new(value.clone()),
                    };
                    inner.insert_item(key.clone(), item);
                }
                BatchOperation::Delete { key } => {
                    inner.remove_item(key);
                }
            }
        }

        // Write operations to AOF if needed
        for operation in &self.operations {
            match operation {
                BatchOperation::Insert { key, value, opts } => {
                    inner.write_to_aof_if_needed(key, value.as_ref(), opts.as_ref())?;
                }
                BatchOperation::Delete { key } => {
                    inner.write_delete_to_aof_if_needed(key)?;
                }
            }
        }

        Ok(())
    }
}
