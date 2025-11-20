// Atomic batch operations.

use super::DBInner;
use crate::config::SetOptions;
use crate::error::Result;
use bytes::Bytes;

/// Atomic batch. All operations succeed or all fail.
pub struct AtomicBatch<'a> {
    inner: &'a mut DBInner,
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

impl<'a> AtomicBatch<'a> {
    pub(crate) fn new(inner: &'a mut DBInner) -> Self {
        Self {
            inner,
            operations: Vec::new(),
        }
    }
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

    pub fn delete(&mut self, key: impl AsRef<[u8]>) -> Result<()> {
        let op = BatchOperation::Delete {
            key: Bytes::copy_from_slice(key.as_ref()),
        };
        self.operations.push(op);
        Ok(())
    }

    pub(crate) fn commit(self) -> Result<()> {
        if self.inner.closed {
            return Err(crate::error::SpatioError::DatabaseClosed);
        }

        // First pass: Apply all in-memory operations and collect AOF data
        let mut aof_ops = Vec::with_capacity(self.operations.len());

        for operation in self.operations {
            match operation {
                BatchOperation::Insert { key, value, opts } => {
                    let item = crate::config::DbItem::from_options(value.clone(), opts.as_ref());
                    let created_at = item.created_at;
                    self.inner.insert_item(key.clone(), item);

                    // Collect for batch AOF write
                    aof_ops.push((key, value, opts, created_at, false));
                }
                BatchOperation::Delete { key } => {
                    self.inner.remove_item(&key);

                    // Collect for batch AOF write (using dummy values for delete)
                    aof_ops.push((key, Bytes::new(), None, std::time::SystemTime::now(), true));
                }
            }
        }

        // Second pass: Write all operations to AOF in one batch
        self.inner.write_batch_to_aof(&aof_ops)?;

        Ok(())
    }
}
