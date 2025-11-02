//! Atomic batch operations.

use crate::DB;
use crate::config::SetOptions;
use crate::error::Result;
use bytes::Bytes;

/// Atomic batch. All operations succeed or all fail.
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
        let mut inner = self.db.write()?;

        if inner.closed {
            return Err(crate::error::SpatioError::DatabaseClosed);
        }

        for operation in self.operations {
            match operation {
                BatchOperation::Insert { key, value, opts } => {
                    let item = crate::config::DbItem::from_options(value.clone(), opts.as_ref());
                    let created_at = item.created_at;
                    inner.insert_item(key.clone(), item);
                    inner.write_to_aof_if_needed(
                        &key,
                        value.as_ref(),
                        opts.as_ref(),
                        created_at,
                    )?;
                }
                BatchOperation::Delete { key } => {
                    inner.remove_item(&key);
                    inner.write_delete_to_aof_if_needed(&key)?;
                }
            }
        }

        Ok(())
    }
}
