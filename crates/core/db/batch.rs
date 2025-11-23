// Atomic batch operations.
//
// Note: Atomic batches are not supported in the new Hot/Cold architecture.
// This struct is kept for API compatibility but operations will return errors or be no-ops.

use crate::config::SetOptions;
use crate::db::DB;
use crate::error::{Result, SpatioError};

/// Atomic batch.
///
/// **Deprecated**: Not supported in Hot/Cold architecture.
pub struct AtomicBatch<'a> {
    _db: &'a mut DB,
}

impl<'a> AtomicBatch<'a> {
    pub(crate) fn new(db: &'a mut DB) -> Self {
        Self { _db: db }
    }

    pub fn insert(
        &mut self,
        _key: impl AsRef<[u8]>,
        _value: impl AsRef<[u8]>,
        _opts: Option<SetOptions>,
    ) -> Result<()> {
        Err(SpatioError::NotSupported(
            "AtomicBatch not supported in Hot/Cold architecture".into(),
        ))
    }

    pub fn delete(&mut self, _key: impl AsRef<[u8]>) -> Result<()> {
        Err(SpatioError::NotSupported(
            "AtomicBatch not supported in Hot/Cold architecture".into(),
        ))
    }

    pub(crate) fn commit(self) -> Result<()> {
        Ok(())
    }
}
