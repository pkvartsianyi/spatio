//! Persistence strategies for Spatio.
//!
//! This module provides different persistence strategies:
//! - `AOFBackend`: Append-only file persistence (requires `aof` feature)
//! - `SnapshotFile`: Point-in-time snapshot persistence (requires `snapshot` feature)

#[cfg(feature = "aof")]
mod aof;
#[cfg(feature = "snapshot")]
pub mod snapshot;

#[cfg(feature = "aof")]
pub use aof::{AOFBackend, AOFCommand, AOFConfig, AOFFile, PersistenceLog};

#[cfg(feature = "snapshot")]
pub use snapshot::{SnapshotConfig, SnapshotFile};
