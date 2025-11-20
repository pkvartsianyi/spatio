//! Embedded spatio-temporal database with 2D/3D indexing, persistence, and lazy TTL support.
//!
//! ## Features
//! - **Spatial indexing**: 2D/3D points, polygons, bounding boxes with R*-tree spatial indexing
//! - **Persistence**: Append-only file (AOF) with configurable sync policies
//! - **Lazy TTL**: Expired items are filtered on read, manual cleanup available
//! - **Atomic batches**: Group multiple operations atomically
//! - **Temporal queries**: Filter by creation time (with `time-index` feature)
//!
//! ## TTL Behavior
//! TTL is **passive/lazy**:
//! - Expired items return `None` on `get()` and are skipped in queries
//! - Items remain in storage until overwritten or manually cleaned with `cleanup_expired()`
//! - No automatic background cleanup or deletion on insert
//!
//! ```rust
//! use spatio::{Point, Spatio, SetOptions};
//! use std::time::Duration;
//!
//! let mut db = Spatio::memory()?;
//! db.insert("key", b"value", None)?;
//!
//! // TTL example (lazy expiration)
//! let opts = SetOptions::with_ttl(Duration::from_secs(60));
//! db.insert("temp", b"expires_soon", Some(opts))?;
//!
//! // Spatial example
//! let point = Point::new(-74.0060, 40.7128);
//! db.insert_point("cities", &point, b"NYC", None)?;
//! let nearby = db.query_within_radius("cities", &point, 1000.0, 10)?;
//!
//! // Manual cleanup of expired items
//! let removed = db.cleanup_expired()?;
//! # Ok::<(), spatio::SpatioError>(())
//! ```

pub mod builder;
pub mod compute;
pub mod config;
pub mod db;
pub mod error;
pub mod storage;

pub use builder::DBBuilder;
pub use db::DB;
pub use error::{Result, SpatioError};

#[cfg(feature = "sync")]
pub use db::SyncDB;

pub type Spatio = DB;

pub use geo::Rect;
pub use spatio_types::geo::{Point, Polygon};

pub use config::{
    BoundingBox2D, BoundingBox3D, Config, DbStats, Point3d, Polygon3D, PolygonDynamic,
    PolygonDynamic3D, SetOptions, SyncMode, SyncPolicy, TemporalBoundingBox2D,
    TemporalBoundingBox3D, TemporalPoint, TemporalPoint3D, Trajectory, Trajectory3D,
};

pub use compute::spatial::DistanceMetric;
#[cfg(feature = "time-index")]
pub use config::{HistoryEntry, HistoryEventKind};

pub use db::{AtomicBatch, ExpiredStats, Namespace, NamespaceManager};

// Re-export validation and GeoJSON utilities
pub use compute::geojson;
pub use compute::validation;

#[cfg(feature = "aof")]
pub use storage::{AOFCommand, PersistenceLog};
pub use storage::{MemoryBackend, StorageBackend, StorageOp, StorageStats};

#[cfg(feature = "snapshot")]
pub use storage::{SnapshotConfig, SnapshotFile};

pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Common imports
pub mod prelude {

    pub use crate::{DBBuilder, Result, Spatio, SpatioError};

    #[cfg(feature = "sync")]
    pub use crate::SyncDB;

    pub use crate::{Point, Polygon};
    pub use geo::Rect;

    pub use crate::{Config, SetOptions, SyncPolicy};

    pub use crate::{AtomicBatch, ExpiredStats, Namespace, NamespaceManager};

    pub use crate::{MemoryBackend, StorageBackend};

    pub use crate::{geojson, validation};

    pub use std::time::Duration;
}
