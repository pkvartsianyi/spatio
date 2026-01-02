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
//! ## Example
//! ```
//! use spatio::{Point3d, Spatio};
//!
//! let db = Spatio::memory()?;
//!
//! // Spatial example
//! let point = Point3d::new(-74.0060, 40.7128, 0.0);
//! db.upsert("cities", "nyc", point.clone(), serde_json::json!({"name": "NYC"}), None)?;
//! let nearby = db.query_radius("cities", &point, 1000.0, 10)?;
//!
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
    TemporalBoundingBox3D, TemporalPoint, TemporalPoint3D,
};

pub use compute::spatial::DistanceMetric;
#[cfg(feature = "time-index")]
pub use config::{HistoryEntry, HistoryEventKind};

pub use db::{Namespace, NamespaceManager};

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

    pub use crate::{Namespace, NamespaceManager};

    pub use crate::{MemoryBackend, StorageBackend};

    pub use crate::{geojson, validation};

    pub use std::time::Duration;
}
