//! Embedded spatio-temporal database with 2D/3D indexing, persistence, and TTL support.
//!
//! ```rust
//! use spatio::{Point, Spatio};
//!
//! let mut db = Spatio::memory()?;
//! db.insert("key", b"value", None)?;
//!
//! let point = Point::new(-74.0060, 40.7128);
//! db.insert_point("cities", &point, b"NYC", None)?;
//! let nearby = db.query_within_radius("cities", &point, 1000.0, 10)?;
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

pub use geo::{Point, Polygon, Rect};

pub use config::{
    BoundingBox2D, BoundingBox3D, Config, DbStats, Point3d, Polygon3D, PolygonDynamic,
    PolygonDynamic3D, SetOptions, SyncMode, SyncPolicy, TemporalBoundingBox2D,
    TemporalBoundingBox3D, TemporalPoint, TemporalPoint3D, Trajectory, Trajectory3D,
};

pub use compute::spatial::DistanceMetric;
#[cfg(feature = "time-index")]
pub use config::{HistoryEntry, HistoryEventKind};

pub use db::{AtomicBatch, Namespace, NamespaceManager};

pub use storage::{MemoryBackend, StorageBackend, StorageOp, StorageStats};

#[cfg(feature = "aof")]
pub use storage::{AOFConfig, AOFFile};

pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Common imports
pub mod prelude {

    pub use crate::{DBBuilder, Result, Spatio, SpatioError};

    #[cfg(feature = "sync")]
    pub use crate::SyncDB;

    pub use geo::{Point, Polygon, Rect};

    pub use crate::{Config, SetOptions, SyncPolicy};

    pub use crate::{AtomicBatch, Namespace, NamespaceManager};

    pub use crate::{MemoryBackend, StorageBackend};

    pub use std::time::Duration;
}
