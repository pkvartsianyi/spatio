//! Embedded spatio-temporal database with 2D/3D indexing, persistence, and TTL support.
//!
//! ```rust
//! use spatio::{Point, Spatio};
//!
//! let db = Spatio::memory()?;
//! db.insert("key", b"value", None)?;
//!
//! let point = Point::new(-74.0060, 40.7128);
//! db.insert_point("cities", &point, b"NYC", None)?;
//! let nearby = db.query_within_radius("cities", &point, 1000.0, 10)?;
//! # Ok::<(), spatio::SpatioError>(())
//! ```

pub mod batch;
pub mod builder;
pub mod db;
pub mod error;
pub mod ffi;
pub mod namespace;
pub mod spatial;
pub mod spatial_index;
pub mod storage;
pub mod types;

#[cfg(feature = "aof")]
pub mod persistence;

pub use builder::DBBuilder;
pub use db::DB;
pub use error::{Result, SpatioError};

pub type Spatio = DB;

pub use geo::{Point, Polygon, Rect};

pub use spatial::{DistanceMetric, bounding_box, convex_hull, distance_between, knn};

pub use spatial_index::{BBoxQuery, CylinderQuery};

pub use types::{
    BoundingBox2D, BoundingBox3D, Config, DbStats, Point3d, Polygon3D, PolygonDynamic,
    PolygonDynamic3D, SetOptions, SyncMode, SyncPolicy, TemporalBoundingBox2D,
    TemporalBoundingBox3D, TemporalPoint, TemporalPoint3D, Trajectory, Trajectory3D,
};
#[cfg(feature = "time-index")]
pub use types::{HistoryEntry, HistoryEventKind};

pub use namespace::{Namespace, NamespaceManager};

pub use storage::{MemoryBackend, StorageBackend, StorageOp, StorageStats};

#[cfg(feature = "aof")]
pub use storage::AOFBackend;

pub use batch::AtomicBatch;

#[cfg(feature = "aof")]
pub use persistence::{AOFConfig, AOFFile};

pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Common imports
pub mod prelude {

    pub use crate::{DBBuilder, Result, Spatio, SpatioError};

    pub use geo::{Point, Polygon, Rect};

    pub use crate::spatial::{DistanceMetric, bounding_box, distance_between, knn};

    pub use crate::{Config, SetOptions, SyncPolicy};

    pub use crate::{Namespace, NamespaceManager};

    pub use crate::{MemoryBackend, StorageBackend};

    #[cfg(feature = "aof")]
    pub use crate::AOFBackend;

    pub use std::time::Duration;
}
