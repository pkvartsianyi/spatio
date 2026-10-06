//! Embedded spatio-temporal database with 2D/3D indexing and persistence.
//!
//! ## Features
//! - **Spatial indexing**: 2D/3D points, polygons, bounding boxes with R*-tree spatial indexing
//! - **Persistence**: CRC-checked append-only log with configurable sync policies and checkpoint recovery
//! - **Temporal queries**: Trajectory history per object over a time range
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

pub use builder::DBBuilder;
pub use db::DB;
pub use error::{Result, SpatioError};

#[doc(inline)]
pub use db::DB as Spatio;

pub use geo::Rect;
pub use spatio_types::geo::{Point, Polygon};

pub use config::{
    BoundingBox2D, BoundingBox3D, Config, DbStats, Point3d, Polygon3D, PolygonDynamic,
    PolygonDynamic3D, SetOptions, SyncMode, SyncPolicy, TemporalBoundingBox2D,
    TemporalBoundingBox3D, TemporalPoint, TemporalPoint3D,
};

pub use compute::spatial::DistanceMetric;

pub use compute::validation;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Common imports
pub mod prelude {

    pub use crate::{DBBuilder, Result, Spatio, SpatioError};

    pub use crate::{Point, Polygon};
    pub use geo::Rect;

    pub use crate::{Config, SetOptions, SyncPolicy};

    pub use crate::validation;

    pub use std::time::Duration;
}
