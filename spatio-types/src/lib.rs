//! # spatio-types
//!
//! Core spatial and temporal data types for the Spatio database.
//!
//! This crate provides fundamental types for working with spatio-temporal data:
//!
//! - **Point types**: `TemporalPoint`, `TemporalPoint3D`, `Point3d`
//! - **Polygon types**: `Polygon3D`, `PolygonDynamic`, `PolygonDynamic3D`
//! - **Trajectory types**: `Trajectory`, `Trajectory3D`
//!
//! All types are serializable with Serde and built on top of the `geo` crate's
//! geometric primitives.
//!
//! ## Examples
//!
//! ```rust
//! use spatio_types::point::TemporalPoint;
//! use geo::Point;
//! use std::time::SystemTime;
//!
//! let point = Point::new(-74.0060, 40.7128); // NYC coordinates
//! let temporal_point = TemporalPoint::new(point, SystemTime::now());
//! ```

pub mod point;
pub mod polygon;
pub mod trajectory;
