//! # spatio-types
//!
//! Core spatial and temporal data types for the Spatio database.
//!
//! This crate provides fundamental types for working with spatio-temporal data:
//!
//! - **Point types**: `TemporalPoint`, `TemporalPoint3D`, `Point3d`
//! - **Polygon types**: `Polygon3D`, `PolygonDynamic`, `PolygonDynamic3D`
//! - **Trajectory types**: `Trajectory`, `Trajectory3D`
//! - **Bounding box types**: `BoundingBox2D`, `BoundingBox3D`, `TemporalBoundingBox2D`, `TemporalBoundingBox3D`
//!
//! All types are serializable with Serde and built on top of the `geo` crate's
//! geometric primitives.
//!
//! ## Examples
//!
//! ```rust
//! use spatio_types::point::TemporalPoint;
//! use spatio_types::bbox::BoundingBox2D;
//! use geo::Point;
//! use std::time::SystemTime;
//!
//! // Create a temporal point
//! let point = Point::new(-74.0060, 40.7128); // NYC coordinates
//! let temporal_point = TemporalPoint::new(point, SystemTime::now());
//!
//! // Create a bounding box
//! let manhattan = BoundingBox2D::new(-74.0479, 40.6829, -73.9067, 40.8820);
//! assert!(manhattan.contains_point(&point));
//! ```

pub mod bbox;
pub mod point;
pub mod polygon;
pub mod trajectory;
