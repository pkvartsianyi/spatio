//! # Spatio - A simple embedded spatial database
//!
//! Spatio is a fast, embedded spatial database designed for applications
//! that need to store and query location-based data efficiently.
//!
//! ## Core Features
//!
//! - **Fast key-value storage** with optional persistence
//! - **Automatic spatial indexing** for geographic points
//! - **Trajectory tracking** for moving objects over time
//! - **TTL support** for automatic data expiration
//! - **Atomic operations** for data consistency
//! - **Thread-safe** concurrent access
//!
//! ## Quick Start
//!
//! ```rust
//! use spatio::{Point, SetOptions, Spatio};
//! use std::time::Duration;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! // Create an in-memory database
//! let db = Spatio::memory()?;
//!
//! // Store a simple key-value pair
//! db.insert("user:123", b"John Doe", None)?;
//!
//! // Store a geographic point (automatically indexed)
//! let nyc = Point::new(40.7128, -74.0060);
//! db.insert_point("cities", &nyc, b"New York City", None)?;
//!
//! // Find nearby points within 100km
//! let nearby = db.find_nearby("cities", &nyc, 100_000.0, 10)?;
//! println!("Found {} cities nearby", nearby.len());
//!
//! // Atomic batch operations
//! db.atomic(|batch| {
//!     batch.insert("sensor:temp", b"22.5C", None)?;
//!     batch.insert("sensor:humidity", b"65%", None)?;
//!     Ok(())
//! })?;
//!
//! // Data with TTL (expires in 5 minutes)
//! let opts = SetOptions::with_ttl(Duration::from_secs(300));
//! db.insert("session:abc", b"user_data", Some(opts))?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Trajectory Tracking
//!
//! ```rust
//! use spatio::{Point, Spatio};
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let db = Spatio::memory()?;
//!
//! // Track a vehicle's movement over time
//! let trajectory = vec![
//!     (Point::new(40.7128, -74.0060), 1640995200), // Start
//!     (Point::new(40.7150, -74.0040), 1640995260), // 1 min later
//!     (Point::new(40.7172, -74.0020), 1640995320), // 2 min later
//! ];
//!
//! db.insert_trajectory("vehicle:truck001", &trajectory, None)?;
//!
//! // Query trajectory for a time range
//! let path = db.query_trajectory("vehicle:truck001", 1640995200, 1640995320)?;
//! println!("Retrieved {} waypoints", path.len());
//! # Ok(())
//! # }
//! ```
//!
//! ## Spatial Queries
//!
//! ```rust
//! use spatio::{Point, Spatio};
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let db = Spatio::memory()?;
//!
//! // Insert some cities
//! let nyc = Point::new(40.7128, -74.0060);
//! let brooklyn = Point::new(40.6782, -73.9442);
//! db.insert_point("cities", &nyc, b"New York", None)?;
//! db.insert_point("cities", &brooklyn, b"Brooklyn", None)?;
//!
//! // Check if any points exist within a circular region
//! let has_nearby = db.contains_point("cities", &nyc, 50_000.0)?; // 50km radius
//! assert!(has_nearby);
//!
//! // Count points within distance
//! let count = db.count_within_distance("cities", &nyc, 50_000.0)?;
//! println!("Found {} cities within 50km", count);
//!
//! // Check if any points exist within a bounding box
//! let has_points = db.intersects_bounds("cities", 40.6, -74.1, 40.8, -73.9)?;
//! assert!(has_points);
//!
//! // Find all points within a bounding box
//! let points = db.find_within_bounds("cities", 40.6, -74.1, 40.8, -73.9, 100)?;
//! println!("Found {} cities in the area", points.len());
//! # Ok(())
//! # }
//! ```

pub mod batch;
pub mod db;
pub mod error;
pub mod index;
pub mod persistence;
pub mod spatial;
pub mod types;

// Core exports
pub use db::DB;
pub use error::{Result, SpatioError};

// Main database type
pub type Spatio = DB;

// Spatial types
pub use spatial::{BoundingBox, Point};

// Configuration and options
pub use types::{Config, DbStats, SetOptions, SyncPolicy};

// Batch operations
pub use batch::AtomicBatch;

// Version information
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Prelude module for common imports
pub mod prelude {
    pub use crate::{BoundingBox, Point, Result, SetOptions, Spatio, SpatioError};
    pub use std::time::Duration;
}
