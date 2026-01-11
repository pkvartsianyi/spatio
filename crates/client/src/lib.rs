//! Spatio Client
//!
//! Native Rust RPC client for Spatio database.
//!
//! # Example
//!
//! ```ignore
//! use spatio_client::SpatioClient;
//!
//! let client = SpatioClient::connect("127.0.0.1:3000".parse()?).await?;
//! client.upsert("ns", "id", point, metadata).await?;
//! ```

mod transport;

// Re-export transport
pub use transport::rpc::{ClientError, Result, SpatioClient};

// Re-export server types for convenience
pub use spatio_server::{CurrentLocation, LocationUpdate, Stats};
