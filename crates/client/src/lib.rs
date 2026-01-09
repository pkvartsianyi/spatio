//! Spatio Client
//!
//! Native Rust client for Spatio spatio-temporal database.
//!
//! # Transports
//!
//! - **RPC** (default): High-performance tarpc-based transport
//! - **HTTP** (optional): REST API client, enable with `http` feature
//!
//! # Example
//!
//! ```ignore
//! use spatio_client::SpatioClient;
//!
//! let client = SpatioClient::connect(addr).await?;
//! client.upsert("ns", "id", point, metadata, None).await?;
//! ```

pub mod transport;

// Re-export the default (RPC) client for convenience
pub use transport::rpc::{ClientError, Result, SpatioClient};

#[cfg(feature = "http")]
pub use transport::http::{HttpClientError, SpatioHttpClient};
