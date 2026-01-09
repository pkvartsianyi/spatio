//! Spatio Server
//!
//! High-performance server for Spatio spatio-temporal database.
//!
//! # Transports
//!
//! - **RPC** (default): High-performance tarpc-based transport
//! - **HTTP** (optional): REST API, enable with `http` feature
//!
//! # Example
//!
//! ```ignore
//! use spatio_server::run_server;
//!
//! run_server(addr, db, shutdown).await?;
//! ```

pub mod handler;
pub mod protocol;
pub mod transport;

// Re-export protocol types for client usage
pub use protocol::{
    CurrentLocation, LocationUpdate, SpatioService, SpatioServiceClient, Stats, UpsertOptions,
};

// Re-export default transport for convenience
pub use transport::rpc::run_server;
