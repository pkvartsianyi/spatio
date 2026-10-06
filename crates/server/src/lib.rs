//! Spatio Server
//!
//! High-performance tarpc-based RPC server for Spatio spatio-temporal database.
//!
//! # Example
//!
//! ```ignore
//! use spatio_server::run_server;
//!
//! run_server(listener, db, shutdown).await?;
//! ```

pub mod handler;
pub mod protocol;
pub mod reader;
mod rpc;

// Re-export protocol types for client usage
pub use protocol::{CurrentLocation, LocationUpdate, SpatioService, SpatioServiceClient, Stats};

pub use rpc::{MAX_FRAME_BYTES, run_server};
