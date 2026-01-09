//! HTTP/REST transport for Spatio server
//!
//! This module provides an HTTP API for Spatio, as an alternative to the tarpc transport.
//!
//! # Example
//!
//! ```ignore
//! use spatio_server::transport::http::run_server;
//!
//! run_server("0.0.0.0:8080".parse()?, db).await?;
//! ```

// TODO: Implement HTTP server using axum
//
// Planned endpoints:
// - POST   /v1/{namespace}/objects           - upsert
// - GET    /v1/{namespace}/objects/{id}      - get
// - DELETE /v1/{namespace}/objects/{id}      - delete
// - POST   /v1/{namespace}/query/radius      - query_radius
// - POST   /v1/{namespace}/query/knn         - knn
// - POST   /v1/{namespace}/query/bbox        - query_bbox
// - POST   /v1/{namespace}/query/polygon     - contains
// - GET    /v1/{namespace}/stats             - stats
// - GET    /v1/{namespace}/bbox              - bounding_box
// - GET    /v1/{namespace}/hull              - convex_hull

use spatio::Spatio;
use std::net::SocketAddr;
use std::sync::Arc;

/// Run the HTTP server (placeholder)
///
/// # Errors
/// Returns an error if the server fails to start.
#[cfg(feature = "http")]
pub async fn run_server(_addr: SocketAddr, _db: Arc<Spatio>) -> anyhow::Result<()> {
    unimplemented!("HTTP server not yet implemented. Implement using axum.")
}
