//! HTTP/REST client for Spatio
//!
//! This module provides an HTTP client for Spatio, as an alternative to the tarpc transport.
//!
//! # Example
//!
//! ```ignore
//! use spatio_client::transport::http::SpatioHttpClient;
//!
//! let client = SpatioHttpClient::new("http://localhost:8080")?;
//! client.upsert("ns", "id", point, metadata, None).await?;
//! ```

// TODO: Implement HTTP client using reqwest
//
// Should mirror the same API as the tarpc SpatioClient for easy switching.

use spatio_types::point::Point3d;
use std::time::Duration;

/// HTTP client for Spatio (placeholder)
#[derive(Clone)]
#[allow(dead_code)]
pub struct SpatioHttpClient {
    base_url: String,
    // TODO: reqwest::Client
}

impl SpatioHttpClient {
    /// Create a new HTTP client
    pub fn new(base_url: &str) -> Result<Self, HttpClientError> {
        Ok(Self {
            base_url: base_url.to_string(),
        })
    }

    /// Upsert an object (placeholder)
    pub async fn upsert(
        &self,
        _namespace: &str,
        _id: &str,
        _point: Point3d,
        _metadata: serde_json::Value,
        _ttl: Option<Duration>,
    ) -> Result<(), HttpClientError> {
        unimplemented!("HTTP client not yet implemented")
    }

    // TODO: Implement all other methods matching SpatioClient API
}

/// Error type for HTTP client operations
#[derive(Debug, thiserror::Error)]
pub enum HttpClientError {
    #[error("HTTP request failed: {0}")]
    Request(String),
    #[error("Invalid response: {0}")]
    Response(String),
    #[error("Server error: {0}")]
    Server(String),
}
