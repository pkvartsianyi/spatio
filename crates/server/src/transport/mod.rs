//! Transport layer for Spatio server
//!
//! Available transports:
//! - `rpc` - tarpc-based RPC (default, high performance)
//! - `http` - HTTP/REST API (requires `http` feature)

pub mod rpc;

#[cfg(feature = "http")]
pub mod http;
