//! Storage backend implementations.
//!
//! This module provides different storage backends for Spatio:
//! - `MemoryBackend`: Fast in-memory storage
//! - `AOFBackend`: Persistent append-only file storage (requires `aof` feature)

#[cfg(feature = "aof")]
mod aof;
mod memory;

#[cfg(feature = "aof")]
pub use aof::AOFBackend;
pub use memory::MemoryBackend;
