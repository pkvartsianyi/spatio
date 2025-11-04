//! Compute layer for query processing and algorithms.
//!
//! This module separates computation and query logic from storage concerns.
//! It provides:
//! - Spatial algorithms and indexing
//! - Temporal query processing
//! - Query execution engine
//!
//! The compute layer is independent of storage implementation and focuses
//! on data processing, indexing, and query optimization.

pub mod spatial;
pub mod temporal;

