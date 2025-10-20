# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Enhanced spatial query methods: `contains_point`, `intersects_bounds`, `count_within_distance`, `find_within_bounds`
- New `BoundingBox` struct for bounding box operations
- Point-level spatial methods: `within_distance`, `contains_point`
- Comprehensive test suite with 15 unit tests and 12 integration tests
- Enhanced spatial indexing with multi-precision geohash search
- Professional documentation and examples

### Changed
- Improved spatial indexing performance with smart fallback strategies
- Updated API documentation with comprehensive examples
- Simplified repository structure and removed outdated files

### Fixed
- Spatial indexing now correctly finds nearby points across different geohash regions
- Linter warnings resolved across all modules

## [0.1.0] - 2025-10-20

### Added
- Initial release of Spatio embedded spatial database
- Core key-value storage with B-tree indexing
- Automatic spatial indexing for geographic points using geohash
- Trajectory tracking for moving objects over time
- Time-to-Live (TTL) support for data expiration
- Atomic batch operations for data consistency
- Thread-safe concurrent access with read-write locks
- Optional persistence with append-only file (AOF) format
- Distance calculations using Haversine formula
- Comprehensive examples: getting_started, spatial_queries, trajectory_tracking, comprehensive_demo
- Full documentation with API reference and usage guides

### Features
- **Storage**: Fast in-memory key-value operations
- **Spatial**: Automatic spatial indexing and nearby point queries
- **Temporal**: Trajectory storage and time-range queries
- **Performance**: Optimized for high-throughput scenarios
- **Safety**: Thread-safe operations with atomic guarantees
- **Persistence**: Optional disk storage with AOF format

### Supported Operations
- `insert`, `get`, `delete` - Basic key-value operations
- `insert_point` - Store geographic points with automatic indexing
- `find_nearby` - Find points within radius with distance sorting
- `insert_trajectory` - Store movement paths over time
- `query_trajectory` - Query movement data for time ranges
- `atomic` - Batch multiple operations atomically

### Dependencies
- `geo` - Geospatial types and operations
- `geohash` - Geographic hash generation
- `s2` - Spherical geometry library
- `serde` - Serialization framework
- `bytes` - Efficient byte buffer management
- `thiserror` - Error handling utilities

[Unreleased]: https://github.com/pkvartsianyi/spatio/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/pkvartsianyi/spatio/releases/tag/v0.1.0
