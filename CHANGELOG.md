# Changelog

All notable changes since the last release are documented below.

## [0.2.0] - 2025-01-XX

### Breaking Changes

- **Dropped Windows support**
  - Removed Windows from all CI/CD pipelines
  - Removed Windows-specific code and workarounds
  - Supported platforms: Linux (x86_64, aarch64) and macOS (x86_64, arm64)
  - Focus on Unix-like systems for better maintenance and performance
  - See [PLATFORMS.md](PLATFORMS.md) for details and migration options

- **Standardized coordinate order to (longitude, latitude)** across all APIs
  - **Rust API**: Already used `Point::new(lon, lat)` - no change needed
  - **Python API**: Changed from `Point(lat, lon)` to `Point(lon, lat)` for consistency
  - Aligns with GeoJSON standard, mathematical (x, y) convention, and industry standards
  - See [COORDINATE_ORDER.md](COORDINATE_ORDER.md) for detailed explanation
  - See [py-spatio/MIGRATION.md](py-spatio/MIGRATION.md) for Python migration guide

- **Removed geohash-based indexing** in favor of unified R-tree spatial index
  - Removed `Config::with_geohash_precision()` method
  - Removed `Config.geohash_precision` field
  - Removed `GeohashRTreeIndex` hybrid implementation
  - Removed `IndexType` enum

### Changed

- **Spatial Indexing**: Now uses single R*-tree implementation (`SpatialIndexManager`)
  - Native 3D support (latitude, longitude, altitude/z-coordinate)
  - Efficient 2D queries (use z=0 for 2D-only data)
  - Automatic optimization based on data distribution
  - No configuration tuning required

### Added

- Improved 3D spatial query performance
- Simplified index management without precision configuration
- **TTL cleanup helpers and documentation**:
  - `DB::count_expired()` - Monitor expired items without removing them
  - `SyncDB::count_expired()` - Thread-safe expired item count
  - Enhanced documentation with cleanup patterns and warnings
  - Production-ready examples for periodic cleanup
  - **⚠️ IMPORTANT**: TTL items require manual cleanup via `cleanup_expired()` to prevent memory leaks

### Python Bindings

- **BREAKING**: Changed `Point` constructor to use `(longitude, latitude)` order
  - **Old**: `Point(latitude, longitude)` - inconsistent with Rust and GeoJSON
  - **New**: `Point(longitude, latitude)` - matches GeoJSON standard and Rust API
  - This aligns with mathematical (x, y) convention and industry standards
  - See [COORDINATE_ORDER.md](COORDINATE_ORDER.md) for detailed explanation
- Removed `spatio.Config.with_geohash_precision()` static method
- Removed `spatio.Config.geohash_precision` property
- Updated all examples and documentation to use (lon, lat) order

### Documentation

- Updated README examples to use `Config::default()`
- Removed geohash references from feature descriptions
- Added Flow deployment configuration examples

## [0.1.6] - 2025-11-03

### Added
- bump script for types
- parking_lot
