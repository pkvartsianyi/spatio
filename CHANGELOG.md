# Changelog

All notable changes since the last release are documented below.

## [0.2.0] - 2025-01-XX

### Breaking Changes

- **Removed geohash-based indexing** in favor of unified R-tree spatial index
  - Removed `Config::with_geohash_precision()` method
  - Removed `Config.geohash_precision` field
  - Removed `GeohashRTreeIndex` hybrid implementation
  - Removed `IndexType` enum

### Changed

- **Spatial Indexing**: Now uses single R-tree implementation (`SpatialIndexManager`)
  - Native 3D support (latitude, longitude, altitude/z-coordinate)
  - Efficient 2D queries (use z=0 for 2D-only data)
  - Automatic optimization based on data distribution
  - No configuration tuning required

### Added

- Improved 3D spatial query performance
- Simplified index management without precision configuration

### Python Bindings

- Removed `spatio.Config.with_geohash_precision()` static method
- Removed `spatio.Config.geohash_precision` property
- Updated examples and documentation to reflect R-tree usage

### Documentation

- Updated README examples to use `Config::default()`
- Removed geohash references from feature descriptions
- Added Flow deployment configuration examples

## [0.1.6] - 2025-11-03

### Added
- bump script for types
- parking_lot
