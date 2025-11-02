# Changelog

All notable changes since the last release are documented below.

## [0.1.4] - 2024-12-XX

### Added
- Structured logging with `log` crate - diagnostic messages now use proper logging framework
- Comprehensive edge case test suite (15 new tests) for production hardening
- Logging documentation in README with setup instructions
- Tests for large datasets, concurrent writes, extreme coordinates, and edge boundaries

### Changed
- Replaced `eprintln!` diagnostic statements with structured `log::warn!` and `log::error!` calls
- Improved code style with modern Rust idioms (collapsible if statements using let chains)

### Fixed
- Clippy warnings for collapsible if statements (7 occurrences)
- Minor code style improvements for better readability

### Developer Experience
- Added `env_logger` to dev dependencies for testing
- Enhanced example files with logging initialization
- Better error context in log messages for debugging

## [0.1.3] - 2025-10-31

### Changed
- 3D Point and Python tools (#47)
- cleanup: refactor test_point_validation (#38)

