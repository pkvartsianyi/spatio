# Spatio Workspace Structure

This document describes the monorepo workspace structure for the Spatio project.

## Overview

Spatio is organized as a Cargo workspace with multiple crates:

```
spatio/
├── Cargo.toml              # Workspace root
├── crates/
│   ├── core/               # Main spatio library
│   ├── types/              # Core types and traits
│   └── py/                 # Python bindings (spatio-py)
├── docs/                   # Documentation
├── scripts/                # Build and release scripts
└── assets/                 # Assets and resources
```

## Crates

### `crates/core` - Main Library

**Package name:** `spatio`  
**Published as:** `spatio` on crates.io

The core implementation of the Spatio spatio-temporal database.

**Features:**
- `default` = `["snapshot", "geojson", "time-index"]`
- `geojson` - GeoJSON import/export support
- `aof` - Append-Only File persistence
- `snapshot` - Snapshot persistence
- `toml` - TOML configuration support
- `time-index` - Temporal indexing and queries
- `sync` - Thread-safe synchronous API
- `bench-prof` - Benchmarking and profiling support
- `full` - All features enabled

**Building:**
```bash
# Build with default features
cargo build -p spatio

# Build with all features
cargo build -p spatio --all-features

# Build specific features
cargo build -p spatio --features "aof,snapshot"
```

**Testing:**
```bash
# Run all tests
cargo test -p spatio --all-features

# Run specific tests
cargo test -p spatio --test aof_rewrite_test --features aof
```

**Examples:**
```bash
# Run examples
cargo run -p spatio --example getting_started
cargo run -p spatio --example spatial_queries
cargo run -p spatio --example trajectory_tracking
cargo run -p spatio --example persistence_lifecycle --features aof
```

**Benchmarks:**
```bash
cargo bench -p spatio
```

### `crates/types` - Core Types

**Package name:** `spatio-types`  
**Published as:** `spatio-types` on crates.io

Core spatial and temporal data types used across the Spatio ecosystem.

**Contains:**
- `Point3d` - 3D point representation
- `BoundingBox2D` / `BoundingBox3D` - Spatial bounding boxes
- `TemporalPoint` / `TemporalPoint3D` - Time-stamped points
- `Trajectory` / `Trajectory3D` - Movement paths over time
- Other shared type definitions

**Building:**
```bash
cargo build -p spatio-types
cargo test -p spatio-types
```

### `crates/py` - Python Bindings

**Package name:** `spatio-py`  
**Published as:** `spatio` on PyPI

Python bindings for Spatio using PyO3 and Maturin.

**Requirements:**
- Python 3.9+
- Maturin build system

**Building:**
```bash
cd crates/py

# Build and install in development mode
maturin develop

# Build wheel
maturin build --release

# Run Python tests
pytest tests/
```

**Note:** If you encounter Python version compatibility issues with PyO3:
```bash
export PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1
cargo build -p spatio-py
```

## Workspace Commands

### Build All Crates (except Python bindings)

```bash
# Build core and types
cargo build -p spatio -p spatio-types

# Build with all features
cargo build -p spatio -p spatio-types --all-features
```

### Test All Crates

```bash
# Test core and types
cargo test -p spatio -p spatio-types --all-features

# Test everything including Python (if environment is set up)
cargo test --workspace --all-features
```

### Format and Lint

```bash
# Format all Rust code
cargo fmt --all

# Run clippy
cargo clippy --workspace --all-features -- -D warnings
```

### Check Everything

```bash
# Quick check without building
cargo check --workspace --all-features
```

## Development Workflow

### Adding a New Crate

1. Create directory: `crates/new-crate/`
2. Add to workspace members in root `Cargo.toml`:
   ```toml
   [workspace]
   members = [
       "crates/core",
       "crates/types",
       "crates/py",
       "crates/new-crate",  # Add here
   ]
   ```
3. Create `crates/new-crate/Cargo.toml` using workspace inheritance:
   ```toml
   [package]
   name = "spatio-new-crate"
   version.workspace = true
   edition.workspace = true
   rust-version.workspace = true
   license.workspace = true
   repository.workspace = true
   
   [dependencies]
   spatio = { path = "../core" }
   # ... other deps
   ```

### Using Workspace Dependencies

Shared dependencies are defined in the root `Cargo.toml`:

```toml
[workspace.dependencies]
geo = "0.31.0"
serde = { version = "1.0", features = ["derive"] }
# ... etc
```

In crate `Cargo.toml`:
```toml
[dependencies]
geo.workspace = true
serde.workspace = true
```

### Cross-Crate References

```toml
# In crates/core/Cargo.toml
[dependencies]
spatio-types = { version = "0.1.7", path = "../types" }

# In crates/py/Cargo.toml
[dependencies]
spatio = { path = "../core", features = ["sync"] }
```

## Release Process

### Version Management

Versions are managed at the workspace level in the root `Cargo.toml`:

```toml
[workspace.package]
version = "0.1.7"
```

All crates inherit this version using:
```toml
version.workspace = true
```

### Publishing Crates

**Order matters!** Publish dependencies first:

1. **spatio-types** (no dependencies)
   ```bash
   cd crates/types
   cargo publish
   ```

2. **spatio** (depends on spatio-types)
   ```bash
   cd crates/core
   cargo publish
   ```

3. **spatio-py** (depends on spatio)
   ```bash
   cd crates/py
   maturin publish
   ```

## CI/CD Integration

### GitHub Actions Example

```yaml
name: CI

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      # Test Rust crates
      - name: Test spatio-types
        run: cargo test -p spatio-types
      
      - name: Test spatio (default features)
        run: cargo test -p spatio
      
      - name: Test spatio (all features)
        run: cargo test -p spatio --all-features
      
      # Test Python bindings
      - uses: actions/setup-python@v4
        with:
          python-version: '3.11'
      
      - name: Test Python bindings
        run: |
          cd crates/py
          pip install maturin pytest
          maturin develop
          pytest tests/
```

## Future Crates (Planned)

- `crates/redb` - redb persistence backend
- `crates/rocksdb` - RocksDB persistence backend  
- `crates/server` - Cloud server with REST API
- `crates/cli` - Command-line interface tool
- `crates/sync` - Replication and synchronization

## Troubleshooting

### Python bindings won't build

**Issue:** `error: the configured Python interpreter version (3.14) is newer than PyO3's maximum supported version (3.13)`

**Solution:**
```bash
export PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1
cargo build -p spatio-py
```

Or exclude from default workspace builds and build separately.

### Path references broken after restructure

Ensure all path references use the correct relative paths:
- From `crates/core` to `crates/types`: `path = "../types"`
- From `crates/py` to `crates/core`: `path = "../core"`

### Tests not found

Tests, examples, and benches must be in the appropriate crate directory:
- `crates/core/tests/`
- `crates/core/examples/`
- `crates/core/benches/`

## Additional Resources

- [Cargo Workspaces Documentation](https://doc.rust-lang.org/cargo/reference/workspaces.html)
- [PyO3 Documentation](https://pyo3.rs/)
- [Maturin Documentation](https://www.maturin.rs/)