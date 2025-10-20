<p align="center">
    <a href="https://github.com/pkvartsianyi/spatio">
        <img src="assets/images/logo-min.png" height="60" alt="Spatio Logo">
    </a>
</p>

<h1 align="center">Spatio</h1>

<p align="center">
  <a href="https://opensource.org/licenses/MIT">
    <img src="https://img.shields.io/badge/License-MIT-yellow.svg" alt="License: MIT">
  </a>
  <a href="https://crates.io/crates/spatio">
    <img src="https://img.shields.io/crates/v/spatio.svg" alt="Crates.io">
  </a>
  <a href="https://pypi.org/project/spatio">
    <img src="https://img.shields.io/pypi/v/spatio.svg" alt="PyPI">
  </a>
  <a href="https://docs.rs/spatio">
    <img src="https://img.shields.io/badge/Docs-Available-blue.svg" alt="Documentation">
  </a>
</p>

**Spatio** is a high-performance, embedded spatial database designed for applications that need to store and query location-based data efficiently. Built with a simple, SQLite-like architecture, Spatio provides powerful spatial operations optimized for single-threaded embedded use cases.

## Features

### **Embedded-First Design**
- **Simple Architecture**: Single-instance, RefCell-based design for embedded use
- **SQLite-like API**: Familiar open/close/read/write model
- **Minimal Dependencies**: No complex thread coordination or global state
- **Synchronous by Default**: Predictable behavior, background operations opt-in

### **High Performance**
- **Fast Key-Value Storage**: ~1.6M ops/sec with optimized in-memory operations
- **Automatic Spatial Indexing**: Geographic points indexed with geohash for efficient queries
- **Low Overhead**: No unnecessary locking or coordination complexity
- **Optional AOF Rewriting**: Configurable file compaction with size thresholds

### **Spatial Operations**
- **Spatial Queries**: Find nearby points, check intersections, and query bounding boxes
- **GeoJSON I/O**: Native support for GeoJSON import/export
- **Distance Calculations**: Haversine formula for accurate geographic distances
- **Trajectory Tracking**: Store and query movement paths over time

### **Data Management**
- **TTL Support**: Built-in data expiration for temporary data
- **Atomic Operations**: Batch multiple operations for data consistency
- **Durable Persistence**: Append-only file (AOF) with configurable paths and sync policies
- **Automatic Startup Replay**: Transparent state restoration on database open
- **Graceful Shutdown**: Automatic data sync on close and Drop
- **Truly Embedded**: No external dependencies, no servers, no setup required

## Installation

### Python

```bash
pip install spatio
```

📦 **PyPI**: https://pypi.org/project/spatio

### Rust

Add this to your `Cargo.toml`:

```toml
[dependencies]
spatio = "0.1"
```

📦 **Crates.io**: https://crates.io/crates/spatio

## Language Support

Spatio is available for multiple languages:

- **Rust** (native): High-performance, zero-cost abstractions
- **Python**: Easy-to-use bindings via PyO3

## Quick Start

### Python
```python
import spatio

# Create an in-memory database
db = spatio.Spatio.memory()

# Namespace support for data organization
namespace_a = spatio.Namespace.new("namespace_a")
namespace_b = spatio.Namespace.new("namespace_b")

# Store data with namespace isolation
db.insert(namespace_a.key("user:123"), b"John Doe")
db.insert(namespace_b.key("user:123"), b"Jane Smith")

# Create persistent database with custom AOF path
db = spatio.DBBuilder.new().aof_path("/data/myapp.aof").build()

# Store geographic points with automatic indexing
nyc = spatio.Point(40.7128, -74.0060)
db.insert_point("cities", nyc, b"New York City")

# Data automatically persists and syncs on close

# GeoJSON support
geojson = nyc.to_geojson()
point_from_json = spatio.Point.from_geojson(geojson)

# Find nearby points within 100km
nearby = db.find_nearby("cities", nyc, 100_000.0, 10)
print(f"Found {len(nearby)} cities nearby")
```

### Rust
```rust
use spatio::prelude::*;

fn main() -> Result<()> {
    // Simplified configuration with serialization
    let config = Config::with_geohash_precision(10)
        .with_default_ttl(Duration::from_secs(3600));
    
    // Create database with custom config
    let db = Spatio::memory_with_config(config)?;
    
    // Namespace support for data organization
    let namespace_a = Namespace::new("namespace_a");
    let namespace_b = Namespace::new("namespace_b");
    
    // Store data with namespace isolation
    db.insert(namespace_a.key("user:123"), b"John Doe", None)?;
    db.insert(namespace_b.key("user:123"), b"Jane Smith", None)?;

    // Create a point for spatial operations
    let nyc = Point::new(40.7128, -74.0060);
    
    // GeoJSON I/O support (requires "geojson" feature)
    #[cfg(feature = "geojson")]
    {
        let geojson = nyc.to_geojson()?;
        let point_from_geojson = Point::from_geojson(&geojson)?;
    }
    
    // Spatial operations with automatic indexing
    db.insert_point("cities", &nyc, b"New York City", None)?;
    let nearby = db.find_nearby("cities", &nyc, 100_000.0, 10)?;
    
    // Advanced spatial queries
    let count = db.count_within_distance("cities", &nyc, 100_000.0)?;
    let in_bounds = db.find_within_bounds("cities", 40.0, -75.0, 41.0, -73.0, 10)?;
    
    // Storage backend abstraction
    let memory_backend = MemoryBackend::new();
    
    // Feature-gated AOF backend
    #[cfg(feature = "aof")]
    let aof_backend = AOFBackend::new("data.aof")?;

    Ok(())
}
```

## Examples

Run the included examples to see Spatio in action:

### Getting Started
```bash
cargo run --example getting_started
```

### Spatial Queries
```bash
cargo run --example spatial_queries
```

### Trajectory Tracking
```bash
cargo run --example trajectory_tracking
```

### Architecture Demo (New!)
```bash
cargo run --example architecture_demo
```

### Comprehensive Demo
```bash
cargo run --example comprehensive_demo
```

## Use Cases

### Local Spatial Analytics
- **Proximity Search**: Efficiently find nearby features or points of interest
- **Containment Queries**: Check if points or geometries lie within defined areas
- **Spatial Relationships**: Analyse intersections, distances, and overlaps between geometries

### Edge & Embedded Systems
- **On-Device Processing**: Run spatial queries directly on IoT, drones, or edge devices
- **Offline Operation**: Perform location analytics without cloud or network access
- **Energy Efficiency**: Optimised for low memory and CPU usage in constrained environments

### Developer & Research Tools
- **Python Integration**: Use Spatio natively in data analysis or geospatial notebooks
- **Simulation Support**: Model trajectories and spatial behaviours locally
- **Lightweight Backend**: Ideal for prototypes, research projects, or local GIS tools

### Offline & Mobile Applications
- **Local Data Storage**: Keep spatial data close to the application
- **Fast Query Engine**: Sub-millisecond lookups for geometry and location queries
- **Self-Contained**: No external dependencies or server required

## API Overview

### Core Operations
```rust
// Basic key-value operations
db.insert("key", b"value", None)?;
let value = db.get("key")?;
db.delete("key")?;
```

### Spatial Operations
```rust
let point = Point::new(40.7128, -74.0060);

// Insert point with automatic spatial indexing
db.insert_point("namespace", &point, b"data", None)?;

// Find nearby points
let nearby = db.find_nearby("namespace", &point, 1000.0, 10)?;

// Check if points exist in region
let exists = db.contains_point("namespace", &point, 1000.0)?;

// Count points within distance
let count = db.count_within_distance("namespace", &point, 1000.0)?;

// Query bounding box
let in_bounds = db.find_within_bounds("namespace", 40.0, -75.0, 41.0, -73.0, 10)?;
let intersects = db.intersects_bounds("namespace", 40.0, -75.0, 41.0, -73.0)?;
```

### Trajectory Tracking
```rust
// Store movement over time
let trajectory = vec![
    (Point::new(40.7128, -74.0060), 1640995200),
    (Point::new(40.7150, -74.0040), 1640995260),
    (Point::new(40.7172, -74.0020), 1640995320),
];
db.insert_trajectory("vehicle:truck001", &trajectory, None)?;

// Query trajectory for time range
let path = db.query_trajectory("vehicle:truck001", 1640995200, 1640995320)?;
```

### Atomic Operations
```rust
db.atomic(|batch| {
    batch.insert("key1", b"value1", None)?;
    batch.insert("key2", b"value2", None)?;
    batch.delete("old_key")?;
    Ok(())
})?;
```

### Time-to-Live (TTL)
```rust
// Data expires in 1 hour
let opts = SetOptions::with_ttl(Duration::from_secs(3600));
db.insert("temp_key", b"temp_value", Some(opts))?;
```

## Performance

Spatio is designed for high performance:

- **In-memory operations** with microsecond latency
- **Automatic spatial indexing** using efficient geohash algorithms
- **Concurrent access** with read-write locks
- **Batch operations** for high-throughput scenarios
- **Optional persistence** with append-only file format

## Spatial Features

### Automatic Indexing
Points are automatically indexed using geohash for efficient spatial queries:
- O(log n) insertion and lookup
- Efficient range queries
- Automatic neighbor finding

### Distance Calculations
Built-in haversine distance calculations for accurate geographic distances:
```rust
let distance = point1.distance_to(&point2); // Returns meters
let nearby = point1.within_distance(&point2, 1000.0); // Within 1km
```

### Bounding Box Operations
```rust
use spatio::BoundingBox;

let bbox = BoundingBox::new(40.0, -75.0, 41.0, -73.0);
let intersects = bbox.intersects(&other_bbox);
```

## Development

### Building from Source
```bash
git clone https://github.com/pkvartsianyi/spatio
cd spatio
cargo build --release
```

### Running Tests
```bash
cargo test
```

### Running Benchmarks
```bash
cargo bench
```

### Documentation
```bash
cargo doc --open
```

## Architecture

Spatio uses a modern, extensible layered architecture:

### **Storage Layer**
- **Backend Abstraction**: Trait-based storage with pluggable implementations
- **Memory Backend**: High-performance in-memory B-trees with prefix operations
- **AOF Backend**: Append-only file storage with background compaction
- **Custom Backends**: Extensible design for RocksDB, SQLite, or cloud storage

### **Namespace Layer** 
- **Data Organization**: Isolated data with automatic key prefixing
- **Namespace Management**: Utilities for parsing and organizing namespaced keys
- **Configurable Separators**: Flexible namespace delimiter configuration

### **Indexing Layer**
- **Spatial Indexing**: Automatic geohash-based geographic point indexing
- **Configurable Precision**: Adjustable spatial resolution (1-12 levels)
- **Multi-Level Search**: Smart fallback across precision levels

### **Query Layer**
- **Optimized Execution**: Efficient spatial query processing
- **Multiple Query Types**: Point-in-radius, bounding box, nearest neighbor
- **Background Cleanup**: Automatic TTL-based data expiration

### **API Layer**
- **Clean Interface**: Organized public API with comprehensive prelude
- **Feature Flags**: Modular compilation for specific use cases
- **Serialization**: JSON/TOML config support with validation
- **Language Bindings**: Native Rust API with Python bindings

## Status

Spatio is in active development for embedded use cases. Current version: **0.1.0-alpha.10**

### **Core Architecture** (New!)
- **Storage Backend Abstraction**: Pluggable storage with trait-based design
- **Namespace Support**: Data isolation with automatic key prefixing  
- **Simplified Configuration**: JSON/TOML serializable config with validation
- **Feature Flags**: Modular compilation (serde, geojson, aof, toml)
- **Clean Public API**: Organized exports with comprehensive prelude module

### **Spatial & Data Features**
- **Enhanced Spatial Operations**: Point-in-radius, bounding box, trajectory tracking
- **GeoJSON I/O**: Native import/export for interoperability
- **Automatic Indexing**: Geohash-based spatial indexing with configurable precision
- **TTL Support**: Time-based data expiration with background cleanup
- **Thread Safety**: Concurrent read/write access with optimized locking

### **Persistence & Performance**
- **Enhanced AOF**: Background rewriting with configurable size thresholds
- **Memory Backend**: High-performance in-memory storage with prefix operations
- **Atomic Operations**: Batch operations for data consistency (Rust API)
- **Python Bindings**: Complete PyO3-based Python API via `pip install spatio`

### **In Development**
- **Python Atomic Operations**: Batch operations for Python API
- **Additional Storage Backends**: RocksDB, SQLite integration
- **Advanced Spatial Types**: Polygons, lines, and complex geometries
- **Query Optimization**: Enhanced spatial index performance

### **Performance Characteristics**
Based on current benchmarks:
- **Key-value operations**: ~1.6M ops/sec (600ns per operation)
- **Spatial insertions**: ~1.9M points/sec (530ns per operation)  
- **Spatial queries**: ~225K queries/sec (4.4μs per operation)
- **Memory efficiency**: Optimized storage with spatial indexing and background compaction

### **Production Readiness**
- **Alpha Status**: Enhanced architecture stabilizing, may have breaking changes
- **Testing**: Comprehensive test suite with 20+ unit tests and integration tests
- **Documentation**: Complete API documentation with architectural examples
- **Extensibility**: Plugin architecture ready for custom storage backends
- **Language Support**: Rust (native) and Python (bindings)

## Contributing

Contributions are welcome! Please read our [Contributing Guidelines](CONTRIBUTING.md) before submitting pull requests.

### Development Setup
```bash
git clone https://github.com/pkvartsianyi/spatio
cd spatio
cargo test
cargo clippy
cargo fmt
```

## Links & Resources

### Package Repositories
- **PyPI**: https://pypi.org/project/spatio
- **Crates.io**: https://crates.io/crates/spatio

### Documentation & Source
- **GitHub Repository**: https://github.com/pkvartsianyi/spatio
- **Rust Documentation**: https://docs.rs/spatio
- **Python Documentation**: https://github.com/pkvartsianyi/spatio/tree/main/py-spatio

### Community
- **Issues & Bug Reports**: https://github.com/pkvartsianyi/spatio/issues
- **Releases & Changelog**: https://github.com/pkvartsianyi/spatio/releases

## License

MIT License ([LICENSE](LICENSE))

## Acknowledgments

- Built with the Rust ecosystem's excellent geospatial libraries
- Inspired by modern embedded databases and spatial indexing research
- Thanks to the Rust community for feedback and contributions
