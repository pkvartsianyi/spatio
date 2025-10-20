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

**Spatio** is a fast, embedded spatial database designed for applications that need to store and query location-based data efficiently. Built with simplicity and performance in mind, Spatio provides a clean API for spatial operations without the complexity of traditional GIS systems.

## Features

- **Fast Key-Value Storage**: High-performance in-memory operations with optional persistence
- **Automatic Spatial Indexing**: Geographic points are automatically indexed for efficient queries
- **Spatial Queries**: Find nearby points, check intersections, and query bounding boxes
- **Trajectory Tracking**: Store and query movement paths over time
- **TTL Support**: Built-in data expiration for temporary data
- **Atomic Operations**: Batch multiple operations for data consistency
- **Thread-Safe**: Concurrent read/write access without blocking
- **Embedded**: No external dependencies or setup required
- **Simple API**: Clean, focused interface that's easy to learn and use

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

# Store a simple key-value pair
db.insert(b"user:123", b"John Doe")

# Store a geographic point (automatically indexed)
nyc = spatio.Point(40.7128, -74.0060)
db.insert_point("cities", nyc, b"New York City")

# Find nearby points within 100km
nearby = db.find_nearby("cities", nyc, 100_000.0, 10)
print(f"Found {len(nearby)} cities nearby")
```

### Rust
```rust
use spatio::{Point, SetOptions, Spatio};
use std::time::Duration;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create an in-memory database
    let db = Spatio::memory()?;

    // Store a simple key-value pair
    db.insert("user:123", b"John Doe", None)?;

    // Store a geographic point (automatically indexed)
    let nyc = Point::new(40.7128, -74.0060);
    db.insert_point("cities", &nyc, b"New York City", None)?;

    // Find nearby points within 100km
    let nearby = db.find_nearby("cities", &nyc, 100_000.0, 10)?;
    println!("Found {} cities nearby", nearby.len());

    // Check if points exist in a region
    let has_cities = db.contains_point("cities", &nyc, 50_000.0)?;
    println!("Cities within 50km: {}", has_cities);

    // Count points within distance
    let count = db.count_within_distance("cities", &nyc, 100_000.0)?;
    println!("City count within 100km: {}", count);

    // Find points in bounding box
    let in_area = db.find_within_bounds("cities", 40.0, -75.0, 41.0, -73.0, 10)?;
    println!("Cities in area: {}", in_area.len());

    // Atomic batch operations
    db.atomic(|batch| {
        batch.insert("sensor:temp", b"22.5C", None)?;
        batch.insert("sensor:humidity", b"65%", None)?;
        Ok(())
    })?;

    // Data with TTL (expires in 5 minutes)
    let opts = SetOptions::with_ttl(Duration::from_secs(300));
    db.insert("session:abc", b"user_data", Some(opts))?;

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

Spatio uses a layered architecture:
- **Storage Layer**: In-memory B-trees with optional AOF persistence
- **Indexing Layer**: Automatic geohash-based spatial indexing
- **Query Layer**: Optimized spatial query execution
- **API Layer**: Clean, type-safe Rust interface

## Status

Spatio is in active development for embedded use cases.

### Completed Features
- **Core Database**: Key-value storage with B-tree indexing
- **Spatial Operations**: Automatic geohash-based spatial indexing
- **Geographic Queries**: Point-in-radius, bounding box, and nearest neighbor searches
- **Trajectory Tracking**: Time-series storage and querying for moving objects
- **TTL Support**: Automatic data expiration with time-to-live
- **Atomic Operations**: Batch operations for data consistency (Rust API)
- **Thread Safety**: Concurrent read/write access with RwLock
- **Persistence**: Optional append-only file (AOF) storage
- **Python Bindings**: Complete PyO3-based Python API via `pip install spatio`

### In Development
- **Python Atomic Operations**: Batch operations for Python API
- **Enhanced Persistence**: Full AOF replay and compaction
- **Performance Optimizations**: Spatial index improvements
- **Additional Spatial Types**: Polygons, lines, and complex geometries

### Performance Characteristics
Based on current benchmarks:
- **Key-value operations**: ~1.6M ops/sec (600ns per operation)
- **Spatial insertions**: ~1.9M points/sec (530ns per operation)
- **Spatial queries**: ~225K queries/sec (4.4μs per operation)
- **Memory efficiency**: Optimized in-memory storage with spatial indexing

### Production Readiness
- **Alpha Status**: API is stabilizing but may have breaking changes
- **Testing**: Comprehensive test suite with 20 unit tests and 13 integration tests
- **Documentation**: Complete API documentation and examples
- **Benchmarks**: Performance regression testing in place
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
