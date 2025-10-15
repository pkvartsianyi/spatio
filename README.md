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

Add Spatio to your `Cargo.toml`:

```toml
[dependencies]
spatio = "0.1"
```

## Quick Start

```rust
use spatio::{Point, SetOptions, Spatio};
use std::time::Duration;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create an in-memory database
    let db = Spatio::memory()?;

    // Store simple key-value data
    db.insert("user:123", b"John Doe", None)?;

    // Store geographic points (automatically indexed)
    let nyc = Point::new(40.7128, -74.0060);
    let london = Point::new(51.5074, -0.1278);

    db.insert_point("cities", &nyc, b"New York", None)?;
    db.insert_point("cities", &london, b"London", None)?;

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

### Location-Based Services
- **Geofencing**: Track when objects enter/leave geographic regions
- **Proximity Search**: Find nearby points of interest, users, or services
- **Location Analytics**: Analyze spatial patterns and relationships

### Vehicle & Asset Tracking
- **Fleet Management**: Track vehicle locations and routes in real-time
- **Delivery Optimization**: Find nearest drivers or optimal delivery routes
- **Movement Analysis**: Analyze movement patterns and efficiency

### IoT & Sensor Networks
- **Environmental Monitoring**: Track sensor data with geographic context
- **Smart Cities**: Manage spatially-distributed infrastructure
- **Asset Management**: Monitor equipment locations and status

### Real-time Analytics
- **Geospatial Dashboards**: Build real-time location-aware applications
- **Event Processing**: Process location-based events with temporal context
- **Spatial Aggregation**: Compute statistics over geographic regions

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

Spatio is production-ready for embedded use cases. Current version: **0.1.0**

### Features
- Key-value storage with spatial indexing
- Geographic point operations
- Trajectory tracking
- TTL support
- Atomic operations
- Thread-safe concurrent access
- Comprehensive spatial queries

### Roadmap
- Enhanced persistence with full AOF replay
- Performance optimizations
- Additional spatial data types
- Query optimization

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

## License

MIT License ([LICENSE-MIT](LICENSE-MIT))

## Acknowledgments

- Built with the Rust ecosystem's excellent geospatial libraries
- Inspired by modern embedded databases and spatial indexing research
- Thanks to the Rust community for feedback and contributions
