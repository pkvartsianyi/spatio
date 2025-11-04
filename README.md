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
  <a href="https://pkvartsianyi.github.io/spatio/">
    <img src="https://img.shields.io/badge/Docs-Available-blue.svg" alt="Documentation">
  </a>
  <a href="https://docs.rs/spatio">
    <img src="https://img.shields.io/badge/docs.rs-spatio-66c2a5" alt="Rust Docs">
  </a>
</p>

**Spatio** is a compact and efficient **embedded spatio-temporal database** written in Rust.
It's designed for **real-time 2D and 3D location data**, with **low memory usage**, **optional persistence**, and **native Python bindings**.

No SQL parser, no external dependencies, and requires no setup.

---

## Features

### Embedded and Lightweight
- **Self-contained** — Runs without external services or dependencies
- **Minimal API surface** — Open, insert, and query
- **Low memory footprint** — Suitable for IoT, edge, and embedded environments
- **Single-Writer Thread Safety** — Uses a shared RwLock (without lock upgrades) to allow concurrent readers and a single writer

### Performance Scope
- **Spatio-temporal queries** — Use a geohash + R-tree hybrid to balance lookup precision and performance for moderate datasets
- **Configurable persistence** — Snapshot-based (default) or AOF with sync policies
- **Startup and Shutdown** — Persistence files are loaded automatically on startup

### Spatio-Temporal Indexing and Querying
- **Spatio-Temporal Indexing** — R-Tree + geohash hybrid indexing with optional history tracking
- **Advanced Spatial Operations** — Distance calculations (Haversine, Geodesic, Rhumb, Euclidean), K-nearest-neighbors, polygon queries, convex hull, bounding box operations
- **Spatio-Temporal Queries** — Nearby search, bounding box, distance, containment, and time slicing
- **3D Spatial Support** — Full 3D point indexing with altitude-aware queries (spherical, cylindrical, bounding box)
- **Trajectory Support** — Store and query movement over time (2D and 3D)
- **GeoJSON I/O** — Supports import and export of geometries in GeoJSON format

### Data Management
- **Namespaces** — Isolate data logically within the same instance
- **TTL Support** — Lazy expiration on read with manual cleanup
- **Temporal Queries** — Filter keys by recent activity with optional history tracking
- **Atomic Batches** — Supports grouped write operations with atomic semantics
- **Custom Configs** — JSON/TOML serializable configuration

### Language Support
- **Rust** — Native
- **Python** — Provides bindings implemented via PyO3 (`pip install spatio`)

### Compile-Time Feature Flags
- `time-index` *(default)* — enables creation-time indexing and per-key history APIs. Disable it for the lightest build: `cargo add spatio --no-default-features --features="snapshot,geojson"`.
- `snapshot` *(default)* — enables snapshot-based persistence (point-in-time saves)
- `aof` — enables append-only file persistence (write-ahead log style)

### Sync Strategy Configuration
- `SyncMode::All` *(default)* — call `fsync`/`File::sync_all` after each batch
  of writes (durable but slower).
- `SyncMode::Data` — use `fdatasync`/`File::sync_data` when your platform
  supports it (data-only durability).
- `sync_batch_size` — number of write operations to batch before a sync when
  `SyncPolicy::Always` is selected (default: 1). Tune via
  `Config::with_sync_batch_size` to reduce syscall frequency.

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


## Quick Start

Python usage lives in the dedicated bindings package—see `py-spatio/README.md`
for up-to-date installation notes and examples.

### Rust
```rust
use spatio::prelude::*;
use spatio::Point3d;
use std::time::Duration;

fn main() -> Result<()> {
    // Configure the database
    let config = Config::with_geohash_precision(9)
        .with_default_ttl(Duration::from_secs(3600));

    // Create an in-memory database with configuration
    let db = Spatio::memory_with_config(config)?;

    // Create a namespace for logical separation
    let ns = db.namespace("vehicles");

    // Insert a point (automatically indexed)
    let truck = Point::new(-74.0060, 40.7128);
    ns.insert_point("truck:001", &truck, b"Truck A", None)?;

    // Query for nearby points
    let results = ns.query_within_radius(&truck, 1000.0, 10)?;
    println!("Found {} nearby objects", results.len());

    // Check if a key exists
    if let Some(data) = ns.get("truck:001")? {
        println!("Data: {:?}", data);
    }

    // 3D spatial tracking (altitude-aware)
    let drone = Point3d::new(-74.0060, 40.7128, 100.0); // lon, lat, altitude
    db.insert_point_3d("drones", &drone, b"Drone Alpha", None)?;

    // Query within 3D sphere
    let nearby_3d = db.query_within_sphere_3d("drones", &drone, 200.0, 10)?;
    println!("Found {} drones within 200m (3D)", nearby_3d.len());

    Ok(())
}
```

For runnable demos and extended use-case walkthroughs, check
`examples/README.md`.

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
use spatio::{distance_between, DistanceMetric};
use geo::polygon;

let point = Point::new(-74.0060, 40.7128);

// Insert point with automatic spatial indexing
db.insert_point("namespace", &point, b"data", None)?;

// Distance calculations with multiple metrics
let nyc = Point::new(-74.0060, 40.7128);
let la = Point::new(-118.2437, 34.0522);
let dist = db.distance_between(&nyc, &la, DistanceMetric::Haversine)?;
println!("Distance: {:.2} km", dist / 1000.0); // ~3,944 km

// K-nearest-neighbors search
let nearest = db.knn("namespace", &point, 5, 500_000.0, DistanceMetric::Haversine)?;
for (pt, data, distance) in nearest {
    println!("Found point at {:.2} km", distance / 1000.0);
}

// Polygon queries (using geo crate)
let area = polygon![
    (x: -74.0, y: 40.7),
    (x: -73.9, y: 40.7),
    (x: -73.9, y: 40.8),
    (x: -74.0, y: 40.8),
];
let in_polygon = db.query_within_polygon("namespace", &area, 100)?;

// Find nearby points
let nearby = db.query_within_radius("namespace", &point, 1000.0, 10)?;

// Check if points exist in region
let exists = db.contains_point("namespace", &point, 1000.0)?;

// Count points within distance
let count = db.count_within_radius("namespace", &point, 1000.0)?;

// Query bounding box
let in_bounds = db.find_within_bounds("namespace", 40.0, -75.0, 41.0, -73.0, 10)?;
let intersects = db.intersects_bounds("namespace", 40.0, -75.0, 41.0, -73.0)?;
```

**Note**: See [SPATIAL_FEATURES.md](SPATIAL_FEATURES.md) for complete documentation on all spatial operations, including convex hull, polygon area calculations, and more.

### 3D Spatial Operations

Spatio provides comprehensive 3D spatial indexing for altitude-aware applications like drone tracking, aviation, and multi-floor building navigation:

```rust
use spatio::{Point3d, BoundingBox3D, Spatio};

let db = Spatio::memory()?;

// Insert 3D points (lon, lat, altitude)
let drone = Point3d::new(-74.0060, 40.7128, 100.0); // 100m altitude
db.insert_point_3d("drones", &drone, b"Drone Alpha", None)?;

// Spherical 3D query (true 3D distance)
let control = Point3d::new(-74.0065, 40.7133, 100.0);
let nearby = db.query_within_sphere_3d("drones", &control, 200.0, 10)?;

// Cylindrical query (altitude range + horizontal radius)
let results = db.query_within_cylinder_3d(
    "drones",
    &control,
    50.0,   // min altitude
    150.0,  // max altitude
    1000.0, // horizontal radius
    10
)?;

// 3D bounding box query
let bbox = BoundingBox3D::new(
    -74.0080, 40.7120, 40.0,  // min x, y, z
    -74.0050, 40.7150, 110.0, // max x, y, z
);
let in_box = db.query_within_bbox_3d("drones", &bbox, 100)?;

// K-nearest neighbors in 3D
let nearest = db.knn_3d("drones", &control, 5)?;

// 3D distance calculations
let dist_3d = db.distance_between_3d(&point_a, &point_b)?;
```

See [examples/3d_spatial_tracking.rs](examples/3d_spatial_tracking.rs) for a complete demonstration.

### Logging

Spatio uses the `log` crate for diagnostics and warnings. To see log output in your application:

```rust
// Add to your Cargo.toml:
[dependencies]
spatio = "0.1"
env_logger = "0.11"  // or any other log implementation

// Initialize logging in your main():
fn main() {
    env_logger::init();
    // ... your code
}
```

Control log verbosity with the `RUST_LOG` environment variable:

```bash
# See all debug logs
RUST_LOG=debug cargo run

# Only warnings and errors
RUST_LOG=warn cargo run

# Only Spatio logs
RUST_LOG=spatio=debug cargo run
```

### Trajectory Tracking
```rust
// Store movement over time
let trajectory = vec![
    TemporalPoint { point: Point::new(-74.0060, 40.7128), timestamp: UNIX_EPOCH + Duration::from_secs(1640995200) },
    TemporalPoint { point: Point::new(-74.0040, 40.7150), timestamp: UNIX_EPOCH + Duration::from_secs(1640995260) },
    TemporalPoint { point: Point::new(-74.0020, 40.7172), timestamp: UNIX_EPOCH + Duration::from_secs(1640995320) },
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

### Persistence Modes

Spatio supports two persistence strategies:

**Snapshot (default)**: Point-in-time saves, fast recovery, predictable overhead
```rust
use spatio::{DBBuilder, Config};

// Manual snapshot
let mut db = DBBuilder::new()
    .snapshot_path("data.snapshot")
    .build()?;
db.insert("key", b"value", None)?;
db.snapshot()?; // Explicit save

// Auto-snapshot every N operations
let config = Config::default().with_snapshot_auto_ops(1000);
let mut db = DBBuilder::new()
    .snapshot_path("data.snapshot")
    .config(config)
    .build()?;
// Automatically snapshots every 1000 operations
```

**AOF (optional)**: Append-only log, durable per-write, replay on startup
```rust
use spatio::DBBuilder;

let mut db = DBBuilder::new()
    .aof_path("data.aof")
    .build()?;
// Requires `aof` feature: cargo add spatio --features aof
```

### Time-to-Live (TTL)

TTL support is **passive/lazy** - expired items are filtered on read and can be manually cleaned up:

```rust
// Data expires in 1 hour
let opts = SetOptions::with_ttl(Duration::from_secs(3600));
db.insert("temp_key", b"temp_value", Some(opts))?;

// Expired items return None on get()
let value = db.get("temp_key")?; // None if expired

// Manual cleanup: removes all expired keys and writes deletions to AOF
let removed = db.cleanup_expired()?;
```

## Architecture Overview

Spatio is organized in layered modules:

- **Storage** – Pluggable backends (in-memory by default, snapshot or AOF for durability) with a common trait surface.
- **Indexing** – Geohash-based point index with configurable precision and smart fallback during searches.
- **Query** – Radius, bounding-box, and trajectory primitives that reuse the shared spatial index.
- **API** – Ergonomic Rust API plus PyO3 bindings that expose the same core capabilities.

See the [docs site](https://pkvartsianyi.github.io/spatio/) for deeper architectural notes.

## Project Status

- Current version: **0.1.X**
- Active development: APIs may still change.
- Follow [releases](https://github.com/pkvartsianyi/spatio/releases) for migration notes and roadmap updates.

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

## Spatial Features

Spatio provides comprehensive geospatial operations powered by the [georust/geo](https://github.com/georust/geo) crate:

- **4 Distance Metrics**: Haversine (fast spherical), Geodesic (accurate ellipsoidal), Rhumb (constant bearing), Euclidean (planar)
- **K-Nearest-Neighbors**: Find the K closest points efficiently using spatial index
- **Polygon Queries**: Point-in-polygon tests, polygon area calculations
- **Geometric Operations**: Convex hull, bounding boxes, bbox expansion and intersection
- **Full geo Integration**: All operations leverage battle-tested geo crate implementations

**Coordinate Convention**:
- **Rust**: `Point::new(longitude, latitude)` - follows geo crate (x, y) convention
- **Python**: `Point(latitude, longitude)` - user-friendly order, converted internally

For complete documentation and examples, see:
- [SPATIAL_FEATURES.md](SPATIAL_FEATURES.md) - Complete API reference
- [examples/advanced_spatial.rs](examples/advanced_spatial.rs) - Comprehensive demo

## Acknowledgments

- Built with the Rust ecosystem's excellent geospatial libraries (especially [georust/geo](https://github.com/georust/geo))
- Inspired by modern embedded databases and spatial indexing research
- Thanks to the Rust community for feedback and contributions
