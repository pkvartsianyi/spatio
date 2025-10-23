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

**Spatio** is a lightweight, high-performance **embedded spatial database** written in Rust.
It’s designed for **real-time location data**, with **low memory usage**, **optional persistence**, and **native Python bindings**.

Unlike traditional GIS or SQL-based systems, Spatio offers a **direct API** for spatial operations —
no SQL parser, no external dependencies, and no setup required.

---

## Features

### Embedded and Lightweight
- **Fully Self-Contained** — No servers, daemons, or dependencies
- **Simple API** — Just open, insert, and query
- **Low Memory Usage** — Ideal for IoT, edge, and embedded devices
- **Single-Writer Thread Safety** — Shared `Arc<RwLock>` (no lock upgrades) allows many readers concurrently, one writer at a time

### Performance Scope
- **High Throughput Reads** — Concurrent readers avoid blocking each other; writes remain single-owner under the global lock
- **Low-Latency Spatial Queries** — Geohash + R-tree hybrid keeps point/radius lookups fast for moderate datasets
- **Configurable Persistence** — Append-Only File (AOF) with sync policies
- **Graceful Startup and Shutdown** — Automatic AOF replay and sync

### Spatial Intelligence
- **Spatial Indexing** — R-Tree + geohash hybrid indexing
- **Spatial Queries** — Nearby search, bounding box, distance, containment
- **Trajectory Support** — Store and query movement over time
- **GeoJSON I/O** — Native import/export of geometries

### Data Management
- **Namespaces** — Isolate data logically within the same instance
- **TTL Support** — Auto-expiring data for temporal use cases
- **Temporal Queries** — Filter keys by recent activity with optional history tracking
- **Atomic Batches** — Transaction-like grouped operations
- **Custom Configs** — JSON/TOML serializable configuration

### Language Support
- **Rust** — Native API for maximum performance
- **Python** — Native bindings via PyO3 (`pip install spatio`)
- **C / C++** — Stable ABI exposed as `extern "C"` functions (see [C ABI](#c-abi))


### Compile-Time Feature Flags
- `time-index` *(default)* — enables creation-time indexing and per-key history APIs. Disable it for the lightest build: `cargo add spatio --no-default-features --features="aof,geojson"`.

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

### Python

```python
from spatio import Point, Spatio

# Open (or create) a persistent database backed by an append-only file
db = Spatio.open("data/spatio.aof")

prefix = "cities"
nyc = Point(40.7128, -74.0060)

# Insert a geographic point; keys under the same prefix are indexed together
db.insert_point(prefix, nyc, b"New York City")

# Run a nearby search (returns Point, value, distance tuples)
nearby = db.find_nearby(prefix, Point(40.7306, -73.9352), 100_000.0, 5)
for point, value, distance in nearby:
    print(point, value.decode(), f"{distance/1000:.1f} km away")

# Store and retrieve plain key-value data alongside spatial items
db.insert(b"user:123", b"Jane Doe")
print(db.get(b"user:123"))  # b'Jane Doe'
```

### Rust
```rust
use spatio::prelude::*;
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
    let truck = Point::new(40.7128, -74.0060);
    ns.insert_point("truck:001", &truck, b"Truck A", None)?;

    // Query for nearby points
    let results = ns.find_nearby(&truck, 1000.0, 10)?;
    println!("Found {} nearby objects", results.len());

    // Check if a key exists
    if let Some(data) = ns.get("truck:001")? {
        println!("Data: {:?}", data);
    }

    Ok(())
}
```

### C ABI

The crate ships with a C-compatible ABI for embedding. Build the shared
library once:

```bash
cargo build --release --lib
# target/release/libspatio.so    (Linux)
# target/release/libspatio.dylib (macOS)
# target/release/spatio.dll      (Windows)
```

Link the resulting library and declare the externs you need (or generate them
with `bindgen`). Minimal usage example:

```c
#include <stdint.h>
#include <stdio.h>
#include <string.h>

typedef struct SpatioHandle SpatioHandle;
typedef struct {
    unsigned char *data;
    size_t len;
} SpatioBuffer;

extern SpatioHandle *spatio_open(const char *path);
extern void spatio_close(SpatioHandle *handle);
extern int spatio_insert(SpatioHandle *handle, const char *key,
                         const unsigned char *value, size_t value_len);
extern int spatio_get(SpatioHandle *handle, const char *key, SpatioBuffer *out);
extern void spatio_free_buffer(SpatioBuffer buffer);

int main(void) {
    SpatioHandle *db = spatio_open("example.aof");
    if (!db) {
        fprintf(stderr, "failed to open database\n");
        return 1;
    }

    const char *key = "greeting";
    const char *value = "hello";
    spatio_insert(db, key, (const unsigned char *)value, strlen(value));

    SpatioBuffer buf = {0};
    if (spatio_get(db, key, &buf) == 0) {
        printf("value = %.*s\n", (int)buf.len, buf.data);
        spatio_free_buffer(buf);
    }

    spatio_close(db);
    return 0;
}
```

> **Safety notes:** callers must pass valid, null-terminated strings and free
> any buffers produced by `spatio_get` via `spatio_free_buffer`. Structured
> error reporting is still in progress, so `spatio_last_error_message` currently
> returns `NULL`.

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

## Architecture Overview

Spatio is organized in layered modules:

- **Storage** – Pluggable backends (in-memory by default, AOF for durability) with a common trait surface.
- **Indexing** – Geohash-based point index with configurable precision and smart fallback during searches.
- **Query** – Radius, bounding-box, and trajectory primitives that reuse the shared index and TTL cleanup workers.
- **API** – Ergonomic Rust API plus PyO3 bindings that expose the same core capabilities.

See the [docs site](https://pkvartsianyi.github.io/spatio/) for deeper architectural notes.

## Project Status

- Current version: **0.1.1**
- Alpha quality: APIs may still change while we lock in the storage layout.
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

## Acknowledgments

- Built with the Rust ecosystem's excellent geospatial libraries
- Inspired by modern embedded databases and spatial indexing research
- Thanks to the Rust community for feedback and contributions
