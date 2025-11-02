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
It’s designed for **real-time location data**, with **low memory usage**, **optional persistence**, and **native Python bindings**.

No SQL parser, no external dependencies, and requires no setup.

---

## Features

### Embedded and Lightweight
- **Self-contained** — Runs without external services or dependencies
- **Minimal API surface** — Open, insert, and query
- **Low memory footprint** — Suitable for IoT, edge, and embedded environments
- **Single-Writer Thread Safety** — Uses a shared Arc<RwLock> (without lock upgrades) to allow concurrent readers and a single writer

### Performance Scope
- **Concurrent read access** — Multiple readers operate without blocking; writes are serialized under a global lock
- **Spatio-temporal queries** — Use a geohash + R-tree hybrid to balance lookup precision and performance for moderate datasets
- **Configurable persistence** — Append-Only File (AOF) with sync policies
- **Startup and Shutdown** — AOF logs are replayed automatically on startup

### Spatio-Temporal Indexing and Querying
- **Spatio-Temporal Indexing** — R-Tree + geohash hybrid indexing with optional history tracking
- **Spatio-Temporal Queries** — Nearby search, bounding box, distance, containment, and time slicing
- **Trajectory Support** — Store and query movement over time
- **GeoJSON I/O** — Supports import and export of geometries in GeoJSON format

### Data Management
- **Namespaces** — Isolate data logically within the same instance
- **TTL Support** — Automatically removes expired data
- **Temporal Queries** — Filter keys by recent activity with optional history tracking
- **Atomic Batches** — Supports grouped write operations with atomic semantics.
- **Custom Configs** — JSON/TOML serializable configuration

### Language Support
- **Rust** — Native
- **Python** — Provides bindings implemented via PyO3 (`pip install spatio`)
- **C / C++** — Provides an extern "C" ABI for interoperability (see [C ABI](#c-abi))


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

Python usage lives in the dedicated bindings package—see `py-spatio/README.md`
for up-to-date installation notes and examples.

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
    let truck = Point::new(-74.0060, 40.7128);
    ns.insert_point("truck:001", &truck, b"Truck A", None)?;

    // Query for nearby points
    let results = ns.query_within_radius(&truck, 1000.0, 10)?;
    println!("Found {} nearby objects", results.len());

    // Check if a key exists
    if let Some(data) = ns.get("truck:001")? {
        println!("Data: {:?}", data);
    }

    Ok(())
}
```

### C ABI

Note: The C ABI is experimental and is not actively developed.

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

> **Safety note:**
> Callers must pass valid, null-terminated strings and free any buffers produced by `spatio_get` using `spatio_free_buffer`.
> Structured error reporting is under development; `spatio_last_error_message` currently returns `NULL`.

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
let point = Point::new(-74.0060, 40.7128);

// Insert point with automatic spatial indexing
db.insert_point("namespace", &point, b"data", None)?;

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

## Acknowledgments

- Built with the Rust ecosystem's excellent geospatial libraries
- Inspired by modern embedded databases and spatial indexing research
- Thanks to the Rust community for feedback and contributions
