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

**Spatio** is a high-performance, **embedded spatio-temporal database** written in Rust.
It's designed for **real-time tracking of moving objects**, with **hot/cold state separation**, **durable persistence**, and **native Python bindings**.

## Quick Start

### Python
```bash
pip install spatio
```

```python
import spatio

# Open or create a database
db = spatio.Spatio.memory()

# Upsert an object's location (longitude, latitude)
nyc = spatio.Point(-74.0060, 40.7128)
db.upsert("cities", "nyc", nyc, {"population": 8000000})

# Find objects nearby
nearby = db.query_radius("cities", nyc, 100000, limit=10)
# Returns: [(object_id, point, metadata, distance), ...]
```

### Rust
```toml
[dependencies]
spatio = "0.2"
```

```rust
use spatio::prelude::*;

fn main() -> Result<()> {
    let db = Spatio::memory()?;

    let nyc = Point3d::new(-74.0060, 40.7128, 0.0);
    db.upsert("cities", "nyc", nyc, serde_json::json!({}), None)?;

    let nearby = db.query_radius("cities", &nyc, 100_000.0, 10)?;
    println!("Found {} cities", nearby.len());

    Ok(())
}
```

## Architecture: Hot/Cold State

Spatio follows a specialized architecture for tracking millions of moving objects:
- **Hot State**: Current locations are kept in-memory using a lock-free `DashMap` and a high-performance R*-tree spatial index.
- **Cold State**: Historical movement (trajectories) is persisted to an append-only log on disk.
- **Recovery**: At startup, the Hot State is automatically rebuilt by scanning the latest entries in the Trajectory Log.

## Features

- **Spatial Queries:** Radius search, 2D/3D Bounding box, K-Nearest Neighbors (KNN), Cylindrical queries.
- **Object-Centric API:** Query around moving objects directly without manual coordinate lookups.
- **Trajectories:** Optimized storage and querying of historical movement paths over time.
- **Durable Persistence:** Sequential Trajectory Log ensures zero data loss for historical data.
- **Lightweight:** No external dependencies, no SQL overhead, and zero-setup embedded engine.
- **Cross-language:** Native high-performance Python bindings via PyO3.
- **Server Mode:** Includes an optional lightweight TCP server for remote access.

## Examples

### Tracking Moving Objects
```rust
// Update location (upsert)
db.upsert("delivery", "truck_001", position, metadata, None)?;

// Find trucks near a warehouse (coordinate-based)
let nearby = db.query_radius("delivery", &warehouse_pos, 5000.0, 10)?;

// Find drones near a specific drone (object-based)
let near_neighbors = db.query_near("drones", "drone_alpha", 500.0, 5)?;
```

### 3D Spatial
```rust
// Track drones with altitude
let drone_pos = Point3d::new(-74.006, 40.712, 100.0);
db.upsert("drones", "drone_1", drone_pos, json!({}), None)?;

// 3D Bounding box query
let in_airspace = db.query_within_bbox_3d("drones", min_x, min_y, min_z, max_x, max_y, max_z, 100)?;
```

### Historical Trajectories
```rust
// Query movement history for a specific vehicle
let start = SystemTime::now() - Duration::from_secs(3600);
let history = db.query_trajectory("logistics", "truck_001", start, SystemTime::now(), 500)?;
```

## API Overview

### Real-time Tracking
- `upsert(namespace, object_id, position, metadata, options)`
- `get(namespace, object_id)`
- `delete(namespace, object_id)`

### Spatial Queries
- `query_radius(namespace, center, radius, limit)`
- `query_bbox(namespace, min_x, min_y, max_x, max_y, limit)`
- `query_within_cylinder(namespace, center, min_z, max_z, radius, limit)`
- `query_within_bbox_3d(namespace, min_x, min_y, min_z, max_x, max_y, max_z, limit)`
- `knn(namespace, center, k)`

### Object-Relative Queries
- `query_near(namespace, object_id, radius, limit)`
- `query_bbox_near_object(namespace, object_id, width, height, limit)`
- `query_cylinder_near_object(namespace, object_id, min_z, max_z, radius, limit)`
- `query_bbox_3d_near_object(namespace, object_id, width, height, depth, limit)`
- `knn_near_object(namespace, object_id, k)`

## Server Mode

Spatio includes a dedicated server crate (`spatio-server`) for multi-process or remote access.
```bash
# Start the server
cargo run -p spatio-server
```

Clients can then connect via direct TCP using the Spatio Binary Protocol (SBP).

## Documentation

- **Python docs:** [crates/py/README.md](crates/py/README.md)
- **Server docs:** [crates/server/README.md](crates/server/README.md)
- **API docs:** [docs.rs/spatio](https://docs.rs/spatio)

## License

MIT - see [LICENSE](LICENSE)
