# spatio-types

[![Crates.io](https://img.shields.io/crates/v/spatio-types.svg)](https://crates.io/crates/spatio-types)
[![Documentation](https://docs.rs/spatio-types/badge.svg)](https://docs.rs/spatio-types)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Core spatial and temporal data types for the [Spatio](https://github.com/pkvartsianyi/spatio) database.

This crate provides fundamental types for working with spatio-temporal data, built on top of the [`geo`](https://crates.io/crates/geo) crate's geometric primitives. All types are serializable with [Serde](https://serde.rs/).

## Installation

```toml
[dependencies]
spatio-types = "0.1"
```

## Types

### Point Types

- **`TemporalPoint`** - 2D point with timestamp
- **`TemporalPoint3D`** - 3D point with timestamp and altitude
- **`Point3d`** - 3D point with altitude

### Polygon Types

- **`PolygonDynamic`** - 2D polygon with dynamic metadata
- **`Polygon3D`** - 3D polygon with altitude
- **`PolygonDynamic3D`** - 3D polygon with dynamic metadata

### Trajectory Types

- **`Trajectory`** - Collection of 2D temporal points representing movement
- **`Trajectory3D`** - Collection of 3D temporal points with altitude

### Bounding Box Types

- **`BoundingBox2D`** - 2D rectangular bounds
- **`BoundingBox3D`** - 3D cuboid bounds
- **`TemporalBoundingBox2D`** - 2D bounds with time range
- **`TemporalBoundingBox3D`** - 3D bounds with time range

## Examples

### Temporal Points

```rust
use spatio_types::point::TemporalPoint;
use geo::Point;
use std::time::SystemTime;

// Create a temporal point (location with timestamp)
let point = Point::new(-74.0060, 40.7128); // NYC coordinates
let temporal_point = TemporalPoint::new(point, SystemTime::now());

println!("Location: {:?} at {:?}", temporal_point.point, temporal_point.timestamp);
```

### 3D Points

```rust
use spatio_types::point::Point3d;

// Create a 3D point with altitude
let drone_position = Point3d::new(-74.0060, 40.7128, 100.0);
println!("Drone at altitude: {} meters", drone_position.altitude);
```

### Bounding Boxes

```rust
use spatio_types::bbox::BoundingBox2D;
use geo::Point;

// Create a bounding box for Manhattan
let manhattan = BoundingBox2D::new(-74.0479, 40.6829, -73.9067, 40.8820);

// Check if a point is within bounds
let empire_state = Point::new(-73.9857, 40.7484);
assert!(manhattan.contains_point(&empire_state));

// Get the center point
let center = manhattan.center();
println!("Manhattan center: {:?}", center);
```

### Trajectories

```rust
use spatio_types::trajectory::Trajectory;
use spatio_types::point::TemporalPoint;
use geo::Point;
use std::time::{SystemTime, Duration};

// Create a movement trajectory
let start_time = SystemTime::now();
let points = vec![
    TemporalPoint::new(
        Point::new(-74.00, 40.71),
        start_time
    ),
    TemporalPoint::new(
        Point::new(-74.01, 40.72),
        start_time + Duration::from_secs(60)
    ),
    TemporalPoint::new(
        Point::new(-74.02, 40.73),
        start_time + Duration::from_secs(120)
    ),
];

let trajectory = Trajectory::new(points);
println!("Trajectory has {} points", trajectory.points.len());
```

### 3D Bounding Boxes

```rust
use spatio_types::bbox::BoundingBox3D;
use spatio_types::point::Point3d;

// Create a 3D airspace boundary
let airspace = BoundingBox3D::new(
    -74.05, 40.68, 0.0,    // min_lon, min_lat, min_altitude
    -73.90, 40.88, 500.0   // max_lon, max_lat, max_altitude
);

// Check if a drone is within airspace
let drone = Point3d::new(-74.00, 40.75, 150.0);
assert!(airspace.contains_point(&drone));
```

## Serialization

All types support Serde serialization:

```rust
use spatio_types::point::TemporalPoint;
use geo::Point;
use std::time::SystemTime;

let point = TemporalPoint::new(Point::new(-74.0, 40.7), SystemTime::now());

// Serialize to JSON
let json = serde_json::to_string(&point).unwrap();

// Deserialize from JSON
let deserialized: TemporalPoint = serde_json::from_str(&json).unwrap();
```

## Use Cases

This crate is ideal for:

- **GPS tracking** - Store and query vehicle, drone, or person locations over time
- **Geofencing** - Define and check polygon/bounding box boundaries
- **Movement analysis** - Track and analyze trajectories and paths
- **3D spatial data** - Work with altitude-aware location data
- **Time-series geospatial** - Combine location and temporal information

## Features

- **Zero-copy operations** where possible
- **Serde integration** for easy serialization
- **Built on `geo` types** for compatibility with the Rust geospatial ecosystem
- **Time-aware** with `SystemTime` timestamps
- **3D support** with altitude/elevation data

## Integration with Spatio

This crate is primarily used by the [Spatio](https://crates.io/crates/spatio) database, but can be used standalone for any application needing spatio-temporal types.

```rust
use spatio::prelude::*;
use spatio_types::point::TemporalPoint;

let mut db = Spatio::memory()?;

// spatio-types work seamlessly with Spatio
let temporal_point = TemporalPoint::new(
    Point::new(-74.0, 40.7),
    SystemTime::now()
);
```

## Documentation

- **API Documentation:** [docs.rs/spatio-types](https://docs.rs/spatio-types)
- **Spatio Database:** [github.com/pkvartsianyi/spatio](https://github.com/pkvartsianyi/spatio)
- **Geo Crate:** [docs.rs/geo](https://docs.rs/geo)

## License

MIT - see [LICENSE](https://github.com/pkvartsianyi/spatio/blob/main/LICENSE)

## Contributing

Contributions are welcome! Please visit the [Spatio repository](https://github.com/pkvartsianyi/spatio) for contribution guidelines.