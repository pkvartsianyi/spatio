# Spatio Examples

This directory contains examples demonstrating Spatio's core features. Each example can be run with:

```bash
cargo run --example <name>
```

## Examples

### `getting_started`
Introduction to Spatio's main features in one place.

**Demonstrates:**
- Basic key-value storage
- Spatial point indexing
- Radius-based queries
- Bounding box queries
- Trajectory tracking
- TTL (time-to-live) expiration
- Atomic batch operations
- Database statistics

**Run:**
```bash
cargo run --example getting_started
```

### `spatial_queries`
Deep dive into spatial query capabilities.

**Demonstrates:**
- `query_within_radius` - Find points within a distance
- `contains_point` - Check if points exist in a radius
- `count_within_radius` - Efficiently count nearby points
- `find_within_bounds` - Rectangular region queries
- `intersects_bounds` - Check bounding box intersection
- Multiple namespaces for organizing different point types
- Query result limiting

**Run:**
```bash
cargo run --example spatial_queries
```

### `trajectory_tracking`
Working with time-series location data.

**Demonstrates:**
- Storing trajectories (sequences of points with timestamps)
- Querying full trajectories
- Querying specific time ranges
- Tracking multiple vehicles/objects
- High-frequency position updates
- Extending existing trajectories

**Run:**
```bash
cargo run --example trajectory_tracking
```

### `advanced_spatial`
Comprehensive demonstration of advanced spatial operations using the geo crate.

**Demonstrates:**
- Distance calculations with 4 metrics (Haversine, Geodesic, Rhumb, Euclidean)
- K-nearest-neighbors (KNN) queries
- Polygon boundary queries
- Bounding box operations and expansion
- Convex hull computation
- Bounding rectangle calculation
- Spatial analytics and distance matrices
- Integration with georust/geo crate

**Run:**
```bash
cargo run --example advanced_spatial
```

### `comprehensive_demo`
End-to-end showcase of all major features.

**Demonstrates:**
- Key-value operations
- TTL configuration
- Atomic batches
- Spatial indexing
- Multiple spatial query types
- Points of interest (POI) management
- Multi-namespace organization
- Trajectory storage and queries
- Data updates and deletes

**Run:**
```bash
cargo run --example comprehensive_demo
```

### `persistence_lifecycle`
Demonstrates AOF (Append-Only File) persistence.

**Demonstrates:**
- Automatic persistence to disk
- Database recovery on restart
- Custom AOF file paths
- Spatial data persistence
- Sync policies and configuration

**Run:**
```bash
cargo run --example persistence_lifecycle
```

## Common Use Cases

### Fleet Management
Track vehicles, query historical routes, analyze movement patterns.

### Delivery Tracking
Store delivery routes, monitor progress, verify completion.

### IoT & Sensor Networks
Manage geographic sensor locations, query nearby sensors, track mobile devices.

### Points of Interest (POI)
Store and query landmarks, restaurants, stores, or any geographic features.

### Asset Tracking
Monitor movement of equipment, vehicles, or valuable items over time.

### Geofencing
Check if points fall within defined geographic boundaries using polygon queries.

### Spatial Analytics
Calculate distances between points, find nearest neighbors, compute convex hulls, and analyze geographic distributions.

## Coordinate Format

Spatio uses the `geo` crate's `Point` type, which follows the standard (longitude, latitude) order:

```rust
use spatio::Point;

// Correct: Point::new(longitude, latitude)
let nyc = Point::new(-74.0060, 40.7128);
let london = Point::new(-0.1278, 51.5074);
```

## Advanced Spatial Features

Spatio leverages the [georust/geo](https://github.com/georust/geo) crate for comprehensive geospatial operations:

### Distance Calculations
```rust
use spatio::{distance_between, DistanceMetric};

let dist = distance_between(&nyc, &london, DistanceMetric::Haversine);
// Also available: Geodesic, Rhumb, Euclidean
```

### K-Nearest-Neighbors
```rust
let nearest = db.knn("cities", &query_point, 5, 500_000.0, DistanceMetric::Haversine)?;
```

### Polygon Queries
```rust
use geo::polygon;

let area = polygon![
    (x: -74.0, y: 40.7),
    (x: -73.9, y: 40.8),
    (x: -74.0, y: 40.8),
];
let in_area = db.query_within_polygon("namespace", &area, 100)?;
```

For complete documentation, see [SPATIAL_FEATURES.md](../SPATIAL_FEATURES.md).

## Next Steps

After running the examples:
1. Check the [main documentation](../README.md) for API details
2. Review [SPATIAL_FEATURES.md](../SPATIAL_FEATURES.md) for complete spatial operations guide
3. Review the [tests](../tests/) for more usage patterns
4. Explore the [benchmarks](../benches/) for performance characteristics
