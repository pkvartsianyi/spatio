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
Check if points fall within defined geographic boundaries.

## Coordinate Format

Spatio uses the `geo` crate's `Point` type, which follows the standard (longitude, latitude) order:

```rust
use spatio::Point;

// Correct: Point::new(longitude, latitude)
let nyc = Point::new(-74.0060, 40.7128);
let london = Point::new(-0.1278, 51.5074);
```

## Next Steps

After running the examples:
1. Check the [main documentation](../README.md) for API details
2. Review the [tests](../tests/) for more usage patterns
3. Explore the [benchmarks](../benches/) for performance characteristics