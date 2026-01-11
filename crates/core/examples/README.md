# Examples

Run any example with:
```bash
cargo run --example <name>
```

## Getting Started

### `getting_started`
Quick intro covering all basics in one file.

```bash
cargo run --example getting_started
```

Shows: key-value ops, spatial indexing, radius queries, bounding boxes, trajectories, atomic batches, stats.

### `spatial_queries`
All spatial query types.

```bash
cargo run --example spatial_queries
```

Shows: radius search, contains/count, bounding boxes, multiple namespaces, result limiting.

### `trajectory_tracking`
Time-series location data.

```bash
cargo run --example trajectory_tracking
```

Shows: storing paths, querying time ranges, tracking multiple objects, high-frequency updates.

### `advanced_spatial`
Full spatial operations suite using georust/geo.

```bash
cargo run --example advanced_spatial
```

Shows: 4 distance metrics, K-nearest-neighbors, polygon queries, bounding box ops, convex hull, distance matrices.

### `comprehensive_demo`
End-to-end feature showcase.

```bash
cargo run --example comprehensive_demo
```

Shows: Everything in one place - key-value, atomics, spatial queries, POI management, trajectories.

### `persistence_lifecycle`
AOF (Append-Only File) persistence demo.

```bash
cargo run --example persistence_lifecycle
```

Shows: Auto-save to disk, recovery on restart, sync policies.

Requires `--features aof`:
```bash
cargo run --example persistence_lifecycle --features aof
```

### `3d_spatial_tracking`
3D spatial operations (altitude-aware).

```bash
cargo run --example 3d_spatial_tracking
```

Shows: 3D point indexing, spherical queries, cylindrical queries, bounding boxes in 3D, KNN-3D.

## Common Use Cases

- **Fleet tracking:**
- **IoT sensors:**
- **POI search:**
- **Geofencing:**
- **3D tracking:**

## Next Steps

1. Check [../README.md](../README.md) for API reference
2. Look at [../tests/](../tests/) for more usage patterns
