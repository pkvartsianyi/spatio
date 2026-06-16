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

### `3d_spatial_tracking`
3D spatial operations (altitude-aware).

```bash
cargo run --example 3d_spatial_tracking
```

Shows: 3D point indexing, spherical queries, cylindrical queries, bounding boxes in 3D, KNN-3D.

## Common Use Cases

- **Fleet tracking:** continuously upsert vehicle positions and query who is near a point.
- **IoT sensors:** record sensor locations and run bounding-box / radius lookups.
- **POI search:** find points of interest within a radius or polygon.
- **Geofencing:** test whether tracked objects fall inside a polygon.
- **3D tracking:** altitude-aware queries for drones or aircraft (sphere/cylinder/3D bbox).

## Next Steps

1. Check [../README.md](../README.md) for API reference
2. Look at [../tests/](../tests/) for more usage patterns
