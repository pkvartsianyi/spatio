# Spatio Examples

This directory contains runnable binaries that demonstrate different parts of
the Spatio API. Each example can be executed with `cargo run --example <name>`
from the workspace root.

## Quick Reference

```bash
# Basic key-value and spatial inserts
cargo run --example getting_started

# Nearby search, bounds queries, and spatial index inspection
cargo run --example spatial_queries

# Time-sequenced trajectory storage and queries
cargo run --example trajectory_tracking

# Architectural tour showing module composition
cargo run --example architecture_demo

# End-to-end showcase combining spatial, temporal, and persistence features
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
