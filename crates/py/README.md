# Spatio for Python

High-performance spatial database for Python, powered by Rust.

[![PyPI](https://img.shields.io/pypi/v/spatio.svg)](https://pypi.org/project/spatio)
[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

## What is it?

Spatio is an embedded database optimized for tracking moving objects. It uses a specialized **Hot/Cold State** architecture to provide real-time spatial queries and historical trajectory tracking with minimal overhead.

## Quick Start

```python
import spatio

# Open an in-memory database
db = spatio.Spatio.memory()

# Track a moving object
pos = spatio.Point(-74.006, 40.712, altitude=10.0)
db.upsert("drones", "drone_1", pos, {"status": "active"})

# Find objects within 500 meters
nearby = db.query_radius("drones", pos, radius=500, limit=10)
for object_id, point, metadata, distance in nearby:
    print(f"Found {object_id} at {distance:.1f}m")
```

## core API

### Spatio (Local Database)

```python
import spatio

# Creation
db = spatio.Spatio.memory()
db = spatio.Spatio.open("data.db")

# Tracking
db.upsert(namespace, object_id, point, metadata=None)
db.get(namespace, object_id)  # Returns CurrentLocation or None
db.delete(namespace, object_id)

# Proximity Queries
# Returns list of (object_id, point, metadata, distance)
db.query_radius(namespace, center_point, radius, limit=100)
db.query_near(namespace, object_id, radius, limit=100)

# K-Nearest Neighbors
db.knn(namespace, center_point, k)
db.knn_near_object(namespace, object_id, k)

# Volume Queries
db.query_bbox(namespace, min_x, min_y, max_x, max_y, limit=100)
db.query_within_cylinder(namespace, center_point, min_z, max_z, radius, limit=100)
db.query_within_bbox_3d(namespace, min_x, min_y, min_z, max_x, max_y, max_z, limit=100)

# Relative Queries
db.query_bbox_near_object(namespace, object_id, width, height, limit=100)
db.query_cylinder_near_object(namespace, object_id, min_z, max_z, radius, limit=100)

# Trajectories
db.insert_trajectory(namespace, object_id, list_of_temporal_points)
db.query_trajectory(namespace, object_id, start_ts, end_ts, limit=100)
```

### SpatioClient (Remote Server)

Connect to a running `spatio-server` instance:

```python
client = spatio.Spatio.server(host="127.0.0.1", port=3000)

# Identical API to local Spatio
client.upsert("cars", "v1", point, {"color": "red"})
results = client.query_radius("cars", point, 1000)
```

## Data Types

### Point
`spatio.Point(lon, lat, altitude=0.0)`
- `point.lon`, `point.lat`, `point.alt`
- `point.distance_to(other_point)` -> distance in meters (Haversine)

### TemporalPoint
`spatio.TemporalPoint(point, timestamp)`
- Used for bulk trajectory ingestion.

## Performance Tips

1. **Namespaces**: Use namespaces to logically separate different types of objects (e.g., "delivery_trucks", "warehouses").
2. **Object Queries**: Use `query_near` or `knn_near_object` whenever possible, as they avoid re-passing coordinates and leverage the internal Hot State index directly.
3. **Lazy TTL**: TTL expiration is passive. If you use TTL for ephemeral data, it will be filtered on read but only removed from storage when overwritten or manually cleaned up.

## License

MIT - see [LICENSE](LICENSE)
