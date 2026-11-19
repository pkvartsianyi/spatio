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
pos = spatio.Point(-74.006, 40.712, z=10.0)
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

# Shapes (stored areas)
db.set_shape(namespace, shape_id, shape)
db.get_shape(namespace, shape_id)  # Returns Shape or None
db.delete_shape(namespace, shape_id)  # Returns True if it existed
db.list_shapes(namespace)  # Returns list of (shape_id, shape)
db.shapes_containing(namespace, point, limit=100)  # Returns list of shape_id
# Returns list of (shape_id, distance), nearest first
db.shapes_within(namespace, point, meters, limit=100)
```

### Shapes and geofencing

Store polygons, circles and corridors (everything within a distance of a
line), then ask which ones contain or are near a point. Distances are in
meters and follow great circles; polygon edges are straight in lon/lat, as in
GeoJSON.

```python
import spatio

db = spatio.Spatio.memory()

depot = spatio.Shape.circle(spatio.Point(-74.006, 40.712), radius_m=500)
route = spatio.Shape.corridor(
    spatio.LineString([(-74.01, 40.70), (-73.98, 40.75)]), half_width_m=50
)
db.set_shape("zones", "depot", depot)
db.set_shape("zones", "route", route)

truck = spatio.Point(-74.005, 40.712)
db.shapes_containing("zones", truck)  # ["depot"]
db.shapes_within("zones", truck, meters=1000)  # [("depot", 0.0), ("route", ...)]
```

Shapes are persisted with the database and validated on construction
(`ValueError` for out-of-range coordinates, negative radii or too few
vertices).

## Data Types

### Point
`spatio.Point(lon, lat, z=None)`
- `point.lon`, `point.lat`, `point.alt`
- `point.distance_to(other_point)` -> distance in meters (Haversine)

### Polygon
`spatio.Polygon(exterior, interiors=None)`
- Rings are lists of `(lon, lat)` tuples.

### LineString
`spatio.LineString([(lon, lat), ...])` (at least 2 vertices)
- `line.coords`, `len(line)`
- `line.length()` -> length in meters
- `line.distance_to(point)` / `line.is_within(point, meters)`
- `line.intersects(other)` / `line.intersections(other)` -> list of `Point`

### Shape
`spatio.Shape.polygon(polygon)`, `spatio.Shape.circle(center, radius_m)`,
`spatio.Shape.corridor(line, half_width_m)`
- `shape.kind` -> `"polygon"`, `"circle"` or `"corridor"`
- `shape.center`, `shape.radius_m`, `shape.line`, `shape.half_width_m`,
  `shape.as_polygon()` (None when not applicable)
- `shape.contains(point)`
- `shape.distance_to(point)` -> meters, 0 inside
- `shape.is_within(point, meters)`

### TemporalPoint
`spatio.TemporalPoint(point, timestamp)`
- Used for bulk trajectory ingestion.

## Performance Tips

1. **Namespaces**: Use namespaces to logically separate different types of objects (e.g., "delivery_trucks", "warehouses").
2. **Object Queries**: Use `query_near` or `knn_near_object` whenever possible, as they avoid re-passing coordinates and leverage the internal Hot State index directly.

## License

MIT - see [LICENSE](LICENSE)
