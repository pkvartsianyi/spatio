# Spatio for Python

Fast spatial database for Python, powered by Rust.

[![PyPI](https://img.shields.io/pypi/v/spatio.svg)](https://pypi.org/project/spatio)
[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

## What is it?

Spatio is an embedded database for location data. Store points on a map, query "what's nearby", track movement over time - all with minimal setup.

## Install

```bash
pip install spatio
```

Supports: Linux (x86_64, aarch64), macOS (Intel, Apple Silicon), Python 3.9-3.13.

**Windows not supported** - use WSL2, Docker, or a Linux VM.

## Quick Start

```python
import spatio

db = spatio.Spatio.memory()

nyc = spatio.Point(-74.0060, 40.7128)
db.insert_point("cities", nyc, b"New York")

# Find nearby (within 100km)
nearby = db.query_within_radius("cities", nyc, 100000, 10)
for point, data, distance in nearby:
    print(f"{data.decode()}: {distance/1000:.1f}km away")
```

## Examples

### Basic Operations

```python
import spatio

# Create database
db = spatio.Spatio.memory()                # In-memory
db = spatio.Spatio.open("data.db")         # Persistent

# Store key-value data
db.insert(b"user:123", b"John Doe")
data = db.get(b"user:123")
db.delete(b"user:123")

# Store points
point = spatio.Point(-74.0060, 40.7128)
db.insert_point("pois", point, b"Restaurant")

# Query spatial
nearby = db.query_within_radius("pois", point, 1000.0, 10)
count = db.count_within_radius("pois", point, 1000.0)
exists = db.contains_point("pois", point, 1000.0)
```

### Distance Calculations

```python
nyc = spatio.Point(-74.0060, 40.7128)
london = spatio.Point(-0.1278, 51.5074)

# Haversine (fast, spherical Earth)
metric = spatio.DistanceMetric("haversine")
distance = db.distance_between(nyc, london, metric)
print(f"{distance/1000:.0f}km")  # ~5570km

# Geodesic (accurate, ellipsoidal Earth - Karney 2013)
metric = spatio.DistanceMetric("geodesic")
distance = db.distance_between(nyc, london, metric)

# Also available: "rhumb", "euclidean"
```

### K-Nearest Neighbors

```python
center = spatio.Point(-74.0, 40.7)

# Find 5 nearest within 50km
metric = spatio.DistanceMetric("haversine")
nearest = db.knn("cities", center, 5, 50000.0, metric)

for point, data, distance in nearest:
    print(f"{data.decode()}: {distance/1000:.1f}km")
```

### Bounding Box Queries

```python
# Find all points in rectangle
results = db.find_within_bounds(
    "cities",
    min_lat=40.0,
    min_lon=-75.0,
    max_lat=41.0,
    max_lon=-73.0,
    limit=100
)

# Check if any exist in box
has_points = db.intersects_bounds("cities", 40.0, -75.0, 41.0, -73.0)
```

### Polygon Queries

```python
# Points within polygon boundary
polygon = [
    (-74.0, 40.7),
    (-73.9, 40.7),
    (-73.9, 40.8),
    (-74.0, 40.8),
]
inside = db.query_within_polygon("pois", polygon, 100)
```

### Trajectories

```python
import time

# Track movement
trajectory = [
    (spatio.Point(-74.00, 40.71), int(time.time())),
    (spatio.Point(-74.01, 40.72), int(time.time()) + 60),
    (spatio.Point(-74.02, 40.73), int(time.time()) + 120),
]
db.insert_trajectory("vehicle:truck001", trajectory)

# Query movement history
start = int(time.time()) - 3600
end = int(time.time())
path = db.query_trajectory("vehicle:truck001", start, end)

for point, timestamp in path:
    print(f"At ({point.lon}, {point.lat}) at {timestamp}")
```

### TTL (Auto-Expiration)

```python
import spatio

db = spatio.Spatio.memory()

# Expire after 5 minutes
opts = spatio.SetOptions.with_ttl(300.0)
db.insert(b"session:abc", b"user data", opts)

# Returns None if expired
data = db.get(b"session:abc")

# Clean up expired items manually
removed = db.cleanup_expired()
print(f"Removed {removed} expired items")
```

**Important:** TTL is lazy. Expired items stay in memory until you call `cleanup_expired()` or they're overwritten. For long-running apps, clean up periodically to avoid memory leaks.

### Configuration

```python
config = spatio.Config()
db = spatio.Spatio.memory_with_config(config)

# That's it - most defaults are sensible
# See Rust docs for advanced options
```

## API Reference

### Spatio Class

```python
# Create
Spatio.memory()
Spatio.open(path)
Spatio.memory_with_config(config)
Spatio.open_with_config(path, config)

# Key-value
insert(key, value, options=None)
get(key) -> bytes | None
delete(key) -> bytes | None

# Spatial
insert_point(prefix, point, value, options=None)
query_within_radius(prefix, center, radius_meters, limit)
count_within_radius(prefix, center, radius_meters)
contains_point(prefix, center, radius_meters)
find_within_bounds(prefix, min_lat, min_lon, max_lat, max_lon, limit)
intersects_bounds(prefix, min_lat, min_lon, max_lat, max_lon)
query_within_polygon(prefix, polygon_coords, limit)

# Distance
distance_between(point1, point2, metric)
knn(prefix, center, k, max_radius, metric)

# Trajectories
insert_trajectory(object_id, trajectory, options=None)
query_trajectory(object_id, start_time, end_time)

# Maintenance
cleanup_expired() -> int
stats() -> dict
close()
```

### Point Class

```python
Point(longitude, latitude, altitude=None)

# Properties
point.lon  # -180 to 180
point.lat  # -90 to 90
point.alt  # Always None (2D only for now)

# Methods
point.distance_to(other_point) -> float  # meters, Haversine
```

### SetOptions Class

```python
SetOptions.with_ttl(seconds)
SetOptions.with_expiration(unix_timestamp)
```

### DistanceMetric Class

```python
DistanceMetric("haversine")   # Fast spherical
DistanceMetric("geodesic")    # Accurate ellipsoidal
DistanceMetric("rhumb")       # Constant bearing
DistanceMetric("euclidean")   # Planar only
```

## Performance Tips

1. **Batch inserts** when possible
2. **Use namespaces** to separate data types
3. **Set reasonable limits** on queries
4. **Call `cleanup_expired()`** periodically if using TTL
5. **Use persistent storage** for data you can't lose

## Building from Source

```bash
git clone https://github.com/pkvartsianyi/spatio
cd spatio/py-spatio

# Install maturin
pip install maturin

# Build and install
maturin develop

# Or build wheel
maturin build --release
```

## Platform Support

**Supported:**
- Linux: x86_64, aarch64 (manylinux)
- macOS: x86_64 (Intel), arm64 (Apple Silicon)
- Python: 3.9, 3.10, 3.11, 3.12, 3.13

## Examples

Check the [examples/](examples/) directory:

- `basic_usage.py` - Getting started
- `performance_demo.py` - Benchmarks
- `trajectory_tracking.py` - Movement tracking

## Links

- **GitHub:** https://github.com/pkvartsianyi/spatio
- **PyPI:** https://pypi.org/project/spatio
- **Rust docs:** https://docs.rs/spatio
- **Issues:** https://github.com/pkvartsianyi/spatio/issues

## License

MIT - see [LICENSE](LICENSE)
