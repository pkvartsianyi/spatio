# Spatio: Python Bindings for High-Performance Spatio-Temporal Database

[![PyPI version](https://badge.fury.io/py/spatio.svg)](https://pypi.org/project/spatio)
[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Python bindings for [Spatio](https://github.com/pkvartsianyi/spatio), a high-performance, embedded spatio-temporal database written in Rust. Spatio brings spatial operations and geographic data management to Python with minimal overhead.


## Features

- **High Performance**: Built on Rust for maximum speed and memory efficiency
- **Spatio-Temporal Operations**: Geographic point storage with automatic spatial indexing and optional time filters
- **Advanced Spatial Queries**: Distance calculations (4 metrics), K-nearest-neighbors, polygon queries, bounding box operations
- **Trajectory Tracking**: Store and query movement data over time
- **TTL Support**: Automatic data expiration with time-to-live
- **Thread-Safe**: Concurrent access (atomic operations coming soon for Python)
- **Persistent Storage**: Optional file-based persistence
## Installation

### From PyPI (Recommended)

```bash
pip install spatio
```

📦 **PyPI Repository**: https://pypi.org/project/spatio

### From Source

```bash
# Clone the repository
git clone https://github.com/pkvartsianyi/spatio.git
cd spatio/py-spatio

# Build and install
pip install maturin
maturin develop
```

## Quick Start

```python
import spatio

# Create an in-memory database
db = spatio.Spatio.memory()

# Store simple key-value data
db.insert(b"user:123", b"John Doe")
user = db.get(b"user:123")
print(f"User: {user.decode()}")  # User: John Doe

# Store geographic points with automatic spatial indexing
# Note: Point uses (longitude, latitude) order - same as GeoJSON and Rust API
nyc = spatio.Point(-74.0060, 40.7128)
london = spatio.Point(-0.1278, 51.5074)

db.insert_point("cities", nyc, b"New York City")
db.insert_point("cities", london, b"London")

# Find nearby points within 6000km
nearby = db.query_within_radius("cities", nyc, 6000000.0, 10)
for point, city_name, distance in nearby:
    print(f"{city_name.decode()}: {distance/1000:.0f}km away")
```

## Core Classes

### Spatio

The main database class for all operations.

```python
# Create databases
db = spatio.Spatio.memory()                    # In-memory
db = spatio.Spatio.open("data.db")             # Persistent
db = spatio.Spatio.memory_with_config(config)  # With custom config

# Basic operations
db.insert(key, value, options=None)
value = db.get(key)
old_value = db.delete(key)

# Spatial operations
db.insert_point(prefix, point, value, options=None)
nearby = db.query_within_radius(prefix, center, radius_meters, limit)
count = db.count_within_radius(prefix, center, radius_meters)

# Advanced spatial operations
distance = db.distance_between(point1, point2, metric)
nearest = db.knn(prefix, center, k, max_radius, metric)
in_polygon = db.query_within_polygon(prefix, polygon_coords, limit)

# Trajectory operations
db.insert_trajectory(object_id, trajectory, options=None)
path = db.query_trajectory(object_id, start_time, end_time)
```

### Point

Represents a geographic coordinate.

**Note**: Uses `(longitude, latitude)` order, matching GeoJSON standard and the Rust API. This is the mathematical (x, y) convention used by most GIS libraries.

```python
# Create points (longitude, latitude order - same as GeoJSON)
point = spatio.Point(longitude, latitude)  # e.g., Point(-74.0060, 40.7128) for NYC
print(f"Location: ({point.lon}, {point.lat})")

# Access coordinates
print(f"Latitude: {point.lat}")   # North-South (-90 to 90)
print(f"Longitude: {point.lon}")  # East-West (-180 to 180)

# Calculate distance (uses Haversine by default)
distance = point1.distance_to(point2)  # Returns meters
```

**Why (lon, lat)?** This order aligns with:
- **GeoJSON standard**: `{"type": "Point", "coordinates": [lon, lat]}`
- **Mathematical convention**: (x, y) where x=longitude, y=latitude
- **Most GIS tools**: PostGIS, Shapely, GDAL, etc.
- **The Rust API**: Ensures consistency across language bindings

### DistanceMetric

Distance calculation methods for spatial operations.

```python
# Available metrics
metric = spatio.DistanceMetric("haversine")  # Fast spherical (default)
metric = spatio.DistanceMetric("geodesic")   # Accurate ellipsoidal (Karney 2013)
metric = spatio.DistanceMetric("rhumb")      # Constant bearing (navigation)
metric = spatio.DistanceMetric("euclidean")  # Planar coordinates only
```

### SetOptions

Configure data storage options.

```python
# TTL (time-to-live)
opts = spatio.SetOptions.with_ttl(300.0)  # 5 minutes
db.insert(b"session", b"data", opts)

# Absolute expiration
import time
future = time.time() + 300
opts = spatio.SetOptions.with_expiration(future)
```

### Config

Database configuration.

```python
# Create a custom configuration
config = spatio.Config()
db = spatio.Spatio.memory_with_config(config)
```

## Usage Examples

### Basic Spatial Queries

```python
import spatio

db = spatio.Spatio.memory()

# Insert city data
# Note: Point uses (longitude, latitude) order - matching GeoJSON standard
cities = [
    (spatio.Point(-74.0060, 40.7128), b"New York"),
    (spatio.Point(-0.1278, 51.5074), b"London"),
    (spatio.Point(139.6503, 35.6762), b"Tokyo"),
    (spatio.Point(2.3522, 48.8566), b"Paris"),
]

for point, name in cities:
    db.insert_point("cities", point, name)

# Find cities within 6000km of New York
nyc = spatio.Point(40.7128, -74.0060)
nearby = db.query_within_radius("cities", nyc, 6000000.0, 10)

print(f"Cities within 6000km of NYC:")
for point, name, distance in nearby:
    print(f"  {name.decode()}: {distance/1000:.0f}km")
```

### Advanced Spatial Operations

```python
import spatio

db = spatio.Spatio.memory()

# Insert cities
db.insert_point("cities", spatio.Point(40.7128, -74.0060), b"NYC")
db.insert_point("cities", spatio.Point(34.0522, -118.2437), b"LA")
db.insert_point("cities", spatio.Point(41.8781, -87.6298), b"Chicago")

# Distance calculation with multiple metrics
nyc = spatio.Point(40.7128, -74.0060)
la = spatio.Point(34.0522, -118.2437)
metric = spatio.DistanceMetric("haversine")
distance = db.distance_between(nyc, la, metric)
print(f"NYC to LA: {distance / 1000:.2f} km")

# K-nearest-neighbors
query = spatio.Point(40.0, -75.0)
nearest = db.knn("cities", query, 2, 500_000.0, metric)
for point, data, dist in nearest:
    print(f"{data.decode()}: {dist / 1000:.2f} km away")

# Polygon queries (coordinates as list of (lon, lat) tuples)
polygon = [
    (-90.0, 35.0),
    (-70.0, 35.0),
    (-70.0, 45.0),
    (-90.0, 45.0),
    (-90.0, 35.0),  # Close the polygon
]
in_region = db.query_within_polygon("cities", polygon, 100)
print(f"Cities in region: {len(in_region)}")
```

### Trajectory Tracking

```python
import spatio
import time

db = spatio.Spatio.memory()

# Create a trajectory (list of (Point, timestamp) tuples)
trajectory = [
    (spatio.Point(40.7128, -74.0060), 1640995200),  # NYC
    (spatio.Point(40.7580, -73.9855), 1640995800),  # Central Park
    (spatio.Point(40.6892, -74.0445), 1640996400),  # Brooklyn
]

# Store trajectory
db.insert_trajectory("vehicle:truck001", trajectory)

# Query trajectory for specific time range
path = db.query_trajectory("vehicle:truck001", 1640995200, 1640996400)

print(f"Vehicle path ({len(path)} points):")
for point, timestamp in path:
    print(f"  {timestamp}: ({point.lat:.4f}, {point.lon:.4f})")
```

### TTL and Expiration

```python
import spatio
import time

db = spatio.Spatio.memory()

# Data that expires in 5 seconds
opts = spatio.SetOptions.with_ttl(5.0)
db.insert(b"session:temp", b"temporary_data", opts)

print("Immediate:", db.get(b"session:temp"))  # b'temporary_data'
time.sleep(6)
print("After TTL:", db.get(b"session:temp"))  # None
```

### Sequential Operations

```python
import spatio

db = spatio.Spatio.memory()

# Sequential operations (atomic operations coming in future version)
db.insert(b"user:1", b"Alice")
db.insert(b"user:2", b"Bob")

point = spatio.Point(40.7128, -74.0060)
db.insert_point("locations", point, b"NYC Office")

print("Operations completed")

# Verify all operations were applied
print(db.get(b"user:1"))  # b'Alice'
nearby = db.query_within_radius("locations", spatio.Point(40.7128, -74.0060), 1000, 10)
print(len(nearby))  # 1
```

### Bounding Box Queries

```python
import spatio

db = spatio.Spatio.memory()

# Insert points across different regions
# Point uses (longitude, latitude) order - same as GeoJSON
points = [
    (spatio.Point(-74.0060, 40.7128), b"NYC"),      # North America
    (spatio.Point(-0.1278, 51.5074), b"London"),    # Europe
    (spatio.Point(139.6503, 35.6762), b"Tokyo"),    # Asia
]

for point, name in points:
    db.insert_point("cities", point, name)

# Find cities in Europe (rough bounding box)
european_cities = db.find_within_bounds(
    "cities",
    40.0, -10.0,  # min_lat, min_lon
    60.0, 10.0,   # max_lat, max_lon
    10            # limit
)

print("European cities:")
for point, name in european_cities:
    print(f"  {name.decode()} at ({point.lat:.2f}, {point.lon:.2f})")
```

## Performance

Spatio-Py is built for high performance:

- **Fast spatial indexing** using R*-tree algorithms
- **Memory efficient** storage with zero-copy operations where possible
- **Concurrent access** with minimal locking overhead
- **Optimized distance calculations** using efficient approximation algorithms

### Benchmarks

Basic performance characteristics (your results may vary):

- **Key-value operations**: ~1.6M ops/sec (600ns per operation)
- **Spatial insertions**: ~1.9M points/sec (530ns per operation)
- **Spatial queries**: ~225K queries/sec (4.4μs per operation)
- **Memory usage**: Efficient in-memory storage with spatial indexing

## Development

### Development Tools

This project uses [`just`](https://github.com/casey/just) as the primary task runner for all development workflows. Just provides a more powerful and expressive alternative to Make.

```bash
# Install just (if not already installed)
cargo install just

# See all available commands
just --list

# Common development tasks
just setup          # Set up development environment
just build           # Build the package
just test            # Run tests
just check           # Run all quality checks (lint, format, typecheck)
just ci              # Run full CI pipeline locally
```

### Building from Source

```bash
# Prerequisites
pip install maturin pytest

# Clone and build
git clone https://github.com/pkvartsianyi/spatio.git
cd spatio/py-spatio

# Development build
maturin develop

# Run tests
just test

# Run examples
python examples/basic_usage.py
```

### Building Multi-Platform Wheels

The project uses [`cibuildwheel`](https://cibuildwheel.readthedocs.io/) to build wheels for all major platforms and Python versions (3.9-3.13):

**Supported Platforms:**
- Linux: `x86_64`, `aarch64` (manylinux)
- macOS: `x86_64` (Intel), `arm64` (Apple Silicon)
**Automated Builds:**
- Wheels are automatically built on every release via GitHub Actions
- All wheels are tested before publishing to PyPI
- The workflow generates 40+ wheel files covering all platform/Python combinations

**Manual Build:**
```bash
# Install cibuildwheel
pip install cibuildwheel

# Build wheels for your platform
cibuildwheel --platform linux  # or macos

# Build for specific Python versions
CIBW_BUILD="cp311-* cp312-*" cibuildwheel

# Output wheels will be in ./wheelhouse/
```

For more details, see `.github/workflows/wheels.yml`

### Testing

```bash
# Run all tests
just test

# Run with coverage
just coverage

# Run performance tests
just bench
```

### Code Formatting

```bash
# Format Python code
just fmt

# Type checking
just typecheck

# Run all checks
just check
```

## API Reference

### Database Operations

| Method | Description |
|--------|-------------|
| `Spatio.memory()` | Create in-memory database |
| `Spatio.open(path)` | Open/create persistent database |
| `insert(key, value, options=None)` | Store key-value pair |
| `get(key)` | Retrieve value by key |
| `delete(key)` | Remove key and return old value |
| *(atomic operations coming soon)* | Execute operations atomically |
| `sync()` | Force sync to disk |
| `stats()` | Get database statistics |

### Spatial Operations

| Method | Description |
|--------|-------------|
| `insert_point(prefix, point, value, options=None)` | Store geographic point |
| `query_within_radius(prefix, center, radius_meters, limit)` | Find points within radius |
| `contains_point(prefix, center, radius_meters)` | Check if any points exist in radius |
| `count_within_radius(prefix, center, radius_meters)` | Count points within radius |
| `intersects_bounds(prefix, min_lat, min_lon, max_lat, max_lon)` | Check if any points in bounding box |
| `find_within_bounds(prefix, min_lat, min_lon, max_lat, max_lon, limit)` | Find points in bounding box |
| `distance_between(point1, point2, metric)` | Calculate distance with specified metric |
| `knn(prefix, center, k, max_radius, metric)` | Find K nearest neighbors |
| `query_within_polygon(prefix, polygon_coords, limit)` | Find points within polygon boundary |

### Trajectory Operations

| Method | Description |
|--------|-------------|
| `insert_trajectory(object_id, trajectory, options=None)` | Store trajectory data |
| `query_trajectory(object_id, start_time, end_time)` | Query trajectory for time range |

## Error Handling

Spatio-Py uses standard Python exceptions:

```python
import spatio

try:
    # Invalid coordinates
    point = spatio.Point(91.0, 0.0)  # Raises ValueError
except ValueError as e:
    print(f"Invalid point: {e}")

try:
    db = spatio.Spatio.open("/invalid/path/db.spatio")
except RuntimeError as e:
    print(f"Database error: {e}")
```

## Project Status

Spatio-Python is in **alpha development**:
- Core spatial operations implemented
- Advanced spatial features (distance, KNN, polygon queries) powered by georust/geo
- Complete Python API via PyO3 bindings
- TTL and persistence support
- Multi-platform wheels (Linux, macOS)
- Python 3.8-3.13 support

Current version: **0.1.0-alpha.10**

## Coordinate Convention

**Important**: Coordinate ordering differs between Python and Rust for user convenience:

- **Python API**: `Point(latitude, longitude)` - familiar order for most users
- **Rust/geo**: `Point::new(longitude, latitude)` - standard (x, y) cartesian convention

The Python bindings automatically convert between these formats, so you don't need to worry about it!

```python
# Python: latitude first (user-friendly)
nyc = spatio.Point(40.7128, -74.0060)
print(nyc.lat)  # 40.7128
print(nyc.lon)  # -74.0060
```

### Platform Support

Pre-built wheels are available for:
- **Linux**: x86_64, aarch64 (manylinux)
- **macOS**: x86_64 (Intel), arm64 (Apple Silicon)
- **Python**: 3.9, 3.10, 3.11, 3.12, 3.13
## License

This project is licensed under the MIT License - see the [LICENSE](../LICENSE) file for details.

## Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](../CONTRIBUTING.md) for guidelines.

## Spatial Features Documentation

For comprehensive documentation on all spatial operations:
- **[SPATIAL_FEATURES.md](../SPATIAL_FEATURES.md)** - Complete API reference with examples
- **[examples/advanced_spatial.rs](../examples/advanced_spatial.rs)** - Comprehensive Rust demo

## Links

- **GitHub Repository**: https://github.com/pkvartsianyi/spatio
- **Documentation**: https://github.com/pkvartsianyi/spatio#readme
- **PyPI Package**: https://pypi.org/project/spatio/
- **Issue Tracker**: https://github.com/pkvartsianyi/spatio/issues
- **georust/geo**: https://github.com/georust/geo
