# Spatio: Python Bindings for High-Performance Spatial Database

[![PyPI version](https://badge.fury.io/py/spatio.svg)](https://badge.fury.io/py/spatio)
[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Python bindings for [Spatio](https://github.com/pkvartsianyi/spatio), a blazingly fast, embedded spatio-temporal database written in Rust. Spatio brings high-performance spatial operations and geographic data management to Python with minimal overhead.

## Features

**High Performance**: Built on Rust for maximum speed and memory efficiency
**Spatial Operations**: Geographic point storage with automatic spatial indexing
**Trajectory Tracking**: Store and query movement data over time
**TTL Support**: Automatic data expiration with time-to-live
**Thread-Safe**: Concurrent access with atomic operations
**Persistent Storage**: Optional file-based persistence
## Installation

### From PyPI (Recommended)

```bash
pip install spatio
```

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
nyc = spatio.Point(40.7128, -74.0060)
london = spatio.Point(51.5074, -0.1278)

db.insert_point("cities", nyc, b"New York City")
db.insert_point("cities", london, b"London")

# Find nearby points within 6000km
nearby = db.find_nearby("cities", nyc, 6000000.0, 10)
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
nearby = db.find_nearby(prefix, center, radius_meters, limit)
count = db.count_within_distance(prefix, center, radius_meters)

# Trajectory operations
db.insert_trajectory(object_id, trajectory, options=None)
path = db.query_trajectory(object_id, start_time, end_time)
```

### Point

Represents a geographic coordinate.

```python
# Create points
point = spatio.Point(latitude, longitude)
print(f"Location: {point.lat}, {point.lon}")

# Calculate distance
distance = point1.distance_to(point2)  # Returns meters
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
# Custom geohash precision (1-12, default: 8)
config = spatio.Config.with_geohash_precision(10)  # ~61cm accuracy
db = spatio.Spatio.memory_with_config(config)

# Manual configuration
config = spatio.Config()
config.geohash_precision = 6  # ~610m accuracy
```

## Usage Examples

### Basic Spatial Queries

```python
import spatio

db = spatio.Spatio.memory()

# Insert city data
cities = [
    (spatio.Point(40.7128, -74.0060), b"New York"),
    (spatio.Point(51.5074, -0.1278), b"London"),
    (spatio.Point(35.6762, 139.6503), b"Tokyo"),
    (spatio.Point(48.8566, 2.3522), b"Paris"),
]

for point, name in cities:
    db.insert_point("cities", point, name)

# Find cities within 6000km of New York
nyc = spatio.Point(40.7128, -74.0060)
nearby = db.find_nearby("cities", nyc, 6000000.0, 10)

print(f"Cities within 6000km of NYC:")
for point, name, distance in nearby:
    print(f"  {name.decode()}: {distance/1000:.0f}km")
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

### Atomic Operations

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
nearby = db.find_nearby("locations", spatio.Point(40.7128, -74.0060), 1000, 10)
print(len(nearby))  # 1
```

### Bounding Box Queries

```python
import spatio

db = spatio.Spatio.memory()

# Insert points across different regions
points = [
    (spatio.Point(40.7128, -74.0060), b"NYC"),      # North America
    (spatio.Point(51.5074, -0.1278), b"London"),    # Europe
    (spatio.Point(35.6762, 139.6503), b"Tokyo"),    # Asia
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

- **Fast spatial indexing** using geohash and R-tree algorithms
- **Memory efficient** storage with zero-copy operations where possible
- **Concurrent access** with minimal locking overhead
- **Optimized distance calculations** using efficient approximation algorithms

### Benchmarks

Basic performance characteristics (your results may vary):

- **Key-value operations**: >1M ops/sec
- **Spatial insertions**: >100K points/sec
- **Spatial queries**: >10K queries/sec
- **Memory usage**: ~1KB per 1000 points

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
| `atomic(func)` | Execute operations atomically |
| `sync()` | Force sync to disk |
| `stats()` | Get database statistics |

### Spatial Operations

| Method | Description |
|--------|-------------|
| `insert_point(prefix, point, value, options=None)` | Store geographic point |
| `find_nearby(prefix, center, radius_meters, limit)` | Find points within radius |
| `contains_point(prefix, center, radius_meters)` | Check if any points exist in radius |
| `count_within_distance(prefix, center, radius_meters)` | Count points within radius |
| `intersects_bounds(prefix, min_lat, min_lon, max_lat, max_lon)` | Check if any points in bounding box |
| `find_within_bounds(prefix, min_lat, min_lon, max_lat, max_lon, limit)` | Find points in bounding box |

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

## License

This project is licensed under the MIT License - see the [LICENSE](../LICENSE) file for details.

## Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](../CONTRIBUTING.md) for guidelines.

## Links

- **GitHub Repository**: https://github.com/pkvartsianyi/spatio
- **Documentation**: https://github.com/pkvartsianyi/spatio#readme
- **PyPI Package**: https://pypi.org/project/spatio/
- **Issue Tracker**: https://github.com/pkvartsianyi/spatio/issues
