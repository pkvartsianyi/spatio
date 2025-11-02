# 3D Spatial Indexing in Spatio

## Overview

Spatio now includes comprehensive 3D spatial indexing capabilities using R-tree data structures. This enables altitude-aware queries for applications like:

- **Drone tracking and management**
- **Aviation and air traffic control**
- **Multi-floor building navigation**
- **Underwater vehicle tracking**
- **3D geofencing and airspace management**

## Key Features

### 1. 3D Point Type (`Point3d`)

The `Point3d` type represents a point in 3D space with x, y (longitude/latitude) and z (altitude/elevation) coordinates.

```rust
use spatio::Point3d;

// Create a 3D point
let drone_position = Point3d::new(-74.0060, 40.7128, 100.0);

// Access coordinates
let lon = drone_position.x();
let lat = drone_position.y();
let altitude = drone_position.altitude(); // or .z()

// Project to 2D
let point_2d = drone_position.to_2d();
```

### 2. 3D Distance Calculations

Spatio provides accurate 3D distance calculations that combine haversine distance (for horizontal distance considering Earth's curvature) with altitude differences.

```rust
let p1 = Point3d::new(-74.0060, 40.7128, 100.0);
let p2 = Point3d::new(-74.0070, 40.7138, 200.0);

// 3D distance (haversine + altitude)
let distance_3d = p1.haversine_3d(&p2);

// Horizontal distance only (ignoring altitude)
let distance_2d = p1.haversine_2d(&p2);

// Altitude difference
let alt_diff = p1.altitude_difference(&p2);
```

### 3. R-tree Based 3D Indexing

Spatio uses R-tree spatial indexing for efficient 3D queries. The R-tree automatically organizes points in 3D space for fast spatial lookups.

**Performance characteristics:**
- Insert: O(log n)
- Query: O(log n + k) where k is the number of results
- Memory: O(n)

## API Reference

### Inserting 3D Points

```rust
use spatio::{Spatio, Point3d};

let db = Spatio::memory()?;

// Insert a 3D point with altitude
let drone_pos = Point3d::new(-74.0060, 40.7128, 100.0);
db.insert_point_3d("drones", &drone_pos, b"Drone-001", None)?;

// With TTL
use std::time::Duration;
use spatio::SetOptions;

let opts = SetOptions::with_ttl(Duration::from_secs(300));
db.insert_point_3d("drones", &drone_pos, b"Drone-002", Some(opts))?;
```

### Query Within 3D Sphere

Find all points within a spherical radius from a center point, considering altitude differences.

```rust
let center = Point3d::new(-74.0065, 40.7133, 125.0);
let radius = 500.0; // meters (3D distance)
let limit = 10;

let results = db.query_within_sphere_3d("drones", &center, radius, limit)?;

for (point, data, distance) in results {
    println!("Found at altitude {}m, distance: {}m", 
             point.altitude(), distance);
}
```

**Use cases:**
- Find nearby drones within 3D proximity
- Detect aircraft conflicts
- Proximity alerts in 3D space

### Query Within Cylindrical Volume

Find points within a horizontal radius and specific altitude range. This is ideal for altitude-constrained searches.

```rust
let center = Point3d::new(-74.0065, 40.7133, 0.0);
let min_altitude = 3000.0;  // minimum altitude in meters
let max_altitude = 7000.0;  // maximum altitude in meters
let horizontal_radius = 10000.0; // 10km horizontal

let results = db.query_within_cylinder_3d(
    "aircraft",
    &center,
    min_altitude,
    max_altitude,
    horizontal_radius,
    100
)?;

for (point, data, h_distance) in results {
    println!("Aircraft at FL{:.0}, horizontal distance: {:.1}km",
             point.altitude() / 30.48 / 100.0,
             h_distance / 1000.0);
}
```

**Use cases:**
- Air traffic control (specific flight levels)
- Drone corridor monitoring
- Altitude-restricted zone queries
- Multi-floor building searches

### Query Within 3D Bounding Box

Search within a 3D rectangular volume defined by minimum and maximum x, y, z coordinates.

```rust
use spatio::BoundingBox3D;

// Define a 3D volume
let bbox = BoundingBox3D::new(
    -74.01, 40.71, 50.0,   // min x, y, z
    -74.00, 40.72, 150.0,  // max x, y, z
);

let results = db.query_within_bbox_3d("drones", &bbox, 100)?;

for (point, data) in results {
    println!("Drone at ({}, {}, {}m)",
             point.x(), point.y(), point.altitude());
}
```

**Use cases:**
- Geofencing in 3D space
- Building floor range queries
- Volume-based spatial searches

### K-Nearest Neighbors in 3D

Find the k nearest points to a query location in 3D space.

```rust
let query_point = Point3d::new(-74.0065, 40.7133, 100.0);
let k = 5; // find 5 nearest

let nearest = db.knn_3d("drones", &query_point, k)?;

for (i, (point, data, distance)) in nearest.iter().enumerate() {
    println!("{}. {} at {:.1}m distance", 
             i + 1, 
             String::from_utf8_lossy(data),
             distance);
}
```

**Use cases:**
- Find nearest available drones
- Emergency response dispatching
- Resource allocation

### 3D Distance Calculation

Calculate distance between two 3D points without querying the database.

```rust
let p1 = Point3d::new(-74.0060, 40.7128, 0.0);
let p2 = Point3d::new(-74.0070, 40.7138, 100.0);

let distance = db.distance_between_3d(&p1, &p2)?;
println!("3D distance: {:.2}m", distance);
```

## Real-World Examples

### Example 1: Drone Fleet Management

```rust
use spatio::{Spatio, Point3d, SetOptions};
use std::time::Duration;

let db = Spatio::memory()?;

// Register active drones
let drones = vec![
    ("drone-001", -74.0060, 40.7128, 50.0, "Delivery"),
    ("drone-002", -74.0070, 40.7138, 75.0, "Survey"),
    ("drone-003", -74.0050, 40.7118, 100.0, "Inspection"),
];

for (id, lon, lat, alt, mission) in drones {
    let position = Point3d::new(lon, lat, alt);
    let info = format!("{}: {}", id, mission);
    
    // Positions expire after 30 seconds (stale data cleanup)
    let opts = SetOptions::with_ttl(Duration::from_secs(30));
    db.insert_point_3d("active-drones", &position, info.as_bytes(), Some(opts))?;
}

// Find drones in a specific altitude corridor (40-80m)
let airspace_center = Point3d::new(-74.0065, 40.7133, 0.0);
let drones_in_corridor = db.query_within_cylinder_3d(
    "active-drones",
    &airspace_center,
    40.0,  // min altitude
    80.0,  // max altitude
    5000.0, // 5km radius
    100
)?;

println!("Drones in low-altitude corridor: {}", drones_in_corridor.len());
```

### Example 2: Air Traffic Control

```rust
use spatio::{Spatio, Point3d, BoundingBox3D};

let db = Spatio::memory()?;

// Track commercial flights
let flights = vec![
    ("AA123", -74.0100, 40.7200, 10000.0, "NYC->BOS"),
    ("UA456", -74.0200, 40.7300, 10500.0, "NYC->LAX"),
    ("DL789", -74.0150, 40.7250, 9800.0, "NYC->MIA"),
];

for (flight, lon, lat, alt, route) in flights {
    let position = Point3d::new(lon, lat, alt);
    let info = format!("{} - {}", flight, route);
    db.insert_point_3d("aircraft", &position, info.as_bytes(), None)?;
}

// Monitor a specific sector (3D volume)
let sector = BoundingBox3D::new(
    -74.03, 40.71, 9500.0,   // min
    -74.00, 40.74, 11000.0,  // max
);

let aircraft_in_sector = db.query_within_bbox_3d("aircraft", &sector, 100)?;
println!("Aircraft in sector: {}", aircraft_in_sector.len());

// Conflict detection: find aircraft too close to each other
for (point, data) in &aircraft_in_sector {
    let nearby = db.query_within_sphere_3d(
        "aircraft",
        point,
        1000.0, // 1km separation minimum
        10
    )?;
    
    if nearby.len() > 1 { // More than just itself
        println!("⚠️  CONFLICT WARNING: {} has traffic within 1km",
                 String::from_utf8_lossy(data));
    }
}
```

### Example 3: Multi-Floor Building Navigation

```rust
use spatio::{Spatio, Point3d};

let db = Spatio::memory()?;

// Building coordinates
let building_lon = -74.0060;
let building_lat = 40.7128;

// Track sensors on each floor (3 meters per floor)
for floor in 0..20 {
    let altitude = floor as f64 * 3.0;
    let position = Point3d::new(building_lon, building_lat, altitude);
    let info = format!("Temperature sensor - Floor {}", floor);
    db.insert_point_3d("building-sensors", &position, info.as_bytes(), None)?;
}

// Query sensors on floors 5-10
let building_pos = Point3d::new(building_lon, building_lat, 0.0);
let sensors = db.query_within_cylinder_3d(
    "building-sensors",
    &building_pos,
    5.0 * 3.0,  // floor 5
    10.0 * 3.0, // floor 10
    10.0,       // 10m horizontal (same building)
    100
)?;

println!("Sensors on floors 5-10: {}", sensors.len());
```

## Performance Considerations

### Index Efficiency

The R-tree 3D index provides:

- **Fast spatial queries**: O(log n + k) where k is result count
- **Efficient memory usage**: Points are clustered spatially in tree nodes
- **Automatic balancing**: The R-tree self-balances for optimal performance

### Best Practices

1. **Choose appropriate precision**: Higher altitude precision may not always be needed
   ```rust
   // For drones: 1-meter altitude precision is usually sufficient
   let drone_pos = Point3d::new(-74.0060, 40.7128, 100.0);
   
   // For aircraft: 10-100 meter precision is often enough
   let aircraft_pos = Point3d::new(-74.0060, 40.7128, 10000.0);
   ```

2. **Use cylindrical queries for altitude ranges**: More efficient than 3D sphere for altitude-constrained searches
   ```rust
   // Better for "find aircraft between FL300-FL400"
   let results = db.query_within_cylinder_3d(...);
   
   // Less efficient for altitude ranges
   let results = db.query_within_sphere_3d(...); // filters entire sphere
   ```

3. **Leverage TTL for moving objects**: Automatically clean up stale position data
   ```rust
   let opts = SetOptions::with_ttl(Duration::from_secs(60));
   db.insert_point_3d("drones", &pos, data, Some(opts))?;
   ```

4. **Batch queries when possible**: Query once, filter in application if needed
   ```rust
   // Query larger area once
   let candidates = db.query_within_cylinder_3d(..., 10000.0, ...)?;
   
   // Filter in memory for specific needs
   let filtered: Vec<_> = candidates.into_iter()
       .filter(|(point, _, _)| point.altitude() > 5000.0)
       .collect();
   ```

## Distance Calculation Details

Spatio uses **haversine formula** for horizontal distance (accounting for Earth's curvature) combined with **Pythagorean theorem** for altitude:

```
horizontal_distance = haversine(lon1, lat1, lon2, lat2)
altitude_difference = |z2 - z1|
distance_3d = sqrt(horizontal_distance² + altitude_difference²)
```

This provides accurate distances for:
- Short to medium distances (< 1000km horizontal)
- Any altitude differences
- Geographic coordinates (WGS84)

## Type Reference

### `Point3d`

```rust
pub struct Point3d {
    pub point: Point<f64>,  // 2D point (lon/lat)
    pub z: f64,              // altitude/elevation
}

// Methods
impl Point3d {
    pub fn new(x: f64, y: f64, z: f64) -> Self;
    pub fn x(&self) -> f64;
    pub fn y(&self) -> f64;
    pub fn z(&self) -> f64;
    pub fn altitude(&self) -> f64;
    pub fn to_2d(&self) -> Point<f64>;
    pub fn haversine_3d(&self, other: &Point3d) -> f64;
    pub fn haversine_2d(&self, other: &Point3d) -> f64;
    pub fn distance_3d(&self, other: &Point3d) -> f64;
    pub fn altitude_difference(&self, other: &Point3d) -> f64;
}
```

### `BoundingBox3D`

```rust
pub struct BoundingBox3D {
    pub min_x: f64,
    pub min_y: f64,
    pub min_z: f64,
    pub max_x: f64,
    pub max_y: f64,
    pub max_z: f64,
}

// Methods
impl BoundingBox3D {
    pub fn new(min_x: f64, min_y: f64, min_z: f64,
               max_x: f64, max_y: f64, max_z: f64) -> Self;
    pub fn contains_point(&self, x: f64, y: f64, z: f64) -> bool;
    pub fn intersects(&self, other: &BoundingBox3D) -> bool;
    pub fn expand(&self, amount: f64) -> Self;
    pub fn to_2d(&self) -> BoundingBox2D;
    pub fn volume(&self) -> f64;
}
```

## Migration from 2D to 3D

If you have existing 2D spatial data and want to add altitude tracking:

```rust
use spatio::{Spatio, Point, Point3d};

let db = Spatio::memory()?;

// Existing 2D data
let point_2d = Point::new(-74.0060, 40.7128);
db.insert_point("locations", &point_2d, b"Ground level", None)?;

// Add 3D tracking for same location
let point_3d = Point3d::new(-74.0060, 40.7128, 100.0);
db.insert_point_3d("locations-3d", &point_3d, b"At altitude", None)?;

// Query both
let nearby_2d = db.query_within_radius("locations", &point_2d, 1000.0, 10)?;
let nearby_3d = db.query_within_sphere_3d("locations-3d", &point_3d, 1000.0, 10)?;
```

## See Also

- [Examples](../examples/3d_spatial_tracking.rs) - Complete working examples
- [API Documentation](https://docs.rs/spatio) - Full API reference
- [2D Spatial Indexing](./SPATIAL_INDEXING.md) - 2D indexing features
- [Geofencing Guide](./GEOFENCING.md) - Geofencing with Spatio