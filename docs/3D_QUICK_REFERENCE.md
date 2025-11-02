# 3D Spatial Indexing - Quick Reference

## Basic Setup

```rust
use spatio::{Spatio, Point3d, BoundingBox3D, SetOptions};
use std::time::Duration;

let db = Spatio::memory()?;
```

## Insert 3D Points

```rust
// Basic insert
let drone_pos = Point3d::new(-74.0060, 40.7128, 100.0); // lon, lat, altitude
db.insert_point_3d("drones", &drone_pos, b"Drone-001", None)?;

// With TTL (auto-expire after 60 seconds)
let opts = SetOptions::with_ttl(Duration::from_secs(60));
db.insert_point_3d("drones", &drone_pos, b"Drone-002", Some(opts))?;
```

## Query Methods

### 1. Spherical Radius Query (3D)

Find points within a sphere around a center point.

```rust
let center = Point3d::new(-74.0065, 40.7133, 125.0);
let radius = 500.0; // meters (3D distance)
let limit = 10;

let results = db.query_within_sphere_3d("drones", &center, radius, limit)?;
// Returns: Vec<(Point3d, Bytes, f64)> - (point, data, distance)

for (point, data, distance) in results {
    println!("Found {} at {}m distance, altitude: {}m",
             String::from_utf8_lossy(&data),
             distance,
             point.altitude());
}
```

**Use when:** Finding nearby objects in true 3D proximity.

### 2. Cylindrical Query (Altitude Range + Horizontal Radius)

Find points within horizontal radius AND specific altitude range.

```rust
let center = Point3d::new(-74.0065, 40.7133, 0.0);
let min_altitude = 3000.0;  // meters
let max_altitude = 7000.0;  // meters
let horizontal_radius = 10000.0; // 10km

let results = db.query_within_cylinder_3d(
    "aircraft",
    &center,
    min_altitude,
    max_altitude,
    horizontal_radius,
    100
)?;
// Returns: Vec<(Point3d, Bytes, f64)> - (point, data, horizontal_distance)
```

**Use when:** 
- Querying specific flight levels
- Floor-specific searches in buildings
- Altitude-constrained proximity

### 3. Bounding Box Query (3D Volume)

Find points within a 3D rectangular box.

```rust
let bbox = BoundingBox3D::new(
    -74.01, 40.71, 50.0,   // min: lon, lat, altitude
    -74.00, 40.72, 150.0   // max: lon, lat, altitude
);

let results = db.query_within_bbox_3d("drones", &bbox, 100)?;
// Returns: Vec<(Point3d, Bytes)>
```

**Use when:** 3D geofencing, volume-based searches.

### 4. K-Nearest Neighbors (3D)

Find the k closest points to a location.

```rust
let query_point = Point3d::new(-74.0065, 40.7133, 100.0);
let k = 5; // find 5 nearest

let nearest = db.knn_3d("drones", &query_point, k)?;
// Returns: Vec<(Point3d, Bytes, f64)> - (point, data, distance)

for (i, (point, data, dist)) in nearest.iter().enumerate() {
    println!("{}. {} at {:.1}m", i+1, String::from_utf8_lossy(data), dist);
}
```

**Use when:** Finding nearest resources, emergency dispatch.

## Distance Calculations

```rust
let p1 = Point3d::new(-74.0060, 40.7128, 100.0);
let p2 = Point3d::new(-74.0070, 40.7138, 200.0);

// 3D distance (haversine horizontal + altitude vertical)
let dist_3d = p1.haversine_3d(&p2);

// Horizontal distance only (ignores altitude)
let dist_horizontal = p1.haversine_2d(&p2);

// Altitude difference only
let alt_diff = p1.altitude_difference(&p2);

// Database method
let distance = db.distance_between_3d(&p1, &p2)?;
```

## Point3d Methods

```rust
let point = Point3d::new(-74.0060, 40.7128, 100.0);

// Accessors
point.x()          // longitude
point.y()          // latitude
point.z()          // altitude (same as .altitude())
point.altitude()   // altitude

// Conversions
point.to_2d()      // Get 2D Point (drops altitude)
point.point_2d()   // Get reference to 2D point

// Create from 2D + altitude
let point_2d = Point::new(-74.0060, 40.7128);
let point_3d = Point3d::from_point_and_altitude(point_2d, 100.0);
```

## BoundingBox3D

```rust
let bbox = BoundingBox3D::new(
    -74.01, 40.71, 50.0,   // min x, y, z
    -74.00, 40.72, 150.0   // max x, y, z
);

// Check containment
bbox.contains_point(-74.005, 40.715, 100.0); // true/false

// Check intersection
let other = BoundingBox3D::new(...);
bbox.intersects(&other); // true/false

// Expand
let expanded = bbox.expand(10.0); // expand by 10m in all directions

// Get properties
bbox.volume();    // volume in cubic units
bbox.width();     // x dimension
bbox.height();    // y dimension
bbox.depth();     // z dimension
bbox.center();    // (x, y, z) tuple

// Project to 2D
let bbox_2d = bbox.to_2d(); // BoundingBox2D
```

## Common Patterns

### Drone Tracking
```rust
// Insert drone position (expires in 30s)
let pos = Point3d::new(-74.006, 40.713, 50.0);
let opts = SetOptions::with_ttl(Duration::from_secs(30));
db.insert_point_3d("active-drones", &pos, b"Drone-001", Some(opts))?;

// Find drones in low-altitude corridor (0-100m, within 5km)
let center = Point3d::new(-74.006, 40.713, 0.0);
let nearby = db.query_within_cylinder_3d(
    "active-drones", &center, 0.0, 100.0, 5000.0, 100
)?;
```

### Air Traffic Control
```rust
// Track aircraft
let pos = Point3d::new(-74.010, 40.720, 10000.0); // FL330
db.insert_point_3d("aircraft", &pos, b"AA123", None)?;

// Find aircraft in specific sector (3D volume)
let sector = BoundingBox3D::new(
    -74.03, 40.71, 9000.0,   // FL295
    -74.00, 40.74, 11000.0   // FL360
);
let in_sector = db.query_within_bbox_3d("aircraft", &sector, 100)?;

// Conflict detection (aircraft within 1000m of each other)
for (point, _) in &in_sector {
    let conflicts = db.query_within_sphere_3d("aircraft", point, 1000.0, 10)?;
    if conflicts.len() > 1 {
        println!("⚠️  CONFLICT WARNING");
    }
}
```

### Multi-Floor Building
```rust
// 3 meters per floor
for floor in 0..20 {
    let altitude = floor as f64 * 3.0;
    let pos = Point3d::new(-74.006, 40.713, altitude);
    let info = format!("Sensor-Floor-{}", floor);
    db.insert_point_3d("building-sensors", &pos, info.as_bytes(), None)?;
}

// Query floors 5-10
let building = Point3d::new(-74.006, 40.713, 0.0);
let sensors = db.query_within_cylinder_3d(
    "building-sensors",
    &building,
    15.0,  // floor 5
    30.0,  // floor 10
    10.0,  // 10m horizontal (same building)
    100
)?;
```

## Performance Tips

### ✅ DO
- Use cylindrical queries for altitude ranges (more efficient than sphere)
- Set appropriate TTL for moving objects
- Use prefix organization to separate different types
- Query larger area once, filter in memory if needed

### ❌ DON'T
- Don't use sphere queries when you need altitude ranges (use cylinder)
- Don't store duplicate data in both 2D and 3D indexes unnecessarily
- Don't query with unlimited results (always set a limit)

## Error Handling

```rust
use spatio::Result;

fn track_drone(db: &Spatio) -> Result<()> {
    let pos = Point3d::new(-74.0, 40.7, 100.0);
    db.insert_point_3d("drones", &pos, b"data", None)?;
    
    let results = db.query_within_sphere_3d("drones", &pos, 1000.0, 10)?;
    
    Ok(())
}
```

## Complete Example

```rust
use spatio::{Spatio, Point3d, SetOptions};
use std::time::Duration;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Spatio::memory()?;
    
    // Insert some drones
    let drones = vec![
        ("D1", -74.00, 40.71, 50.0),
        ("D2", -74.01, 40.72, 75.0),
        ("D3", -74.02, 40.73, 100.0),
    ];
    
    for (id, lon, lat, alt) in drones {
        let pos = Point3d::new(lon, lat, alt);
        let opts = SetOptions::with_ttl(Duration::from_secs(60));
        db.insert_point_3d("drones", &pos, id.as_bytes(), Some(opts))?;
    }
    
    // Find drones within 500m in 3D space
    let center = Point3d::new(-74.01, 40.72, 75.0);
    let nearby = db.query_within_sphere_3d("drones", &center, 500.0, 10)?;
    
    println!("Found {} drones nearby", nearby.len());
    for (point, data, distance) in nearby {
        println!("  {} at altitude {}m, distance {:.0}m",
                 String::from_utf8_lossy(&data),
                 point.altitude(),
                 distance);
    }
    
    Ok(())
}
```

## See Also

- [Full Documentation](./3D_SPATIAL_INDEXING.md)
- [Examples](../examples/3d_spatial_tracking.rs)
- [API Reference](https://docs.rs/spatio)