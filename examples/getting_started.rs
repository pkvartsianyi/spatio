use spatio::{Point, SetOptions, Spatio};
use std::time::Duration;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Spatio - Getting Started Example");
    println!("=================================");

    // Create an in-memory database
    let db = Spatio::memory()?;
    println!("Created in-memory database");

    // Basic key-value operations
    db.insert("user:123", b"John Doe", None)?;
    let value = db.get("user:123")?.unwrap();
    println!(
        "Basic storage: user:123 = {}",
        String::from_utf8_lossy(&value)
    );

    // Store data with TTL (time-to-live)
    let ttl_options = SetOptions::with_ttl(Duration::from_secs(5));
    db.insert("session:abc", b"expires_soon", Some(ttl_options))?;
    println!("Stored session data with 5-second TTL");

    // Spatial point operations
    let new_york = Point::new(40.7128, -74.0060);
    let london = Point::new(51.5074, -0.1278);
    let tokyo = Point::new(35.6762, 139.6503);

    // Store geographic points (automatically indexed for spatial queries)
    db.insert_point("cities", &new_york, b"New York", None)?;
    db.insert_point("cities", &london, b"London", None)?;
    db.insert_point("cities", &tokyo, b"Tokyo", None)?;
    println!("Stored geographic points for major cities");

    // Calculate distance between cities
    let distance_km = new_york.distance_to(&london) / 1000.0;
    println!("Distance NYC ↔ London: {:.0} km", distance_km);

    // Find nearby cities (within 2000km of NYC)
    let nearby = db.find_nearby("cities", &new_york, 2_000_000.0, 5)?;
    println!("Found {} cities within 2000km of NYC", nearby.len());
    for (point, data) in &nearby {
        println!(
            "  - {} at ({:.2}, {:.2})",
            String::from_utf8_lossy(data),
            point.lat,
            point.lon
        );
    }

    // Atomic batch operations
    db.atomic(|batch| {
        batch.insert("sensor:temperature", b"22.5C", None)?;
        batch.insert("sensor:humidity", b"65%", None)?;
        batch.insert("sensor:pressure", b"1013.25 hPa", None)?;
        Ok(())
    })?;
    println!("Performed atomic batch insert of sensor data");

    // Trajectory tracking example
    let vehicle_path = vec![
        (Point::new(40.7128, -74.0060), 1640995200), // Start: NYC
        (Point::new(40.7150, -74.0040), 1640995260), // 1 minute later
        (Point::new(40.7172, -74.0020), 1640995320), // 2 minutes later
        (Point::new(40.7194, -74.0000), 1640995380), // 3 minutes later
    ];

    db.insert_trajectory("vehicle:truck001", &vehicle_path, None)?;
    println!(
        "Stored vehicle trajectory with {} waypoints",
        vehicle_path.len()
    );

    // Query trajectory for a time range
    let path_segment = db.query_trajectory("vehicle:truck001", 1640995200, 1640995320)?;
    println!(
        "Retrieved {} waypoints for first 2 minutes",
        path_segment.len()
    );

    // Check database statistics
    let stats = db.stats()?;
    println!("Database contains {} keys", stats.key_count);

    // Wait a moment to see TTL in action
    println!("\nWaiting 6 seconds to demonstrate TTL...");
    std::thread::sleep(Duration::from_secs(6));

    // Try to retrieve expired data
    match db.get("session:abc")? {
        Some(_) => println!("TTL data still exists (unexpected)"),
        None => println!("TTL data expired as expected"),
    }

    println!("\nGetting started example completed successfully!");
    println!("\nKey features demonstrated:");
    println!("- Simple key-value storage");
    println!("- Automatic spatial indexing for points");
    println!("- Nearby point queries");
    println!("- Trajectory tracking over time");
    println!("- TTL (time-to-live) support");
    println!("- Atomic batch operations");

    println!("\nNext steps:");
    println!("- Try the 'spatial_queries' example for more spatial operations");
    println!("- See 'trajectory_tracking' for advanced movement analysis");

    Ok(())
}
