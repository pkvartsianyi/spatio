use spatio::{Point, SetOptions, Spatio, TemporalPoint};
use std::time::{Duration, UNIX_EPOCH};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging (set RUST_LOG=debug to see detailed logs)
    env_logger::init();

    println!("=== Spatio - Getting Started ===\n");

    // Create an in-memory database
    let mut db = Spatio::memory()?;
    println!("✓ Created in-memory database\n");

    // === BASIC KEY-VALUE STORAGE ===
    println!("1. Basic Key-Value Storage");
    println!("---------------------------");

    db.insert("user:123", b"John Doe", None)?;
    let value = db.get("user:123")?.unwrap();
    println!("   Stored: user:123 = {}", String::from_utf8_lossy(&value));

    // Store data with TTL (time-to-live)
    let ttl_options = SetOptions::with_ttl(Duration::from_secs(5));
    db.insert("session:abc", b"expires_soon", Some(ttl_options))?;
    println!("   Stored session data with 5-second TTL\n");

    // === SPATIAL OPERATIONS ===
    println!("2. Spatial Point Storage");
    println!("------------------------");

    // Store geographic points (lon, lat format)
    let nyc = Point::new(-74.0060, 40.7128);
    let london = Point::new(-0.1278, 51.5074);
    let paris = Point::new(2.3522, 48.8566);

    db.insert_point("cities", &nyc, b"New York", None)?;
    db.insert_point("cities", &london, b"London", None)?;
    db.insert_point("cities", &paris, b"Paris", None)?;
    println!("   Stored 3 cities with automatic spatial indexing");

    // Find nearby cities within radius (in meters)
    let nearby = db.query_within_radius("cities", &london, 500_000.0, 10)?;
    println!("   Found {} cities within 500km of London:", nearby.len());
    for (_, data, _) in &nearby {
        println!("     - {}", String::from_utf8_lossy(data));
    }
    println!();

    // === SPATIAL QUERY METHODS ===
    println!("3. Spatial Query Methods");
    println!("------------------------");

    // Check if any points exist within radius
    let has_nearby = db.intersects_radius("cities", &paris, 1_000_000.0)?;
    println!("   Cities within 1000km of Paris: {}", has_nearby);

    // Count points within radius
    let count = db.count_within_radius("cities", &nyc, 6_000_000.0)?;
    println!("   Cities within 6000km of NYC: {}", count);

    // Find points within bounding box (min_lon, min_lat, max_lon, max_lat)
    let results = db.find_within_bounds("cities", -10.0, 40.0, 10.0, 55.0, 10)?;
    println!("   Cities in European bounding box: {}", results.len());
    for (_point, data) in results {
        println!("     - {}", String::from_utf8_lossy(&data));
    }
    println!();

    // === ATOMIC OPERATIONS ===
    println!("4. Atomic Batch Operations");
    println!("---------------------------");

    db.atomic(|batch| {
        batch.insert("sensor:temp", b"22.5C", None)?;
        batch.insert("sensor:humidity", b"65%", None)?;
        batch.insert("sensor:pressure", b"1013 hPa", None)?;
        Ok(())
    })?;
    println!("   Atomically inserted 3 sensor readings\n");

    // === TRAJECTORY TRACKING ===
    println!("5. Trajectory Tracking");
    println!("----------------------");

    let vehicle_path = vec![
        TemporalPoint {
            point: Point::new(-74.0060, 40.7128),
            timestamp: UNIX_EPOCH + Duration::from_secs(1640995200),
        },
        TemporalPoint {
            point: Point::new(-74.0040, 40.7150),
            timestamp: UNIX_EPOCH + Duration::from_secs(1640995260),
        },
        TemporalPoint {
            point: Point::new(-74.0020, 40.7172),
            timestamp: UNIX_EPOCH + Duration::from_secs(1640995320),
        },
    ];

    db.insert_trajectory("vehicle:truck001", &vehicle_path, None)?;
    println!(
        "   Stored vehicle trajectory with {} waypoints",
        vehicle_path.len()
    );

    // Query trajectory for time range
    let path_segment = db.query_trajectory("vehicle:truck001", 1640995200, 1640995320)?;
    println!(
        "   Retrieved {} waypoints from trajectory\n",
        path_segment.len()
    );

    // === DATABASE STATS ===
    println!("6. Database Statistics");
    println!("----------------------");

    let stats = db.stats();
    println!("   Total keys stored: {}", stats.key_count);
    println!("   Total operations: {}\n", stats.operations_count);

    // === TTL DEMONSTRATION (LAZY) ===
    println!("7. TTL Expiration (Lazy Deletion)");
    println!("----------------------------------");
    println!("   Waiting 6 seconds for TTL expiration...");
    std::thread::sleep(Duration::from_secs(6));

    match db.get("session:abc")? {
        Some(_) => println!("   Session still active (unexpected)"),
        None => println!("   ✓ Session expired (lazy check on read)"),
    }

    // Manual cleanup
    let removed = db.cleanup_expired()?;
    println!("   Cleaned up {} expired keys from storage\n", removed);

    println!("=== Getting Started Complete! ===");
    println!("\nKey Features Demonstrated:");
    println!("  • Simple key-value storage");
    println!("  • Automatic spatial indexing");
    println!("  • Radius-based queries");
    println!("  • Bounding box queries");
    println!("  • Trajectory tracking");
    println!("  • TTL support (lazy expiration + manual cleanup)");
    println!("  • Atomic operations");
    println!("\nNext: Try 'spatial_queries' example for more advanced queries");

    Ok(())
}
