use spatio::{Point, SetOptions, Spatio, TemporalPoint};
use std::time::{Duration, UNIX_EPOCH};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Spatio - Comprehensive Demo ===\n");

    let mut db = Spatio::memory()?;
    println!("✓ Created in-memory database\n");

    // === 1. BASIC KEY-VALUE OPERATIONS ===
    println!("1. Basic Key-Value Operations");
    println!("------------------------------");

    db.insert("app:name", b"Spatio Demo", None)?;
    db.insert("app:version", b"1.0.0", None)?;
    db.insert("user:alice", b"Alice Johnson", None)?;

    let app_name = db.get("app:name")?.unwrap();
    println!("   Stored: {}", String::from_utf8_lossy(&app_name));

    let count = db.stats().key_count;
    println!("   Total keys: {}\n", count);

    // === 2. TTL (TIME-TO-LIVE) ===
    println!("2. TTL (Time-to-Live)");
    println!("---------------------");

    let session_opts = SetOptions::with_ttl(Duration::from_secs(10));
    db.insert("session:temp", b"expires_soon", Some(session_opts))?;
    println!("   Created session with 10-second TTL");

    let cache_opts = SetOptions::with_ttl(Duration::from_secs(300));
    db.insert("cache:weather", b"sunny, 22C", Some(cache_opts))?;
    println!("   Cached data with 5-minute TTL\n");

    // === 3. ATOMIC BATCH OPERATIONS ===
    println!("3. Atomic Batch Operations");
    println!("--------------------------");

    db.atomic(|batch| {
        batch.insert("sensor:temp", b"23.5", None)?;
        batch.insert("sensor:humidity", b"68", None)?;
        batch.insert("sensor:pressure", b"1013", None)?;
        Ok(())
    })?;
    println!("   Atomically inserted 3 sensor readings\n");

    // === 4. SPATIAL POINT STORAGE ===
    println!("4. Spatial Point Storage");
    println!("------------------------");

    let cities = vec![
        ("New York", Point::new(-74.0060, 40.7128)),
        ("London", Point::new(-0.1278, 51.5074)),
        ("Paris", Point::new(2.3522, 48.8566)),
        ("Tokyo", Point::new(139.6503, 35.6762)),
        ("Sydney", Point::new(151.2093, -33.8688)),
    ];

    for (name, point) in &cities {
        db.insert_point("cities", point, name.as_bytes(), None)?;
    }
    println!("   Stored {} cities with spatial indexing", cities.len());

    // Find nearby cities
    let london = Point::new(-0.1278, 51.5074);
    let nearby = db.query_within_radius("cities", &london, 1_000_000.0, 10)?;
    println!("   Cities within 1000km of London: {}", nearby.len());
    for (_, data) in &nearby {
        println!("     - {}", String::from_utf8_lossy(data));
    }
    println!();

    // === 5. SPATIAL QUERIES ===
    println!("5. Spatial Query Methods");
    println!("------------------------");

    // Existence check
    let has_nearby = db.contains_point("cities", &london, 500_000.0)?;
    println!("   Cities within 500km of London exist: {}", has_nearby);

    // Count points
    let count = db.count_within_radius("cities", &london, 2_000_000.0)?;
    println!("   Cities within 2000km of London: {}", count);

    // Bounding box query (Europe)
    let europe = db.find_within_bounds("cities", -10.0, 40.0, 20.0, 60.0, 10)?;
    println!("   Cities in European bounding box: {}", europe.len());
    for (_, data) in &europe {
        println!("     - {}", String::from_utf8_lossy(data));
    }
    println!();

    // === 6. POINTS OF INTEREST ===
    println!("6. Multiple Namespaces (POI)");
    println!("----------------------------");

    let landmarks = vec![
        ("Big Ben", Point::new(-0.1245, 51.4994)),
        ("Tower Bridge", Point::new(-0.0754, 51.5055)),
        ("London Eye", Point::new(-0.1195, 51.5033)),
    ];

    for (name, point) in &landmarks {
        db.insert_point("landmarks", point, name.as_bytes(), None)?;
    }
    println!("   Added {} London landmarks", landmarks.len());

    // Query different namespaces
    let nearby_landmarks = db.query_within_radius("landmarks", &london, 5_000.0, 10)?;
    println!(
        "   Landmarks within 5km of London: {}",
        nearby_landmarks.len()
    );
    for (_, data) in &nearby_landmarks {
        println!("     - {}", String::from_utf8_lossy(data));
    }
    println!();

    // === 7. TRAJECTORY TRACKING ===
    println!("7. Trajectory Tracking");
    println!("----------------------");

    let delivery_route = vec![
        TemporalPoint {
            point: Point::new(-0.1278, 51.5074),
            timestamp: UNIX_EPOCH + Duration::from_secs(1640995200),
        },
        TemporalPoint {
            point: Point::new(-0.0931, 51.5055),
            timestamp: UNIX_EPOCH + Duration::from_secs(1640995260),
        },
        TemporalPoint {
            point: Point::new(-0.0865, 51.5045),
            timestamp: UNIX_EPOCH + Duration::from_secs(1640995320),
        },
        TemporalPoint {
            point: Point::new(-0.1245, 51.4994),
            timestamp: UNIX_EPOCH + Duration::from_secs(1640995380),
        },
    ];

    db.insert_trajectory("delivery:truck001", &delivery_route, None)?;
    println!(
        "   Stored delivery trajectory ({} waypoints)",
        delivery_route.len()
    );

    // Query trajectory for time range
    let path = db.query_trajectory("delivery:truck001", 1640995200, 1640995320)?;
    println!(
        "   Retrieved {} waypoints for first 2 minutes\n",
        path.len()
    );

    // === 8. DATA UPDATES & DELETES ===
    println!("8. Updates and Deletes");
    println!("----------------------");

    // Update existing key
    db.insert("app:version", b"1.0.1", None)?;
    let new_version = db.get("app:version")?.unwrap();
    println!(
        "   Updated version to: {}",
        String::from_utf8_lossy(&new_version)
    );

    // Delete key
    let deleted = db.delete("user:alice")?;
    println!("   Deleted key: {}", deleted.is_some());
    println!();

    // === 9. DATABASE STATISTICS ===
    println!("9. Database Statistics");
    println!("----------------------");

    let stats = db.stats();
    println!("   Total keys: {}", stats.key_count);
    println!("   Total operations: {}\n", stats.operations_count);

    // === 10. TTL EXPIRATION DEMO ===
    println!("10. TTL Expiration (waiting 11 seconds...)");
    println!("------------------------------------------");

    std::thread::sleep(Duration::from_secs(11));

    match db.get("session:temp")? {
        Some(_) => println!("   Session still active (unexpected)"),
        None => println!("   ✓ Session expired as expected\n"),
    }

    println!("=== Comprehensive Demo Complete! ===");
    println!("\nFeatures Demonstrated:");
    println!("  • Key-value storage");
    println!("  • TTL (time-to-live)");
    println!("  • Atomic batch operations");
    println!("  • Spatial point indexing");
    println!("  • Radius queries");
    println!("  • Bounding box queries");
    println!("  • Multiple namespaces");
    println!("  • Trajectory tracking");
    println!("  • Updates and deletes");
    println!("  • Automatic expiration");

    Ok(())
}
