use spatio::{Point, SetOptions, Spatio};
use std::time::Duration;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Spatio - Comprehensive Demo");
    println!("===========================");

    // Create an in-memory database
    let db = Spatio::memory()?;
    println!("Created in-memory database");

    // === BASIC KEY-VALUE OPERATIONS ===
    println!("\nBasic Key-Value Operations");

    // Simple string storage
    db.insert("app:name", b"Spatio Demo App", None)?;
    db.insert("app:version", b"1.0.0", None)?;
    db.insert("app:author", b"Spatio Team", None)?;

    // Retrieve and display
    let app_name = db.get("app:name")?.unwrap();
    println!("  App: {}", String::from_utf8_lossy(&app_name));

    // === TTL (TIME-TO-LIVE) OPERATIONS ===
    println!("\nTTL (Time-to-Live) Operations");

    // Short-lived session data
    let session_opts = SetOptions::with_ttl(Duration::from_secs(10));
    db.insert("session:user123", b"active", Some(session_opts))?;
    println!("  Created session with 10-second TTL");

    // Cache data with different TTL
    let cache_opts = SetOptions::with_ttl(Duration::from_secs(300)); // 5 minutes
    db.insert("cache:weather", b"sunny, 22C", Some(cache_opts.clone()))?;
    db.insert(
        "cache:news",
        b"Latest tech news...",
        Some(cache_opts.clone()),
    )?;
    println!("  Cached data with 5-minute TTL");

    // === ATOMIC BATCH OPERATIONS ===
    println!("\nAtomic Batch Operations");

    // User profile creation (all-or-nothing)
    db.atomic(|batch| {
        batch.insert("user:123:name", b"Alice Johnson", None)?;
        batch.insert("user:123:email", b"alice@example.com", None)?;
        batch.insert("user:123:role", b"admin", None)?;
        batch.insert("user:123:created", b"2024-01-01", None)?;
        Ok(())
    })?;
    println!("  Created user profile atomically");

    // Sensor data batch insert
    db.atomic(|batch| {
        batch.insert("sensor:temp", b"23.5", None)?;
        batch.insert("sensor:humidity", b"68", None)?;
        batch.insert("sensor:pressure", b"1013.2", None)?;
        batch.insert("sensor:timestamp", b"1640995200", None)?;
        Ok(())
    })?;
    println!("  Recorded sensor readings atomically");

    // === SPATIAL POINT OPERATIONS ===
    println!("\nSpatial Point Operations");

    // Major world cities
    let cities = vec![
        ("New York", Point::new(40.7128, -74.0060)),
        ("London", Point::new(51.5074, -0.1278)),
        ("Tokyo", Point::new(35.6762, 139.6503)),
        ("Paris", Point::new(48.8566, 2.3522)),
        ("Sydney", Point::new(-33.8688, 151.2093)),
        ("São Paulo", Point::new(-23.5505, -46.6333)),
        ("Mumbai", Point::new(19.0760, 72.8777)),
        ("Cairo", Point::new(30.0444, 31.2357)),
    ];

    // Insert cities with automatic spatial indexing
    for (name, point) in &cities {
        db.insert_point("cities", point, name.as_bytes(), None)?;
    }
    println!(
        "  Added {} cities with automatic spatial indexing",
        cities.len()
    );

    // Calculate distances between cities
    let london = Point::new(51.5074, -0.1278);
    let paris = Point::new(48.8566, 2.3522);
    let distance_km = london.distance_to(&paris) / 1000.0;
    println!("  London ↔ Paris: {:.0} km", distance_km);

    // Find cities near London (within 1000km)
    let nearby_london = db.find_nearby("cities", &london, 1_000_000.0, 5)?;
    println!("  Cities within 1000km of London:");
    for (point, data) in &nearby_london {
        let city_name = String::from_utf8_lossy(data);
        let distance = london.distance_to(point) / 1000.0;
        println!("    - {} ({:.0} km)", city_name, distance);
    }

    // === RESTAURANT/POI DATA ===
    println!("\nPoints of Interest");

    let london_restaurants = vec![
        ("The Shard Restaurant", Point::new(51.5045, -0.0865)),
        ("Sketch", Point::new(51.5115, -0.1442)),
        ("Dishoom", Point::new(51.5145, -0.1270)),
        ("Borough Market", Point::new(51.5055, -0.0931)),
    ];

    for (name, point) in &london_restaurants {
        db.insert_point("london_food", point, name.as_bytes(), None)?;
    }
    println!("  Added {} London restaurants", london_restaurants.len());

    // Find restaurants near a specific location (Covent Garden)
    let covent_garden = Point::new(51.5118, -0.1226);
    let nearby_food = db.find_nearby("london_food", &covent_garden, 2000.0, 10)?;
    println!("  Restaurants within 2km of Covent Garden:");
    for (point, data) in &nearby_food {
        let restaurant_name = String::from_utf8_lossy(data);
        let distance = covent_garden.distance_to(point);
        println!("    - {} ({:.0}m away)", restaurant_name, distance);
    }

    // === TRAJECTORY TRACKING ===
    println!("\nTrajectory Tracking");

    // Delivery truck route through London
    let delivery_route = vec![
        (Point::new(51.5074, -0.1278), 1640995200), // Start: London center
        (Point::new(51.5055, -0.0931), 1640995260), // Stop 1: Borough Market (1 min)
        (Point::new(51.5045, -0.0865), 1640995320), // Stop 2: The Shard (2 min)
        (Point::new(51.4994, -0.1245), 1640995380), // Stop 3: Big Ben (3 min)
        (Point::new(51.5014, -0.1419), 1640995440), // Stop 4: Buckingham Palace (4 min)
        (Point::new(51.5118, -0.1226), 1640995500), // End: Covent Garden (5 min)
    ];

    db.insert_trajectory("delivery:truck001", &delivery_route, None)?;
    println!(
        "  Stored delivery truck trajectory ({} waypoints)",
        delivery_route.len()
    );

    // Taxi route
    let taxi_route = vec![
        (Point::new(51.4700, -0.4543), 1640995200), // Heathrow Airport
        (Point::new(51.4900, -0.1743), 1640995800), // Kensington (10 min)
        (Point::new(51.5074, -0.1278), 1640996100), // Central London (15 min)
    ];

    db.insert_trajectory("taxi:cab042", &taxi_route, None)?;
    println!("  Stored taxi trajectory ({} waypoints)", taxi_route.len());

    // Query trajectories for specific time ranges
    let truck_morning = db.query_trajectory("delivery:truck001", 1640995200, 1640995380)?;
    println!(
        "  Truck trajectory (first 3 minutes): {} points",
        truck_morning.len()
    );

    let taxi_full = db.query_trajectory("taxi:cab042", 1640995200, 1640996200)?;
    println!("  Full taxi journey: {} points", taxi_full.len());

    // === SENSOR NETWORK SIMULATION ===
    println!("\nIoT Sensor Network");

    // Simulate temperature sensors across London
    let sensors = vec![
        ("sensor001", Point::new(51.5074, -0.1278), "22.5°C"), // Central
        ("sensor002", Point::new(51.5200, -0.1000), "21.8°C"), // North
        ("sensor003", Point::new(51.4900, -0.1500), "23.1°C"), // South
        ("sensor004", Point::new(51.5100, -0.0800), "22.9°C"), // East
        ("sensor005", Point::new(51.5000, -0.1800), "21.5°C"), // West
    ];

    for (sensor_id, point, reading) in &sensors {
        db.insert_point(
            "sensors",
            point,
            format!("{}:{}", sensor_id, reading).as_bytes(),
            None,
        )?;
    }
    println!("  Deployed {} temperature sensors", sensors.len());

    // Find sensors near a specific location
    let monitoring_center = Point::new(51.5100, -0.1200);
    let nearby_sensors = db.find_nearby("sensors", &monitoring_center, 5000.0, 10)?;
    println!("  Sensors within 5km of monitoring center:");
    for (point, data) in &nearby_sensors {
        let sensor_info = String::from_utf8_lossy(data);
        let distance = monitoring_center.distance_to(point);
        println!("    - {} ({:.0}m away)", sensor_info, distance);
    }

    // === REAL-TIME UPDATES ===
    println!("\nReal-time Updates");

    // Simulate updating sensor readings
    db.insert_point(
        "sensors",
        &Point::new(51.5074, -0.1278),
        b"sensor001:24.2C",
        None,
    )?;
    println!("  Updated sensor001 reading");

    // Add new vehicle to tracking
    let bus_route = vec![
        (Point::new(51.5155, -0.0922), 1640995600), // Liverpool Street
        (Point::new(51.5074, -0.1278), 1640995660), // Central London
    ];
    db.insert_trajectory("bus:route25", &bus_route, None)?;
    println!("  Added new bus to tracking system");

    // === DATABASE STATISTICS ===
    println!("\nDatabase Statistics");

    let stats = db.stats()?;
    println!("  Total keys: {}", stats.key_count);
    println!("  Operations performed: {}", stats.operations_count);

    // Count items by namespace
    let mut namespace_counts = std::collections::HashMap::new();
    // This is a simplified count - in practice you'd query by prefix
    namespace_counts.insert("cities", cities.len());
    namespace_counts.insert("restaurants", london_restaurants.len());
    namespace_counts.insert("sensors", sensors.len());
    namespace_counts.insert("trajectories", 3); // truck, taxi, bus

    println!("  Data distribution:");
    for (namespace, count) in &namespace_counts {
        println!("    - {}: {} items", namespace, count);
    }

    // === CLEANUP DEMONSTRATION ===
    println!("\nCleanup & TTL Demo");

    // Check if session has expired (it should have by now)
    if let Some(_session) = db.get("session:user123")? {
        println!("  Session still active");
    } else {
        println!("  Session expired as expected");
    }

    // Delete specific items
    db.delete("app:version")?;
    println!("  Removed app version info");

    // Final statistics
    let final_stats = db.stats()?;
    println!("  Final key count: {}", final_stats.key_count);

    println!("\nComprehensive demo completed successfully!");
    println!("\nFeatures demonstrated:");
    println!("- Key-value storage with TTL");
    println!("- Atomic batch operations");
    println!("- Automatic spatial indexing");
    println!("- Geographic point queries");
    println!("- Distance calculations");
    println!("- Trajectory tracking");
    println!("- Multi-namespace organization");
    println!("- Real-time updates");
    println!("- Data expiration");

    Ok(())
}
