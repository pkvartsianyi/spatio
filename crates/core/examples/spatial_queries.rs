use spatio::{Point, Spatio};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Spatio - Spatial Queries ===\n");

    let mut db = Spatio::memory()?;
    println!("✓ Created in-memory database\n");

    // === SETUP: Insert World Cities ===
    println!("Setting up test data...");

    let cities = vec![
        ("New York", Point::new(-74.0060, 40.7128)),
        ("London", Point::new(-0.1278, 51.5074)),
        ("Paris", Point::new(2.3522, 48.8566)),
        ("Berlin", Point::new(13.4050, 52.5200)),
        ("Madrid", Point::new(-3.7038, 40.4168)),
        ("Rome", Point::new(12.4964, 41.9028)),
        ("Tokyo", Point::new(139.6503, 35.6762)),
        ("Sydney", Point::new(151.2093, -33.8688)),
        ("Mumbai", Point::new(72.8777, 19.0760)),
        ("Cairo", Point::new(31.2357, 30.0444)),
        ("São Paulo", Point::new(-46.6333, -23.5505)),
        ("Mexico City", Point::new(-99.1332, 19.4326)),
    ];

    for (name, point) in &cities {
        db.insert_point("world_cities", point, name.as_bytes(), None)?;
    }
    println!("   Added {} cities to spatial index\n", cities.len());

    // === 1. RADIUS QUERIES ===
    println!("1. Radius-Based Queries (query_within_radius)");
    println!("----------------------------------------------");

    let london = Point::new(-0.1278, 51.5074);

    // Find cities within 500km of London
    let nearby_500km = db.query_within_radius("world_cities", &london, 500_000.0, 10)?;
    println!("   Cities within 500km of London: {}", nearby_500km.len());
    for (_, data, _) in &nearby_500km {
        println!("     - {}", String::from_utf8_lossy(data));
    }

    // Find cities within 2000km of London
    let nearby_2000km = db.query_within_radius("world_cities", &london, 2_000_000.0, 10)?;
    println!(
        "\n   Cities within 2000km of London: {}",
        nearby_2000km.len()
    );
    for (_, data, _) in &nearby_2000km {
        println!("     - {}", String::from_utf8_lossy(data));
    }

    // Use limit to get only closest N cities
    let closest_3 = db.query_within_radius("world_cities", &london, f64::INFINITY, 3)?;
    println!("\n   Closest 3 cities to London:");
    for (i, (_, data, _)) in closest_3.iter().enumerate() {
        println!("     {}. {}", i + 1, String::from_utf8_lossy(data));
    }
    println!();

    // === 2. EXISTENCE CHECKS ===
    println!("2. Existence Checks (contains_point)");
    println!("-------------------------------------");

    let has_nearby_500km = db.intersects_radius("world_cities", &london, 500_000.0)?;
    let has_nearby_100km = db.intersects_radius("world_cities", &london, 100_000.0)?;

    println!("   Any cities within 500km of London? {}", has_nearby_500km);
    println!("   Any cities within 100km of London? {}", has_nearby_100km);

    // === 3. COUNTING POINTS ===
    println!("3. Counting Points (count_within_radius)");
    println!("----------------------------------------");

    let count_500km = db.count_within_radius("world_cities", &london, 500_000.0)?;
    let count_1000km = db.count_within_radius("world_cities", &london, 1_000_000.0)?;
    let count_2000km = db.count_within_radius("world_cities", &london, 2_000_000.0)?;

    println!("   Cities within 500km of London: {}", count_500km);
    println!("   Cities within 1000km of London: {}", count_1000km);
    println!("   Cities within 2000km of London: {}", count_2000km);
    println!();

    // === 4. BOUNDING BOX QUERIES ===
    println!("4. Bounding Box Queries");
    println!("-----------------------");

    // European bounding box (min_lon, min_lat, max_lon, max_lat)
    println!("   European region (lon: -10 to 20, lat: 40 to 60):");
    let europe_cities = db.find_within_bounds("world_cities", -10.0, 40.0, 20.0, 60.0, 20)?;
    println!("     Found {} cities:", europe_cities.len());
    for (point, data) in &europe_cities {
        println!(
            "       - {} at ({:.2}°, {:.2}°)",
            String::from_utf8_lossy(data),
            point.x(),
            point.y()
        );
    }

    // Asia-Pacific region
    println!("\n   Asia-Pacific region (lon: 70 to 180, lat: -40 to 40):");
    let asia_cities = db.find_within_bounds("world_cities", 70.0, -40.0, 180.0, 40.0, 20)?;
    println!("     Found {} cities:", asia_cities.len());
    for (_, data) in &asia_cities {
        println!("       - {}", String::from_utf8_lossy(data));
    }

    // Americas region
    println!("\n   Americas region (lon: -130 to -30, lat: -60 to 60):");
    let americas_cities = db.find_within_bounds("world_cities", -130.0, -60.0, -30.0, 60.0, 20)?;
    println!("     Found {} cities:", americas_cities.len());
    for (_, data) in &americas_cities {
        println!("       - {}", String::from_utf8_lossy(data));
    }
    println!();

    // === 5. BOUNDING BOX INTERSECTION ===
    println!("5. Bounding Box Intersection (intersects_bounds)");
    println!("------------------------------------------------");

    let has_european = db.intersects_bounds("world_cities", -10.0, 40.0, 20.0, 60.0)?;
    let has_antarctica = db.intersects_bounds("world_cities", -180.0, -90.0, 180.0, -60.0)?;

    println!("   European region has cities? {}", has_european);
    println!("   Antarctica region has cities? {}", has_antarctica);
    println!();

    // === 6. MULTIPLE NAMESPACES ===
    println!("6. Multiple Namespaces");
    println!("----------------------");

    // Add some landmarks in London
    let london_landmarks = vec![
        ("Big Ben", Point::new(-0.1245, 51.4994)),
        ("Tower Bridge", Point::new(-0.0754, 51.5055)),
        ("London Eye", Point::new(-0.1195, 51.5033)),
        ("Buckingham Palace", Point::new(-0.1419, 51.5014)),
    ];

    for (name, point) in &london_landmarks {
        db.insert_point("landmarks", point, name.as_bytes(), None)?;
    }

    println!("   Added {} London landmarks", london_landmarks.len());

    // Query different namespaces from same location
    let center_london = Point::new(-0.1278, 51.5074);

    let nearby_cities = db.query_within_radius("world_cities", &center_london, 10_000.0, 10)?;
    let nearby_landmarks = db.query_within_radius("landmarks", &center_london, 10_000.0, 10)?;

    println!("   Within 10km of center London:");
    println!("     Cities: {}", nearby_cities.len());
    println!("     Landmarks: {}", nearby_landmarks.len());

    println!("\n   Landmarks within 2km:");
    let close_landmarks = db.query_within_radius("landmarks", &center_london, 2_000.0, 10)?;
    for (_, data, _) in &close_landmarks {
        println!("     - {}", String::from_utf8_lossy(data));
    }
    println!();

    // === 7. QUERY LIMITS ===
    println!("7. Query Result Limiting");
    println!("------------------------");

    let all_cities = db.query_within_radius("world_cities", &london, f64::INFINITY, 100)?;
    let top_5 = db.query_within_radius("world_cities", &london, f64::INFINITY, 5)?;
    let top_3 = db.query_within_radius("world_cities", &london, f64::INFINITY, 3)?;

    println!("   Total cities in database: {}", all_cities.len());
    println!("   With limit=5: {} cities", top_5.len());
    println!("   With limit=3: {} cities", top_3.len());
    println!();

    // === SUMMARY ===
    let stats = db.stats();
    println!("=== Query Summary ===");
    println!("Database statistics:");
    println!("  Total keys: {}", stats.key_count);
    println!("  Operations: {}", stats.operations_count);

    println!("\nSpatial query methods demonstrated:");
    println!("  • query_within_radius - Find points within distance");
    println!("  • contains_point - Check if points exist in radius");
    println!("  • count_within_radius - Count points efficiently");
    println!("  • find_within_bounds - Rectangular region queries");
    println!("  • intersects_bounds - Check bounding box intersection");
    println!("  • Multiple namespaces - Organize different point types");
    println!("  • Result limiting - Control query result size");

    Ok(())
}
