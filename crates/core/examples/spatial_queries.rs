use spatio::{Point3d, Spatio};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Spatio - Spatial Queries ===\n");

    let db = Spatio::memory()?;
    println!("✓ Created in-memory database\n");

    // === SETUP: Insert World Cities ===
    println!("Setting up test data...");

    let cities = vec![
        ("New York", Point3d::new(-74.0060, 40.7128, 0.0)),
        ("London", Point3d::new(-0.1278, 51.5074, 0.0)),
        ("Paris", Point3d::new(2.3522, 48.8566, 0.0)),
        ("Berlin", Point3d::new(13.4050, 52.5200, 0.0)),
        ("Madrid", Point3d::new(-3.7038, 40.4168, 0.0)),
        ("Rome", Point3d::new(12.4964, 41.9028, 0.0)),
        ("Tokyo", Point3d::new(139.6503, 35.6762, 0.0)),
        ("Sydney", Point3d::new(151.2093, -33.8688, 0.0)),
        ("Mumbai", Point3d::new(72.8777, 19.0760, 0.0)),
        ("Cairo", Point3d::new(31.2357, 30.0444, 0.0)),
        ("São Paulo", Point3d::new(-46.6333, -23.5505, 0.0)),
        ("Mexico City", Point3d::new(-99.1332, 19.4326, 0.0)),
    ];

    for (name, point) in &cities {
        // Using name as object_id for simplicity
        let object_id = name.to_lowercase().replace(" ", "_");
        db.upsert(
            "world_cities",
            &object_id,
            point.clone(),
            serde_json::json!({"name": name}),
            None,
        )?;
    }
    println!("   Added {} cities to spatial index\n", cities.len());

    // Create a grid of points
    for i in 0..10 {
        for j in 0..10 {
            let id = format!("point-{}-{}", i, j);
            let lon = -74.0 + (i as f64) * 0.01;
            let lat = 40.7 + (j as f64) * 0.01;
            let pos = Point3d::new(lon, lat, 0.0);

            db.upsert(
                "nyc_grid",
                &id,
                pos,
                serde_json::json!({"category": "grid_point"}),
                None,
            )?;
        }
    }
    println!("   Populated database with 100 points in 'nyc_grid' namespace\n");

    // === 1. RADIUS QUERIES ===
    println!("1. Radius-Based Queries (query_radius)");
    println!("-----------------------------------------------------");

    let london = Point3d::new(-0.1278, 51.5074, 0.0);

    // Find cities within 500km of London
    let nearby_500km = db.query_radius("world_cities", &london, 500_000.0, 10)?;
    println!("   Cities within 500km of London: {}", nearby_500km.len());
    for (loc, dist) in &nearby_500km {
        println!("     - {} ({:.1}m)", loc.metadata, dist);
    }

    // Find cities within 2000km of London
    let nearby_2000km = db.query_radius("world_cities", &london, 2_000_000.0, 10)?;
    println!(
        "\n   Cities within 2000km of London: {}",
        nearby_2000km.len()
    );
    for (loc, dist) in &nearby_2000km {
        println!("     - {} ({:.1}m)", loc.metadata, dist);
    }

    // Use limit to get only closest N cities
    let closest_3 = db.query_radius("world_cities", &london, f64::INFINITY, 3)?;
    println!("\n   Closest 3 cities to London:");
    for (i, (loc, dist)) in closest_3.iter().enumerate() {
        println!("     {}. {} ({:.1}m)", i + 1, loc.metadata, dist);
    }
    println!();

    // === 2. EXISTENCE CHECKS ===
    println!("2. Existence Checks (simulated via radius query)");
    println!("------------------------------------------------");

    let has_nearby_500km = !db
        .query_radius("world_cities", &london, 500_000.0, 1)?
        .is_empty();
    let has_nearby_100km = !db
        .query_radius("world_cities", &london, 100_000.0, 1)?
        .is_empty();

    println!("   Any cities within 500km of London? {}", has_nearby_500km);
    println!("   Any cities within 100km of London? {}", has_nearby_100km);
    println!();

    // === 3. BOUNDING BOX QUERIES ===
    println!("3. Bounding Box Queries (query_bbox)");
    println!("---------------------------------------------------");

    // European bounding box (min_lon, min_lat, max_lon, max_lat)
    println!("   European region (lon: -10 to 20, lat: 40 to 60):");
    let europe_cities = db.query_bbox("world_cities", -10.0, 40.0, 20.0, 60.0, 20)?;
    println!("     Found {} cities:", europe_cities.len());
    for loc in &europe_cities {
        println!(
            "       - {} at ({:.2}°, {:.2}°)",
            loc.metadata,
            loc.position.x(),
            loc.position.y()
        );
    }

    let min_x = -74.06;
    let min_y = 40.74;
    let max_x = -74.04;
    let max_y = 40.76;

    println!(
        "\n   NYC grid narrow bbox: [{}, {}] to [{}, {}]",
        min_x, min_y, max_x, max_y
    );

    let bbox_results = db.query_bbox("nyc_grid", min_x, min_y, max_x, max_y, 100)?;
    println!("   Found {} points in bounding box", bbox_results.len());
    for loc in &bbox_results {
        println!(
            "       - {} at ({:.4}, {:.4})",
            loc.object_id,
            loc.position.x(),
            loc.position.y()
        );
    }

    // Asia-Pacific region
    println!("\n   Asia-Pacific region (lon: 70 to 180, lat: -40 to 40):");
    let asia_cities = db.query_bbox("world_cities", 70.0, -40.0, 180.0, 40.0, 20)?;
    println!("     Found {} cities:", asia_cities.len());
    for loc in &asia_cities {
        println!("       - {}", loc.metadata);
    }

    // Americas region
    println!("\n   Americas region (lon: -130 to -30, lat: -60 to 60):");
    let americas_cities = db.query_bbox("world_cities", -130.0, -60.0, -30.0, 60.0, 20)?;
    println!("     Found {} cities:", americas_cities.len());
    for loc in &americas_cities {
        println!("       - {}", loc.metadata);
    }
    println!();

    // === 4. BOUNDING BOX INTERSECTION ===
    println!("4. Bounding Box Intersection (simulated)");
    println!("----------------------------------------");

    let has_european = !db
        .query_bbox("world_cities", -10.0, 40.0, 20.0, 60.0, 1)?
        .is_empty();
    let has_antarctica = !db
        .query_bbox("world_cities", -180.0, -90.0, 180.0, -60.0, 1)?
        .is_empty();

    println!("   European region has cities? {}", has_european);
    println!("   Antarctica region has cities? {}", has_antarctica);
    println!();

    // === 5. MULTIPLE NAMESPACES ===
    println!("5. Multiple Namespaces");
    println!("----------------------");

    // Add some landmarks in London
    let london_landmarks = vec![
        ("Big Ben", Point3d::new(-0.1245, 51.4994, 0.0)),
        ("Tower Bridge", Point3d::new(-0.0754, 51.5055, 0.0)),
        ("London Eye", Point3d::new(-0.1195, 51.5033, 0.0)),
        ("Buckingham Palace", Point3d::new(-0.1419, 51.5014, 0.0)),
    ];

    for (name, point) in &london_landmarks {
        let object_id = name.to_lowercase().replace(" ", "_");
        db.upsert(
            "landmarks",
            &object_id,
            point.clone(),
            serde_json::json!({"name": name}),
            None,
        )?;
    }

    println!("   Added {} London landmarks", london_landmarks.len());

    let center = Point3d::new(-74.05, 40.75, 0.0);
    let radius = 3000.0; // 3km
    println!("\n   NYC grid radius query");
    println!("   Center: ({:.4}, {:.4})", center.x(), center.y());
    println!("   Radius: {:.0}m", radius);

    let results = db.query_radius("nyc_grid", &center, radius, 100)?;
    println!("   Found {} points within radius:", results.len());

    if let Some((loc, dist)) = results.first() {
        println!("     - Sample: {} at {:.1}m", loc.object_id, dist);
    }

    // Performance check
    let start = std::time::Instant::now();
    for _ in 0..1000 {
        db.query_radius("nyc_grid", &center, radius, 100)?;
    }
    println!("   Perf: 1000 radius queries in {:?}", start.elapsed());

    // Query different namespaces
    let center_london = Point3d::new(-0.1278, 51.5074, 0.0);

    let nearby_cities = db.query_radius("world_cities", &center_london, 10_000.0, 10)?;
    let nearby_landmarks = db.query_radius("landmarks", &center_london, 10_000.0, 10)?;

    println!("   Within 10km of center London:");
    println!("     Cities: {}", nearby_cities.len());
    println!("     Landmarks: {}", nearby_landmarks.len());

    println!("\n   Landmarks within 2km:");
    let close_landmarks = db.query_radius("landmarks", &center_london, 2_000.0, 10)?;
    for (loc, dist) in &close_landmarks {
        println!("     - {} ({:.1}m)", loc.metadata, dist);
    }
    println!();

    // === 6. QUERY RESULT LIMITING ===
    println!("6. Query Result Limiting");
    println!("------------------------");

    let all_cities = db.query_radius("world_cities", &london, f64::INFINITY, 100)?;
    let top_5 = db.query_radius("world_cities", &london, f64::INFINITY, 5)?;
    let top_3 = db.query_radius("world_cities", &london, f64::INFINITY, 3)?;

    println!("   Total cities in database: {}", all_cities.len());
    println!("   With limit=5: {} cities", top_5.len());
    println!("   With limit=3: {} cities", top_3.len());
    println!();

    // === SUMMARY ===
    let stats = db.stats();
    println!("=== Query Summary ===");
    println!("Database statistics:");
    println!("  Operations: {}", stats.operations_count);

    println!("\nSpatial query methods demonstrated:");
    println!("  • query_radius - Find points within distance");
    println!("  • query_bbox - Rectangular region queries");
    println!("  • Multiple namespaces - Organize different point types");
    println!("  • Result limiting - Control query result size");

    Ok(())
}
