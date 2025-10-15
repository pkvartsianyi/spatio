use spatio::{BoundingBox, Point, Spatio};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Spatio - Spatial Queries Example");
    println!("=================================");

    // Create an in-memory database
    let db = Spatio::memory()?;
    println!("Created spatial database");

    // Add world cities with their coordinates
    let cities = vec![
        ("New York", Point::new(40.7128, -74.0060)),
        ("London", Point::new(51.5074, -0.1278)),
        ("Tokyo", Point::new(35.6762, 139.6503)),
        ("Sydney", Point::new(-33.8688, 151.2093)),
        ("Paris", Point::new(48.8566, 2.3522)),
        ("Berlin", Point::new(52.5200, 13.4050)),
        ("Mumbai", Point::new(19.0760, 72.8777)),
        ("Cairo", Point::new(30.0444, 31.2357)),
        ("São Paulo", Point::new(-23.5505, -46.6333)),
        ("Mexico City", Point::new(19.4326, -99.1332)),
    ];

    // Insert cities into the database (automatically indexed)
    for (name, point) in &cities {
        db.insert_point("world_cities", point, name.as_bytes(), None)?;
    }
    println!("Added {} cities to spatial index", cities.len());

    // Define a reference point (London)
    let reference_point = Point::new(51.5074, -0.1278);
    println!("Using London as reference point: {}", reference_point);

    // Find cities within 1000km of London
    println!("\nCities within 1000km of London:");
    let nearby_cities = db.find_nearby("world_cities", &reference_point, 1_000_000.0, 10)?;

    for (point, data) in &nearby_cities {
        let city_name = String::from_utf8_lossy(data);
        let distance_km = reference_point.distance_to(point) / 1000.0;
        println!("  - {} ({:.0} km away)", city_name, distance_km);
    }

    // Find cities within 2000km of London
    println!("\nCities within 2000km of London:");
    let medium_range_cities = db.find_nearby("world_cities", &reference_point, 2_000_000.0, 10)?;

    for (point, data) in &medium_range_cities {
        let city_name = String::from_utf8_lossy(data);
        let distance_km = reference_point.distance_to(point) / 1000.0;
        println!("  - {} ({:.0} km away)", city_name, distance_km);
    }

    // Find the 3 closest cities to London (regardless of distance)
    println!("\n3 closest cities to London:");
    let closest_cities = db.find_nearby("world_cities", &reference_point, f64::INFINITY, 3)?;

    for (i, (point, data)) in closest_cities.iter().enumerate() {
        let city_name = String::from_utf8_lossy(data);
        let distance_km = reference_point.distance_to(point) / 1000.0;
        println!("  {}. {} ({:.0} km away)", i + 1, city_name, distance_km);
    }

    // Demonstrate distance calculations between specific cities
    println!("\nDistance calculations:");
    let nyc = Point::new(40.7128, -74.0060);
    let tokyo = Point::new(35.6762, 139.6503);
    let sydney = Point::new(-33.8688, 151.2093);

    println!(
        "  - New York ↔ London: {:.0} km",
        nyc.distance_to(&reference_point) / 1000.0
    );
    println!(
        "  - London ↔ Tokyo: {:.0} km",
        reference_point.distance_to(&tokyo) / 1000.0
    );
    println!(
        "  - Tokyo ↔ Sydney: {:.0} km",
        tokyo.distance_to(&sydney) / 1000.0
    );

    // Add some points of interest in London
    let london_poi = vec![
        ("Tower Bridge", Point::new(51.5055, -0.0754)),
        ("Big Ben", Point::new(51.4994, -0.1245)),
        ("London Eye", Point::new(51.5033, -0.1195)),
        ("Buckingham Palace", Point::new(51.5014, -0.1419)),
        ("Hyde Park", Point::new(51.5074, -0.1657)),
    ];

    for (name, point) in &london_poi {
        db.insert_point("london_poi", point, name.as_bytes(), None)?;
    }
    println!("\nAdded {} London points of interest", london_poi.len());

    // Find POI within 2km of Big Ben
    let big_ben = Point::new(51.4994, -0.1245);
    println!("\nPoints of interest within 2km of Big Ben:");
    let nearby_poi = db.find_nearby("london_poi", &big_ben, 2000.0, 10)?;

    for (point, data) in &nearby_poi {
        let poi_name = String::from_utf8_lossy(data);
        let distance_m = big_ben.distance_to(point);
        if distance_m < 10.0 {
            println!("  - {} (same location)", poi_name);
        } else {
            println!("  - {} ({:.0}m away)", poi_name, distance_m);
        }
    }

    // Database statistics
    let stats = db.stats()?;
    println!("\nDatabase statistics:");
    println!("  - Total keys: {}", stats.key_count);
    println!("  - Operations performed: {}", stats.operations_count);

    // Demonstrate new spatial query methods
    println!("\nAdvanced Spatial Query Methods:");

    // Test contains_point - check if there are any cities within 1000km of London
    let has_nearby_cities = db.contains_point("world_cities", &reference_point, 1_000_000.0)?;
    println!(
        "  Cities within 1000km of London exist: {}",
        has_nearby_cities
    );

    // Count cities within different distances from London
    let count_500km = db.count_within_distance("world_cities", &reference_point, 500_000.0)?;
    let count_1000km = db.count_within_distance("world_cities", &reference_point, 1_000_000.0)?;
    let count_2000km = db.count_within_distance("world_cities", &reference_point, 2_000_000.0)?;

    println!("  Cities within 500km: {}", count_500km);
    println!("  Cities within 1000km: {}", count_1000km);
    println!("  Cities within 2000km: {}", count_2000km);

    // Test bounding box queries - European region
    let europe_bounds = (45.0, -10.0, 60.0, 30.0); // min_lat, min_lon, max_lat, max_lon
    let has_european_cities = db.intersects_bounds(
        "world_cities",
        europe_bounds.0,
        europe_bounds.1,
        europe_bounds.2,
        europe_bounds.3,
    )?;
    println!("  European cities exist: {}", has_european_cities);

    // Find all cities in European region
    let european_cities = db.find_within_bounds(
        "world_cities",
        europe_bounds.0,
        europe_bounds.1,
        europe_bounds.2,
        europe_bounds.3,
        10,
    )?;

    println!("  Cities in European region:");
    for (point, data) in &european_cities {
        let city_name = String::from_utf8_lossy(data);
        println!("    - {} at {}", city_name, point);
    }

    // Test bounding box around London area
    let london_area = (51.0, -1.0, 52.0, 1.0);
    let london_area_cities = db.find_within_bounds(
        "world_cities",
        london_area.0,
        london_area.1,
        london_area.2,
        london_area.3,
        5,
    )?;

    println!("  Cities in London area:");
    for (point, data) in &london_area_cities {
        let city_name = String::from_utf8_lossy(data);
        println!("    - {} at {}", city_name, point);
    }

    // Demonstrate BoundingBox struct usage
    let asia_pacific = BoundingBox::new(-50.0, 100.0, 50.0, 180.0);
    let europe_box = BoundingBox::new(35.0, -10.0, 70.0, 40.0);

    println!("  BoundingBox intersection test:");
    println!(
        "    - Asia-Pacific and Europe intersect: {}",
        asia_pacific.intersects(&europe_box)
    );

    // Test point containment methods
    println!("  Point containment tests:");
    let central_london = Point::new(51.5074, -0.1278);
    let tower_bridge = Point::new(51.5055, -0.0754);

    println!(
        "    - Tower Bridge within 5km of Central London: {}",
        central_london.contains_point(&tower_bridge, 5000.0)
    );
    println!(
        "    - Central London within 2km of Tower Bridge: {}",
        tower_bridge.contains_point(&central_london, 2000.0)
    );

    println!("\nEnhanced spatial queries example completed successfully!");
    println!("\nNew spatial query features demonstrated:");
    println!("- contains_point: Check if points exist within a circular region");
    println!("- count_within_distance: Count points within a radius (efficient)");
    println!("- intersects_bounds: Check if points exist within a bounding box");
    println!("- find_within_bounds: Find all points within a rectangular region");
    println!("- BoundingBox: Dedicated struct for bounding box operations");
    println!("- Point containment methods for distance-based checks");
    println!("\nPrevious features:");
    println!("- Automatic spatial indexing of geographic points");
    println!("- Efficient nearby point searches with distance filtering");
    println!("- Distance calculations between any two points");
    println!("- Multiple namespaces (world_cities, london_poi)");
    println!("- Flexible radius-based and count-based queries");

    Ok(())
}
