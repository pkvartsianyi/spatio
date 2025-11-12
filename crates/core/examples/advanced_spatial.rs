//! Advanced Spatial Operations Example
//!
//! This example demonstrates the advanced spatial capabilities of Spatio,
//! including distance calculations, K-nearest-neighbors, polygon queries,
//! and bounding box operations using the geo crate.

use spatio::{
    Point, Polygon, Spatio,
    compute::spatial::{
        DistanceMetric, bounding_box, bounding_rect_for_points, convex_hull, distance_between,
    },
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Advanced Spatial Operations Demo ===\n");

    // Create an in-memory database
    let mut db = Spatio::memory()?;

    // ========================================
    // 1. Distance Calculations
    // ========================================
    println!("1. Distance Calculations");
    println!("{}", "=".repeat(50));

    let new_york = Point::new(-74.0060, 40.7128);
    let los_angeles = Point::new(-118.2437, 34.0522);
    let london = Point::new(-0.1278, 51.5074);

    // Calculate distances using different metrics
    let dist_haversine = distance_between(&new_york, &los_angeles, DistanceMetric::Haversine);
    let dist_geodesic = distance_between(&new_york, &los_angeles, DistanceMetric::Geodesic);
    let dist_rhumb = distance_between(&new_york, &los_angeles, DistanceMetric::Rhumb);

    println!("NYC to LA:");
    println!("  Haversine:  {:.2} km", dist_haversine / 1000.0);
    println!("  Geodesic:   {:.2} km", dist_geodesic / 1000.0);
    println!("  Rhumb:      {:.2} km", dist_rhumb / 1000.0);

    // Using the database method
    let db_distance = db.distance_between(&new_york, &london, DistanceMetric::Haversine)?;
    println!("\nNYC to London: {:.2} km", db_distance / 1000.0);

    // ========================================
    // 2. K-Nearest-Neighbors (KNN)
    // ========================================
    println!("\n2. K-Nearest-Neighbors Query");
    println!("{}", "=".repeat(50));

    // Insert major cities
    let cities = vec![
        (Point::new(-74.0060, 40.7128), "New York", "USA"),
        (Point::new(-118.2437, 34.0522), "Los Angeles", "USA"),
        (Point::new(-87.6298, 41.8781), "Chicago", "USA"),
        (Point::new(-95.3698, 29.7604), "Houston", "USA"),
        (Point::new(-75.1652, 39.9526), "Philadelphia", "USA"),
        (Point::new(-122.4194, 37.7749), "San Francisco", "USA"),
        (Point::new(-0.1278, 51.5074), "London", "UK"),
        (Point::new(2.3522, 48.8566), "Paris", "France"),
        (Point::new(139.6917, 35.6895), "Tokyo", "Japan"),
    ];

    for (point, name, country) in &cities {
        let data = format!("{},{}", name, country);
        db.insert_point("world_cities", point, data.as_bytes(), None)?;
    }

    // Find 3 nearest cities to a query point (somewhere in New Jersey)
    let query_point = Point::new(-74.1719, 40.7357);
    let nearest = db.knn(
        "world_cities",
        &query_point,
        3,
        500_000.0, // Search within 500km
        DistanceMetric::Haversine,
    )?;

    println!("3 nearest cities to query point:");
    for (i, (_point, data, distance)) in nearest.iter().enumerate() {
        let city_info = String::from_utf8_lossy(data);
        println!(
            "  {}. {} ({:.2} km away)",
            i + 1,
            city_info,
            distance / 1000.0
        );
    }

    // ========================================
    // 3. Polygon Queries
    // ========================================
    println!("\n3. Polygon Queries");
    println!("{}", "=".repeat(50));

    // Define a polygon covering parts of the eastern US
    use geo::polygon;
    let east_coast_polygon = polygon![
        (x: -80.0, y: 35.0),  // South
        (x: -70.0, y: 35.0),  // Southeast
        (x: -70.0, y: 45.0),  // Northeast
        (x: -80.0, y: 45.0),  // Northwest
        (x: -80.0, y: 35.0),  // Close the polygon
    ];
    let east_coast_polygon: Polygon = east_coast_polygon.into();

    let cities_in_polygon = db.query_within_polygon("world_cities", &east_coast_polygon, 100)?;

    println!("Cities within East Coast polygon:");
    for (point, data) in &cities_in_polygon {
        let city_info = String::from_utf8_lossy(data);
        println!("  - {} at ({:.4}, {:.4})", city_info, point.x(), point.y());
    }

    // ========================================
    // 4. Bounding Box Operations
    // ========================================
    println!("\n4. Bounding Box Operations");
    println!("{}", "=".repeat(50));

    // Create a bounding box around the New York area
    let ny_bbox = bounding_box(-74.5, 40.5, -73.5, 41.0)?;
    println!("NY Area Bounding Box: {:?}", ny_bbox);

    // Find cities in bounding box
    let cities_in_bbox = db.find_within_bounds("world_cities", 40.5, -74.5, 41.0, -73.5, 100)?;
    println!("\nCities in NY area bounding box:");
    for (_point, data) in &cities_in_bbox {
        let city_info = String::from_utf8_lossy(data);
        println!("  - {}", city_info);
    }

    // ========================================
    // 5. Convex Hull
    // ========================================
    println!("\n5. Convex Hull Calculation");
    println!("{}", "=".repeat(50));

    // Get all city points
    let city_points: Vec<Point> = cities.iter().map(|(p, _, _)| *p).collect();

    // Calculate convex hull
    if let Some(hull) = convex_hull(&city_points) {
        println!("Convex hull of all cities:");
        println!("  Exterior points: {}", hull.exterior().0.len() - 1);
        for coord in hull.exterior().0.iter().take(5) {
            println!("    ({:.4}, {:.4})", coord.x, coord.y);
        }
    }

    // ========================================
    // 6. Bounding Rectangle
    // ========================================
    println!("\n6. Bounding Rectangle");
    println!("{}", "=".repeat(50));

    if let Some(bbox) = bounding_rect_for_points(&city_points) {
        println!("Bounding rectangle of all cities:");
        println!("  Min: ({:.4}, {:.4})", bbox.min().x, bbox.min().y);
        println!("  Max: ({:.4}, {:.4})", bbox.max().x, bbox.max().y);
        println!("  Width:  {:.2}°", bbox.max().x - bbox.min().x);
        println!("  Height: {:.2}°", bbox.max().y - bbox.min().y);
    }

    // ========================================
    // 7. Advanced Radius Queries
    // ========================================
    println!("\n7. Advanced Radius Queries");
    println!("{}", "=".repeat(50));

    // Count cities within 1000km of NYC
    let count = db.count_within_radius("world_cities", &new_york, 1_000_000.0)?;
    println!("Cities within 1000km of NYC: {}", count);

    // Check if any cities exist within 100km
    let has_nearby = db.contains_point("world_cities", &new_york, 100_000.0)?;
    println!("Has cities within 100km of NYC: {}", has_nearby);

    // Query with radius
    let nearby = db.query_within_radius("world_cities", &new_york, 200_000.0, 10)?;
    println!("\nCities within 200km of NYC:");
    for (point, data) in &nearby {
        let city_info = String::from_utf8_lossy(data);
        let dist = distance_between(&new_york, point, DistanceMetric::Haversine);
        println!("  - {} ({:.2} km)", city_info, dist / 1000.0);
    }

    // ========================================
    // 8. Spatial Analytics
    // ========================================
    println!("\n8. Spatial Analytics");
    println!("{}", "=".repeat(50));

    // Find the two most distant cities (brute force)
    let mut max_distance = 0.0;
    let mut furthest_pair = ("", "");

    for (i, (p1, n1, _)) in cities.iter().enumerate() {
        for (p2, n2, _) in cities.iter().skip(i + 1) {
            let dist = distance_between(p1, p2, DistanceMetric::Geodesic);
            if dist > max_distance {
                max_distance = dist;
                furthest_pair = (n1, n2);
            }
        }
    }

    println!(
        "Most distant city pair: {} ↔ {}",
        furthest_pair.0, furthest_pair.1
    );
    println!("Distance: {:.2} km", max_distance / 1000.0);

    // Calculate average distance between all US cities
    let us_cities: Vec<_> = cities
        .iter()
        .filter(|(_, _, country)| *country == "USA")
        .collect();

    if us_cities.len() > 1 {
        let mut total_distance = 0.0;
        let mut count = 0;

        for (i, (p1, _, _)) in us_cities.iter().enumerate() {
            for (p2, _, _) in us_cities.iter().skip(i + 1) {
                total_distance += distance_between(p1, p2, DistanceMetric::Haversine);
                count += 1;
            }
        }

        let avg_distance = total_distance / count as f64;
        println!(
            "\nAverage distance between US cities: {:.2} km",
            avg_distance / 1000.0
        );
    }

    // ========================================
    // Summary
    // ========================================
    println!("\n{}", "=".repeat(50));
    println!("Summary:");
    println!("  - Demonstrated multiple distance metrics");
    println!("  - Performed K-nearest-neighbor searches");
    println!("  - Queried points within polygons");
    println!("  - Used bounding box operations");
    println!("  - Calculated convex hulls");
    println!("  - Performed spatial analytics");
    println!("\nAll spatial operations leverage the geo crate!");

    Ok(())
}
