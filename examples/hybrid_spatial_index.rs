//! Comprehensive example demonstrating the Geohash-RTree hybrid spatial index.
//!
//! This example showcases the two-level indexing system inspired by Tile38,
//! combining geohash partitioning with R-tree precision for efficient spatial queries.
//!
//! Run this example with:
//! ```bash
//! cargo run --example hybrid_spatial_index
//! ```

use bytes::Bytes;
use geo::{Point, Rect, polygon};
use spatio::compute::spatial::hybrid::GeohashRTreeIndex;
use std::time::Instant;

fn main() {
    println!("=== Geohash-RTree Hybrid Spatial Index Demo ===\n");

    // Part 1: Basic Usage
    println!("Part 1: Basic Point Indexing and Querying");
    println!("{}", "-".repeat(50));
    basic_point_indexing();
    println!();

    // Part 2: Precision Comparison
    println!("Part 2: Geohash Precision Comparison");
    println!("{}", "-".repeat(50));
    precision_comparison();
    println!();

    // Part 3: Real-world City Data
    println!("Part 3: Real-world City Queries");
    println!("{}", "-".repeat(50));
    city_queries();
    println!();

    // Part 4: Performance Characteristics
    println!("Part 4: Performance Analysis");
    println!("{}", "-".repeat(50));
    performance_analysis();
    println!();

    // Part 5: Complex Spatial Objects
    println!("Part 5: Polygons and Bounding Boxes");
    println!("{}", "-".repeat(50));
    complex_objects();
    println!();

    // Part 6: K-Nearest Neighbors
    println!("Part 6: K-Nearest Neighbors (KNN)");
    println!("{}", "-".repeat(50));
    knn_demonstration();
    println!();

    // Part 7: Advanced Filtering
    println!("Part 7: Custom Filters and Statistics");
    println!("{}", "-".repeat(50));
    advanced_queries();
    println!();

    println!("=== Demo Complete ===");
}

fn basic_point_indexing() {
    // Create index with street-level precision (7)
    let mut index = GeohashRTreeIndex::new(7);

    // Insert famous landmarks
    let landmarks = vec![
        ("empire_state", -73.9857, 40.7484, "Empire State Building"),
        ("statue_liberty", -74.0445, 40.6892, "Statue of Liberty"),
        ("central_park", -73.9654, 40.7829, "Central Park"),
        ("times_square", -73.9855, 40.7580, "Times Square"),
    ];

    for (key, lon, lat, name) in &landmarks {
        index.insert_point(*key, Point::new(*lon, *lat), Bytes::from(*name));
        println!("  ✓ Indexed: {}", name);
    }

    println!("\n  Index stats:");
    println!("    - Total objects: {}", index.object_count());
    println!("    - Geohash cells: {}", index.cell_count());
    println!("    - Precision: {}", index.precision());

    // Query around Times Square (1km radius)
    let times_square = Point::new(-73.9855, 40.7580);
    let results = index.query_within_radius(&times_square, 1000.0, 10);

    println!("\n  Points within 1km of Times Square:");
    for result in results {
        let distance_m = result.distance.unwrap();
        println!(
            "    - {} ({:.0}m away)",
            String::from_utf8_lossy(&result.data),
            distance_m
        );
    }
}

fn precision_comparison() {
    // Demonstrate how different precisions affect partitioning
    let _test_point = Point::new(-74.0060, 40.7128); // NYC coordinates

    println!("  Testing point: NYC (-74.0060, 40.7128)\n");

    for precision in [4, 5, 6, 7, 8, 9] {
        let mut index = GeohashRTreeIndex::new(precision);

        // Insert points in a grid around NYC
        for x_offset in -5..=5 {
            for y_offset in -5..=5 {
                let lon = -74.0 + (x_offset as f64 * 0.01);
                let lat = 40.7 + (y_offset as f64 * 0.01);
                index.insert_point(
                    format!("p_{}_{}", x_offset, y_offset),
                    Point::new(lon, lat),
                    Bytes::new(),
                );
            }
        }

        let stats = index.stats();
        let cell_size = match precision {
            4 => "~20km",
            5 => "~4.9km",
            6 => "~1.2km",
            7 => "~153m",
            8 => "~38m",
            9 => "~4.8m",
            _ => "unknown",
        };

        println!(
            "  Precision {}: {} cells, {:.1} points/cell (cell size: {})",
            precision, stats.cell_count, stats.avg_objects_per_cell, cell_size
        );
    }

    println!("\n  Key insight: Higher precision = more cells, fewer points per cell");
    println!("  Recommended: Precision 6-8 for most applications");
}

fn city_queries() {
    let mut index = GeohashRTreeIndex::new(7);

    // Major world cities
    let cities = vec![
        ("new_york", -74.0060, 40.7128, "New York, USA", 8_336_817),
        ("london", -0.1276, 51.5074, "London, UK", 8_982_000),
        ("tokyo", 139.6917, 35.6895, "Tokyo, Japan", 13_960_000),
        ("paris", 2.3522, 48.8566, "Paris, France", 2_161_000),
        ("sydney", 151.2093, -33.8688, "Sydney, Australia", 5_312_000),
        ("dubai", 55.2708, 25.2048, "Dubai, UAE", 3_331_000),
        ("singapore", 103.8198, 1.3521, "Singapore", 5_704_000),
        (
            "san_francisco",
            -122.4194,
            37.7749,
            "San Francisco, USA",
            883_305,
        ),
        ("berlin", 13.4050, 52.5200, "Berlin, Germany", 3_645_000),
        ("toronto", -79.3832, 43.6532, "Toronto, Canada", 2_930_000),
    ];

    for (key, lon, lat, name, population) in &cities {
        let data = format!("{}|pop:{}", name, population);
        index.insert_point(*key, Point::new(*lon, *lat), Bytes::from(data));
    }

    println!("  Indexed {} major cities\n", cities.len());

    // Query 1: Cities near New York (within 500km)
    let nyc = Point::new(-74.0060, 40.7128);
    let results = index.query_within_radius(&nyc, 500_000.0, 10);

    println!("  Cities within 500km of New York:");
    for result in results {
        let parts: Vec<&str> = std::str::from_utf8(&result.data)
            .unwrap()
            .split('|')
            .collect();
        let distance_km = result.distance.unwrap() / 1000.0;
        println!("    - {} ({:.0} km)", parts[0], distance_km);
    }

    // Query 2: European cities (bbox query)
    let europe_bbox = Rect::new(
        geo::coord! { x: -10.0, y: 35.0 }, // Southwest corner
        geo::coord! { x: 40.0, y: 60.0 },  // Northeast corner
    );

    let results = index.query_within_bbox(&europe_bbox, 10);
    println!("\n  European cities:");
    for result in results {
        let parts: Vec<&str> = std::str::from_utf8(&result.data)
            .unwrap()
            .split('|')
            .collect();
        println!("    - {}", parts[0]);
    }

    // Query 3: Find 3 nearest cities to a point in the Pacific
    let pacific_point = Point::new(-150.0, 20.0);
    let results = index.knn(&pacific_point, 3);

    println!("\n  3 nearest cities to point in Pacific Ocean:");
    for (i, result) in results.iter().enumerate() {
        let parts: Vec<&str> = std::str::from_utf8(&result.data)
            .unwrap()
            .split('|')
            .collect();
        let distance_km = result.distance.unwrap() / 1000.0;
        println!("    {}. {} ({:.0} km)", i + 1, parts[0], distance_km);
    }
}

fn performance_analysis() {
    println!("  Comparing query performance at different scales...\n");

    for scale in [100, 1_000, 10_000] {
        let mut index = GeohashRTreeIndex::new(7);

        // Insert random points around NYC
        let start = Instant::now();
        for i in 0..scale {
            let lon = -74.0 + ((i * 7919) % 1000) as f64 * 0.0001;
            let lat = 40.7 + ((i * 7919) % 1000) as f64 * 0.0001;
            index.insert_point(
                format!("point_{}", i),
                Point::new(lon, lat),
                Bytes::from(format!("Data {}", i)),
            );
        }
        let insert_time = start.elapsed();

        // Query performance
        let query_point = Point::new(-74.0, 40.7);
        let start = Instant::now();
        let (results, stats) = index.query_within_radius_with_stats(&query_point, 5000.0, 100);
        let query_time = start.elapsed();

        println!("  Dataset size: {} points", scale);
        println!(
            "    Insert time: {:.2}ms ({:.2}µs per point)",
            insert_time.as_secs_f64() * 1000.0,
            insert_time.as_micros() as f64 / scale as f64
        );
        println!("    Query time: {:.2}µs", query_time.as_micros());
        println!("    Geohash cells: {}", index.cell_count());
        println!("    Cells examined: {}", stats.cells_examined);
        println!("    Candidates: {}", stats.candidates_examined);
        println!("    Results: {}", results.len());
        println!();
    }

    println!("  Key insight: Two-level index keeps query time low even as data scales");
}

fn complex_objects() {
    let mut index = GeohashRTreeIndex::new(7);

    // Insert Central Park as a polygon
    let central_park = polygon![
        (x: -73.9812, y: 40.7681),
        (x: -73.9581, y: 40.7681),
        (x: -73.9581, y: 40.7967),
        (x: -73.9812, y: 40.7967),
        (x: -73.9812, y: 40.7681),
    ];
    index.insert_polygon("central_park", &central_park, Bytes::from("Central Park"));
    println!("  ✓ Indexed: Central Park (polygon)");

    // Insert Manhattan as a bounding box
    let manhattan_bbox = Rect::new(
        geo::coord! { x: -74.0479, y: 40.6829 },
        geo::coord! { x: -73.9067, y: 40.8820 },
    );
    index.insert_bbox("manhattan", &manhattan_bbox, Bytes::from("Manhattan"));
    println!("  ✓ Indexed: Manhattan (bounding box)");

    // Insert point landmarks
    index.insert_point(
        "empire_state",
        Point::new(-73.9857, 40.7484),
        Bytes::from("Empire State Building"),
    );
    println!("  ✓ Indexed: Empire State Building (point)");

    let stats = index.stats();
    println!("\n  Index contains {} unique objects", stats.unique_objects);
    println!(
        "  Stored across {} cells (objects may span multiple cells)",
        stats.cell_count
    );

    // Query for objects containing a specific point
    let query_point = Point::new(-73.97, 40.78);
    let results = index.query_contains_point(&query_point, 10);

    println!("\n  Objects containing point (-73.97, 40.78):");
    for result in results {
        println!(
            "    - {} (type: {:?})",
            String::from_utf8_lossy(&result.data),
            result.object.object_type
        );
    }
}

fn knn_demonstration() {
    let mut index = GeohashRTreeIndex::new(7);

    // Create a scenario: restaurants in a neighborhood
    let restaurants = vec![
        ("pizza_place", -73.9851, 40.7589, "Joe's Pizza", 4.5),
        ("sushi_bar", -73.9871, 40.7579, "Sushi Heaven", 4.7),
        ("burger_joint", -73.9841, 40.7599, "Best Burgers", 4.2),
        ("italian", -73.9881, 40.7569, "Mama's Kitchen", 4.8),
        ("chinese", -73.9831, 40.7609, "Golden Dragon", 4.3),
        ("mexican", -73.9861, 40.7619, "Taco Fiesta", 4.6),
        ("thai", -73.9891, 40.7559, "Thai Spice", 4.4),
        ("french", -73.9821, 40.7629, "Le Petit Café", 4.9),
    ];

    for (key, lon, lat, name, rating) in &restaurants {
        let data = format!("{}|rating:{}", name, rating);
        index.insert_point(*key, Point::new(*lon, *lat), Bytes::from(data));
    }

    println!("  Indexed {} restaurants\n", restaurants.len());

    // User location
    let user_location = Point::new(-73.9850, 40.7590);
    println!("  User location: Times Square area\n");

    // Find 5 nearest restaurants
    let results = index.knn(&user_location, 5);

    println!("  Top 5 nearest restaurants:");
    for (i, result) in results.iter().enumerate() {
        let parts: Vec<&str> = std::str::from_utf8(&result.data)
            .unwrap()
            .split('|')
            .collect();
        let distance_m = result.distance.unwrap();
        println!(
            "    {}. {} - {:.0}m away ({})",
            i + 1,
            parts[0],
            distance_m,
            parts[1]
        );
    }

    // Custom filtering: only highly-rated restaurants (>= 4.5)
    let bbox = Rect::new(
        geo::coord! { x: -74.0, y: 40.75 },
        geo::coord! { x: -73.98, y: 40.77 },
    );

    let results = index.query_with_filter(
        &bbox,
        |obj| {
            if let Ok(data_str) = std::str::from_utf8(&obj.data)
                && let Some(rating_part) = data_str.split('|').nth(1)
                && let Some(rating_str) = rating_part.strip_prefix("rating:")
                && let Ok(rating) = rating_str.parse::<f64>()
            {
                return rating >= 4.5;
            }
            false
        },
        10,
    );

    println!("\n  Highly-rated restaurants (≥ 4.5 stars) in search area:");
    for result in results {
        let parts: Vec<&str> = std::str::from_utf8(&result.data)
            .unwrap()
            .split('|')
            .collect();
        println!("    - {} ({})", parts[0], parts[1]);
    }
}

fn advanced_queries() {
    let mut index = GeohashRTreeIndex::new(7);

    // Insert a larger dataset
    for i in 0..500 {
        let angle = (i as f64 / 500.0) * 2.0 * std::f64::consts::PI;
        let radius = 0.1 * (i as f64 / 500.0);
        let lon = -74.0 + radius * angle.cos();
        let lat = 40.7 + radius * angle.sin();

        let category = match i % 3 {
            0 => "restaurant",
            1 => "hotel",
            _ => "attraction",
        };

        let data = format!("Item {} | category:{}", i, category);
        index.insert_point(
            format!("item_{}", i),
            Point::new(lon, lat),
            Bytes::from(data),
        );
    }

    // Get detailed statistics
    let stats = index.stats();
    println!("  Index Statistics:");
    println!("    - Total unique objects: {}", stats.unique_objects);
    println!("    - Total cell count: {}", stats.cell_count);
    println!(
        "    - Average objects per cell: {:.2}",
        stats.avg_objects_per_cell
    );
    println!("    - Precision level: {}", stats.precision);

    println!("\n  Top 5 most populated cells:");
    for (i, cell) in stats.cells.iter().take(5).enumerate() {
        println!(
            "    {}. Geohash '{}': {} objects (~{} KB)",
            i + 1,
            cell.geohash,
            cell.object_count,
            cell.estimated_memory / 1024
        );
    }

    // Query with statistics
    let (_results, query_stats) =
        index.query_within_radius_with_stats(&Point::new(-74.0, 40.7), 2000.0, 50);

    println!("\n  Query Statistics:");
    println!("    - Cells examined: {}", query_stats.cells_examined);
    println!(
        "    - Candidates examined: {}",
        query_stats.candidates_examined
    );
    println!("    - Results returned: {}", query_stats.results_returned);
    println!("    - Deduplication applied: {}", query_stats.deduplicated);
    println!(
        "    - Filtering efficiency: {:.1}%",
        (query_stats.results_returned as f64 / query_stats.candidates_examined as f64) * 100.0
    );

    println!(
        "\n  Key insight: Two-level index examines only {} cells instead of entire dataset",
        query_stats.cells_examined
    );
}
