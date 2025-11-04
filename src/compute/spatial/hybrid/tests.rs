//! Comprehensive integration tests for the geohash-rtree hybrid index.

use super::*;
use bytes::Bytes;
use geo::{Point, Rect, polygon};

#[test]
fn test_basic_insert_and_query() {
    let mut index = GeohashRTreeIndex::new(7);

    let nyc = Point::new(-74.0060, 40.7128);
    index.insert_point("nyc", nyc, Bytes::from("New York City"));

    let results = index.query_within_radius(&nyc, 1000.0, 10);
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].key, "nyc");
    assert_eq!(results[0].data, Bytes::from("New York City"));
}

#[test]
fn test_multiple_cities() {
    let mut index = GeohashRTreeIndex::new(7);

    // Major US cities
    let cities = vec![
        ("nyc", -74.0060, 40.7128, "New York"),
        ("la", -118.2437, 34.0522, "Los Angeles"),
        ("chicago", -87.6298, 41.8781, "Chicago"),
        ("houston", -95.3698, 29.7604, "Houston"),
        ("phoenix", -112.0740, 33.4484, "Phoenix"),
        ("philly", -75.1652, 39.9526, "Philadelphia"),
        ("san_antonio", -98.4936, 29.4241, "San Antonio"),
        ("san_diego", -117.1611, 32.7157, "San Diego"),
        ("dallas", -96.7970, 32.7767, "Dallas"),
        ("sf", -122.4194, 37.7749, "San Francisco"),
    ];

    for (key, lon, lat, name) in &cities {
        index.insert_point(*key, Point::new(*lon, *lat), Bytes::from(*name));
    }

    assert_eq!(index.object_count(), 10);

    // Query around NYC (should get at least NYC)
    let nyc = Point::new(-74.0060, 40.7128);
    let results = index.query_within_radius(&nyc, 200_000.0, 10);
    assert!(!results.is_empty()); // At least NYC should be found
}

#[test]
fn test_precision_levels() {
    // Test different precision levels
    for precision in 4..=9 {
        let mut index = GeohashRTreeIndex::new(precision);

        index.insert_point("p1", Point::new(-74.0, 40.7), Bytes::from("Point 1"));
        index.insert_point("p2", Point::new(-74.1, 40.7), Bytes::from("Point 2"));

        assert_eq!(index.object_count(), 2);
        assert_eq!(index.precision(), precision);
    }
}

#[test]
fn test_large_scale_insertion() {
    let mut index = GeohashRTreeIndex::new(7);

    // Insert 1000 random points around NYC
    for i in 0..1000 {
        let lon = -74.0 + (i as f64 % 100.0) * 0.01;
        let lat = 40.7 + (i as f64 / 100.0) * 0.01;
        index.insert_point(
            format!("point_{}", i),
            Point::new(lon, lat),
            Bytes::from(format!("Data {}", i)),
        );
    }

    assert_eq!(index.object_count(), 1000);

    let stats = index.stats();
    println!("Large scale test stats: {:?}", stats);
    assert!(stats.cell_count > 0);
    assert_eq!(stats.unique_objects, 1000);
}

#[test]
fn test_polygon_insertion_and_query() {
    let mut index = GeohashRTreeIndex::new(7);

    let central_park = polygon![
        (x: -73.9812, y: 40.7681),
        (x: -73.9581, y: 40.7681),
        (x: -73.9581, y: 40.7967),
        (x: -73.9812, y: 40.7967),
        (x: -73.9812, y: 40.7681),
    ];

    index.insert_polygon("central_park", &central_park, Bytes::from("Central Park"));

    let bbox = Rect::new(
        geo::coord! { x: -74.0, y: 40.76 },
        geo::coord! { x: -73.95, y: 40.80 },
    );

    let results = index.query_within_bbox(&bbox, 10);
    assert!(!results.is_empty());
}

#[test]
fn test_deduplication() {
    let mut index = GeohashRTreeIndex::new(6); // Lower precision = larger cells

    // Insert a large polygon that spans multiple cells
    let large_area = polygon![
        (x: -74.5, y: 40.5),
        (x: -73.5, y: 40.5),
        (x: -73.5, y: 41.0),
        (x: -74.5, y: 41.0),
        (x: -74.5, y: 40.5),
    ];

    index.insert_polygon("large_area", &large_area, Bytes::from("Large Area"));

    // The polygon should span multiple cells
    let stats = index.stats();
    println!(
        "Deduplication test - cells: {}, total objects: {}, unique: {}",
        stats.cell_count, stats.total_objects, stats.unique_objects
    );
    assert_eq!(stats.unique_objects, 1); // Only one unique object

    // But it might be stored in multiple cells
    // Query should return it only once due to deduplication
    let query_point = Point::new(-74.0, 40.75);
    let results = index.query_within_radius(&query_point, 100_000.0, 10);

    // Count how many times we see the same key
    let mut key_counts = std::collections::HashMap::new();
    for result in &results {
        *key_counts.entry(result.key.as_str()).or_insert(0) += 1;
    }

    // Each key should appear exactly once
    for (key, count) in key_counts {
        assert_eq!(
            count, 1,
            "Key '{}' appeared {} times, expected 1",
            key, count
        );
    }
}

#[test]
fn test_remove_operation() {
    let mut index = GeohashRTreeIndex::new(7);

    index.insert_point("p1", Point::new(-74.0, 40.7), Bytes::from("Point 1"));
    index.insert_point("p2", Point::new(-74.1, 40.7), Bytes::from("Point 2"));
    index.insert_point("p3", Point::new(-74.2, 40.7), Bytes::from("Point 3"));

    assert_eq!(index.object_count(), 3);

    // Remove one point
    assert!(index.remove("p2"));
    assert_eq!(index.object_count(), 2);
    assert!(!index.contains_key("p2"));

    // Try to remove again
    assert!(!index.remove("p2"));

    // Remaining points should still be queryable
    let results = index.query_within_radius(&Point::new(-74.0, 40.7), 100_000.0, 10);
    assert!(!results.is_empty()); // At least one point should be found
}

#[test]
fn test_update_object() {
    let mut index = GeohashRTreeIndex::new(7);

    // Insert initial point
    index.insert_point("city", Point::new(-74.0, 40.7), Bytes::from("New York"));

    let results = index.query_within_radius(&Point::new(-74.0, 40.7), 1000.0, 10);
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].data, Bytes::from("New York"));

    // Update to different location and data
    index.insert_point(
        "city",
        Point::new(-122.4, 37.7),
        Bytes::from("San Francisco"),
    );

    // Should still have only one object
    assert_eq!(index.object_count(), 1);

    // Old location should return nothing
    let results = index.query_within_radius(&Point::new(-74.0, 40.7), 1000.0, 10);
    assert_eq!(results.len(), 0);

    // New location should return the updated object
    let results = index.query_within_radius(&Point::new(-122.4, 37.7), 1000.0, 10);
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].data, Bytes::from("San Francisco"));
}

#[test]
fn test_knn_ordering() {
    let mut index = GeohashRTreeIndex::new(7);

    // Insert points at different distances
    let base = Point::new(0.0, 0.0);
    index.insert_point("near", Point::new(0.01, 0.0), Bytes::from("Near"));
    index.insert_point("medium", Point::new(0.1, 0.0), Bytes::from("Medium"));
    index.insert_point("far", Point::new(1.0, 0.0), Bytes::from("Far"));

    let results = index.knn(&base, 3);

    assert_eq!(results.len(), 3);
    assert_eq!(results[0].key, "near");
    assert_eq!(results[1].key, "medium");
    assert_eq!(results[2].key, "far");

    // Check distances are increasing
    assert!(results[0].distance.unwrap() < results[1].distance.unwrap());
    assert!(results[1].distance.unwrap() < results[2].distance.unwrap());
}

#[test]
fn test_bbox_query_accuracy() {
    let mut index = GeohashRTreeIndex::new(7);

    // Insert points in a grid
    for x in 0..10 {
        for y in 0..10 {
            let lon = -74.0 + x as f64 * 0.01;
            let lat = 40.7 + y as f64 * 0.01;
            index.insert_point(format!("p_{}_{}", x, y), Point::new(lon, lat), Bytes::new());
        }
    }

    // Query a specific bbox
    let bbox = Rect::new(
        geo::coord! { x: -73.95, y: 40.75 },
        geo::coord! { x: -73.90, y: 40.80 },
    );

    let results = index.query_within_bbox(&bbox, 100);

    // Verify all results are actually within the bbox
    for result in &results {
        let center = result.center();
        assert!(
            center.x() >= bbox.min().x && center.x() <= bbox.max().x,
            "X coordinate out of bounds"
        );
        assert!(
            center.y() >= bbox.min().y && center.y() <= bbox.max().y,
            "Y coordinate out of bounds"
        );
    }
}

#[test]
fn test_3d_points() {
    let mut index = GeohashRTreeIndex::new(7);

    // Insert 3D points (with altitude)
    index.insert_point_3d("ground", -74.0, 40.7, 0.0, Bytes::from("Ground level"));
    index.insert_point_3d("tower", -74.0, 40.7, 443.2, Bytes::from("Empire State"));
    index.insert_point_3d("plane", -74.0, 40.7, 10000.0, Bytes::from("Aircraft"));

    assert_eq!(index.object_count(), 3);

    // Query should find all three (they're at same lat/lon)
    let results = index.query_within_radius(&Point::new(-74.0, 40.7), 100.0, 10);
    assert_eq!(results.len(), 3);
}

#[test]
fn test_stats_accuracy() {
    let mut index = GeohashRTreeIndex::new(7);

    // Insert known number of objects
    for i in 0..50 {
        let lon = -74.0 + (i as f64 * 0.01);
        index.insert_point(
            format!("p{}", i),
            Point::new(lon, 40.7),
            Bytes::from(format!("Point {}", i)),
        );
    }

    let stats = index.stats();

    assert_eq!(stats.unique_objects, 50);
    assert_eq!(stats.precision, 7);
    assert!(stats.cell_count > 0);
    assert!(stats.avg_objects_per_cell > 0.0);

    // Verify total objects >= unique objects (due to potential duplicates across cells)
    assert!(stats.total_objects >= stats.unique_objects);
}

#[test]
fn test_query_with_limit() {
    let mut index = GeohashRTreeIndex::new(7);

    // Insert many points
    for i in 0..100 {
        let lon = -74.0 + (i as f64 * 0.0001);
        index.insert_point(
            format!("p{}", i),
            Point::new(lon, 40.7),
            Bytes::from(format!("Point {}", i)),
        );
    }

    // Query with small limit
    let results = index.query_within_radius(&Point::new(-74.0, 40.7), 10_000.0, 5);
    assert_eq!(results.len(), 5);

    // Query with large limit
    let results = index.query_within_radius(&Point::new(-74.0, 40.7), 10_000.0, 1000);
    assert!(results.len() <= 100); // Can't exceed actual number of results
}

#[test]
fn test_empty_index_queries() {
    let index = GeohashRTreeIndex::new(7);

    let results = index.query_within_radius(&Point::new(0.0, 0.0), 1000.0, 10);
    assert_eq!(results.len(), 0);

    let bbox = Rect::new(
        geo::coord! { x: -1.0, y: -1.0 },
        geo::coord! { x: 1.0, y: 1.0 },
    );
    let results = index.query_within_bbox(&bbox, 10);
    assert_eq!(results.len(), 0);

    let results = index.knn(&Point::new(0.0, 0.0), 10);
    assert_eq!(results.len(), 0);
}

#[test]
fn test_clear_operation() {
    let mut index = GeohashRTreeIndex::new(7);

    // Insert data
    for i in 0..10 {
        index.insert_point(
            format!("p{}", i),
            Point::new(-74.0 + i as f64 * 0.01, 40.7),
            Bytes::new(),
        );
    }

    assert_eq!(index.object_count(), 10);

    // Clear
    index.clear();

    assert_eq!(index.object_count(), 0);
    assert_eq!(index.cell_count(), 0);

    let stats = index.stats();
    assert_eq!(stats.unique_objects, 0);
    assert_eq!(stats.total_objects, 0);
}

#[test]
fn test_edge_cases_coordinates() {
    let mut index = GeohashRTreeIndex::new(7);

    // Test extreme coordinates
    index.insert_point(
        "north",
        Point::new(0.0, 89.0),
        Bytes::from("Near North Pole"),
    );
    index.insert_point(
        "south",
        Point::new(0.0, -89.0),
        Bytes::from("Near South Pole"),
    );
    index.insert_point(
        "dateline",
        Point::new(179.0, 0.0),
        Bytes::from("Near Dateline"),
    );
    index.insert_point(
        "greenwich",
        Point::new(0.0, 0.0),
        Bytes::from("Prime Meridian"),
    );

    assert_eq!(index.object_count(), 4);

    // Each should be queryable
    let results = index.query_within_radius(&Point::new(0.0, 89.0), 10_000.0, 10);
    assert!(results.iter().any(|r| r.key == "north"));
}

#[test]
fn test_query_with_stats() {
    let mut index = GeohashRTreeIndex::new(7);

    for i in 0..20 {
        index.insert_point(
            format!("p{}", i),
            Point::new(-74.0 + i as f64 * 0.01, 40.7),
            Bytes::new(),
        );
    }

    let (results, stats) =
        index.query_within_radius_with_stats(&Point::new(-74.0, 40.7), 5_000.0, 10);

    assert!(results.len() <= 10); // Respects limit
    assert_eq!(stats.results_returned, results.len());
    assert!(stats.cells_examined > 0);
    assert!(stats.candidates_examined >= results.len());
    assert!(stats.deduplicated);
}

#[test]
fn test_concurrent_cell_access() {
    // Test that multiple queries don't interfere with each other
    let mut index = GeohashRTreeIndex::new(7);

    // Insert points in different regions
    index.insert_point("nyc", Point::new(-74.0, 40.7), Bytes::from("NYC"));
    index.insert_point("sf", Point::new(-122.4, 37.7), Bytes::from("SF"));

    // Run multiple queries
    let results1 = index.query_within_radius(&Point::new(-74.0, 40.7), 10_000.0, 10);
    let results2 = index.query_within_radius(&Point::new(-122.4, 37.7), 10_000.0, 10);

    // Results should be independent
    assert_eq!(results1.len(), 1);
    assert_eq!(results1[0].key, "nyc");

    assert_eq!(results2.len(), 1);
    assert_eq!(results2[0].key, "sf");
}

#[test]
fn test_spatial_object_types() {
    let mut index = GeohashRTreeIndex::new(7);

    // Insert different object types
    index.insert_point("point", Point::new(-74.0, 40.7), Bytes::from("A point"));

    let poly = polygon![
        (x: -74.1, y: 40.6),
        (x: -74.0, y: 40.6),
        (x: -74.0, y: 40.7),
        (x: -74.1, y: 40.7),
        (x: -74.1, y: 40.6),
    ];
    index.insert_polygon("polygon", &poly, Bytes::from("A polygon"));

    let bbox = Rect::new(
        geo::coord! { x: -74.2, y: 40.5 },
        geo::coord! { x: -74.1, y: 40.6 },
    );
    index.insert_bbox("bbox", &bbox, Bytes::from("A bbox"));

    assert_eq!(index.object_count(), 3);

    // All should be findable
    let all_bbox = Rect::new(
        geo::coord! { x: -74.3, y: 40.4 },
        geo::coord! { x: -73.9, y: 40.8 },
    );
    let results = index.query_within_bbox(&all_bbox, 10);
    // Should find at least some objects
    assert!(!results.is_empty());
}
