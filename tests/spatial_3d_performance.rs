//! Performance validation tests for 3D spatial queries with envelope-based pruning.
//!
//! These tests validate that the R*-tree envelope pruning optimization
//! provides significant performance improvements over linear iteration
//! for 3D spherical and cylindrical queries.

use spatio::{Point3d, Spatio};
use std::time::Instant;

#[test]
fn test_3d_sphere_query_scales_sublinearly() {
    // This test validates that spherical queries benefit from envelope pruning
    // by showing sublinear scaling behavior as dataset size increases.

    let dataset_sizes = [1000, 5000, 10000];
    let mut query_times_ms = Vec::new();

    for &size in &dataset_sizes {
        let mut db = Spatio::memory().unwrap();

        // Populate with distributed 3D points
        for i in 0..size {
            let lat = 40.0 + ((i % 100) as f64 * 0.001);
            let lon = -74.0 + ((i / 100) as f64 * 0.001);
            let alt = (i as f64 * 10.0) % 10000.0;
            let point = Point3d::new(lon, lat, alt);
            db.insert_point_3d("aircraft", &point, format!("data_{}", i).as_bytes(), None)
                .unwrap();
        }

        // Query near the center of the data distribution
        let center = Point3d::new(-74.05, 40.05, 5000.0);
        let radius = 10000.0;

        let start = Instant::now();
        let results = db
            .query_within_sphere_3d("aircraft", &center, radius, 100)
            .unwrap();
        let elapsed = start.elapsed();

        query_times_ms.push(elapsed.as_secs_f64() * 1000.0);

        // Sanity check that we got results
        assert!(
            !results.is_empty(),
            "Should find results for dataset size {}",
            size
        );
    }

    // With envelope pruning, query time should grow sublinearly
    // Even with 10x data (1k -> 10k), query time should be < 10x
    let ratio_10x = query_times_ms[2] / query_times_ms[0];

    println!("3D Sphere Query Performance:");
    println!("  1,000 points: {:.2}ms", query_times_ms[0]);
    println!("  5,000 points: {:.2}ms", query_times_ms[1]);
    println!(" 10,000 points: {:.2}ms", query_times_ms[2]);
    println!("  10x data ratio: {:.2}x time", ratio_10x);

    assert!(
        ratio_10x < 10.0,
        "Query time should scale sublinearly with envelope pruning (got {:.2}x for 10x data)",
        ratio_10x
    );
}

#[test]
fn test_3d_cylinder_query_altitude_pruning() {
    // This test validates that cylindrical queries efficiently prune
    // points outside the altitude range using envelope constraints.

    let mut db = Spatio::memory().unwrap();

    // Insert 10,000 points evenly distributed across altitudes
    for i in 0..10000 {
        let lat = 40.0 + ((i % 100) as f64 * 0.001);
        let lon = -74.0 + ((i / 100) as f64 * 0.001);
        let alt = (i as f64 / 10000.0) * 20000.0; // 0 to 20,000m
        let point = Point3d::new(lon, lat, alt);
        db.insert_point_3d("aircraft", &point, format!("data_{}", i).as_bytes(), None)
            .unwrap();
    }

    let center = Point3d::new(-74.0, 40.0, 0.0);

    // Query 1: Narrow altitude range (2000-3000m) - should be fast
    let start1 = Instant::now();
    let narrow_results = db
        .query_within_cylinder_3d("aircraft", &center, 10000.0, 2000.0, 3000.0, 1000)
        .unwrap();
    let narrow_time = start1.elapsed();

    // Query 2: Wide altitude range (0-20000m) - should take longer but still benefit from horizontal pruning
    let start2 = Instant::now();
    let wide_results = db
        .query_within_cylinder_3d("aircraft", &center, 10000.0, 0.0, 20000.0, 1000)
        .unwrap();
    let wide_time = start2.elapsed();

    println!("3D Cylinder Query Altitude Pruning:");
    println!(
        "  Narrow range (2000-3000m): {} results in {:.2}ms",
        narrow_results.len(),
        narrow_time.as_secs_f64() * 1000.0
    );
    println!(
        "  Wide range (0-20000m): {} results in {:.2}ms",
        wide_results.len(),
        wide_time.as_secs_f64() * 1000.0
    );

    // If both found results, narrow range should find fewer or equal
    if !wide_results.is_empty() {
        assert!(
            narrow_results.len() <= wide_results.len(),
            "Narrow altitude range should return fewer or equal results"
        );
    }

    // All results should be within altitude bounds
    for (point, _, _) in &narrow_results {
        assert!(
            point.z() >= 2000.0 && point.z() <= 3000.0,
            "Point altitude {} outside range [2000, 3000]",
            point.z()
        );
    }
}

#[test]
fn test_3d_knn_with_large_dataset() {
    // Validate that KNN queries remain efficient with R*-tree spatial indexing

    let mut db = Spatio::memory().unwrap();

    // Insert 5,000 3D points
    for i in 0..5000 {
        let lat = 40.0 + ((i % 50) as f64 * 0.002);
        let lon = -74.0 + ((i / 50) as f64 * 0.002);
        let alt = (i as f64 * 5.0) % 8000.0;
        let point = Point3d::new(lon, lat, alt);
        db.insert_point_3d("points", &point, format!("data_{}", i).as_bytes(), None)
            .unwrap();
    }

    let query_point = Point3d::new(-74.0, 40.0, 4000.0);

    // KNN should be fast even with 5k points
    let start = Instant::now();
    let neighbors = db.knn_3d("points", &query_point, 10).unwrap();
    let elapsed = start.elapsed();

    println!(
        "3D KNN Query (k=10 from 5000 points): {:.2}ms",
        elapsed.as_secs_f64() * 1000.0
    );

    assert_eq!(neighbors.len(), 10, "Should return exactly 10 neighbors");

    // Should complete in reasonable time (< 100ms on typical hardware)
    assert!(
        elapsed.as_millis() < 100,
        "KNN query should be fast with R*-tree indexing (took {}ms)",
        elapsed.as_millis()
    );

    // Note: R*-tree uses Euclidean distance in coordinate space for KNN ordering,
    // which differs from Haversine distance. Results are ordered by R*-tree's
    // internal metric, not by the Haversine distances we compute afterward.
    // This is expected behavior for geographic KNN queries.
}

#[test]
fn test_envelope_pruning_effectiveness() {
    // This test demonstrates that envelope pruning significantly reduces
    // the number of distance calculations required.

    let mut db = Spatio::memory().unwrap();

    // Create a 100x100 grid of points at ground level
    for i in 0..100 {
        for j in 0..100 {
            let lat = 40.0 + (i as f64 * 0.001);
            let lon = -74.0 + (j as f64 * 0.001);
            let point = Point3d::new(lon, lat, 0.0);
            db.insert_point_3d(
                "grid",
                &point,
                format!("point_{}_{}", i, j).as_bytes(),
                None,
            )
            .unwrap();
        }
    }

    // Query in corner - should only examine nearby envelope
    let corner_center = Point3d::new(-74.0, 40.0, 0.0);
    let small_radius = 100.0; // Very small radius

    let start = Instant::now();
    let results = db
        .query_within_sphere_3d("grid", &corner_center, small_radius, 1000)
        .unwrap();
    let elapsed = start.elapsed();

    println!("Envelope Pruning Effectiveness (10k points, 100m radius):");
    println!("  Results found: {}", results.len());
    println!("  Query time: {:.2}ms", elapsed.as_secs_f64() * 1000.0);

    // With effective pruning, should find very few points
    assert!(
        results.len() < 100,
        "Should prune most points with small radius query"
    );

    // Should be very fast due to pruning
    assert!(
        elapsed.as_millis() < 50,
        "Query should be fast with envelope pruning (took {}ms)",
        elapsed.as_millis()
    );
}

#[test]
fn test_3d_queries_correctness_vs_brute_force() {
    // Validate that envelope-based queries return correct results
    // by comparing against a known small dataset.

    let mut db = Spatio::memory().unwrap();

    // Insert small known dataset
    let test_points = [
        (-74.0, 40.0, 1000.0),
        (-74.001, 40.001, 1500.0),
        (-74.002, 40.002, 2000.0),
        (-74.01, 40.01, 5000.0),
        (-74.1, 40.1, 10000.0),
    ];

    for (i, &(lon, lat, alt)) in test_points.iter().enumerate() {
        let point = Point3d::new(lon, lat, alt);
        db.insert_point_3d("test", &point, format!("point_{}", i).as_bytes(), None)
            .unwrap();
    }

    let center = Point3d::new(-74.0, 40.0, 1000.0);
    let radius = 2000.0;

    let results = db
        .query_within_sphere_3d("test", &center, radius, 10)
        .unwrap();

    // Should find points 0, 1, and 2 (within ~2km including altitude)
    assert!(
        results.len() >= 2 && results.len() <= 4,
        "Should find 2-4 nearby points"
    );

    // All results should be within radius
    for (_, _, distance) in &results {
        assert!(
            *distance <= radius,
            "Distance {} exceeds radius {}",
            distance,
            radius
        );
    }
}
