use spatio::prelude::*;
use std::time::Duration;
use tempfile::TempDir;

#[test]
fn test_aof_rewrite_functionality() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.aof");

    // Create database with AOF persistence
    let db = Spatio::open(&db_path).unwrap();

    // Write data using simple key-value pairs to avoid spatial indexing issues
    for i in 0..50 {
        db.insert(format!("key_{}", i), format!("value_{}", i), None)
            .unwrap();
    }

    let initial_size = std::fs::metadata(&db_path).unwrap().len();
    assert!(initial_size > 0, "AOF file should have been created");

    // Write more data to trigger potential rewrite
    for i in 50..100 {
        db.insert(format!("key_{}", i), format!("value_{}", i), None)
            .unwrap();
    }

    // Add more data after potential rewrite
    for i in 100..150 {
        db.insert(format!("key_{}", i), format!("value_{}", i), None)
            .unwrap();
    }

    let final_size = std::fs::metadata(&db_path).unwrap().len();
    assert!(final_size > initial_size, "AOF file should have grown");

    // Verify data integrity - all keys should be accessible
    for i in 0..150 {
        let key = format!("key_{}", i);
        let value = db.get(&key).unwrap().expect("Key should exist");
        assert_eq!(
            String::from_utf8(value.to_vec()).unwrap(),
            format!("value_{}", i)
        );
    }

    // Test recovery by reopening the database
    drop(db);

    let recovered_db = Spatio::open(&db_path).unwrap();

    // Verify all data persisted correctly
    for i in 0..150 {
        let key = format!("key_{}", i);
        let value = recovered_db
            .get(&key)
            .unwrap()
            .expect("Key should exist after recovery");
        assert_eq!(
            String::from_utf8(value.to_vec()).unwrap(),
            format!("value_{}", i),
            "Value should match after recovery for key {}",
            key
        );
    }
}

#[test]
fn test_aof_file_handle_consistency() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("handle_test.aof");

    // Create a database with AOF persistence
    let db = Spatio::open(&db_path).unwrap();

    // Write initial data
    for i in 0..20 {
        let lat = 40.0 + (i as f64) * 0.01;
        let lon = -74.0 + (i as f64) * 0.01;
        let point = Point::new(lat, lon);
        db.insert_point("initial", &point, format!("initial_{}", i).as_bytes(), None)
            .unwrap();
    }

    let size_before_operations = std::fs::metadata(&db_path).unwrap().len();

    // Write more data that could trigger rewrite
    for i in 20..40 {
        let lat = 41.0 + (i as f64) * 0.01;
        let lon = -73.0 + (i as f64) * 0.01;
        let point = Point::new(lat, lon);
        db.insert_point("trigger", &point, format!("trigger_{}", i).as_bytes(), None)
            .unwrap();
    }

    // Write more data after potential rewrite
    for i in 40..60 {
        let lat = 42.0 + (i as f64) * 0.01;
        let lon = -72.0 + (i as f64) * 0.01;
        let point = Point::new(lat, lon);
        db.insert_point("after", &point, format!("after_{}", i).as_bytes(), None)
            .unwrap();
    }

    let size_after_operations = std::fs::metadata(&db_path).unwrap().len();
    assert!(size_after_operations > size_before_operations);

    // Verify all data is accessible by searching in regions with generous bounds
    let initial_results = db
        .find_within_bounds("initial", 39.0, -75.0, 41.0, -72.0, 100)
        .unwrap();
    assert_eq!(initial_results.len(), 20);

    let trigger_results = db
        .find_within_bounds("trigger", 39.0, -75.0, 42.0, -71.0, 100)
        .unwrap();
    assert_eq!(trigger_results.len(), 20);

    let after_results = db
        .find_within_bounds("after", 39.0, -75.0, 43.0, -70.0, 100)
        .unwrap();
    assert_eq!(after_results.len(), 20);

    // Test that the AOF file is valid by reopening
    drop(db);
    let recovered_db = Spatio::open(&db_path).unwrap();

    // Verify recovery worked correctly
    let initial_recovered = recovered_db
        .find_within_bounds("initial", 39.0, -75.0, 41.0, -72.0, 100)
        .unwrap();
    let trigger_recovered = recovered_db
        .find_within_bounds("trigger", 39.0, -75.0, 42.0, -71.0, 100)
        .unwrap();
    let after_recovered = recovered_db
        .find_within_bounds("after", 39.0, -75.0, 43.0, -70.0, 100)
        .unwrap();
    assert_eq!(
        initial_recovered.len() + trigger_recovered.len() + after_recovered.len(),
        60
    );
}

#[test]
fn test_aof_rewrite_atomicity() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("atomicity_test.aof");

    let db = Spatio::open(&db_path).unwrap();

    // Write data that will create a significant AOF file
    for i in 0..30 {
        let lat = 40.0 + (i as f64) * 0.01;
        let lon = -74.0 + (i as f64) * 0.01;
        let point = Point::new(lat, lon);
        db.insert_point("test", &point, format!("key_{}", i).as_bytes(), None)
            .unwrap();
    }

    // At this point, a rewrite might have occurred
    // Verify that we can still read all data consistently
    let results = db
        .find_within_bounds("test", 39.5, -74.5, 40.5, -73.5, 100)
        .unwrap();
    assert_eq!(results.len(), 30);

    // Write more data after potential rewrite
    for i in 30..50 {
        let lat = 40.0 + (i as f64) * 0.01;
        let lon = -74.0 + (i as f64) * 0.01;
        let point = Point::new(lat, lon);
        db.insert_point("test", &point, format!("key_{}", i).as_bytes(), None)
            .unwrap();
    }

    // Verify all data is still accessible
    let all_results = db
        .find_within_bounds("test", 39.5, -74.5, 40.8, -73.2, 100)
        .unwrap();
    assert_eq!(all_results.len(), 50);

    // Test recovery to ensure AOF file is valid
    drop(db);
    let recovered_db = Spatio::open(&db_path).unwrap();

    let recovered_results = recovered_db
        .find_within_bounds("test", 39.5, -74.5, 40.8, -73.2, 100)
        .unwrap();
    assert_eq!(recovered_results.len(), 50);

    // Verify the data content is correct
    for (point, data) in recovered_results {
        // Extract the index from the data
        let data_str = String::from_utf8(data.to_vec()).unwrap();
        let index: i32 = data_str.strip_prefix("key_").unwrap().parse().unwrap();

        // Verify point coordinates match our formula: lat = 40.0 + index * 0.01
        let expected_lat = 40.0 + (index as f64) * 0.01;
        let expected_lon = -74.0 + (index as f64) * 0.01;
        assert!((point.lat - expected_lat).abs() < 0.001);
        assert!((point.lon - expected_lon).abs() < 0.001);
    }
}

#[test]
fn test_aof_persistence_across_restarts() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("persistence_test.aof");

    // First session: write data
    {
        let db = Spatio::open(&db_path).unwrap();

        // Insert various types of data
        db.insert("simple_key", b"simple_value", None).unwrap();

        let point = Point::new(40.7128, -74.0060);
        db.insert_point("cities", &point, b"New York City", None)
            .unwrap();

        // Insert with TTL (but long enough that it won't expire during test)
        let opts = SetOptions::with_ttl(Duration::from_secs(3600));
        db.insert("temp_key", b"temp_value", Some(opts)).unwrap();

        // Insert trajectory
        let trajectory = vec![
            (Point::new(40.7128, -74.0060), 1640995200),
            (Point::new(40.7150, -74.0040), 1640995260),
        ];
        db.insert_trajectory("vehicle:001", &trajectory, None)
            .unwrap();
    }

    // Second session: verify data persistence
    {
        let db = Spatio::open(&db_path).unwrap();

        // Check simple key-value
        let value = db.get("simple_key").unwrap().unwrap();
        assert_eq!(value.as_ref(), b"simple_value");

        // Check point by searching nearby
        let search_point = Point::new(40.7128, -74.0060);
        let nearby = db.find_nearby("cities", &search_point, 1000.0, 10).unwrap();
        assert!(!nearby.is_empty());
        assert_eq!(nearby[0].1.as_ref(), b"New York City");

        // Check TTL key (should still exist)
        let value = db.get("temp_key").unwrap().unwrap();
        assert_eq!(value.as_ref(), b"temp_value");

        // Check trajectory
        let path = db
            .query_trajectory("vehicle:001", 1640995200, 1640995260)
            .unwrap();
        assert_eq!(path.len(), 2);
        assert_eq!(path[0].0.lat, 40.7128);
        assert_eq!(path[1].0.lat, 40.7150);
    }

    // Third session: modify data and verify persistence
    {
        let db = Spatio::open(&db_path).unwrap();

        // Update existing data
        db.insert("simple_key", b"updated_value", None).unwrap();

        // Delete a key
        db.delete("temp_key").unwrap();

        // Add more data
        let point = Point::new(34.0522, -118.2437);
        db.insert_point("cities", &point, b"Los Angeles", None)
            .unwrap();
    }

    // Fourth session: verify modifications persisted
    {
        let db = Spatio::open(&db_path).unwrap();

        // Check updated value
        let value = db.get("simple_key").unwrap().unwrap();
        assert_eq!(value.as_ref(), b"updated_value");

        // Check deleted key
        let value = db.get("temp_key").unwrap();
        assert!(value.is_none());

        // Check both cities exist
        let all_cities = db
            .find_within_bounds("cities", 30.0, -120.0, 45.0, -70.0, 10)
            .unwrap();
        assert_eq!(all_cities.len(), 2);

        // Original data should still exist
        let nyc_search = Point::new(40.7128, -74.0060);
        let nyc_nearby = db.find_nearby("cities", &nyc_search, 1000.0, 10).unwrap();
        assert!(!nyc_nearby.is_empty());

        let la_search = Point::new(34.0522, -118.2437);
        let la_nearby = db.find_nearby("cities", &la_search, 1000.0, 10).unwrap();
        assert!(!la_nearby.is_empty());
    }
}

#[test]
fn test_synchronous_rewrite_behavior() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("sync_rewrite_test.aof");

    let db = Spatio::open(&db_path).unwrap();

    // Write enough data to potentially trigger rewrite
    for i in 0..100 {
        let lat = 40.0 + (i as f64) * 0.001; // Smaller increments for denser data
        let lon = -74.0 + (i as f64) * 0.001;
        let point = Point::new(lat, lon);
        db.insert_point(
            "dense",
            &point,
            format!("dense_point_{}", i).as_bytes(),
            None,
        )
        .unwrap();
    }

    // Verify all data is immediately accessible (synchronous behavior)
    let results = db
        .find_within_bounds("dense", 39.0, -75.0, 41.0, -73.0, 200)
        .unwrap();
    assert_eq!(
        results.len(),
        100,
        "All data should be immediately accessible"
    );

    // Write more data
    for i in 100..200 {
        let lat = 40.0 + (i as f64) * 0.001;
        let lon = -74.0 + (i as f64) * 0.001;
        let point = Point::new(lat, lon);
        db.insert_point(
            "dense",
            &point,
            format!("dense_point_{}", i).as_bytes(),
            None,
        )
        .unwrap();
    }

    // Again, verify immediate accessibility
    let final_results = db
        .find_within_bounds("dense", 39.0, -75.0, 41.0, -73.0, 300)
        .unwrap();
    assert_eq!(
        final_results.len(),
        200,
        "All data should remain accessible"
    );

    // Test persistence
    drop(db);
    let recovered_db = Spatio::open(&db_path).unwrap();

    let recovered_results = recovered_db
        .find_within_bounds("dense", 39.0, -75.0, 41.0, -73.0, 300)
        .unwrap();
    assert_eq!(
        recovered_results.len(),
        200,
        "All data should persist correctly"
    );
}
