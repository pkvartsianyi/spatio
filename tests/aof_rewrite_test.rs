use spatio::prelude::*;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;
use tempfile::TempDir;

#[test]
fn test_aof_rewrite_with_concurrent_operations() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.aof");

    // Create database with default config (AOF enabled for file-based DB)
    let db = Spatio::open(&db_path).unwrap();
    let db = Arc::new(db);

    // Write initial data to exceed rewrite threshold
    for i in 0..50 {
        let lat = 40.0 + (i as f64) * 0.01; // Valid latitude range around NYC
        let lon = -74.0 + (i as f64) * 0.01; // Valid longitude range
        let point = Point::new(lat, lon);
        db.insert_point("points", &point, format!("point_{}", i).as_bytes(), None)
            .unwrap();
    }

    let initial_size = std::fs::metadata(&db_path).unwrap().len();

    // Create barrier for coordinating threads
    let barrier = Arc::new(Barrier::new(3));
    let mut handles = vec![];

    // Thread 1: Write data that could trigger rewrite
    {
        let db = Arc::clone(&db);
        let barrier = Arc::clone(&barrier);
        let handle = thread::spawn(move || {
            barrier.wait();

            // Write more data
            for i in 50..100 {
                let lat = 40.0 + (i as f64) * 0.01;
                let lon = -74.0 + (i as f64) * 0.01;
                let point = Point::new(lat, lon);
                db.insert_point(
                    "points",
                    &point,
                    format!("rewrite_point_{}", i).as_bytes(),
                    None,
                )
                .unwrap();
            }
        });
        handles.push(handle);
    }

    // Thread 2: Continue writing during potential rewrite
    {
        let db = Arc::clone(&db);
        let barrier = Arc::clone(&barrier);
        let handle = thread::spawn(move || {
            barrier.wait();

            // Wait a bit then start writing
            thread::sleep(Duration::from_millis(10));

            for i in 100..150 {
                let lat = 40.0 + (i as f64) * 0.01;
                let lon = -74.0 + (i as f64) * 0.01;
                let point = Point::new(lat, lon);
                db.insert_point(
                    "points",
                    &point,
                    format!("concurrent_point_{}", i).as_bytes(),
                    None,
                )
                .unwrap();
            }
        });
        handles.push(handle);
    }

    // Thread 3: Read operations during writes
    {
        let db = Arc::clone(&db);
        let barrier = Arc::clone(&barrier);
        let handle = thread::spawn(move || {
            barrier.wait();

            // Perform reads while other threads are writing
            for _i in 0..10 {
                thread::sleep(Duration::from_millis(5));

                // Try to read some nearby points
                let center = Point::new(40.5, -73.5); // Center of our coordinate range
                let nearby = db.find_nearby("points", &center, 10000.0, 10);
                if let Ok(results) = nearby {
                    // Verify that we can read the results without errors
                    for (_point, data) in results {
                        assert!(!data.is_empty());
                    }
                }
            }
        });
        handles.push(handle);
    }

    // Wait for all threads to complete
    for handle in handles {
        handle.join().unwrap();
    }

    // Verify data integrity after concurrent operations
    let final_size = std::fs::metadata(&db_path).unwrap().len();
    assert!(final_size > initial_size, "AOF file should have grown");

    // Test recovery by reopening the database
    drop(db);

    let recovered_db = Spatio::open(&db_path).unwrap();

    // Verify we can find points near different locations
    let center = Point::new(40.5, -73.5);
    let nearby = recovered_db
        .find_nearby("points", &center, 10000.0, 100)
        .unwrap();
    assert!(!nearby.is_empty(), "Should find points after recovery");

    // Verify we can find points in different regions
    let results = recovered_db
        .find_within_bounds("points", 39.0, -75.0, 42.0, -72.0, 1000)
        .unwrap();
    assert!(
        results.len() >= 150,
        "Should find at least 150 points after recovery"
    );
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

    // Verify recovery worked correctly - search across all prefixes with generous bounds
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
