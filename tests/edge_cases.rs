use spatio::{Config, Point, Spatio};
use std::thread;
use std::time::Duration;

/// Test 1: Large dataset stress test
#[test]
fn test_large_dataset_insertion() {
    let mut db = Spatio::memory().expect("Failed to create database");

    // Insert 10K points (keeping it reasonable for CI)
    for i in 0..10_000 {
        let lat = 40.0 + (i as f64 * 0.00001);
        let lon = -74.0 + (i as f64 * 0.00001);
        let point = Point::new(lon, lat);
        db.insert_point("stress", &point, format!("data{}", i).as_bytes(), None)
            .unwrap_or_else(|_| panic!("Failed to insert point {}", i));
    }

    // Query should still be fast
    let center = Point::new(-74.0, 40.0);
    let results = db
        .query_within_radius("stress", &center, 1000.0, 100)
        .expect("Query failed");

    assert!(!results.is_empty());
}

/// Test 2: Concurrent write contention
/// DISABLED: DB is now !Send + !Sync by design. Use SyncDB wrapper or actor pattern for concurrency.
#[test]
#[ignore]
fn test_concurrent_write_contention() {
    // This test is incompatible with the new single-threaded DB design.
    // For multi-threaded usage, use:
    // 1. SyncDB wrapper (with 'sync' feature)
    // 2. Actor pattern (recommended for async usage)
    // 3. Manual Arc<RwLock<DB>> wrapper
}

/// Test 3: Extreme coordinate values
#[test]
fn test_extreme_coordinates() {
    let mut db = Spatio::memory().expect("Failed to create database");

    // Valid edge cases
    let north_pole = Point::new(0.0, 90.0);
    let south_pole = Point::new(0.0, -90.0);
    let date_line_west = Point::new(180.0, 0.0);
    let date_line_east = Point::new(-180.0, 0.0);

    db.insert_point("poles", &north_pole, b"North Pole", None)
        .expect("Failed to insert north pole");
    db.insert_point("poles", &south_pole, b"South Pole", None)
        .expect("Failed to insert south pole");
    db.insert_point("poles", &date_line_west, b"Date Line West", None)
        .expect("Failed to insert date line west");
    db.insert_point("poles", &date_line_east, b"Date Line East", None)
        .expect("Failed to insert date line east");

    // Should handle these without panic
    let results = db
        .query_within_radius("poles", &north_pole, 1000.0, 10)
        .expect("Query failed");
    assert!(!results.is_empty());
}

/// Test 4: Very long keys
#[test]
fn test_very_long_keys() {
    let mut db = Spatio::memory().expect("Failed to create database");

    // 1KB key
    let long_key = "x".repeat(1_000);
    db.insert(&long_key, b"value", None)
        .expect("Should handle long keys");

    let value = db
        .get(&long_key)
        .expect("Get failed")
        .expect("Key not found");
    assert_eq!(value.as_ref(), b"value");

    // Very long value too
    let long_value = vec![0u8; 10_000];
    db.insert("normal_key", &long_value, None)
        .expect("Should handle long values");

    let retrieved = db
        .get("normal_key")
        .expect("Get failed")
        .expect("Value not found");
    assert_eq!(retrieved.len(), 10_000);
}

/// Test 5: Empty queries
#[test]
fn test_empty_namespace_queries() {
    let db = Spatio::memory().expect("Failed to create database");

    // Query namespace that doesn't exist
    let results = db
        .query_within_radius("nonexistent", &Point::new(0.0, 0.0), 1000.0, 10)
        .expect("Query should not fail");

    assert!(results.is_empty());

    // Count should also return 0
    let count = db
        .count_within_radius("nonexistent", &Point::new(0.0, 0.0), 1000.0)
        .expect("Count should not fail");
    assert_eq!(count, 0);
}

/// Test 6: Binary keys with special characters
#[test]
fn test_binary_keys_with_special_chars() {
    let mut db = Spatio::memory().expect("Failed to create database");

    // Keys with various special bytes
    let keys = [
        b"key\x00with\x00nulls".to_vec(),
        b"\xFF\xFE\xFD\xFC".to_vec(),
        b"emoji_\xF0\x9F\x98\x80".to_vec(),
        b"\t\n\r".to_vec(),
    ];

    for (i, key) in keys.iter().enumerate() {
        let value = format!("value{}", i);
        db.insert(key, value.as_bytes(), None)
            .expect("Should handle binary keys");

        let retrieved = db.get(key).expect("Get failed").expect("Key not found");
        assert_eq!(retrieved.as_ref(), value.as_bytes());
    }
}

/// Test 7: Massive TTL cleanup
#[test]
fn test_massive_ttl_cleanup() {
    let mut db = Spatio::memory().expect("Failed to create database");

    // Insert many keys with very short TTL
    let opts = spatio::SetOptions::with_ttl(Duration::from_millis(50));
    for i in 0..1000 {
        let key = format!("ttl_key_{}", i);
        db.insert(&key, b"expires_soon", Some(opts.clone()))
            .expect("Insert failed");
    }

    // Wait for expiration
    thread::sleep(Duration::from_millis(100));

    // Cleanup should handle all of them
    let removed = db.cleanup_expired().expect("Cleanup failed");
    assert_eq!(removed, 1000);
}

/// Test 8: Spatial queries at edge boundaries
#[test]
fn test_spatial_queries_at_boundaries() {
    let mut db = Spatio::memory().expect("Failed to create database");

    // Insert points along the equator
    for lon in -180..180 {
        let point = Point::new(lon as f64, 0.0);
        db.insert_point("equator", &point, format!("lon{}", lon).as_bytes(), None)
            .expect("Insert failed");
    }

    // Query at the date line should work
    let date_line = Point::new(180.0, 0.0);
    let results = db
        .query_within_radius("equator", &date_line, 100_000.0, 50)
        .expect("Query failed");
    assert!(!results.is_empty());

    // Query at prime meridian
    let prime = Point::new(0.0, 0.0);
    let results = db
        .query_within_radius("equator", &prime, 100_000.0, 50)
        .expect("Query failed");
    assert!(!results.is_empty());
}

/// Test 9: Database reopen consistency
#[test]
#[cfg(feature = "aof")]
fn test_database_reopen_consistency() {
    use tempfile::NamedTempFile;

    let temp_file = NamedTempFile::new().expect("Failed to create temp file");
    let path = temp_file.path();

    // First session: write data
    {
        let mut db = Spatio::open(path).expect("Failed to open database");
        db.insert("persistent_key", b"persistent_value", None)
            .expect("Insert failed");

        let point = Point::new(-74.0, 40.7);
        db.insert_point("cities", &point, b"NYC", None)
            .expect("Insert point failed");

        db.sync().expect("Sync failed");
    }

    // Second session: verify data persists
    {
        let db = Spatio::open(path).expect("Failed to reopen database");

        let value = db
            .get("persistent_key")
            .expect("Get failed")
            .expect("Key not found");
        assert_eq!(value.as_ref(), b"persistent_value");

        let results = db
            .query_within_radius("cities", &Point::new(-74.0, 40.7), 100.0, 10)
            .expect("Query failed");
        assert!(!results.is_empty());
    }
}

/// Test 10: Concurrent reads while writing
/// DISABLED: DB is now !Send + !Sync by design. Use SyncDB wrapper or actor pattern for concurrency.
#[test]
#[ignore]
fn test_concurrent_reads_during_writes() {
    // This test is incompatible with the new single-threaded DB design.
    // For multi-threaded usage, use:
    // 1. SyncDB wrapper (with 'sync' feature)
    // 2. Actor pattern (recommended for async usage)
    // 3. Manual Arc<RwLock<DB>> wrapper
}

/// Test 11: Zero-radius spatial query
#[test]
fn test_zero_radius_spatial_query() {
    let mut db = Spatio::memory().expect("Failed to create database");

    let point = Point::new(-74.0, 40.7);
    db.insert_point("test", &point, b"data", None)
        .expect("Insert failed");

    // Query with zero radius should return only exact matches (or nothing)
    let results = db
        .query_within_radius("test", &point, 0.0, 10)
        .expect("Query failed");

    // Should either be empty or contain only the exact point
    assert!(results.len() <= 1);
}

/// Test 12: Very large radius spatial query
#[test]
fn test_very_large_radius_query() {
    let mut db = Spatio::memory().expect("Failed to create database");

    // Insert points around the world
    let points = [
        Point::new(-74.0, 40.7),  // NYC
        Point::new(0.0, 51.5),    // London
        Point::new(139.7, 35.7),  // Tokyo
        Point::new(-122.4, 37.8), // SF
    ];

    for (i, point) in points.iter().enumerate() {
        db.insert_point("world", point, format!("city{}", i).as_bytes(), None)
            .expect("Insert failed");
    }

    // Query with radius that covers entire earth (40,000 km)
    let center = Point::new(0.0, 0.0);
    let results = db
        .query_within_radius("world", &center, 40_000_000.0, 100)
        .expect("Query failed");

    // Should find all points
    assert_eq!(results.len(), 4);
}

/// Test 13: Atomic batch with many operations
#[test]
fn test_large_atomic_batch() {
    let mut db = Spatio::memory().expect("Failed to create database");

    // Large atomic batch
    db.atomic(|batch| {
        for i in 0..1000 {
            batch.insert(format!("batch_key_{}", i), b"value", None)?;
        }
        Ok(())
    })
    .expect("Batch failed");

    // Verify all keys exist
    for i in 0..1000 {
        assert!(
            db.get(format!("batch_key_{}", i))
                .expect("Get failed")
                .is_some()
        );
    }
}

/// Test 14: Delete non-existent keys
#[test]
fn test_delete_non_existent_keys() {
    let mut db = Spatio::memory().expect("Failed to create database");

    // Deleting non-existent key should return None, not error
    let result = db
        .delete("does_not_exist")
        .expect("Delete should not error");
    assert!(result.is_none());

    // Multiple deletes of same key
    db.insert("temp", b"value", None).expect("Insert failed");
    let first_delete = db.delete("temp").expect("Delete failed");
    assert!(first_delete.is_some());

    let second_delete = db.delete("temp").expect("Second delete failed");
    assert!(second_delete.is_none());
}

/// Test 15: Config validation edge cases
#[test]
fn test_config_edge_cases() {
    // Very high precision should work
    let config = Config::with_geohash_precision(12);
    let mut db = Spatio::memory_with_config(config).expect("Failed to create db");
    db.insert("test", b"value", None).expect("Insert failed");

    // Very low precision should also work
    let config = Config::with_geohash_precision(1);
    let mut db = Spatio::memory_with_config(config).expect("Failed to create db");
    db.insert("test", b"value", None).expect("Insert failed");
}
