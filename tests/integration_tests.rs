use bytes::Bytes;
use spatio::spatial::DistanceMetric;
use spatio::{Config, Point, SetOptions, Spatio, TemporalPoint};
use std::time::{Duration, UNIX_EPOCH};
use tempfile::NamedTempFile;

#[test]
fn test_basic_operations() {
    let db = Spatio::memory().unwrap();

    // Basic key-value operations
    db.insert("test_key", b"test_value", None).unwrap();
    let value = db.get("test_key").unwrap().unwrap();
    assert_eq!(value.as_ref(), b"test_value");

    // Delete operation
    let deleted = db.delete("test_key").unwrap();
    assert_eq!(deleted.unwrap().as_ref(), b"test_value");
    assert!(db.get("test_key").unwrap().is_none());
}

#[test]
fn test_ttl_operations() {
    let db = Spatio::memory().unwrap();

    // Insert with TTL
    let ttl_opts = SetOptions::with_ttl(Duration::from_millis(100));
    db.insert("ttl_key", b"ttl_value", Some(ttl_opts)).unwrap();

    // Should exist initially
    assert!(db.get("ttl_key").unwrap().is_some());

    // Wait for expiration
    std::thread::sleep(Duration::from_millis(150));

    // Should be expired now
    assert!(db.get("ttl_key").unwrap().is_none());
}

#[test]
fn test_atomic_operations() {
    let db = Spatio::memory().unwrap();

    // Atomic batch
    db.atomic(|batch| {
        batch.insert("key1", b"value1", None)?;
        batch.insert("key2", b"value2", None)?;
        batch.insert("key3", b"value3", None)?;
        Ok(())
    })
    .unwrap();

    // All keys should exist
    assert_eq!(db.get("key1").unwrap().unwrap().as_ref(), b"value1");
    assert_eq!(db.get("key2").unwrap().unwrap().as_ref(), b"value2");
    assert_eq!(db.get("key3").unwrap().unwrap().as_ref(), b"value3");
}

#[test]
fn test_spatial_operations() {
    let db = Spatio::memory().unwrap();

    // Insert points
    let nyc = Point::new(-74.0060, 40.7128);
    let london = Point::new(-0.1278, 51.5074);
    let paris = Point::new(2.3522, 48.8566);

    db.insert_point("cities", &nyc, b"New York", None).unwrap();
    db.insert_point("cities", &london, b"London", None).unwrap();
    db.insert_point("cities", &paris, b"Paris", None).unwrap();

    // Find nearby cities
    let nearby = db
        .query_within_radius("cities", &london, 1_000_000.0, 10)
        .unwrap();
    assert!(!nearby.is_empty());

    // Should find at least London itself
    let found_london = nearby.iter().any(|(_, data)| data.as_ref() == b"London");
    assert!(found_london);
}

#[test]
fn test_trajectory_operations() {
    let db = Spatio::memory().unwrap();

    // Create trajectory
    let trajectory = vec![
        TemporalPoint {
            point: Point::new(-74.0060, 40.7128),
            timestamp: UNIX_EPOCH + Duration::from_secs(1000),
        },
        TemporalPoint {
            point: Point::new(-74.0040, 40.7150),
            timestamp: UNIX_EPOCH + Duration::from_secs(1060),
        },
        TemporalPoint {
            point: Point::new(-74.0020, 40.7172),
            timestamp: UNIX_EPOCH + Duration::from_secs(1120),
        },
    ];

    db.insert_trajectory("vehicle:1", &trajectory, None)
        .unwrap();

    // Query trajectory
    let retrieved = db.query_trajectory("vehicle:1", 1000, 1120).unwrap();
    assert_eq!(retrieved.len(), 3);

    // Verify first point
    assert_eq!(retrieved[0].point, Point::new(-74.0060, 40.7128));
    assert_eq!(
        retrieved[0].timestamp,
        UNIX_EPOCH + Duration::from_secs(1000)
    );
}

#[test]
fn test_distance_calculations() {
    let db = Spatio::memory().unwrap();
    let nyc = Point::new(-74.0060, 40.7128);
    let london = Point::new(-0.1278, 51.5074);

    let distance = db
        .distance_between(&nyc, &london, DistanceMetric::Haversine)
        .unwrap();

    // Distance should be approximately 5585 km (allowing some variance)
    assert!((distance - 5_585_000.0f64).abs() < 100_000.0f64);
}

#[test]
fn test_points_sharing_geohash_are_preserved() {
    let config = Config {
        geohash_precision: 5, // coarse precision to force shared buckets
        ..Config::default()
    };

    let db = Spatio::memory_with_config(config).unwrap();

    let point_a = Point::new(-74.0060, 40.7128);
    let point_b = Point::new(-74.0062, 40.7130); // Nearby point likely in same geohash

    db.insert_point("cities", &point_a, b"A", None).unwrap();
    db.insert_point("cities", &point_b, b"B", None).unwrap();

    let results = db
        .query_within_radius("cities", &point_a, 500.0, 10)
        .unwrap();
    assert_eq!(results.len(), 2);

    let values: std::collections::HashSet<_> =
        results.into_iter().map(|(_, value)| value).collect();
    assert!(values.contains(&Bytes::from_static(b"A")));
    assert!(values.contains(&Bytes::from_static(b"B")));
}

#[test]
fn test_geohash_generation() {
    let point = Point::new(-74.0060, 40.7128);
    let geohash = geohash::encode(point.into(), 8).unwrap();

    assert_eq!(geohash.len(), 8);
    assert!(!geohash.is_empty());
}

#[test]
fn test_persistence() {
    let temp_file = NamedTempFile::new().unwrap();
    let db_path = temp_file.path();

    // Create database with data
    {
        let db = Spatio::open(db_path).unwrap();
        db.insert("persistent_key", b"persistent_value", None)
            .unwrap();

        let point = Point::new(-74.0060, 40.7128);
        db.insert_point("cities", &point, b"NYC", None).unwrap();

        // Insert trajectory data
        let trajectory = vec![
            TemporalPoint {
                point: Point::new(-74.0060, 40.7128),
                timestamp: UNIX_EPOCH + Duration::from_secs(1640995200),
            },
            TemporalPoint {
                point: Point::new(-74.0040, 40.7150),
                timestamp: UNIX_EPOCH + Duration::from_secs(1640995260),
            },
        ];
        db.insert_trajectory("vehicle:car1", &trajectory, None)
            .unwrap();

        // Force sync to ensure data is written to AOF
        db.sync().unwrap();
    }

    // Reopen database and verify data persistence via AOF replay
    {
        let db = Spatio::open(db_path).unwrap();

        // Verify basic key-value data
        let value = db.get("persistent_key").unwrap().unwrap();
        assert_eq!(value.as_ref(), b"persistent_value");

        // Verify geographic point data
        let point = Point::new(-74.0060, 40.7128);
        let nearby = db
            .query_within_radius("cities", &point, 1000.0, 10)
            .unwrap();
        assert_eq!(nearby.len(), 1);
        assert_eq!(nearby[0].1.as_ref(), b"NYC");

        // Verify trajectory data
        let trajectory_data = db
            .query_trajectory("vehicle:car1", 1640995200, 1640995260)
            .unwrap();
        assert_eq!(trajectory_data.len(), 2);
        assert_eq!(
            trajectory_data[0].timestamp,
            UNIX_EPOCH + Duration::from_secs(1640995200)
        );
        assert_eq!(
            trajectory_data[1].timestamp,
            UNIX_EPOCH + Duration::from_secs(1640995260)
        );
    }
}

#[test]
fn test_database_stats() {
    let db = Spatio::memory().unwrap();

    // Initially empty
    let stats = db.stats().unwrap();
    assert_eq!(stats.key_count, 0);

    // Add some data
    db.insert("key1", b"value1", None).unwrap();
    db.insert("key2", b"value2", None).unwrap();

    let point = Point::new(-74.0060, 40.7128);
    db.insert_point("cities", &point, b"NYC", None).unwrap();

    let stats = db.stats().unwrap();
    assert!(stats.key_count > 0);
}

#[test]
fn test_multiple_namespaces() {
    let db = Spatio::memory().unwrap();

    let nyc = Point::new(-74.0060, 40.7128);
    let london = Point::new(-0.1278, 51.5074);

    // Insert into different namespaces
    db.insert_point("cities", &nyc, b"New York", None).unwrap();
    db.insert_point("airports", &nyc, b"JFK Area", None)
        .unwrap();
    db.insert_point("cities", &london, b"London", None).unwrap();

    // Query each namespace separately
    let cities = db.query_within_radius("cities", &nyc, 1000.0, 10).unwrap();
    let airports = db
        .query_within_radius("airports", &nyc, 1000.0, 10)
        .unwrap();

    assert_eq!(cities.len(), 1); // Only NYC in cities
    assert_eq!(airports.len(), 1); // Only JFK in airports
    assert_eq!(cities[0].1.as_ref(), b"New York");
    assert_eq!(airports[0].1.as_ref(), b"JFK Area");
}

#[test]
fn test_spatial_query_methods() {
    let db = Spatio::memory().unwrap();

    // Insert test points
    let nyc = Point::new(-74.0060, 40.7128);
    let brooklyn = Point::new(-73.9442, 40.6782);
    let manhattan = Point::new(-73.9712, 40.7831);
    let london = Point::new(-0.1278, 51.5074);

    db.insert_point("cities", &nyc, b"New York", None).unwrap();
    db.insert_point("cities", &brooklyn, b"Brooklyn", None)
        .unwrap();
    db.insert_point("cities", &manhattan, b"Manhattan", None)
        .unwrap();
    db.insert_point("cities", &london, b"London", None).unwrap();

    // Test contains_point - check if there are points within radius
    let has_nearby_nyc = db.contains_point("cities", &nyc, 5000.0).unwrap();
    assert!(has_nearby_nyc); // Should find NYC itself at minimum

    let has_nearby_middle_ocean = db
        .contains_point("cities", &Point::new(30.0, -30.0), 1000.0)
        .unwrap();
    assert!(!has_nearby_middle_ocean); // Should find nothing in middle of ocean

    // Test count_within_radius
    let count_near_nyc = db.count_within_radius("cities", &nyc, 50_000.0).unwrap();
    assert!(count_near_nyc >= 3); // Should find at least NYC, Brooklyn, Manhattan

    let count_near_london = db.count_within_radius("cities", &london, 50_000.0).unwrap();
    assert_eq!(count_near_london, 1); // Should only find London

    // Test intersects_bounds - Manhattan area
    let intersects_manhattan = db
        .intersects_bounds("cities", 40.7, -74.1, 40.8, -73.9)
        .unwrap();
    assert!(intersects_manhattan); // Should find Manhattan and NYC

    // Test intersects_bounds - empty area in Pacific Ocean
    let intersects_pacific = db
        .intersects_bounds("cities", 10.0, -170.0, 20.0, -160.0)
        .unwrap();
    assert!(!intersects_pacific); // Should find nothing

    // Test find_within_bounds - NYC area
    let points_in_nyc_area = db
        .find_within_bounds("cities", 40.6, -74.1, 40.8, -73.9, 10)
        .unwrap();
    assert!(points_in_nyc_area.len() >= 3); // Should find NYC, Brooklyn, Manhattan

    // Verify the points are actually in the expected area
    for (point, _) in &points_in_nyc_area {
        assert!(point.x() >= -74.1 && point.x() <= -73.9 && point.y() >= 40.6 && point.y() <= 40.8);
    }

    // Test find_within_bounds - London area
    let points_in_london_area = db
        .find_within_bounds("cities", 51.0, -1.0, 52.0, 1.0, 10)
        .unwrap();
    assert_eq!(points_in_london_area.len(), 1); // Should only find London
    assert_eq!(points_in_london_area[0].1.as_ref(), b"London");
}

#[test]
fn test_point_spatial_methods() {
    let db = Spatio::memory().unwrap();

    let nyc = Point::new(-74.0060, 40.7128);
    let brooklyn = Point::new(-73.9442, 40.6782);
    let london = Point::new(-0.1278, 51.5074);

    // Insert test cities
    db.insert_point("cities", &nyc, b"NYC", None).unwrap();
    db.insert_point("cities", &brooklyn, b"Brooklyn", None)
        .unwrap();
    db.insert_point("cities", &london, b"London", None).unwrap();

    // Test distance_between using our API
    let dist_brooklyn_nyc = db
        .distance_between(&brooklyn, &nyc, DistanceMetric::Haversine)
        .unwrap();
    let dist_london_nyc = db
        .distance_between(&london, &nyc, DistanceMetric::Haversine)
        .unwrap();

    assert!(dist_brooklyn_nyc <= 20_000.0); // Brooklyn is close to NYC
    assert!(dist_london_nyc > 1_000_000.0); // London is far from NYC

    // Test contains_point using our API
    assert!(db.contains_point("cities", &nyc, 20_000.0).unwrap()); // Brooklyn is within 20km
    assert!(db.contains_point("cities", &nyc, 100.0).unwrap()); // NYC itself is within 100m

    // Test empty location
    let empty_location = Point::new(0.0, 0.0);
    assert!(
        !db.contains_point("cities", &empty_location, 1000.0)
            .unwrap()
    ); // No cities at 0,0

    // Test count_within_radius using our API
    let count_near_nyc = db.count_within_radius("cities", &nyc, 50_000.0).unwrap();
    assert_eq!(count_near_nyc, 2); // NYC and Brooklyn

    let count_near_london = db.count_within_radius("cities", &london, 50_000.0).unwrap();
    assert_eq!(count_near_london, 1); // Only London

    // Test intersects_bounds using our API
    assert!(
        db.intersects_bounds("cities", 40.0, -75.0, 41.0, -73.0)
            .unwrap()
    ); // NYC area has points
    assert!(
        db.intersects_bounds("cities", 51.0, -1.0, 52.0, 1.0)
            .unwrap()
    ); // London area has points
    assert!(!db.intersects_bounds("cities", 0.0, 0.0, 1.0, 1.0).unwrap()); // Empty area

    // Test find_within_bounds using our API
    let nyc_area = db
        .find_within_bounds("cities", 40.0, -75.0, 41.0, -73.0, 10)
        .unwrap();
    assert_eq!(nyc_area.len(), 2); // NYC and Brooklyn

    let london_area = db
        .find_within_bounds("cities", 51.0, -1.0, 52.0, 1.0, 10)
        .unwrap();
    assert_eq!(london_area.len(), 1); // Only London
}

#[test]
fn test_geohash_precision_configuration() {
    // Test different geohash configurations
    let custom_config = Config::with_geohash_precision(10);
    let default_config = Config::default();

    // Verify configurations have expected values
    assert_eq!(custom_config.geohash_precision, 10);
    assert_eq!(default_config.geohash_precision, 8);

    // Create databases with different configurations
    let custom_db = Spatio::memory_with_config(custom_config).unwrap();
    let default_db = Spatio::memory_with_config(default_config).unwrap();

    // Test that both configurations work with spatial operations
    let point = Point::new(-74.0060, 40.7128);
    let data = b"New York City";

    // Insert points into both databases
    custom_db
        .insert_point("cities", &point, data, None)
        .unwrap();
    default_db
        .insert_point("cities", &point, data, None)
        .unwrap();

    // Test spatial queries work with both configurations
    let custom_nearby = custom_db
        .query_within_radius("cities", &point, 1000.0, 10)
        .unwrap();
    let default_nearby = default_db
        .query_within_radius("cities", &point, 1000.0, 10)
        .unwrap();

    // Both should find the inserted point
    assert_eq!(custom_nearby.len(), 1);
    assert_eq!(default_nearby.len(), 1);

    // Test contains_point queries work with both configurations
    assert!(custom_db.contains_point("cities", &point, 100.0).unwrap());
    assert!(default_db.contains_point("cities", &point, 100.0).unwrap());
}
