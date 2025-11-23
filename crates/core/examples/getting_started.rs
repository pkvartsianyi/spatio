use spatio::{Point3d, Spatio, TemporalPoint};
use spatio_types::geo::Point;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging (set RUST_LOG=debug to see detailed logs)
    env_logger::init();

    println!("=== Spatio - Getting Started ===\n");

    // Create an in-memory database
    let db = Spatio::memory()?;
    println!("✓ Created in-memory database\n");

    // === SPATIAL OPERATIONS ===
    println!("1. Spatial Point Storage (Hot State)");
    println!("------------------------------------");

    // Store geographic points (lon, lat, alt)
    let nyc = Point3d::new(-74.0060, 40.7128, 0.0);
    let london = Point3d::new(-0.1278, 51.5074, 0.0);
    let paris = Point3d::new(2.3522, 48.8566, 0.0);

    // Update current locations
    db.update_location("cities", "nyc", nyc.clone(), serde_json::json!({"data": "New York"}))?;
    db.update_location("cities", "london", london.clone(), serde_json::json!({"data": "London"}))?;
    db.update_location("cities", "paris", paris.clone(), serde_json::json!({"data": "Paris"}))?;
    println!("   Stored 3 cities with automatic spatial indexing");

    // Find nearby cities within radius (in meters)
    let nearby = db.query_current_within_radius("cities", &london, 500_000.0, 10)?;
    println!("   Found {} cities within 500km of London:", nearby.len());
    for loc in &nearby {
        println!("     - {}", loc.metadata.to_string());
    }
    println!();

    // === TRAJECTORY TRACKING ===
    println!("2. Trajectory Tracking (Cold State)");
    println!("-----------------------------------");

    let vehicle_path = vec![
        TemporalPoint {
            point: Point::new(-74.0060, 40.7128),
            timestamp: UNIX_EPOCH + Duration::from_secs(1640995200),
        },
        TemporalPoint {
            point: Point::new(-74.0040, 40.7150),
            timestamp: UNIX_EPOCH + Duration::from_secs(1640995260),
        },
        TemporalPoint {
            point: Point::new(-74.0020, 40.7172),
            timestamp: UNIX_EPOCH + Duration::from_secs(1640995320),
        },
    ];

    db.insert_trajectory("logistics", "vehicle:truck001", &vehicle_path)?;
    println!(
        "   Stored vehicle trajectory with {} waypoints",
        vehicle_path.len()
    );

    // Query trajectory for time range
    let path_segment = db.query_trajectory(
        "logistics",
        "vehicle:truck001",
        UNIX_EPOCH + Duration::from_secs(1640995200),
        UNIX_EPOCH + Duration::from_secs(1640995320),
        100,
    )?;
    println!(
        "   Retrieved {} waypoints from trajectory\n",
        path_segment.len()
    );

    // === HISTORICAL INGESTION ===
    println!("3. Historical Data Ingestion");
    println!("----------------------------");

    // Insert a point with a specific timestamp in the past
    let past_time = SystemTime::now() - Duration::from_secs(3600);
    let past_pos = Point3d::new(10.0, 10.0, 0.0);

    db.update_location_at(
        "fleet",
        "old_truck",
        past_pos,
        serde_json::json!({"data": "Historical Data"}),
        past_time,
    )?;
    println!("   Ingested historical data point\n");

    // === DATABASE STATS ===
    println!("4. Database Statistics");
    println!("----------------------");

    let stats = db.stats();
    // Note: Stats implementation is currently minimal in new architecture
    println!("   Stats available: {:?}", stats);

    println!("=== Getting Started Complete! ===");
    println!("\nKey Features Demonstrated:");
    println!("  • Real-time location updates (Hot State)");
    println!("  • Spatial radius queries");
    println!("  • Trajectory tracking (Cold State)");
    println!("  • Historical data ingestion");

    Ok(())
}
