use spatio::{Point, Spatio, TemporalPoint};
use std::time::{Duration, UNIX_EPOCH};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Spatio - Trajectory Tracking ===\n");

    let db = Spatio::memory()?;
    println!("✓ Created in-memory database\n");

    // === 1. BASIC TRAJECTORY STORAGE ===
    println!("1. Basic Trajectory Storage");
    println!("---------------------------");

    // Create a simple delivery route
    let delivery_route = vec![
        TemporalPoint {
            point: Point::new(-74.0060, 40.7128), // NYC
            timestamp: UNIX_EPOCH + Duration::from_secs(1640995200),
        },
        TemporalPoint {
            point: Point::new(-74.0040, 40.7150),
            timestamp: UNIX_EPOCH + Duration::from_secs(1640995260), // +1 min
        },
        TemporalPoint {
            point: Point::new(-74.0020, 40.7172),
            timestamp: UNIX_EPOCH + Duration::from_secs(1640995320), // +2 min
        },
        TemporalPoint {
            point: Point::new(-74.0000, 40.7194),
            timestamp: UNIX_EPOCH + Duration::from_secs(1640995380), // +3 min
        },
    ];

    db.insert_trajectory("vehicle:truck001", &delivery_route, None)?;
    println!(
        "   Stored delivery truck trajectory with {} waypoints\n",
        delivery_route.len()
    );

    // === 2. QUERY FULL TRAJECTORY ===
    println!("2. Query Full Trajectory");
    println!("------------------------");

    let full_path = db.query_trajectory("vehicle:truck001", 1640995200, 1640995400)?;
    println!("   Retrieved {} waypoints:", full_path.len());
    for (i, tp) in full_path.iter().enumerate() {
        println!(
            "     {}. Point ({:.4}°, {:.4}°) at timestamp {}",
            i + 1,
            tp.point.x(),
            tp.point.y(),
            tp.timestamp.duration_since(UNIX_EPOCH).unwrap().as_secs()
        );
    }
    println!();

    // === 3. QUERY TIME RANGE ===
    println!("3. Query Specific Time Range");
    println!("----------------------------");

    // Get only first 2 minutes of trajectory
    let partial_path = db.query_trajectory("vehicle:truck001", 1640995200, 1640995320)?;
    println!("   First 2 minutes: {} waypoints", partial_path.len());

    // Get only middle segment
    let middle_segment = db.query_trajectory("vehicle:truck001", 1640995260, 1640995320)?;
    println!("   Middle segment: {} waypoints\n", middle_segment.len());

    // === 4. MULTIPLE TRAJECTORIES ===
    println!("4. Multiple Trajectories");
    println!("------------------------");

    // Add taxi route
    let taxi_route = vec![
        TemporalPoint {
            point: Point::new(-0.1278, 51.5074), // London
            timestamp: UNIX_EPOCH + Duration::from_secs(1640995200),
        },
        TemporalPoint {
            point: Point::new(-0.1195, 51.5033),
            timestamp: UNIX_EPOCH + Duration::from_secs(1640995260),
        },
        TemporalPoint {
            point: Point::new(-0.1245, 51.4994),
            timestamp: UNIX_EPOCH + Duration::from_secs(1640995320),
        },
    ];

    db.insert_trajectory("vehicle:taxi042", &taxi_route, None)?;
    println!(
        "   Stored taxi trajectory with {} waypoints",
        taxi_route.len()
    );

    // Add bus route
    let bus_route = vec![
        TemporalPoint {
            point: Point::new(2.3522, 48.8566), // Paris
            timestamp: UNIX_EPOCH + Duration::from_secs(1640995200),
        },
        TemporalPoint {
            point: Point::new(2.3550, 48.8580),
            timestamp: UNIX_EPOCH + Duration::from_secs(1640995300),
        },
        TemporalPoint {
            point: Point::new(2.3580, 48.8600),
            timestamp: UNIX_EPOCH + Duration::from_secs(1640995400),
        },
        TemporalPoint {
            point: Point::new(2.3610, 48.8620),
            timestamp: UNIX_EPOCH + Duration::from_secs(1640995500),
        },
        TemporalPoint {
            point: Point::new(2.3640, 48.8640),
            timestamp: UNIX_EPOCH + Duration::from_secs(1640995600),
        },
    ];

    db.insert_trajectory("vehicle:bus123", &bus_route, None)?;
    println!(
        "   Stored bus trajectory with {} waypoints\n",
        bus_route.len()
    );

    // === 5. QUERY DIFFERENT VEHICLES ===
    println!("5. Query Different Vehicles");
    println!("---------------------------");

    let truck_path = db.query_trajectory("vehicle:truck001", 1640995200, 1640995400)?;
    let taxi_path = db.query_trajectory("vehicle:taxi042", 1640995200, 1640995400)?;
    let bus_path = db.query_trajectory("vehicle:bus123", 1640995200, 1640995700)?;

    println!("   Truck waypoints: {}", truck_path.len());
    println!("   Taxi waypoints: {}", taxi_path.len());
    println!("   Bus waypoints: {}\n", bus_path.len());

    // === 6. LONG-RUNNING TRAJECTORY ===
    println!("6. Long-Running Trajectory (High Frequency)");
    println!("-------------------------------------------");

    // Simulate a drone with frequent position updates
    let mut drone_path = Vec::new();
    let start_time = 1641000000;

    for i in 0..60 {
        // 60 waypoints, 1 per second
        drone_path.push(TemporalPoint {
            point: Point::new(
                -122.4194 + (i as f64 * 0.0001), // San Francisco area
                37.7749 + (i as f64 * 0.0001),
            ),
            timestamp: UNIX_EPOCH + Duration::from_secs(start_time + i),
        });
    }

    db.insert_trajectory("drone:delivery001", &drone_path, None)?;
    println!(
        "   Stored drone trajectory with {} waypoints",
        drone_path.len()
    );

    // Query specific time window (10 seconds)
    let window = db.query_trajectory("drone:delivery001", start_time, start_time + 10)?;
    println!(
        "   Retrieved 10-second window: {} waypoints\n",
        window.len()
    );

    // === 7. TRAJECTORY UPDATES ===
    println!("7. Trajectory Updates");
    println!("---------------------");

    // Add more waypoints to existing trajectory
    let extended_route = vec![
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
        TemporalPoint {
            point: Point::new(-74.0000, 40.7194),
            timestamp: UNIX_EPOCH + Duration::from_secs(1640995380),
        },
        // New waypoints
        TemporalPoint {
            point: Point::new(-73.9980, 40.7216),
            timestamp: UNIX_EPOCH + Duration::from_secs(1640995440),
        },
        TemporalPoint {
            point: Point::new(-73.9960, 40.7238),
            timestamp: UNIX_EPOCH + Duration::from_secs(1640995500),
        },
    ];

    db.insert_trajectory("vehicle:truck001", &extended_route, None)?;
    let updated_path = db.query_trajectory("vehicle:truck001", 1640995200, 1640995600)?;
    println!(
        "   Extended truck trajectory from 4 to {} waypoints\n",
        updated_path.len()
    );

    // === 8. DATABASE STATISTICS ===
    println!("8. Database Statistics");
    println!("----------------------");

    let stats = db.stats()?;
    println!("   Total keys in database: {}", stats.key_count);
    println!("   Total operations: {}\n", stats.operations_count);

    // === 9. USE CASES ===
    println!("=== Common Use Cases ===\n");

    println!("Fleet Management:");
    println!("  • Track multiple vehicles in real-time");
    println!("  • Query historical routes for analysis");
    println!("  • Retrieve specific time windows for incidents\n");

    println!("Delivery Tracking:");
    println!("  • Store complete delivery routes");
    println!("  • Query progress during specific periods");
    println!("  • Analyze route efficiency\n");

    println!("Drone Operations:");
    println!("  • High-frequency position updates");
    println!("  • Flight path analysis");
    println!("  • Time-based route queries\n");

    println!("Asset Tracking:");
    println!("  • Monitor movement of valuable items");
    println!("  • Historical location queries");
    println!("  • Route verification\n");

    println!("=== Trajectory Tracking Complete! ===");
    println!("\nKey Features Demonstrated:");
    println!("  • Store trajectories with timestamps");
    println!("  • Query full trajectories");
    println!("  • Query specific time ranges");
    println!("  • Track multiple vehicles/objects");
    println!("  • High-frequency updates");
    println!("  • Trajectory extensions/updates");

    Ok(())
}
