use geo::Distance;
use geo::Haversine;
use geo::Point;
use spatio::{SetOptions, Spatio, TemporalPoint};
use std::time::{Duration, UNIX_EPOCH};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Spatio - Trajectory Tracking Example");
    println!("========================================");

    // Create an in-memory database
    let db = Spatio::memory()?;
    println!("Created in-memory database");

    // === VEHICLE TRAJECTORY TRACKING ===
    println!("\n--- Vehicle Trajectory Tracking ---");

    // Simulate a delivery truck route through Manhattan
    let delivery_truck_route = vec![
        TemporalPoint {
            point: Point::new(40.7128, -74.0060),
            timestamp: UNIX_EPOCH + Duration::from_secs(1640995200),
        }, // Start: Financial District
        TemporalPoint {
            point: Point::new(40.7180, -74.0020),
            timestamp: UNIX_EPOCH + Duration::from_secs(1640995260),
        }, // Move north
        TemporalPoint {
            point: Point::new(40.7230, -73.9980),
            timestamp: UNIX_EPOCH + Duration::from_secs(1640995320),
        }, // Continue north
        TemporalPoint {
            point: Point::new(40.7280, -73.9940),
            timestamp: UNIX_EPOCH + Duration::from_secs(1640995380),
        }, // Midtown approach
        TemporalPoint {
            point: Point::new(40.7330, -73.9900),
            timestamp: UNIX_EPOCH + Duration::from_secs(1640995440),
        }, // Midtown
        TemporalPoint {
            point: Point::new(40.7380, -73.9860),
            timestamp: UNIX_EPOCH + Duration::from_secs(1640995500),
        }, // Times Square area
        TemporalPoint {
            point: Point::new(40.7430, -73.9820),
            timestamp: UNIX_EPOCH + Duration::from_secs(1640995560),
        }, // Continue north
        TemporalPoint {
            point: Point::new(40.7480, -73.9780),
            timestamp: UNIX_EPOCH + Duration::from_secs(1640995620),
        }, // Central Park area
        TemporalPoint {
            point: Point::new(40.7530, -73.9740),
            timestamp: UNIX_EPOCH + Duration::from_secs(1640995680),
        }, // Upper West Side
        TemporalPoint {
            point: Point::new(40.7580, -73.9700),
            timestamp: UNIX_EPOCH + Duration::from_secs(1640995740),
        }, // End: Upper Manhattan
    ];

    db.insert_trajectory("vehicle:truck001", &delivery_truck_route, None)?;
    println!(
        "Inserted delivery truck trajectory with {} waypoints",
        delivery_truck_route.len()
    );

    // Simulate a taxi route with more frequent updates
    let taxi_route = vec![
        TemporalPoint {
            point: Point::new(40.7484, -73.9857),
            timestamp: UNIX_EPOCH + Duration::from_secs(1640995200),
        }, // Times Square
        TemporalPoint {
            point: Point::new(40.7490, -73.9850),
            timestamp: UNIX_EPOCH + Duration::from_secs(1640995210),
        }, // 10 seconds later
        TemporalPoint {
            point: Point::new(40.7496, -73.9843),
            timestamp: UNIX_EPOCH + Duration::from_secs(1640995220),
        }, // Moving northeast
        TemporalPoint {
            point: Point::new(40.7502, -73.9836),
            timestamp: UNIX_EPOCH + Duration::from_secs(1640995230),
        }, // Continuing
        TemporalPoint {
            point: Point::new(40.7508, -73.9829),
            timestamp: UNIX_EPOCH + Duration::from_secs(1640995240),
        }, // Heading to Central Park
        TemporalPoint {
            point: Point::new(40.7514, -73.9822),
            timestamp: UNIX_EPOCH + Duration::from_secs(1640995250),
        }, // Almost there
        TemporalPoint {
            point: Point::new(40.7520, -73.9815),
            timestamp: UNIX_EPOCH + Duration::from_secs(1640995260),
        }, // At Central Park South
        TemporalPoint {
            point: Point::new(40.7526, -73.9808),
            timestamp: UNIX_EPOCH + Duration::from_secs(1640995270),
        }, // Into the park area
        TemporalPoint {
            point: Point::new(40.7532, -73.9801),
            timestamp: UNIX_EPOCH + Duration::from_secs(1640995280),
        }, // Deeper into park
        TemporalPoint {
            point: Point::new(40.7538, -73.9794),
            timestamp: UNIX_EPOCH + Duration::from_secs(1640995290),
        }, // End point
    ];

    db.insert_trajectory("vehicle:taxi042", &taxi_route, None)?;
    println!(
        "Inserted taxi trajectory with {} high-frequency waypoints",
        taxi_route.len()
    );

    // === DRONE FLIGHT PATH ===
    println!("\n--- Drone Flight Path ---");

    // Simulate a drone surveillance pattern
    let drone_pattern = vec![
        TemporalPoint {
            point: Point::new(40.7589, -73.9851),
            timestamp: UNIX_EPOCH + Duration::from_secs(1640995300),
        }, // Start: Bryant Park
        TemporalPoint {
            point: Point::new(40.7600, -73.9851),
            timestamp: UNIX_EPOCH + Duration::from_secs(1640995330),
        }, // North
        TemporalPoint {
            point: Point::new(40.7600, -73.9840),
            timestamp: UNIX_EPOCH + Duration::from_secs(1640995360),
        }, // East
        TemporalPoint {
            point: Point::new(40.7589, -73.9840),
            timestamp: UNIX_EPOCH + Duration::from_secs(1640995390),
        }, // South
        TemporalPoint {
            point: Point::new(40.7589, -73.9851),
            timestamp: UNIX_EPOCH + Duration::from_secs(1640995420),
        }, // Back to start
        TemporalPoint {
            point: Point::new(40.7600, -73.9851),
            timestamp: UNIX_EPOCH + Duration::from_secs(1640995450),
        }, // Repeat pattern
        TemporalPoint {
            point: Point::new(40.7600, -73.9840),
            timestamp: UNIX_EPOCH + Duration::from_secs(1640995480),
        }, // East again
        TemporalPoint {
            point: Point::new(40.7589, -73.9840),
            timestamp: UNIX_EPOCH + Duration::from_secs(1640995510),
        }, // South again
        TemporalPoint {
            point: Point::new(40.7589, -73.9851),
            timestamp: UNIX_EPOCH + Duration::from_secs(1640995540),
        }, // Complete square
    ];

    db.insert_trajectory("drone:survey001", &drone_pattern, None)?;
    println!(
        "Inserted drone surveillance pattern with {} waypoints",
        drone_pattern.len()
    );

    // === PEDESTRIAN TRACKING ===
    println!("\n--- Pedestrian Tracking ---");

    // Simulate a jogger\'s route through Central Park
    let jogger_route = vec![
        TemporalPoint {
            point: Point::new(40.7679, -73.9781),
            timestamp: UNIX_EPOCH + Duration::from_secs(1640995600),
        }, // Enter at 72nd St
        TemporalPoint {
            point: Point::new(40.7700, -73.9770),
            timestamp: UNIX_EPOCH + Duration::from_secs(1640995660),
        }, // Move into park
        TemporalPoint {
            point: Point::new(40.7720, -73.9750),
            timestamp: UNIX_EPOCH + Duration::from_secs(1640995720),
        }, // North along path
        TemporalPoint {
            point: Point::new(40.7740, -73.9730),
            timestamp: UNIX_EPOCH + Duration::from_secs(1640995780),
        }, // Continue north
        TemporalPoint {
            point: Point::new(40.7760, -73.9710),
            timestamp: UNIX_EPOCH + Duration::from_secs(1640995840),
        }, // Reservoir area
        TemporalPoint {
            point: Point::new(40.7780, -73.9730),
            timestamp: UNIX_EPOCH + Duration::from_secs(1640995900),
        }, // Around reservoir
        TemporalPoint {
            point: Point::new(40.7800, -73.9750),
            timestamp: UNIX_EPOCH + Duration::from_secs(1640995960),
        }, // North side
        TemporalPoint {
            point: Point::new(40.7820, -73.9770),
            timestamp: UNIX_EPOCH + Duration::from_secs(1640996020),
        }, // Continue around
        TemporalPoint {
            point: Point::new(40.7800, -73.9790),
            timestamp: UNIX_EPOCH + Duration::from_secs(1640996080),
        }, // West side
        TemporalPoint {
            point: Point::new(40.7780, -73.9810),
            timestamp: UNIX_EPOCH + Duration::from_secs(1640996140),
        }, // Complete loop
    ];

    let ttl_opts = Some(SetOptions::with_ttl(Duration::from_secs(3600))); // 1 hour TTL
    db.insert_trajectory("pedestrian:jogger123", &jogger_route, ttl_opts)?;
    println!(
        "Inserted jogger trajectory with {} waypoints (1-hour TTL)",
        jogger_route.len()
    );

    // === TRAJECTORY QUERIES ===
    println!("\n--- Trajectory Queries ---");

    // Query full trajectories
    let truck_path = db.query_trajectory("vehicle:truck001", 1640995200, 1640995740)?;
    println!("Retrieved truck trajectory: {} points", truck_path.len());

    let taxi_path = db.query_trajectory("vehicle:taxi042", 1640995200, 1640995290)?;
    println!("Retrieved taxi trajectory: {} points", taxi_path.len());

    // Query partial trajectories (time windows)
    let truck_midjourney = db.query_trajectory("vehicle:truck001", 1640995320, 1640995560)?;
    println!(
        "Truck mid-journey segment: {} points",
        truck_midjourney.len()
    );

    let taxi_start = db.query_trajectory("vehicle:taxi042", 1640995200, 1640995240)?;
    println!("Taxi first 40 seconds: {} points", taxi_start.len());

    // === TRAJECTORY ANALYSIS ===
    println!("\n--- Trajectory Analysis ---");

    // Calculate trajectory distances
    println!("Calculating trajectory metrics...");

    // Truck route analysis
    let mut truck_total_distance = 0.0;
    for i in 1..delivery_truck_route.len() {
        let distance = Haversine.distance(
            delivery_truck_route[i - 1].point,
            delivery_truck_route[i].point,
        );
        truck_total_distance += distance;
    }
    let truck_duration = delivery_truck_route
        .last()
        .unwrap()
        .timestamp
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        - delivery_truck_route
            .first()
            .unwrap()
            .timestamp
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
    let truck_avg_speed = (truck_total_distance / truck_duration as f64) * 3.6; // km/h

    println!("Delivery Truck Analysis:");
    println!("  Total distance: {:.2} km", truck_total_distance / 1000.0);
    println!("  Duration: {} seconds", truck_duration);
    println!("  Average speed: {:.1} km/h", truck_avg_speed);

    // Taxi route analysis
    let mut taxi_total_distance = 0.0;
    for i in 1..taxi_route.len() {
        let distance = Haversine.distance(taxi_route[i - 1].point, taxi_route[i].point);
        taxi_total_distance += distance;
    }
    let taxi_duration = taxi_route
        .last()
        .unwrap()
        .timestamp
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        - taxi_route
            .first()
            .unwrap()
            .timestamp
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
    let taxi_avg_speed = (taxi_total_distance / taxi_duration as f64) * 3.6; // km/h

    println!("\nTaxi Analysis:");
    println!("  Total distance: {:.2} km", taxi_total_distance / 1000.0);
    println!("  Duration: {} seconds", taxi_duration);
    println!("  Average speed: {:.1} km/h", taxi_avg_speed);

    // === REAL-TIME TRACKING SIMULATION ===
    println!("\n--- Real-Time Tracking Simulation ---");

    // Simulate a bike messenger with real-time updates
    let current_time = UNIX_EPOCH + Duration::from_secs(1640996200);
    let bike_positions = [
        Point::new(40.7505, -73.9934), // Start
        Point::new(40.7510, -73.9930), // Moving
        Point::new(40.7515, -73.9926), // Moving
        Point::new(40.7520, -73.9922), // Moving
        Point::new(40.7525, -73.9918), // End
    ];

    for (i, position) in bike_positions.iter().enumerate() {
        let timestamp = current_time + Duration::from_secs(i as u64 * 30); // 30-second intervals
        let single_point_trajectory = vec![TemporalPoint {
            point: *position,
            timestamp,
        }];

        // In real-time, you would append to existing trajectory
        db.insert_trajectory(
            &format!("vehicle:bike007:segment_{}", i),
            &single_point_trajectory,
            Some(SetOptions::with_ttl(Duration::from_secs(1800))), // 30-minute TTL
        )?;
    }
    println!("Inserted real-time bike messenger updates");

    // === GEOFENCING AND ALERTS ===
    println!("\n--- Geofencing and Alerts ---");

    // Define a restricted zone (e.g., around a hospital)
    let restricted_center = Point::new(40.7614, -73.9776); // Near Central Park
    let restricted_radius = 200.0; // 200 meters

    println!("Checking trajectories for geofence violations...");
    println!(
        "Restricted zone: {:.4}°N, {:.4}°E (radius: {}m)",
        restricted_center.y(),
        restricted_center.x(),
        restricted_radius
    );

    // Check each trajectory for violations
    let trajectories = [
        ("vehicle:truck001", &delivery_truck_route),
        ("vehicle:taxi042", &taxi_route),
        ("drone:survey001", &drone_pattern),
        ("pedestrian:jogger123", &jogger_route),
    ];

    for (vehicle_id, trajectory) in &trajectories {
        let mut violations = 0;
        for temporal_point in trajectory.iter() {
            let distance = Haversine.distance(restricted_center, temporal_point.point);
            if distance <= restricted_radius {
                violations += 1;
                if violations == 1 {
                    println!(
                        "WARNING: {} entered restricted zone at timestamp {:?}",
                        vehicle_id, temporal_point.timestamp
                    );
                }
            }
        }
        if violations == 0 {
            println!("{} stayed outside restricted zone", vehicle_id);
        } else {
            println!(
                "   {} had {} geofence violations total",
                vehicle_id, violations
            );
        }
    }

    // === TRAJECTORY INTERSECTIONS ===
    println!("\n--- Trajectory Intersections ---");

    // Find where vehicles came close to each other
    println!("Analyzing trajectory intersections (within 100m)...");

    let proximity_threshold = 100.0; // meters
    let mut intersections_found = 0;

    for truck_temporal_point in &delivery_truck_route {
        for taxi_temporal_point in &taxi_route {
            let distance =
                Haversine.distance(truck_temporal_point.point, taxi_temporal_point.point);
            let time_diff = truck_temporal_point
                .timestamp
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs()
                .abs_diff(
                    taxi_temporal_point
                        .timestamp
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_secs(),
                );

            if distance <= proximity_threshold && time_diff <= 60 {
                intersections_found += 1;
                println!(
                    "   Truck and taxi within {:.0}m at times {:?} and {:?} ({}s apart)",
                    distance,
                    truck_temporal_point.timestamp,
                    taxi_temporal_point.timestamp,
                    time_diff
                );
            }
        }
    }

    if intersections_found == 0 {
        println!("   No close encounters found between truck and taxi");
    }

    // === DATABASE STATISTICS ===
    println!("\n--- Database Statistics ---");

    let stats = db.stats()?;

    println!("Total database keys: {}", stats.key_count);

    // Note: In a real application, you could track trajectory keys separately
    println!("Trajectory-related operations completed successfully");

    println!("\nTrajectory tracking example completed successfully!");
    println!("\nKey capabilities demonstrated:");
    println!("- Multi-vehicle trajectory storage and retrieval");
    println!("- Time-windowed trajectory queries");
    println!("- Real-time position updates with TTL");
    println!("- Trajectory analysis (distance, speed, duration)");
    println!("- Geofencing and violation detection");
    println!("- Trajectory intersection analysis");
    println!("- Mixed vehicle types (truck, taxi, drone, pedestrian, bike)");

    Ok(())
}
