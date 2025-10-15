use spatio::{Point, SetOptions, Spatio};
use std::time::Duration;

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
        (Point::new(40.7128, -74.0060), 1640995200), // Start: Financial District
        (Point::new(40.7180, -74.0020), 1640995260), // Move north
        (Point::new(40.7230, -73.9980), 1640995320), // Continue north
        (Point::new(40.7280, -73.9940), 1640995380), // Midtown approach
        (Point::new(40.7330, -73.9900), 1640995440), // Midtown
        (Point::new(40.7380, -73.9860), 1640995500), // Times Square area
        (Point::new(40.7430, -73.9820), 1640995560), // Continue north
        (Point::new(40.7480, -73.9780), 1640995620), // Central Park area
        (Point::new(40.7530, -73.9740), 1640995680), // Upper West Side
        (Point::new(40.7580, -73.9700), 1640995740), // End: Upper Manhattan
    ];

    db.insert_trajectory("vehicle:truck001", &delivery_truck_route, None)?;
    println!(
        "Inserted delivery truck trajectory with {} waypoints",
        delivery_truck_route.len()
    );

    // Simulate a taxi route with more frequent updates
    let taxi_route = vec![
        (Point::new(40.7484, -73.9857), 1640995200), // Times Square
        (Point::new(40.7490, -73.9850), 1640995210), // 10 seconds later
        (Point::new(40.7496, -73.9843), 1640995220), // Moving northeast
        (Point::new(40.7502, -73.9836), 1640995230), // Continuing
        (Point::new(40.7508, -73.9829), 1640995240), // Heading to Central Park
        (Point::new(40.7514, -73.9822), 1640995250), // Almost there
        (Point::new(40.7520, -73.9815), 1640995260), // At Central Park South
        (Point::new(40.7526, -73.9808), 1640995270), // Into the park area
        (Point::new(40.7532, -73.9801), 1640995280), // Deeper into park
        (Point::new(40.7538, -73.9794), 1640995290), // End point
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
        (Point::new(40.7589, -73.9851), 1640995300), // Start: Bryant Park
        (Point::new(40.7600, -73.9851), 1640995330), // North
        (Point::new(40.7600, -73.9840), 1640995360), // East
        (Point::new(40.7589, -73.9840), 1640995390), // South
        (Point::new(40.7589, -73.9851), 1640995420), // Back to start
        (Point::new(40.7600, -73.9851), 1640995450), // Repeat pattern
        (Point::new(40.7600, -73.9840), 1640995480), // East again
        (Point::new(40.7589, -73.9840), 1640995510), // South again
        (Point::new(40.7589, -73.9851), 1640995540), // Complete square
    ];

    db.insert_trajectory("drone:survey001", &drone_pattern, None)?;
    println!(
        "Inserted drone surveillance pattern with {} waypoints",
        drone_pattern.len()
    );

    // === PEDESTRIAN TRACKING ===
    println!("\n--- Pedestrian Tracking ---");

    // Simulate a jogger's route through Central Park
    let jogger_route = vec![
        (Point::new(40.7679, -73.9781), 1640995600), // Enter at 72nd St
        (Point::new(40.7700, -73.9770), 1640995660), // Move into park
        (Point::new(40.7720, -73.9750), 1640995720), // North along path
        (Point::new(40.7740, -73.9730), 1640995780), // Continue north
        (Point::new(40.7760, -73.9710), 1640995840), // Reservoir area
        (Point::new(40.7780, -73.9730), 1640995900), // Around reservoir
        (Point::new(40.7800, -73.9750), 1640995960), // North side
        (Point::new(40.7820, -73.9770), 1640996020), // Continue around
        (Point::new(40.7800, -73.9790), 1640996080), // West side
        (Point::new(40.7780, -73.9810), 1640996140), // Complete loop
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
        let distance = delivery_truck_route[i - 1]
            .0
            .distance_to(&delivery_truck_route[i].0);
        truck_total_distance += distance;
    }
    let truck_duration =
        delivery_truck_route.last().unwrap().1 - delivery_truck_route.first().unwrap().1;
    let truck_avg_speed = (truck_total_distance / truck_duration as f64) * 3.6; // km/h

    println!("Delivery Truck Analysis:");
    println!("  Total distance: {:.2} km", truck_total_distance / 1000.0);
    println!("  Duration: {} seconds", truck_duration);
    println!("  Average speed: {:.1} km/h", truck_avg_speed);

    // Taxi route analysis
    let mut taxi_total_distance = 0.0;
    for i in 1..taxi_route.len() {
        let distance = taxi_route[i - 1].0.distance_to(&taxi_route[i].0);
        taxi_total_distance += distance;
    }
    let taxi_duration = taxi_route.last().unwrap().1 - taxi_route.first().unwrap().1;
    let taxi_avg_speed = (taxi_total_distance / taxi_duration as f64) * 3.6; // km/h

    println!("\nTaxi Analysis:");
    println!("  Total distance: {:.2} km", taxi_total_distance / 1000.0);
    println!("  Duration: {} seconds", taxi_duration);
    println!("  Average speed: {:.1} km/h", taxi_avg_speed);

    // === REAL-TIME TRACKING SIMULATION ===
    println!("\n--- Real-Time Tracking Simulation ---");

    // Simulate a bike messenger with real-time updates
    let current_time = 1640996200u64;
    let bike_positions = [
        Point::new(40.7505, -73.9934), // Start
        Point::new(40.7510, -73.9930), // Moving
        Point::new(40.7515, -73.9926), // Moving
        Point::new(40.7520, -73.9922), // Moving
        Point::new(40.7525, -73.9918), // End
    ];

    for (i, position) in bike_positions.iter().enumerate() {
        let timestamp = current_time + (i as u64 * 30); // 30-second intervals
        let single_point_trajectory = vec![(*position, timestamp)];

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
        restricted_center.lat, restricted_center.lon, restricted_radius
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
        for (point, timestamp) in trajectory.iter() {
            let distance = point.distance_to(&restricted_center);
            if distance <= restricted_radius {
                violations += 1;
                if violations == 1 {
                    println!(
                        "WARNING: {} entered restricted zone at timestamp {}",
                        vehicle_id, timestamp
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

    for (truck_point, truck_time) in &delivery_truck_route {
        for (taxi_point, taxi_time) in &taxi_route {
            let distance = truck_point.distance_to(taxi_point);
            let time_diff = truck_time.abs_diff(*taxi_time);

            if distance <= proximity_threshold && time_diff <= 60 {
                intersections_found += 1;
                println!(
                    "   Truck and taxi within {:.0}m at times {} and {} ({}s apart)",
                    distance, truck_time, taxi_time, time_diff
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
