//! 3D Spatial Tracking Example
//!
//! This example demonstrates Spatio's 3D spatial indexing capabilities for
//! altitude-aware applications like drone tracking, aviation, and multi-floor navigation.

use spatio::{Point3d, Spatio};
use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    println!("=== 3D Spatial Tracking with Spatio ===\n");

    // Create an in-memory database
    let db = Spatio::memory()?;

    // === Example 1: Drone Fleet Tracking ===
    println!("1. Drone Fleet Tracking");
    println!("   Tracking multiple drones at different altitudes\n");

    // Insert drones at various positions and altitudes
    let drones = vec![
        (
            "drone-001",
            -74.0060,
            40.7128,
            50.0,
            "Delivery drone - Package A",
        ),
        (
            "drone-002",
            -74.0070,
            40.7138,
            75.0,
            "Delivery drone - Package B",
        ),
        (
            "drone-003",
            -74.0050,
            40.7118,
            100.0,
            "Survey drone - Area mapping",
        ),
        (
            "drone-004",
            -74.0080,
            40.7148,
            125.0,
            "Inspection drone - Building check",
        ),
        (
            "drone-005",
            -74.0065,
            40.7133,
            150.0,
            "Emergency response drone",
        ),
    ];

    for (id, lon, lat, alt, description) in &drones {
        let position = Point3d::new(*lon, *lat, *alt);
        db.update_location(
            "drones",
            id,
            position,
            serde_json::json!({"description": description}),
        )?;
        println!("   ✓ Registered {}: altitude {}m", id, alt);
    }
    println!();

    // === Example 2: Spherical 3D Query ===
    println!("2. Spherical 3D Query");
    println!("   Finding drones within 3D radius\n");

    let control_center = Point3d::new(-74.0065, 40.7133, 100.0);
    let search_radius = 200.0; // 200 meters in 3D space

    // Note: query_current_within_radius uses 3D distance if points are 3D
    let nearby_drones =
        db.query_current_within_radius("drones", &control_center, search_radius, 10)?;

    println!(
        "   Control center: ({:.4}, {:.4}, {}m)",
        control_center.x(),
        control_center.y(),
        control_center.z()
    );
    println!("   Search radius: {}m (3D)\n", search_radius);
    println!("   Found {} drones within range:", nearby_drones.len());

    for loc in &nearby_drones {
        let description = loc.metadata.to_string();
        // Calculate distance manually for display
        let distance = control_center.distance_3d(&loc.position);
        println!(
            "   - {} at ({:.4}, {:.4}, {}m) - distance: {:.1}m",
            description,
            loc.position.x(),
            loc.position.y(),
            loc.position.z(),
            distance
        );
    }
    println!();

    // === Example 3: Cylindrical Query (Altitude Range) ===
    println!("3. Cylindrical Query");
    println!("   Finding drones in specific altitude corridor\n");

    let airspace_center = spatio_types::geo::Point::new(-74.0065, 40.7133);
    let min_altitude = 60.0;
    let max_altitude = 120.0;
    let horizontal_radius = 5000.0; // 5km horizontal

    let corridor_drones = db.query_within_cylinder(
        "drones",
        airspace_center,
        min_altitude,
        max_altitude,
        horizontal_radius,
        10,
    )?;

    println!(
        "   Altitude corridor: {}m - {}m",
        min_altitude, max_altitude
    );
    println!("   Horizontal radius: {}m\n", horizontal_radius);
    println!("   Found {} drones in corridor:", corridor_drones.len());

    for (loc, h_dist) in &corridor_drones {
        let description = loc.metadata.to_string();
        println!(
            "   - {} at altitude {}m (horizontal: {:.1}m)",
            description,
            loc.position.z(),
            h_dist
        );
    }
    println!();

    // === Example 4: 3D Bounding Box Query ===
    println!("4. 3D Bounding Box Query");
    println!("   Searching within a 3D volume\n");

    // Define a 3D box covering a specific area and altitude range
    let min_x = -74.0080;
    let min_y = 40.7120;
    let min_z = 40.0;
    let max_x = -74.0050;
    let max_y = 40.7150;
    let max_z = 110.0;

    let boxed_drones =
        db.query_within_bbox_3d("drones", min_x, min_y, min_z, max_x, max_y, max_z, 100)?;

    println!("   Bounding box:");
    println!("   - X: {:.4} to {:.4}", min_x, max_x);
    println!("   - Y: {:.4} to {:.4}", min_y, max_y);
    println!("   - Z: {}m to {}m\n", min_z, max_z);
    println!("   Found {} drones in volume:", boxed_drones.len());

    for loc in &boxed_drones {
        let description = loc.metadata.to_string();
        println!(
            "   - {} at ({:.4}, {:.4}, {}m)",
            description,
            loc.position.x(),
            loc.position.y(),
            loc.position.z()
        );
    }
    println!();

    // === Example 5: K-Nearest Neighbors in 3D ===
    println!("5. K-Nearest Neighbors (3D)");
    println!("   Finding closest drones to emergency location\n");

    let emergency_location = Point3d::new(-74.0062, 40.7130, 80.0);
    let k = 3;

    let nearest = db.knn_3d("drones", &emergency_location, k)?;

    println!(
        "   Emergency at: ({:.4}, {:.4}, {}m)",
        emergency_location.x(),
        emergency_location.y(),
        emergency_location.z()
    );
    println!("   Finding {} nearest drones:\n", k);

    for (i, (loc, distance)) in nearest.iter().enumerate() {
        let description = loc.metadata.to_string();
        println!("   {}. {} - {:.1}m away", i + 1, description, distance);
        println!(
            "      Position: ({:.4}, {:.4}, {}m)",
            loc.position.x(),
            loc.position.y(),
            loc.position.z()
        );
    }
    println!();

    // === Example 6: Aircraft Tracking ===
    println!("6. Aircraft Tracking");
    println!("   Managing commercial flights at cruising altitude\n");

    let flights = vec![
        ("AA123", -74.0100, 40.7200, 10000.0, "NYC to BOS"),
        ("UA456", -74.0200, 40.7300, 10500.0, "NYC to LAX"),
        ("DL789", -74.0150, 40.7250, 9800.0, "NYC to MIA"),
        ("SW321", -74.0050, 40.7150, 11000.0, "NYC to CHI"),
    ];

    for (flight, lon, lat, alt, route) in &flights {
        let position = Point3d::new(*lon, *lat, *alt);
        let info = format!("{} - {}", flight, route);
        db.update_location(
            "aircraft",
            flight,
            position,
            serde_json::json!({"info": info}),
        )?;
        println!("   ✓ Tracking {}: {}m altitude", flight, alt);
    }
    println!();

    // Query aircraft in specific flight level
    let fl_center = spatio_types::geo::Point::new(-74.0150, 40.7250);
    let fl_min = 9500.0; // Flight level 310 (approx)
    let fl_max = 10500.0; // Flight level 345 (approx)
    let radar_range = 50000.0; // 50km

    let tracked_flights =
        db.query_within_cylinder("aircraft", fl_center, fl_min, fl_max, radar_range, 20)?;

    println!("   Air traffic in flight levels FL310-FL345:");
    println!("   Radar range: {}km\n", radar_range / 1000.0);

    for (loc, h_dist) in &tracked_flights {
        let info = loc.metadata.to_string();
        println!(
            "   - {} at FL{:.0} ({}km away)",
            info,
            loc.position.z() / 30.48 / 100.0,
            h_dist / 1000.0
        );
    }
    println!();

    // === Example 7: 3D Distance Calculations ===
    println!("7. 3D Distance Calculations");
    println!("   Computing distances between 3D points\n");

    let point_a = Point3d::new(-74.0060, 40.7128, 100.0);
    let point_b = Point3d::new(-74.0070, 40.7138, 200.0);

    // Use Point3d methods directly
    let dist_3d = point_a.distance_3d(&point_b);
    // Convert to GeoPoint for haversine
    let geo_a = spatio_types::geo::Point::new(point_a.x(), point_a.y());
    let geo_b = spatio_types::geo::Point::new(point_b.x(), point_b.y());
    let horizontal_dist = geo_a.haversine_distance(&geo_b);
    let altitude_diff = (point_a.z() - point_b.z()).abs();

    println!(
        "   Point A: ({:.4}, {:.4}, {}m)",
        point_a.x(),
        point_a.y(),
        point_a.z()
    );
    println!(
        "   Point B: ({:.4}, {:.4}, {}m)",
        point_b.x(),
        point_b.y(),
        point_b.z()
    );
    println!();
    println!("   3D distance:        {:.2}m", dist_3d);
    println!("   Horizontal distance: {:.2}m", horizontal_dist);
    println!("   Altitude difference: {:.2}m", altitude_diff);
    println!();

    // === Example 8: Multi-Floor Building Navigation ===
    println!("8. Multi-Floor Building Navigation");
    println!("   Tracking sensors in a multi-story building\n");

    // Simulate a 10-floor building with sensors on each floor
    // Each floor is ~3 meters tall
    for floor in 0..10 {
        let altitude = floor as f64 * 3.0;
        let sensor_id = format!("sensor-floor-{:02}", floor);
        let position = Point3d::new(-74.0060, 40.7128, altitude);
        let info = format!("Temperature sensor - Floor {}", floor);
        db.update_location(
            "building-sensors",
            &sensor_id,
            position,
            serde_json::json!({"info": info}),
        )?;
    }

    // Query sensors on floors 3-7
    let building_location = spatio_types::geo::Point::new(-74.0060, 40.7128);
    let floor_3_altitude = 3.0 * 3.0;
    let floor_7_altitude = 7.0 * 3.0;

    let mid_floor_sensors = db.query_within_cylinder(
        "building-sensors",
        building_location,
        floor_3_altitude,
        floor_7_altitude,
        10.0, // 10m horizontal tolerance (same building)
        20,
    )?;

    println!("   Building sensors on floors 3-7:");
    for (loc, _) in &mid_floor_sensors {
        let info = loc.metadata.to_string();
        let floor = (loc.position.z() / 3.0).round() as i32;
        println!("   - Floor {}: {}", floor, info);
    }
    println!();

    // === Summary ===
    println!("=== Summary ===");
    println!("Demonstrated 3D spatial capabilities:");
    println!("✓ 3D point insertion with altitude");
    println!("✓ Spherical queries (3D radius)");
    println!("✓ Cylindrical queries (altitude corridors)");
    println!("✓ 3D bounding box queries");
    println!("✓ K-nearest neighbors in 3D space");
    println!("✓ 3D distance calculations");
    println!("✓ Multi-altitude tracking (drones, aircraft, buildings)");

    Ok(())
}
