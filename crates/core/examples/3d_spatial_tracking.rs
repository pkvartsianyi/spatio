//! 3D Spatial Tracking Example
//!
//! This example demonstrates Spatio's 3D spatial indexing capabilities for
//! altitude-aware applications like drone tracking, aviation, and multi-floor navigation.

use spatio::{BoundingBox3D, Point3d, Spatio};
use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    println!("=== 3D Spatial Tracking with Spatio ===\n");

    // Create an in-memory database
    let mut db = Spatio::memory()?;

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
        db.insert_point_3d("drones", &position, description.as_bytes(), None)?;
        println!("   ✓ Registered {}: altitude {}m", id, alt);
    }
    println!();

    // === Example 2: Spherical 3D Query ===
    println!("2. Spherical 3D Query");
    println!("   Finding drones within 3D radius\n");

    let control_center = Point3d::new(-74.0065, 40.7133, 100.0);
    let search_radius = 200.0; // 200 meters in 3D space

    let nearby_drones = db.query_within_sphere_3d("drones", &control_center, search_radius, 10)?;

    println!(
        "   Control center: ({:.4}, {:.4}, {}m)",
        control_center.x(),
        control_center.y(),
        control_center.z()
    );
    println!("   Search radius: {}m (3D)\n", search_radius);
    println!("   Found {} drones within range:", nearby_drones.len());

    for (point, data, distance) in &nearby_drones {
        let description = String::from_utf8_lossy(data);
        println!(
            "   - {} at ({:.4}, {:.4}, {}m) - distance: {:.1}m",
            description,
            point.x(),
            point.y(),
            point.z(),
            distance
        );
    }
    println!();

    // === Example 3: Cylindrical Query (Altitude Range) ===
    println!("3. Cylindrical Query");
    println!("   Finding drones in specific altitude corridor\n");

    let airspace_center = Point3d::new(-74.0065, 40.7133, 0.0);
    let min_altitude = 60.0;
    let max_altitude = 120.0;
    let horizontal_radius = 5000.0; // 5km horizontal

    let corridor_drones = db.query_within_cylinder_3d(
        "drones",
        &airspace_center,
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

    for (point, data, h_dist) in &corridor_drones {
        let description = String::from_utf8_lossy(data);
        println!(
            "   - {} at altitude {}m (horizontal: {:.1}m)",
            description,
            point.z(),
            h_dist
        );
    }
    println!();

    // === Example 4: 3D Bounding Box Query ===
    println!("4. 3D Bounding Box Query");
    println!("   Searching within a 3D volume\n");

    // Define a 3D box covering a specific area and altitude range
    let bbox = BoundingBox3D::new(
        -74.0080, 40.7120, 40.0, // min x, y, z
        -74.0050, 40.7150, 110.0, // max x, y, z
    );

    let boxed_drones = db.query_within_bbox_3d("drones", &bbox, 100)?;

    println!("   Bounding box:");
    println!("   - X: {:.4} to {:.4}", bbox.min_x, bbox.max_x);
    println!("   - Y: {:.4} to {:.4}", bbox.min_y, bbox.max_y);
    println!("   - Z: {}m to {}m\n", bbox.min_z, bbox.max_z);
    println!("   Found {} drones in volume:", boxed_drones.len());

    for (point, data) in &boxed_drones {
        let description = String::from_utf8_lossy(data);
        println!(
            "   - {} at ({:.4}, {:.4}, {}m)",
            description,
            point.x(),
            point.y(),
            point.z()
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

    for (i, (point, data, distance)) in nearest.iter().enumerate() {
        let description = String::from_utf8_lossy(data);
        println!("   {}. {} - {:.1}m away", i + 1, description, distance);
        println!(
            "      Position: ({:.4}, {:.4}, {}m)",
            point.x(),
            point.y(),
            point.z()
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
        db.insert_point_3d("aircraft", &position, info.as_bytes(), None)?;
        println!("   ✓ Tracking {}: {}m altitude", flight, alt);
    }
    println!();

    // Query aircraft in specific flight level
    let fl_center = Point3d::new(-74.0150, 40.7250, 0.0);
    let fl_min = 9500.0; // Flight level 310 (approx)
    let fl_max = 10500.0; // Flight level 345 (approx)
    let radar_range = 50000.0; // 50km

    let tracked_flights =
        db.query_within_cylinder_3d("aircraft", &fl_center, fl_min, fl_max, radar_range, 20)?;

    println!("   Air traffic in flight levels FL310-FL345:");
    println!("   Radar range: {}km\n", radar_range / 1000.0);

    for (point, data, h_dist) in &tracked_flights {
        let info = String::from_utf8_lossy(data);
        println!(
            "   - {} at FL{:.0} ({}km away)",
            info,
            point.z() / 30.48 / 100.0,
            h_dist / 1000.0
        );
    }
    println!();

    // === Example 7: 3D Distance Calculations ===
    println!("7. 3D Distance Calculations");
    println!("   Computing distances between 3D points\n");

    let point_a = Point3d::new(-74.0060, 40.7128, 100.0);
    let point_b = Point3d::new(-74.0070, 40.7138, 200.0);

    let dist_3d = db.distance_between_3d(&point_a, &point_b)?;
    let horizontal_dist = point_a.haversine_2d(&point_b);
    let altitude_diff = point_a.altitude_difference(&point_b);

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
        let _sensor_id = format!("sensor-floor-{:02}", floor);
        let position = Point3d::new(-74.0060, 40.7128, altitude);
        let info = format!("Temperature sensor - Floor {}", floor);
        db.insert_point_3d("building-sensors", &position, info.as_bytes(), None)?;
    }

    // Query sensors on floors 3-7
    let building_location = Point3d::new(-74.0060, 40.7128, 0.0);
    let floor_3_altitude = 3.0 * 3.0;
    let floor_7_altitude = 7.0 * 3.0;

    let mid_floor_sensors = db.query_within_cylinder_3d(
        "building-sensors",
        &building_location,
        floor_3_altitude,
        floor_7_altitude,
        10.0, // 10m horizontal tolerance (same building)
        20,
    )?;

    println!("   Building sensors on floors 3-7:");
    for (point, data, _) in &mid_floor_sensors {
        let info = String::from_utf8_lossy(data);
        let floor = (point.z() / 3.0).round() as i32;
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
