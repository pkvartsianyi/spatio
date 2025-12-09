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
    // 4. Register drones (using upsert)
    // Note: upsert replaces update_location
    for i in 1..=5 {
        let id = format!("drone-{:03}", i);
        let altitude = 50.0 + (i as f64 - 1.0) * 25.0;
        let pos = Point3d::new(-74.0060, 40.7128, altitude);

        // Simulating different drone types
        let metadata = if i % 2 == 0 {
            serde_json::json!({"type": "delivery", "description": "Delivery drone - Package B"})
        } else if i == 5 {
            serde_json::json!({"type": "emergency", "description": "Emergency response drone"})
        } else if i == 3 {
            serde_json::json!({"type": "survey", "description": "Survey drone - Area mapping"})
        } else {
            serde_json::json!({"type": "delivery", "description": "Delivery drone - Package A"})
        };

        db.upsert("drones", &id, pos, metadata, None)?;
        println!("   ✓ Registered {}: altitude {}m", id, altitude);
    }
    println!();

    // === Example 2: Spherical 3D Query ===
    println!("2. Spherical 3D Query");
    println!("   Finding drones within 3D radius\n");

    let control_center = Point3d::new(-74.0065, 40.7133, 100.0);
    let search_radius = 200.0; // 200 meters in 3D space

    // Persist control center so we can query relative to it (using key)
    db.upsert(
        "drones",
        "control_center",
        control_center.clone(),
        serde_json::json!({"type": "infrastructure", "name": "Main Base"}),
        None,
    )?;

    // Query drones near the control center using its ID
    // query_near always returns distance now
    let nearby_drones = db.query_near("drones", "control_center", search_radius, 10)?;

    println!(
        "   Control center: ({:.4}, {:.4}, {}m)",
        control_center.x(),
        control_center.y(),
        control_center.z()
    );
    println!("   Search radius: {}m (3D)\n", search_radius);
    println!("   Found {} drones within range:", nearby_drones.len());

    for (loc, distance) in &nearby_drones {
        let description = loc.metadata.to_string();
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

    // knn always returns distance
    let nearest_drones = db.knn("drones", &emergency_location, 3)?;
    println!(
        "   Emergency at: ({:.4}, {:.4}, {}m)",
        emergency_location.x(),
        emergency_location.y(),
        emergency_location.z()
    );
    println!("   Finding {} nearest drones:\n", k);

    for (i, (loc, distance)) in nearest_drones.iter().enumerate() {
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

    for (id, lon, lat, alt, route) in &flights {
        let pos = Point3d::new(*lon, *lat, *alt);
        db.upsert(
            "aircraft",
            id,
            pos,
            serde_json::json!({"info": format!("{} - {}", id, route)}),
            None,
        )?;
        println!("   ✓ Tracking {}: {}m altitude", id, alt);
    }

    println!("\n   Air traffic in flight levels FL310-FL345:");
    println!("   Radar range: 50km\n");

    let radar_pos = Point3d::new(-74.0060, 40.7128, 0.0); // Ground radar
    let traffic = db.query_within_cylinder(
        "aircraft",
        spatio_types::geo::Point::new(radar_pos.x(), radar_pos.y()),
        310.0 * 30.48, // FL310 in meters
        345.0 * 30.48, // FL345 in meters
        50000.0,
        10,
    )?;

    for (loc, h_dist) in &traffic {
        let info = loc.metadata.to_string();
        println!(
            "   - {} at FL{:.0} ({}km away)",
            info,
            loc.position.z() / 30.48 / 100.0,
            h_dist / 1000.0
        );
    }
    println!();

    println!("7. 3D Distance Calculations (Native)");
    println!("   Computing distances natively in DB query\n");

    let point_a = Point3d::new(-74.0060, 40.7128, 100.0);
    let point_b = Point3d::new(-74.0070, 40.7138, 200.0);

    // Use Point3d methods directly
    let dist_3d = point_a.distance_3d(&point_b);

    // Insert points to query relative to each other
    db.upsert(
        "calc_demo",
        "point_a",
        point_a.clone(),
        serde_json::json!({}),
        None,
    )?;
    db.upsert(
        "calc_demo",
        "point_b",
        point_b.clone(),
        serde_json::json!({}),
        None,
    )?;

    // Query distance between objects using keys
    // query_near replaces query_near_object_with_distance
    let results = db.query_near("calc_demo", "point_a", 1000.0, 10)?;

    // Print native DB results
    println!("   Results near point_a:");
    for (loc, dist) in &results {
        println!("   - Distance to {}: {:.2}m", loc.object_id, dist);
    }

    // Convert to GeoPoint for validation info (optional)
    let geo_a = spatio_types::geo::Point::new(point_a.x(), point_a.y());
    let geo_b = spatio_types::geo::Point::new(point_b.x(), point_b.y());
    let horizontal_dist = geo_a.haversine_distance(&geo_b);
    let altitude_diff = (point_a.z() - point_b.z()).abs();

    println!();
    println!("   Comparison:");
    println!("   - Native DB distance: see above");
    println!("   - Manual 3D distance: {:.2}m", dist_3d);
    println!("   - Horizontal distance: {:.2}m", horizontal_dist);
    println!("   - Altitude difference: {:.2}m", altitude_diff);
    println!();

    // === Example 8: Multi-Floor Building Navigation ===
    println!("8. Multi-Floor Building Navigation");
    println!("   Tracking sensors in a multi-story building\n");

    // Simulate a 10-floor building with sensors on each floor
    // Each floor is ~3 meters tall
    for floor in 0..10 {
        let height = floor as f64 * 3.0;
        let fmt_id = format!("sensor-floor-{:02}", floor);
        db.upsert(
            "building-sensors",
            &fmt_id,
            Point3d::new(-74.0060, 40.7128, height),
            serde_json::json!({"info": fmt_id}),
            None,
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
