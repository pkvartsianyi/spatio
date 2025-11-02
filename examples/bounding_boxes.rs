//! Bounding Box Example
//!
//! This example demonstrates how to use BoundingBox2D and BoundingBox3D types
//! for spatial queries and region management.

use spatio::{BoundingBox2D, BoundingBox3D, Point, Spatio, TemporalBoundingBox2D};
use std::error::Error;
use std::time::SystemTime;

fn main() -> Result<(), Box<dyn Error>> {
    println!("=== Spatio - Bounding Box Examples ===\n");

    // Create an in-memory database
    let db = Spatio::memory()?;
    println!("✓ Created in-memory database\n");

    // ========================================
    // 1. Basic 2D Bounding Box Operations
    // ========================================
    println!("1. Basic 2D Bounding Box");
    println!("-------------------------");

    // Create a bounding box for Manhattan
    let manhattan = BoundingBox2D::new(
        -74.0479, 40.6829, // Southwest corner (Battery Park)
        -73.9067, 40.8820, // Northeast corner (Inwood)
    );

    println!("   Manhattan bounding box:");
    println!(
        "     Min: ({:.4}, {:.4})",
        manhattan.min_x(),
        manhattan.min_y()
    );
    println!(
        "     Max: ({:.4}, {:.4})",
        manhattan.max_x(),
        manhattan.max_y()
    );
    println!(
        "     Center: ({:.4}, {:.4})",
        manhattan.center().x(),
        manhattan.center().y()
    );
    println!(
        "     Width: {:.4}°, Height: {:.4}°",
        manhattan.width(),
        manhattan.height()
    );

    // Check if points are within Manhattan
    let times_square = Point::new(-73.9855, 40.7580);
    let brooklyn_bridge = Point::new(-73.9969, 40.7061);
    let statue_of_liberty = Point::new(-74.0445, 40.6892);
    let jfk_airport = Point::new(-73.7781, 40.6413);

    println!("\n   Point containment checks:");
    println!(
        "     Times Square: {}",
        manhattan.contains_point(&times_square)
    );
    println!(
        "     Brooklyn Bridge: {}",
        manhattan.contains_point(&brooklyn_bridge)
    );
    println!(
        "     Statue of Liberty: {}",
        manhattan.contains_point(&statue_of_liberty)
    );
    println!(
        "     JFK Airport: {}",
        manhattan.contains_point(&jfk_airport)
    );

    // ========================================
    // 2. Bounding Box Intersection
    // ========================================
    println!("\n2. Bounding Box Intersection");
    println!("-----------------------------");

    // Create bounding boxes for different NYC boroughs
    let manhattan_bbox = BoundingBox2D::new(-74.0479, 40.6829, -73.9067, 40.8820);
    let brooklyn_bbox = BoundingBox2D::new(-74.0421, 40.5707, -73.8333, 40.7395);
    let queens_bbox = BoundingBox2D::new(-73.9626, 40.5431, -73.7004, 40.8007);

    println!(
        "   Manhattan ∩ Brooklyn: {}",
        manhattan_bbox.intersects(&brooklyn_bbox)
    );
    println!(
        "   Manhattan ∩ Queens: {}",
        manhattan_bbox.intersects(&queens_bbox)
    );
    println!(
        "   Brooklyn ∩ Queens: {}",
        brooklyn_bbox.intersects(&queens_bbox)
    );

    // ========================================
    // 3. Expanding Bounding Boxes
    // ========================================
    println!("\n3. Expanding Bounding Boxes");
    println!("----------------------------");

    let central_park = BoundingBox2D::new(-73.9812, 40.7644, -73.9492, 40.8003);
    println!("   Central Park original:");
    println!(
        "     Width: {:.4}°, Height: {:.4}°",
        central_park.width(),
        central_park.height()
    );

    // Expand by 0.01 degrees (~1 km)
    let expanded = central_park.expand(0.01);
    println!("\n   Expanded by 0.01°:");
    println!(
        "     Width: {:.4}°, Height: {:.4}°",
        expanded.width(),
        expanded.height()
    );
    println!(
        "     Growth: {:.4}° in each direction",
        (expanded.width() - central_park.width()) / 2.0
    );

    // ========================================
    // 4. 3D Bounding Boxes
    // ========================================
    println!("\n4. 3D Bounding Boxes");
    println!("--------------------");

    // Create a 3D bounding box for a tall building's footprint with height
    let one_world_trade = BoundingBox3D::new(
        -74.0134, 40.7127, 0.0, // Ground level southwest
        -74.0118, 40.7143, 541.0, // Top level northeast (541m height)
    );

    println!("   One World Trade Center:");
    println!(
        "     Footprint: {:.4}° × {:.4}°",
        one_world_trade.width(),
        one_world_trade.height()
    );
    println!("     Height: {:.1} meters", one_world_trade.depth());
    println!(
        "     Volume: {:.6} cubic degrees×meters",
        one_world_trade.volume()
    );

    let (cx, cy, cz) = one_world_trade.center();
    println!("     Center: ({:.4}, {:.4}, {:.1}m)", cx, cy, cz);

    // Check if points at different altitudes are within the building
    println!("\n   Altitude containment checks:");
    println!(
        "     Ground level (0m): {}",
        one_world_trade.contains_point(-74.0126, 40.7135, 0.0)
    );
    println!(
        "     Mid-level (270m): {}",
        one_world_trade.contains_point(-74.0126, 40.7135, 270.0)
    );
    println!(
        "     Top (540m): {}",
        one_world_trade.contains_point(-74.0126, 40.7135, 540.0)
    );
    println!(
        "     Above (600m): {}",
        one_world_trade.contains_point(-74.0126, 40.7135, 600.0)
    );

    // ========================================
    // 5. 3D to 2D Projection
    // ========================================
    println!("\n5. 3D to 2D Projection");
    println!("----------------------");

    let building_3d = BoundingBox3D::new(-74.0, 40.7, 0.0, -73.9, 40.8, 200.0);
    let building_2d = building_3d.to_2d();

    println!("   3D Bounding Box:");
    println!(
        "     Dimensions: {:.4}° × {:.4}° × {:.1}m",
        building_3d.width(),
        building_3d.height(),
        building_3d.depth()
    );

    println!("\n   Projected to 2D:");
    println!(
        "     Dimensions: {:.4}° × {:.4}°",
        building_2d.width(),
        building_2d.height()
    );

    // ========================================
    // 6. Temporal Bounding Boxes
    // ========================================
    println!("\n6. Temporal Bounding Boxes");
    println!("--------------------------");

    // Track how a delivery zone changes over time
    let morning_zone = BoundingBox2D::new(-74.01, 40.71, -73.99, 40.73);
    let afternoon_zone = BoundingBox2D::new(-74.02, 40.70, -73.98, 40.74);

    let morning_time = SystemTime::now();
    let afternoon_time = SystemTime::now();

    let temporal_morning = TemporalBoundingBox2D::new(morning_zone.clone(), morning_time);
    let temporal_afternoon = TemporalBoundingBox2D::new(afternoon_zone.clone(), afternoon_time);

    println!("   Morning delivery zone:");
    println!(
        "     Area: {:.4}° × {:.4}°",
        temporal_morning.bbox().width(),
        temporal_morning.bbox().height()
    );

    println!("\n   Afternoon delivery zone:");
    println!(
        "     Area: {:.4}° × {:.4}°",
        temporal_afternoon.bbox().width(),
        temporal_afternoon.bbox().height()
    );

    println!(
        "     Expansion: {:.4}° wider, {:.4}° taller",
        afternoon_zone.width() - morning_zone.width(),
        afternoon_zone.height() - morning_zone.height()
    );

    // ========================================
    // 7. Storing Bounding Boxes in Database
    // ========================================
    println!("\n7. Storing Bounding Boxes");
    println!("-------------------------");

    // Serialize and store bounding boxes
    let bbox_json = serde_json::to_vec(&manhattan)?;
    db.insert("zones:manhattan", bbox_json, None)?;

    let bbox3d_json = serde_json::to_vec(&one_world_trade)?;
    db.insert("buildings:wtc", bbox3d_json, None)?;

    println!("   ✓ Stored Manhattan bounding box");
    println!("   ✓ Stored One World Trade Center 3D box");

    // Retrieve and deserialize
    if let Some(data) = db.get("zones:manhattan")? {
        let retrieved: BoundingBox2D = serde_json::from_slice(&data)?;
        println!("\n   Retrieved Manhattan box:");
        println!(
            "     Center: ({:.4}, {:.4})",
            retrieved.center().x(),
            retrieved.center().y()
        );
    }

    // ========================================
    // 8. Practical Use Cases
    // ========================================
    println!("\n8. Practical Use Cases");
    println!("----------------------");

    // Geofencing
    let delivery_area = BoundingBox2D::new(-74.02, 40.70, -73.98, 40.75);
    let current_location = Point::new(-74.00, 40.72);

    if delivery_area.contains_point(&current_location) {
        println!("   ✓ Delivery driver is within service area");
    } else {
        println!("   ✗ Delivery driver is outside service area");
    }

    // Airspace management
    let airspace = BoundingBox3D::new(
        -74.1, 40.6, 0.0, // Ground level
        -73.8, 40.9, 3000.0, // 3000m ceiling
    );
    let drone_altitude = 150.0; // meters
    let drone_location = (-74.0, 40.75, drone_altitude);

    if airspace.contains_point(drone_location.0, drone_location.1, drone_location.2) {
        println!("   ✓ Drone is within authorized airspace");
    }

    // Region overlap detection
    let zone_a = BoundingBox2D::new(-74.05, 40.70, -74.00, 40.75);
    let zone_b = BoundingBox2D::new(-74.02, 40.72, -73.97, 40.77);

    if zone_a.intersects(&zone_b) {
        println!("   ⚠ Service zones A and B overlap - coordination needed");
    }

    println!("\n=== Bounding Box Examples Complete! ===");
    println!("\nKey Features Demonstrated:");
    println!("  • Create and manipulate 2D/3D bounding boxes");
    println!("  • Check point containment");
    println!("  • Detect box intersections");
    println!("  • Expand regions");
    println!("  • Project 3D to 2D");
    println!("  • Track temporal changes");
    println!("  • Serialize and store in database");
    println!("  • Geofencing and airspace management");

    Ok(())
}
