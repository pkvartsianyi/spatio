//! Bounding Box Database Integration Example
//!
//! This example demonstrates how to use bounding boxes with the Spatio database
//! for storing and querying geographic regions and spatial data.

use spatio::{BoundingBox2D, Point, Spatio};
use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    println!("=== Spatio - Bounding Box Database ===\n");

    let mut db = Spatio::memory()?;
    println!("✓ Created in-memory database\n");

    // ========================================
    // 1. Store Geographic Regions
    // ========================================
    println!("1. Storing Geographic Regions");
    println!("------------------------------");

    // Define NYC boroughs as bounding boxes
    let manhattan = BoundingBox2D::new(-74.0479, 40.6829, -73.9067, 40.8820);
    let brooklyn = BoundingBox2D::new(-74.0421, 40.5707, -73.8333, 40.7395);
    let queens = BoundingBox2D::new(-73.9626, 40.5431, -73.7004, 40.8007);
    let bronx = BoundingBox2D::new(-73.9338, 40.7855, -73.7654, 40.9176);
    let staten_island = BoundingBox2D::new(-74.2558, 40.4960, -74.0526, 40.6490);

    // Store regions in database
    db.insert_bbox("region:manhattan", &manhattan, None)?;
    db.insert_bbox("region:brooklyn", &brooklyn, None)?;
    db.insert_bbox("region:queens", &queens, None)?;
    db.insert_bbox("region:bronx", &bronx, None)?;
    db.insert_bbox("region:staten_island", &staten_island, None)?;

    println!("   ✓ Stored 5 NYC borough boundaries");

    // ========================================
    // 2. Retrieve Stored Regions
    // ========================================
    println!("\n2. Retrieving Stored Regions");
    println!("-----------------------------");

    if let Some(retrieved_manhattan) = db.get_bbox("region:manhattan")? {
        println!("   Manhattan:");
        println!(
            "     Area: {:.4}° × {:.4}°",
            retrieved_manhattan.width(),
            retrieved_manhattan.height()
        );
        println!(
            "     Center: ({:.4}, {:.4})",
            retrieved_manhattan.center().x(),
            retrieved_manhattan.center().y()
        );
    }

    // ========================================
    // 3. Store POIs and Query by Region
    // ========================================
    println!("\n3. Store POIs and Query by Region");
    println!("-----------------------------------");

    // Store points of interest
    db.insert_point("poi", &Point::new(-73.9855, 40.7580), b"times_square", None)?;
    db.insert_point("poi", &Point::new(-73.9665, 40.7829), b"central_park", None)?;
    db.insert_point("poi", &Point::new(-73.9857, 40.7484), b"empire_state", None)?;
    db.insert_point(
        "poi",
        &Point::new(-73.9969, 40.7061),
        b"brooklyn_bridge",
        None,
    )?;
    db.insert_point(
        "poi",
        &Point::new(-73.9690, 40.6602),
        b"prospect_park",
        None,
    )?;
    db.insert_point("poi", &Point::new(-73.9799, 40.5755), b"coney_island", None)?;

    println!("   ✓ Stored 6 points of interest");

    // Query POIs in Manhattan
    let manhattan_pois = db.query_within_bbox("poi", &manhattan, 100)?;
    println!("\n   POIs in Manhattan: {}", manhattan_pois.len());
    for (point, data) in &manhattan_pois {
        let name = String::from_utf8_lossy(data);
        println!("     - {} at ({:.4}, {:.4})", name, point.x(), point.y());
    }

    // Query POIs in Brooklyn
    let brooklyn_pois = db.query_within_bbox("poi", &brooklyn, 100)?;
    println!("\n   POIs in Brooklyn: {}", brooklyn_pois.len());

    // ========================================
    // 4. Find Intersecting Regions
    // ========================================
    println!("\n4. Finding Intersecting Regions");
    println!("--------------------------------");

    // Create a search area (covering parts of Manhattan and Queens)
    let search_area = BoundingBox2D::new(-73.98, 40.72, -73.92, 40.78);

    let intersecting = db.find_intersecting_bboxes("region:", &search_area)?;
    println!(
        "   Search area intersects with {} boroughs:",
        intersecting.len()
    );
    for (key, _bbox) in &intersecting {
        println!("     - {}", key.replace("region:", ""));
    }

    // ========================================
    // 5. Delivery Zone Management
    // ========================================
    println!("\n5. Delivery Zone Management");
    println!("----------------------------");

    // Define delivery zones
    let zone_a = BoundingBox2D::new(-74.02, 40.70, -73.98, 40.74);
    let zone_b = BoundingBox2D::new(-74.00, 40.72, -73.96, 40.76);
    let zone_c = BoundingBox2D::new(-73.98, 40.74, -73.94, 40.78);

    db.insert_bbox("delivery:zone_a", &zone_a, None)?;
    db.insert_bbox("delivery:zone_b", &zone_b, None)?;
    db.insert_bbox("delivery:zone_c", &zone_c, None)?;

    println!("   ✓ Created 3 delivery zones");

    // Check which zones overlap
    let overlapping_with_a = db.find_intersecting_bboxes("delivery:", &zone_a)?;
    println!(
        "\n   Zone A overlaps with {} zones (including itself)",
        overlapping_with_a.len()
    );

    // ========================================
    // 6. Service Area Lookup
    // ========================================
    println!("\n6. Service Area Lookup");
    println!("-----------------------");

    // Check if various locations are in service areas
    let locations = vec![
        ("Customer 1", Point::new(-74.00, 40.72)),
        ("Customer 2", Point::new(-73.97, 40.75)),
        ("Customer 3", Point::new(-74.05, 40.68)),
    ];

    for (name, location) in &locations {
        let mut in_zone = false;
        for (zone_key, zone_bbox) in &overlapping_with_a {
            if zone_bbox.contains_point(location) {
                println!(
                    "   {} at ({:.4}, {:.4}) is in {}",
                    name,
                    location.x(),
                    location.y(),
                    zone_key.replace("delivery:", "")
                );
                in_zone = true;
                break;
            }
        }
        if !in_zone {
            println!(
                "   {} at ({:.4}, {:.4}) is outside all zones",
                name,
                location.x(),
                location.y()
            );
        }
    }

    // ========================================
    // 7. Geofencing Example
    // ========================================
    println!("\n7. Geofencing Example");
    println!("----------------------");

    // Define restricted areas
    let airport_zone = BoundingBox2D::new(-73.82, 40.63, -73.76, 40.66);
    db.insert_bbox("restricted:jfk_airport", &airport_zone, None)?;

    let military_zone = BoundingBox2D::new(-74.08, 40.60, -74.03, 40.64);
    db.insert_bbox("restricted:military", &military_zone, None)?;

    println!("   ✓ Defined 2 restricted zones");

    // Check if drone locations are in restricted areas
    let drone_locations = vec![
        ("Drone 1", Point::new(-73.79, 40.64)),
        ("Drone 2", Point::new(-74.00, 40.70)),
    ];

    for (drone, location) in &drone_locations {
        let restricted = db.find_intersecting_bboxes(
            "restricted:",
            &BoundingBox2D::new(
                location.x() - 0.001,
                location.y() - 0.001,
                location.x() + 0.001,
                location.y() + 0.001,
            ),
        )?;

        if !restricted.is_empty() {
            println!(
                "   ⚠ {} at ({:.4}, {:.4}) is in restricted area: {}",
                drone,
                location.x(),
                location.y(),
                restricted[0].0.replace("restricted:", "")
            );
        } else {
            println!(
                "   ✓ {} at ({:.4}, {:.4}) is in safe area",
                drone,
                location.x(),
                location.y()
            );
        }
    }

    // ========================================
    // 8. Spatial Analytics
    // ========================================
    println!("\n8. Spatial Analytics");
    println!("---------------------");

    // Calculate coverage area
    let mut total_width = 0.0;
    let mut total_height = 0.0;
    let mut count = 0;

    let all_regions = vec![
        ("Manhattan", &manhattan),
        ("Brooklyn", &brooklyn),
        ("Queens", &queens),
        ("Bronx", &bronx),
        ("Staten Island", &staten_island),
    ];

    for (name, bbox) in &all_regions {
        total_width += bbox.width();
        total_height += bbox.height();
        count += 1;
        println!(
            "   {} coverage: {:.4}° × {:.4}°",
            name,
            bbox.width(),
            bbox.height()
        );
    }

    let avg_width = total_width / count as f64;
    let avg_height = total_height / count as f64;

    println!(
        "\n   Average borough size: {:.4}° × {:.4}°",
        avg_width, avg_height
    );

    // ========================================
    // 9. Database Statistics
    // ========================================
    println!("\n9. Database Statistics");
    println!("-----------------------");

    let stats = db.stats();
    println!("   Total keys: {}", stats.key_count);
    println!("   Total operations: {}", stats.operations_count);

    // Count different types of data
    let poi_count = manhattan_pois.len() + brooklyn_pois.len();
    println!("   Stored POIs: {}", poi_count);
    println!("   Stored regions: 5");
    println!("   Delivery zones: 3");
    println!("   Restricted zones: 2");

    println!("\n=== Bounding Box Database Integration Complete! ===");
    println!("\nKey Features Demonstrated:");
    println!("  • Store and retrieve bounding boxes");
    println!("  • Query POIs within regions");
    println!("  • Find intersecting regions");
    println!("  • Delivery zone management");
    println!("  • Service area lookups");
    println!("  • Geofencing and restricted areas");
    println!("  • Spatial analytics");

    Ok(())
}
