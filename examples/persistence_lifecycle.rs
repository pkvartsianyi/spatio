//! Comprehensive example demonstrating SpatioLite's persistence features:
//! - Configurable AOF paths
//! - Automatic startup replay
//! - Graceful shutdown hooks
//!
//! Run this example multiple times to see persistence in action:
//! ```
//! cargo run --example persistence_lifecycle
//! ```

use spatio::{Config, DBBuilder, Point, SetOptions, Spatio, SyncPolicy};
use std::time::Duration;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== SpatioLite Persistence Lifecycle Demo ===\n");

    // Demo 1: Basic persistence with automatic shutdown
    demo_basic_persistence()?;

    // Demo 2: Custom AOF path with DBBuilder
    demo_custom_aof_path()?;

    // Demo 3: Spatial data persistence and replay
    demo_spatial_persistence()?;

    // Demo 4: Graceful shutdown with error handling
    demo_graceful_shutdown()?;

    // Demo 5: Configuration-based persistence
    demo_config_based_persistence()?;

    println!("\n=== All demos completed successfully! ===");
    Ok(())
}

/// Demo 1: Basic persistence with automatic shutdown on drop
fn demo_basic_persistence() -> Result<(), Box<dyn std::error::Error>> {
    println!("--- Demo 1: Basic Persistence ---");

    let db_path = "/tmp/spatio_demo_basic.db";

    // First session: Write data
    {
        println!("Session 1: Writing data...");
        let db = Spatio::open(db_path)?;

        db.insert("user:001", b"Alice Johnson", None)?;
        db.insert("user:002", b"Bob Smith", None)?;
        db.insert("counter", b"42", None)?;

        println!("  ✓ Inserted 3 keys");
        println!("  ✓ Database will automatically sync on drop");

        // Database automatically closes and syncs when dropped here
    }

    // Second session: Read data (automatic replay)
    {
        println!("Session 2: Reading data (automatic startup replay)...");
        let db = Spatio::open(db_path)?;

        let alice = db.get("user:001")?.unwrap();
        let bob = db.get("user:002")?.unwrap();
        let counter = db.get("counter")?.unwrap();

        println!(
            "  ✓ Retrieved user:001: {:?}",
            String::from_utf8_lossy(&alice)
        );
        println!(
            "  ✓ Retrieved user:002: {:?}",
            String::from_utf8_lossy(&bob)
        );
        println!(
            "  ✓ Retrieved counter: {:?}",
            String::from_utf8_lossy(&counter)
        );

        let stats = db.stats()?;
        println!("  ✓ Total keys: {}", stats.key_count);
    }

    println!();
    Ok(())
}

/// Demo 2: Custom AOF path using DBBuilder
fn demo_custom_aof_path() -> Result<(), Box<dyn std::error::Error>> {
    println!("--- Demo 2: Custom AOF Path ---");

    let aof_path = "/tmp/spatio_custom.aof";

    // First session: Create database with custom AOF path
    {
        println!("Session 1: Creating database with custom AOF path...");
        let db = DBBuilder::new().aof_path(aof_path).build()?;

        db.insert("config:version", b"1.0.0", None)?;
        db.insert("config:environment", b"production", None)?;

        println!("  ✓ AOF file: {}", aof_path);
        println!("  ✓ Inserted configuration data");

        // Force sync to ensure data is on disk
        db.sync()?;
        println!("  ✓ Manually synced to disk");
    }

    // Second session: Reopen with same AOF path
    {
        println!("Session 2: Reopening database...");
        let db = DBBuilder::new().aof_path(aof_path).build()?;

        let version = db.get("config:version")?.unwrap();
        let environment = db.get("config:environment")?.unwrap();

        println!("  ✓ Version: {}", String::from_utf8_lossy(&version));
        println!("  ✓ Environment: {}", String::from_utf8_lossy(&environment));
    }

    println!();
    Ok(())
}

/// Demo 3: Spatial data persistence and index reconstruction
fn demo_spatial_persistence() -> Result<(), Box<dyn std::error::Error>> {
    println!("--- Demo 3: Spatial Data Persistence ---");

    let db_path = "/tmp/spatio_demo_spatial.db";

    // First session: Insert spatial data
    {
        println!("Session 1: Inserting spatial data...");
        let db = Spatio::open(db_path)?;

        // Insert major cities
        let cities = vec![
            ("New York", Point::new(40.7128, -74.0060)),
            ("Los Angeles", Point::new(34.0522, -118.2437)),
            ("Chicago", Point::new(41.8781, -87.6298)),
            ("Houston", Point::new(29.7604, -95.3698)),
            ("Phoenix", Point::new(33.4484, -112.0740)),
        ];

        for (name, point) in cities {
            db.insert_point("cities", &point, name.as_bytes(), None)?;
            println!("  ✓ Inserted city: {}", name);
        }

        let stats = db.stats()?;
        println!("  ✓ Total keys: {}", stats.key_count);
    }

    // Second session: Query spatial data (indexes automatically rebuilt)
    {
        println!("Session 2: Querying spatial data (indexes rebuilt)...");
        let db = Spatio::open(db_path)?;

        // Find cities near New York
        let nyc = Point::new(40.7128, -74.0060);
        let nearby = db.find_nearby("cities", &nyc, 500_000.0, 10)?; // 500km radius

        println!(
            "  ✓ Found {} cities within 500km of New York:",
            nearby.len()
        );
        for (point, data) in nearby {
            let name = String::from_utf8_lossy(&data);
            println!("    - {} at ({:.4}, {:.4})", name, point.lat, point.lon);
        }

        // Count cities in a bounding box
        let count = db.count_within_distance("cities", &nyc, 500_000.0)?;
        println!("  ✓ Count verification: {} cities", count);
    }

    println!();
    Ok(())
}

/// Demo 4: Explicit graceful shutdown with error handling
fn demo_graceful_shutdown() -> Result<(), Box<dyn std::error::Error>> {
    println!("--- Demo 4: Graceful Shutdown ---");

    let db_path = "/tmp/spatio_demo_shutdown.db";

    println!("Creating database and inserting critical data...");
    let mut db = Spatio::open(db_path)?;

    // Insert critical data
    db.insert("transaction:001", b"payment_processed", None)?;
    db.insert("transaction:002", b"refund_pending", None)?;

    println!("  ✓ Critical data inserted");

    // Force sync before critical operations
    db.sync()?;
    println!("  ✓ Forced sync to disk");

    // Add TTL data
    let opts = SetOptions::with_ttl(Duration::from_secs(300));
    db.insert("session:xyz", b"temporary_session", Some(opts))?;
    println!("  ✓ Added temporary session with 5-minute TTL");

    // Explicit close with error handling
    println!("Performing explicit graceful shutdown...");
    match db.close() {
        Ok(_) => println!("  ✓ Database closed successfully"),
        Err(e) => {
            eprintln!("  ✗ Error during close: {}", e);
            return Err(e.into());
        }
    }

    // Verify we can't use closed database
    match db.insert("should_fail", b"data", None) {
        Err(e) => println!("  ✓ Correctly rejected operation on closed database: {}", e),
        Ok(_) => println!("  ✗ Unexpectedly accepted operation on closed database"),
    }

    println!();
    Ok(())
}

/// Demo 5: Configuration-based persistence with different sync policies
fn demo_config_based_persistence() -> Result<(), Box<dyn std::error::Error>> {
    println!("--- Demo 5: Configuration-Based Persistence ---");

    // High-durability configuration (sync always)
    {
        println!("Config 1: Always sync (maximum durability)");
        let config = Config::default().with_sync_policy(SyncPolicy::Always);

        let db = DBBuilder::new()
            .aof_path("/tmp/spatio_always_sync.aof")
            .config(config)
            .build()?;

        db.insert("critical:data", b"financial_transaction", None)?;
        println!("  ✓ Every write immediately synced to disk");
    }

    // Balanced configuration (sync every second)
    {
        println!("Config 2: Sync every second (recommended)");
        let config = Config::with_geohash_precision(10)
            .with_sync_policy(SyncPolicy::EverySecond)
            .with_default_ttl(Duration::from_secs(3600));

        let db = DBBuilder::new()
            .aof_path("/tmp/spatio_balanced.aof")
            .config(config)
            .build()?;

        db.insert("balanced:data", b"application_data", None)?;
        println!("  ✓ High precision (10) with periodic sync");
        println!("  ✓ Default TTL: 1 hour");
    }

    // In-memory configuration (no persistence)
    {
        println!("Config 3: In-memory (no persistence)");
        let db = DBBuilder::new().in_memory().build()?;

        db.insert("cache:key", b"temporary_value", None)?;
        println!("  ✓ Fast in-memory storage, no disk I/O");
    }

    println!();
    Ok(())
}
