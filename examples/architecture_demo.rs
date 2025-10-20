//! Architecture Demo - Showcasing Spatio's Enhanced Architecture
//!
//! This example demonstrates all the new architectural improvements:
//! - Storage backend abstraction
//! - Namespace support for multi-tenancy
//! - Simplified configuration with serialization
//! - GeoJSON I/O support
//! - Feature flags usage
//! - Background AOF rewriting

use spatio::prelude::*;
use spatio::{MemoryBackend, Namespace, NamespaceManager};
use std::time::Duration;

#[cfg(feature = "aof")]
use spatio::AOFConfig;

fn main() -> Result<()> {
    println!("Spatio Architecture Demo");
    println!("========================\n");

    // 1. Demonstrate simplified configuration with serialization
    demonstrate_config_serialization()?;

    // 2. Show namespace support for multi-tenancy
    demonstrate_namespace_support()?;

    // 3. Demonstrate GeoJSON I/O
    demonstrate_geojson_support()?;

    // 4. Show storage backend abstraction
    demonstrate_storage_backends()?;

    // 5. Demonstrate AOF improvements (if feature enabled)
    #[cfg(feature = "aof")]
    demonstrate_aof_improvements()?;

    println!("\nArchitecture demo completed successfully!");
    println!("\nKey architectural improvements demonstrated:");
    println!("- Simplified, serializable configuration");
    println!("- Namespace support for data organization");
    println!("- GeoJSON I/O for interoperability");
    println!("- Storage backend abstraction");
    #[cfg(feature = "aof")]
    println!("- Enhanced AOF with background rewriting");
    #[cfg(not(feature = "aof"))]
    println!("- AOF features not enabled (compile with --features aof)");

    Ok(())
}

/// Demonstrate simplified configuration with JSON/TOML serialization
fn demonstrate_config_serialization() -> Result<()> {
    println!("1. Configuration Serialization");
    println!("-------------------------------");

    // Create a custom configuration
    let config = Config::with_geohash_precision(10)
        .with_default_ttl(Duration::from_secs(3600))
        .with_sync_policy(SyncPolicy::Always);

    // Serialize to JSON
    let json = config.to_json().unwrap();
    println!("Configuration as JSON:");
    println!("{}", json);

    // Load from JSON
    let loaded_config = Config::from_json(&json).unwrap();
    println!("Successfully loaded config from JSON");
    println!("   Geohash precision: {}", loaded_config.geohash_precision);
    println!("   Default TTL: {:?}", loaded_config.default_ttl());
    println!("   Sync policy: {:?}", loaded_config.sync_policy);

    // Demonstrate TOML support (if feature enabled)
    #[cfg(feature = "toml")]
    {
        let toml = config.to_toml().unwrap();
        println!("\nConfiguration as TOML:");
        println!("{}", toml);

        let _loaded_from_toml = Config::from_toml(&toml).unwrap();
        println!("Successfully loaded config from TOML");
    }

    #[cfg(not(feature = "toml"))]
    println!("TOML support not enabled (compile with --features toml)");

    println!();
    Ok(())
}

/// Demonstrate namespace support for multi-tenant isolation
fn demonstrate_namespace_support() -> Result<()> {
    println!("2. Namespace Support");
    println!("--------------------");

    // Create a database
    let db = Spatio::memory()?;

    // Create namespaces for data organization
    let namespace_a = Namespace::new("namespace_a");
    let namespace_b = Namespace::new("namespace_b");
    let admin_ns = Namespace::new("admin");

    println!(
        "Created namespaces: {}, {}, {}",
        namespace_a, namespace_b, admin_ns
    );

    // Store data with namespace isolation
    let user_data = b"John Doe - Premium Customer";
    let admin_data = b"System Configuration";

    db.insert(namespace_a.key("user:123"), user_data, None)?;
    db.insert(namespace_b.key("user:123"), b"Jane Smith - Standard", None)?;
    db.insert(admin_ns.key("config:rate_limit"), admin_data, None)?;

    // Demonstrate namespace isolation
    let namespace_a_user = db.get(namespace_a.key("user:123"))?.unwrap();
    let namespace_b_user = db.get(namespace_b.key("user:123"))?.unwrap();

    println!(
        "Namespace A user:123 = {}",
        String::from_utf8_lossy(&namespace_a_user)
    );
    println!(
        "Namespace B user:123 = {}",
        String::from_utf8_lossy(&namespace_b_user)
    );

    // Demonstrate namespace management
    let manager = NamespaceManager::new();

    // Store some spatial data with namespaces
    let london = Point::new(51.5074, -0.1278);
    let paris = Point::new(48.8566, 2.3522);

    db.insert_point(namespace_a.name(), &london, b"London Office", None)?;
    db.insert_point(namespace_b.name(), &paris, b"Paris Office", None)?;

    // Find nearby points within each namespace
    let namespace_a_offices = db.find_nearby(namespace_a.name(), &london, 1000.0, 10)?;
    let namespace_b_offices = db.find_nearby(namespace_b.name(), &london, 500_000.0, 10)?;

    println!(
        "Namespace A offices near London: {}",
        namespace_a_offices.len()
    );
    println!(
        "Namespace B offices near London: {}",
        namespace_b_offices.len()
    );

    // Demonstrate key parsing
    let namespaced_key = namespace_a.key("some:complex:key");
    if let Some((namespace, original_key)) = manager.parse_key(&namespaced_key) {
        println!(
            "Parsed key - Namespace: '{}', Key: '{}'",
            namespace,
            String::from_utf8_lossy(&original_key)
        );
    }

    println!();
    Ok(())
}

/// Demonstrate GeoJSON I/O support
fn demonstrate_geojson_support() -> Result<()> {
    println!("3. GeoJSON I/O Support");
    println!("----------------------");

    // Create points from various sources
    let points = [
        Point::new(40.7128, -74.0060), // NYC
        Point::new(51.5074, -0.1278),  // London
        Point::new(35.6762, 139.6503), // Tokyo
    ];

    let names = ["New York", "London", "Tokyo"];

    println!("Converting points to GeoJSON:");
    for (point, name) in points.iter().zip(names.iter()) {
        let geojson = point.to_geojson()?;
        println!("{}: {}", name, geojson);
    }

    // Parse GeoJSON back to points
    println!("\nParsing GeoJSON back to points:");
    let geojson_samples = vec![
        r#"{"type":"Point","coordinates":[-73.9857,40.7484]}"#, // Empire State Building
        r#"{"type":"Point","coordinates":[2.2945,48.8584]}"#,   // Eiffel Tower
    ];

    for geojson in geojson_samples {
        let point = Point::from_geojson(geojson)?;
        println!("Parsed: ({:.4}, {:.4})", point.lat, point.lon);
    }

    // Demonstrate coordinate conversion
    let empire_state = Point::new(40.7484, -73.9857);
    let coords = empire_state.to_geojson_coords();
    println!(
        "\nEmpire State Building coordinates: [{}, {}]",
        coords[0], coords[1]
    );

    let reconstructed = Point::from_geojson_coords(&coords)?;
    println!(
        "Reconstructed point: ({}, {})",
        reconstructed.lat, reconstructed.lon
    );

    println!();
    Ok(())
}

/// Demonstrate storage backend abstraction
fn demonstrate_storage_backends() -> Result<()> {
    println!("4. Storage Backend Abstraction");
    println!("------------------------------");

    // Create different storage backends
    let mut memory_backend = MemoryBackend::new();
    println!("Created in-memory storage backend");

    // Demonstrate storage operations
    let key = b"test_key";
    let item = spatio::types::DbItem::new("test_value");

    memory_backend.put(key, &item)?;
    println!("Stored item in memory backend");

    let retrieved = memory_backend.get(key)?.unwrap();
    println!("Retrieved: {}", String::from_utf8_lossy(&retrieved.value));

    // Demonstrate prefix operations
    let prefix_items = vec![
        ("prefix:item1", "value1"),
        ("prefix:item2", "value2"),
        ("other:item3", "value3"),
    ];

    for (key, value) in &prefix_items {
        let item = spatio::types::DbItem::new(*value);
        memory_backend.put(key.as_bytes(), &item)?;
    }

    let prefix_keys = memory_backend.keys_with_prefix(b"prefix:")?;
    println!("Keys with prefix 'prefix:': {}", prefix_keys.len());

    let prefix_scan = memory_backend.scan_prefix(b"prefix:")?;
    println!("Prefix scan results:");
    for (key, item) in prefix_scan {
        println!(
            "   {} = {}",
            String::from_utf8_lossy(&key),
            String::from_utf8_lossy(&item.value)
        );
    }

    // Show storage stats
    let stats = memory_backend.stats()?;
    println!(
        "Backend stats: {} keys, {} bytes",
        stats.key_count, stats.size_bytes
    );

    #[cfg(feature = "aof")]
    {
        println!("\nAOF backend available with 'aof' feature");
    }

    #[cfg(not(feature = "aof"))]
    {
        println!("\nAOF backend not available (compile with --features aof)");
    }

    println!();
    Ok(())
}

/// Demonstrate AOF improvements with background rewriting
#[cfg(feature = "aof")]
fn demonstrate_aof_improvements() -> Result<()> {
    println!("5. Enhanced AOF with Background Rewriting");
    println!("-----------------------------------------");

    // Create AOF configuration
    let aof_config = AOFConfig {
        rewrite_size_threshold: 1024, // Small threshold for demo
        rewrite_growth_percentage: 50.0,
        background_rewrite: true,
    };

    println!("AOF Configuration:");
    println!(
        "   Rewrite threshold: {} bytes",
        aof_config.rewrite_size_threshold
    );
    println!(
        "   Growth percentage: {}%",
        aof_config.rewrite_growth_percentage
    );
    println!("   Background rewrite: {}", aof_config.background_rewrite);

    // Create temporary AOF file
    let temp_dir = std::env::temp_dir();
    let aof_path = temp_dir.join("demo.aof");

    let mut aof = spatio::persistence::AOFFile::open_with_config(&aof_path, aof_config)?;
    println!("Created AOF file at: {}", aof_path.display());

    // Write some data to trigger potential rewrite
    println!("\nWriting data to AOF...");
    for i in 0..20 {
        let key = format!("key_{}", i).into_bytes().into();
        let value = format!("value_{}_with_some_extra_data_to_increase_size", i)
            .into_bytes()
            .into();
        aof.write_set(&key, &value, None)?;
    }

    let current_size = aof.size()?;
    println!("Current AOF size: {} bytes", current_size);

    if aof.is_rewrite_in_progress() {
        println!("Background rewrite in progress...");
    } else {
        println!("No background rewrite triggered yet");
    }

    // Force a manual rewrite for demonstration
    println!("\nTriggering manual AOF rewrite...");
    aof.rewrite()?;

    // Give background thread a moment
    std::thread::sleep(Duration::from_millis(100));

    if aof.is_rewrite_in_progress() {
        println!("Rewrite in progress...");
    } else {
        println!("Rewrite completed");
    }

    aof.sync()?;
    println!("AOF synced to disk");

    // Clean up
    std::fs::remove_file(&aof_path).ok();
    println!("Cleaned up temporary files");

    println!();
    Ok(())
}

/// Helper function to format bytes as human readable
#[allow(dead_code)]
fn format_bytes(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KB", "MB", "GB"];
    let mut size = bytes as f64;
    let mut unit_index = 0;

    while size >= 1024.0 && unit_index < UNITS.len() - 1 {
        size /= 1024.0;
        unit_index += 1;
    }

    format!("{:.1} {}", size, UNITS[unit_index])
}
