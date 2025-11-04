//! Example demonstrating multi-threaded database access with SyncDB.
//!
//! This example shows how to use the thread-safe `SyncDB` wrapper to safely
//! share a database across multiple threads.
//!
//! Run with: cargo run --example sync_wrapper --features sync --release

#![cfg(feature = "sync")]

use spatio::SetOptions;
use spatio::SyncDB;
use std::thread;
use std::time::Duration;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== SyncDB Multi-threaded Example ===\n");

    // Create a thread-safe in-memory database
    let db = SyncDB::memory()?;

    println!("1. Basic multi-threaded writes");
    println!("   Spawning 4 threads, each writing 100 keys...");

    let mut handles = vec![];

    for thread_id in 0..4 {
        let db = db.clone(); // Clone the Arc wrapper
        let handle = thread::spawn(move || {
            for i in 0..100 {
                let key = format!("thread_{}:key_{}", thread_id, i);
                let value = format!("value_{}", i);
                db.insert(&key, value.as_bytes(), None).unwrap();
            }
            println!("   Thread {} completed 100 writes", thread_id);
        });
        handles.push(handle);
    }

    // Wait for all threads to complete
    for handle in handles {
        handle.join().unwrap();
    }

    let stats = db.stats();
    println!("   Total keys written: {}\n", stats.key_count);

    // Example 2: Concurrent reads and writes
    println!("2. Concurrent reads and writes");
    println!("   Starting mixed read/write workload...");

    // Pre-populate some data
    for i in 0..50 {
        db.insert(format!("shared_key_{}", i), b"shared_value", None)?;
    }

    let mut handles = vec![];

    // Spawn reader threads
    for thread_id in 0..3 {
        let db = db.clone();
        let handle = thread::spawn(move || {
            for i in 0..50 {
                let key = format!("shared_key_{}", i);
                let _ = db.get(&key).unwrap();
            }
            println!("   Reader thread {} completed", thread_id);
        });
        handles.push(handle);
    }

    // Spawn writer threads
    for thread_id in 0..2 {
        let db = db.clone();
        let handle = thread::spawn(move || {
            for i in 0..25 {
                let key = format!("writer_{}:key_{}", thread_id, i);
                db.insert(&key, b"new_value", None).unwrap();
            }
            println!("   Writer thread {} completed", thread_id);
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let stats = db.stats();
    println!("   Final key count: {}\n", stats.key_count);

    // Example 3: Atomic batch operations from multiple threads
    println!("3. Atomic batch operations");
    println!("   Each thread performs atomic batches...");

    let mut handles = vec![];

    for thread_id in 0..3 {
        let db = db.clone();
        let handle = thread::spawn(move || {
            db.atomic(|batch| {
                for i in 0..10 {
                    let key = format!("batch_thread_{}:item_{}", thread_id, i);
                    batch.insert(&key, b"batch_value", None)?;
                }
                Ok(())
            })
            .unwrap();
            println!("   Thread {} completed atomic batch", thread_id);
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let stats = db.stats();
    println!("   Keys after batches: {}\n", stats.key_count);

    // Example 4: Using TTL with concurrent access
    println!("4. Concurrent TTL operations");

    let mut handles = vec![];

    for thread_id in 0..2 {
        let db = db.clone();
        let handle = thread::spawn(move || {
            let opts = SetOptions::with_ttl(Duration::from_secs(60));
            for i in 0..20 {
                let key = format!("ttl_thread_{}:key_{}", thread_id, i);
                db.insert(&key, b"expires_in_60s", Some(opts.clone()))
                    .unwrap();
            }
            println!("   Thread {} inserted 20 TTL keys", thread_id);
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    println!("   TTL keys inserted successfully\n");

    // Example 5: Direct lock access for batch operations
    println!("5. Advanced: Direct lock access");
    println!("   Holding lock for multiple operations...");

    {
        // Acquire write lock once for multiple operations
        let mut guard = db.write();
        guard.insert("bulk_1", b"value_1", None)?;
        guard.insert("bulk_2", b"value_2", None)?;
        guard.insert("bulk_3", b"value_3", None)?;
        println!("   Performed 3 writes under single lock");
        // Lock released when guard is dropped
    }

    {
        // Acquire read lock for multiple reads
        let guard = db.read();
        let val1 = guard.get("bulk_1")?;
        let val2 = guard.get("bulk_2")?;
        let val3 = guard.get("bulk_3")?;
        println!(
            "   Read {} values under single lock",
            [val1, val2, val3].iter().filter(|v| v.is_some()).count()
        );
        // Lock released when guard is dropped
    }

    println!();

    // Final statistics
    let stats = db.stats();
    println!("=== Final Statistics ===");
    println!("Total keys: {}", stats.key_count);
    println!("Total operations: {}", stats.operations_count);

    println!("\n✓ All multi-threaded operations completed successfully!");
    println!("\nKey Takeaways:");
    println!("  • SyncDB is Clone - all clones share the same database");
    println!("  • Safe to use across multiple threads");
    println!("  • Read operations can happen concurrently");
    println!("  • Write operations are exclusive (block all access)");
    println!("  • Use atomic() for transactional semantics");
    println!("  • Use read()/write() for batch operations under single lock");

    Ok(())
}
