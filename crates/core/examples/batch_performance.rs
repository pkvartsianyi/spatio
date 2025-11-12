//! Demonstrates batch write
//!
//! Run with: cargo run --example batch_performance --release

use spatio::{Config, Spatio, SyncPolicy};
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Batch Write Performance Comparison\n");

    // Test with different batch sizes
    let batch_sizes = vec![1, 10, 100, 1000];

    for batch_size in batch_sizes {
        println!("=== Batch Size: {} ===", batch_size);

        // Test with SyncPolicy::Always (worst case for AOF)
        let config = Config::default()
            .with_sync_policy(SyncPolicy::Always)
            .with_sync_batch_size(1);
        let mut db = Spatio::memory_with_config(config)?;

        let start = Instant::now();
        db.atomic(|batch| {
            for i in 0..batch_size {
                let key = format!("key_{}", i);
                batch.insert(&key, b"value", None)?;
            }
            Ok(())
        })?;
        let elapsed = start.elapsed();

        let ops_per_sec = (batch_size as f64 / elapsed.as_secs_f64()) as u64;
        println!("  Time: {:?}", elapsed);
        println!("  Throughput: {} ops/sec", ops_per_sec);
        println!("  Time per op: {:?}\n", elapsed / batch_size);
    }

    println!("=== With Never Sync (Memory-only) ===");
    let config = Config::default().with_sync_policy(SyncPolicy::Never);
    let mut db = Spatio::memory_with_config(config)?;

    let batch_size = 10_000;
    let start = Instant::now();
    db.atomic(|batch| {
        for i in 0..batch_size {
            let key = format!("key_{}", i);
            batch.insert(&key, b"value", None)?;
        }
        Ok(())
    })?;
    let elapsed = start.elapsed();

    let ops_per_sec = (batch_size as f64 / elapsed.as_secs_f64()) as u64;
    println!("  Batch size: {}", batch_size);
    println!("  Time: {:?}", elapsed);
    println!("  Throughput: {} ops/sec", ops_per_sec);
    println!("  Time per op: {:?}", elapsed / batch_size);

    Ok(())
}
