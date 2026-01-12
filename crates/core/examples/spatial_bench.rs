use spatio::{Point3d, Spatio};
use std::time::Instant;

fn main() {
    println!("=== Spatio Spatial Index Microbenchmark ===\n");

    for size in [1000, 10000, 100000] {
        println!("--- Dataset size: {} ---", size);

        // Measure INSERT
        let db = Spatio::memory().unwrap();
        let start = Instant::now();

        for i in 0..size {
            let point = Point3d::new(-74.0 + (i as f64 * 0.0001), 40.7 + (i as f64 * 0.0001), 0.0);
            db.upsert(
                "bench",
                &format!("point_{}", i),
                point,
                serde_json::json!({}),
                None,
            )
            .unwrap();
        }

        let insert_duration = start.elapsed();
        let insert_ops_per_sec = size as f64 / insert_duration.as_secs_f64();
        println!(
            "  INSERT: {:>8.0} ops/s  ({:.2}ms total)",
            insert_ops_per_sec,
            insert_duration.as_secs_f64() * 1000.0
        );

        // Measure DELETE (remove half the points)
        let delete_count = size / 2;
        let start = Instant::now();

        for i in 0..delete_count {
            db.delete("bench", &format!("point_{}", i)).unwrap();
        }

        let delete_duration = start.elapsed();
        let delete_ops_per_sec = delete_count as f64 / delete_duration.as_secs_f64();
        println!(
            "  DELETE: {:>8.0} ops/s  ({:.2}ms total)",
            delete_ops_per_sec,
            delete_duration.as_secs_f64() * 1000.0
        );

        println!();
    }
}
