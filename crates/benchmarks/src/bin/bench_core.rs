use spatio::{Point3d, Spatio};
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    let quiet = args.iter().any(|arg| arg == "-q");

    let db = Spatio::memory()?;
    let namespace = "bench";
    let count = 100_000;

    // Helper to print results
    let report = |name: &str, start: Instant, ops: usize| {
        let elapsed = start.elapsed();
        let ops_per_sec = ops as f64 / elapsed.as_secs_f64();
        if quiet {
            println!("{}: {:.2} operations per second", name, ops_per_sec);
        } else {
            println!(
                "{}: {:.2} operations per second ({:?})",
                name, ops_per_sec, elapsed
            );
        }
    };

    // UPSERT
    let start = Instant::now();
    let side_len = (count as f64).sqrt() as usize;
    for x in 0..side_len {
        for y in 0..side_len {
            let i = x * side_len + y;
            db.upsert(
                "spatial",
                &format!("obj:{}", i),
                Point3d::new(x as f64 * 0.01, y as f64 * 0.01, 0.0),
                serde_json::Value::Null,
                None,
            )?;
        }
    }
    let spatial_count = side_len * side_len;
    report("UPSERT", start, spatial_count);

    // GET
    let start = Instant::now();
    for i in 0..count {
        db.get(namespace, &format!("key:{}", i))?;
    }
    report("GET", start, count);

    Ok(())
}
