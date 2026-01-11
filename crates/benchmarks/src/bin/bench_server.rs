use futures::stream::{self, StreamExt};
use spatio_client::SpatioClient;
use std::sync::Arc;
use std::time::Instant;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    let quiet = args.iter().any(|arg| arg == "-q");
    let addr = "127.0.0.1:3000".parse()?;

    // Create client
    let client = Arc::new(SpatioClient::connect(addr).await?);
    let namespace = "bench";
    let count = 100_000;
    let concurrency = 100;

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
    println!("Running UPSERT benchmark...");
    let start = Instant::now();
    let side_len = (count as f64).sqrt() as usize;
    let spatial_count = side_len * side_len;

    stream::iter(0..side_len)
        .flat_map(|x| stream::iter(0..side_len).map(move |y| (x, y)))
        .map(|(x, y)| {
            let client = client.clone();
            async move {
                let i = x * side_len + y;
                client
                    .upsert(
                        "spatial",
                        &format!("obj:{}", i),
                        spatio_types::point::Point3d::new(x as f64 * 0.01, y as f64 * 0.01, 0.0),
                        serde_json::Value::Null,
                        None,
                    )
                    .await
            }
        })
        .buffer_unordered(concurrency)
        .count()
        .await;
    report("UPSERT", start, spatial_count);

    // GET
    println!("Running GET benchmark...");
    let start = Instant::now();
    stream::iter(0..count)
        .map(|i| {
            let client = client.clone();
            async move { client.get(namespace, &format!("key:{}", i)).await }
        })
        .buffer_unordered(concurrency)
        .count()
        .await;
    report("GET", start, count);

    Ok(())
}
