use spatio::prelude::*;
use spatio_client::SpatioClient;
use spatio_types::point::Point3d;
use std::sync::Arc;
use tokio::time::{Duration, sleep};

#[tokio::test]
async fn test_remote_client_ops() -> anyhow::Result<()> {
    tracing_subscriber::fmt::try_init().ok();

    // 1. Start a local server
    let db = Arc::new(Spatio::builder().build()?);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let bound_addr = listener.local_addr()?;

    let db_for_server = db.clone();
    tokio::spawn(async move {
        let _ =
            spatio_server::run_server(listener, db_for_server, futures::future::pending()).await;
    });

    // Wait for server to start
    sleep(Duration::from_millis(200)).await;

    // 2. Connect with SpatioClient
    let client = SpatioClient::connect(bound_addr).await?;

    // 3. Perform operations
    let nyc = Point3d::new(-74.0060, 40.7128, 0.0);
    client
        .upsert("cities", "nyc", nyc, serde_json::json!({"pop": 8000000}))
        .await?;

    // 4. Verify with get
    let result = client
        .get("cities", "nyc")
        .await?
        .expect("Object should exist");
    assert_eq!(result.position.x(), -74.0060);

    // 5. Verify stats
    let stats = client.stats().await?;
    assert_eq!(stats.object_count, 1);

    // 6. Query radius
    let nyc_3d = Point3d::new(-74.0060, 40.7128, 0.0);
    let nearby = client.query_radius("cities", nyc_3d, 100_000.0, 10).await?;
    assert_eq!(nearby.len(), 1);
    assert_eq!(nearby[0].0.object_id, "nyc");

    Ok(())
}
