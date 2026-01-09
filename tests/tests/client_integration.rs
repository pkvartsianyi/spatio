use spatio::{Point3d, Spatio};
use spatio_client::SpatioClient;
use spatio_server::run_server;
use std::sync::Arc;
use std::time::Duration;

async fn spawn_test_server() -> anyhow::Result<std::net::SocketAddr> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "spatio_server=info,spatio=info,info".into()),
        )
        .try_init()
        .ok();
    let db = Arc::new(Spatio::builder().build()?);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let bound_addr = listener.local_addr()?;

    // Spawn server in background
    tokio::spawn(async move {
        // Use pending future for shutdown signal to keep running indefinitely during test
        let _ = run_server(listener, db, futures::future::pending()).await;
    });

    // Give it a moment to start
    tokio::time::sleep(Duration::from_millis(50)).await;

    Ok(bound_addr)
}

#[tokio::test]
async fn test_client_lifecycle_and_crud() -> anyhow::Result<()> {
    let addr = spawn_test_server().await?;
    let client = SpatioClient::connect(addr).await?;

    // Upsert
    client
        .upsert(
            "test_ns",
            "p1",
            Point3d::new(10.0, 20.0, 30.0),
            serde_json::json!({"city": "Berlin"}),
            None,
        )
        .await?;

    // Get
    let loc = client.get("test_ns", "p1").await?.expect("should exist");
    assert_eq!(loc.object_id, "p1");
    assert_eq!(loc.position.x(), 10.0);

    // Deserialize metadata
    let meta: serde_json::Value = serde_json::from_slice(&loc.metadata)?;
    assert_eq!(meta["city"], "Berlin");

    // Delete
    client.delete("test_ns", "p1").await?;
    let loc = client.get("test_ns", "p1").await?;
    assert!(loc.is_none());

    Ok(())
}

#[tokio::test]
async fn test_spatial_queries() -> anyhow::Result<()> {
    let addr = spawn_test_server().await?;
    let client = SpatioClient::connect(addr).await?;

    // Setup data
    // p1: (0,0,0)
    // p2: (0.0001, 0, 0) -> ~11 meters east
    // p3: (1.0, 0, 0) -> ~111 km east
    client
        .upsert(
            "geo",
            "p1",
            Point3d::new(0.0, 0.0, 0.0),
            serde_json::json!({}),
            None,
        )
        .await?;
    client
        .upsert(
            "geo",
            "p2",
            Point3d::new(0.0001, 0.0, 0.0),
            serde_json::json!({}),
            None,
        )
        .await?;
    client
        .upsert(
            "geo",
            "p3",
            Point3d::new(1.0, 0.0, 0.0),
            serde_json::json!({}),
            None,
        )
        .await?;

    // Query Radius (search around 0,0 with r=15 meters -> expect p1, p2)
    let results = client
        .query_radius("geo", Point3d::new(0.0, 0.0, 0.0), 15.0, 10)
        .await?;
    assert_eq!(results.len(), 2);
    // Sort by ID to ensure consistent order for assertion if not guaranteed by server
    // (Though basic implementation usually returns in some order, safe to check inclusion)
    let ids: Vec<String> = results.iter().map(|(l, _)| l.object_id.clone()).collect();
    assert!(ids.contains(&"p1".to_string()));
    assert!(ids.contains(&"p2".to_string()));

    // KNN (k=2 near 0,0 -> p1, p2)
    let results = client.knn("geo", Point3d::new(0.0, 0.0, 0.0), 2).await?;
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].0.object_id, "p1");

    // BBox (containing p1, p2 but not p3)
    let results = client
        .query_bbox("geo", -0.01, -0.01, 0.01, 0.01, 10)
        .await?;
    assert_eq!(results.len(), 2);

    Ok(())
}

#[tokio::test]
async fn test_trajectory() -> anyhow::Result<()> {
    let addr = spawn_test_server().await?;
    let client = SpatioClient::connect(addr).await?;

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs_f64();

    // Insert trajectory
    let points = vec![
        (
            now - 100.0,
            Point3d::new(0.0, 0.0, 0.0),
            serde_json::json!({}),
        ),
        (
            now - 50.0,
            Point3d::new(10.0, 10.0, 0.0),
            serde_json::json!({}),
        ),
        (now, Point3d::new(20.0, 20.0, 0.0), serde_json::json!({})),
    ];

    client.insert_trajectory("traj", "v1", points).await?;

    // Query whole range
    let traj = client
        .query_trajectory("traj", "v1", None, None, 100)
        .await?;
    assert_eq!(traj.len(), 3);

    // Query subset
    let traj = client
        .query_trajectory("traj", "v1", Some(now - 60.0), Some(now - 40.0), 100)
        .await?;
    assert_eq!(traj.len(), 1);
    assert_eq!(traj[0].position.x(), 10.0);

    Ok(())
}
