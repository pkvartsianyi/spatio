use spatio::{Point3d, Spatio};
use spatio_client::SpatioClient;
use spatio_server::run_server;
use std::sync::Arc;

#[tokio::test]
async fn test_rpc_lifecycle() -> anyhow::Result<()> {
    tracing_subscriber::fmt::try_init().ok();
    // Start server in background
    let db = Arc::new(Spatio::builder().build()?);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let bound_addr = listener.local_addr()?;

    let server_db = db.clone();
    tokio::spawn(async move {
        let _ = run_server(listener, server_db, futures::future::pending()).await;
    });

    // Wait for server to start
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Connect client
    let client = SpatioClient::connect(bound_addr).await?;

    // UPSERT
    client
        .upsert(
            "test_ns",
            "obj1",
            Point3d::new(1.0, 2.0, 3.0),
            serde_json::json!({"key": "val"}),
        )
        .await?;

    // GET
    let obj = client.get("test_ns", "obj1").await?;
    assert!(obj.is_some());
    let obj = obj.unwrap();
    assert_eq!(obj.object_id, "obj1");

    Ok(())
}

#[tokio::test]
async fn test_trajectory_rpc() -> anyhow::Result<()> {
    tracing_subscriber::fmt::try_init().ok();
    let db = Arc::new(Spatio::builder().build()?);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let bound_addr = listener.local_addr()?;

    let server_db = db.clone();
    tokio::spawn(async move {
        let _ = run_server(listener, server_db, futures::future::pending()).await;
    });

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let client = SpatioClient::connect(bound_addr).await?;

    // InsertTrajectory
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs_f64();

    let trajectory = vec![(now, Point3d::new(10.0, 10.0, 0.0), serde_json::json!({}))];
    client
        .insert_trajectory("traj_ns", "truck1", trajectory)
        .await?;

    // QueryTrajectory
    let updates = client
        .query_trajectory("traj_ns", "truck1", Some(now - 60.0), Some(now + 60.0), 10)
        .await?;

    assert_eq!(updates.len(), 1);

    Ok(())
}
