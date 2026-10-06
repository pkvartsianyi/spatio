use spatio::Spatio;
use spatio_client::SpatioClient;
use spatio_server::run_server;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[tokio::test]
async fn test_max_frame_size() -> anyhow::Result<()> {
    tracing_subscriber::fmt::try_init().ok();

    let db = Arc::new(Spatio::builder().build()?);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let bound_addr = listener.local_addr()?;

    tokio::spawn(async move {
        let _ = run_server(listener, db, futures::future::pending()).await;
    });

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Connect and send an oversized frame manually
    let mut stream = TcpStream::connect(bound_addr).await?;

    // Send garbage data that's too large - tarpc/serde will reject it
    let garbage = vec![0u8; 11 * 1024 * 1024]; // 11MB
    let _ = stream.write_all(&garbage).await;

    // The server should close the connection (either 0 bytes read or connection reset)
    let mut buf = [0u8; 1];
    match stream.read(&mut buf).await {
        Ok(0) => {}  // Server closed connection gracefully
        Err(_) => {} // Connection reset is also acceptable
        Ok(_) => panic!("Server should have closed connection"),
    }

    Ok(())
}

#[tokio::test]
async fn test_idle_timeout() -> anyhow::Result<()> {
    tracing_subscriber::fmt::try_init().ok();

    let db = Arc::new(Spatio::builder().build()?);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let bound_addr = listener.local_addr()?;

    tokio::spawn(async move {
        let _ = run_server(listener, db, futures::future::pending()).await;
    });

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Connect with SpatioClient and verify stats works
    let client = SpatioClient::connect(bound_addr).await?;
    // Just verify stats() works - the value itself is usize so always >= 0
    let _stats = client.stats().await?;

    Ok(())
}

/// A malformed trajectory timestamp (negative seconds) must return an error
/// rather than panicking the background writer thread via Duration::from_secs_f64
/// and disabling all future writes.
#[tokio::test]
async fn test_malformed_trajectory_timestamp_does_not_kill_writer() -> anyhow::Result<()> {
    use spatio_types::point::Point3d;

    tracing_subscriber::fmt::try_init().ok();

    let db = Arc::new(Spatio::builder().build()?);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let bound_addr = listener.local_addr()?;

    tokio::spawn(async move {
        let _ = run_server(listener, db, futures::future::pending()).await;
    });
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let client = SpatioClient::connect(bound_addr).await?;

    // A negative timestamp would previously panic the writer thread.
    let bad = vec![(-1.0_f64, spatio::Point::new(1.0, 2.0))];
    let err = client.insert_trajectory("ns", "obj", bad).await;
    assert!(err.is_err(), "malformed timestamp must be rejected");

    // The writer must still be alive: a subsequent valid write succeeds.
    client
        .upsert(
            "ns",
            "obj2",
            Point3d::new(3.0, 4.0, 0.0),
            serde_json::json!({}),
        )
        .await?;
    assert!(client.get("ns", "obj2").await?.is_some());

    Ok(())
}

async fn spawn_server() -> anyhow::Result<(std::net::SocketAddr, Arc<Spatio>)> {
    let db = Arc::new(Spatio::builder().build()?);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let server_db = db.clone();
    tokio::spawn(async move {
        let _ = run_server(listener, server_db, futures::future::pending()).await;
    });
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    Ok((addr, db))
}

fn grid_point(i: usize) -> spatio::Point3d {
    spatio::Point3d::new((i % 100) as f64 * 0.001, (i / 100) as f64 * 0.001, 0.0)
}

#[tokio::test]
async fn test_large_limit_is_capped_and_fits_in_a_frame() -> anyhow::Result<()> {
    let (addr, db) = spawn_server().await?;
    for i in 0..12_000 {
        db.upsert(
            "ns",
            &format!("o{i}"),
            grid_point(i),
            serde_json::json!({"i": i}),
            None,
        )?;
    }
    let client = SpatioClient::connect(addr).await?;
    let res = client
        .query_bbox("ns", -1.0, -1.0, 1.0, 1.0, 60_000)
        .await?;
    assert_eq!(res.len(), 10_000);
    assert!(res[0].metadata["i"].is_number());
    Ok(())
}

#[tokio::test]
async fn test_client_recovers_after_oversized_reply() -> anyhow::Result<()> {
    let (addr, db) = spawn_server().await?;
    let blob = "x".repeat(2048);
    for i in 0..5_000 {
        db.upsert(
            "ns",
            &format!("o{i}"),
            grid_point(i),
            serde_json::json!({"b": blob}),
            None,
        )?;
    }
    let client = SpatioClient::connect(addr).await?;
    assert!(client
        .query_bbox("ns", -1.0, -1.0, 1.0, 1.0, 10_000)
        .await
        .is_err());
    assert!(client.get("ns", "o1").await?.is_some());
    Ok(())
}
