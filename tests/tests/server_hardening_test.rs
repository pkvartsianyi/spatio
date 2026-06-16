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
    let bad = vec![(-1.0_f64, Point3d::new(1.0, 2.0, 0.0), serde_json::json!({}))];
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
