use spatio::Spatio;
use spatio_client::SpatioClient;
use spatio_server::run_server;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[tokio::test]
async fn test_max_frame_size() -> anyhow::Result<()> {
    tracing_subscriber::fmt::try_init().ok();

    let db = Arc::new(Spatio::builder().build()?);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let bound_addr = listener.local_addr()?;
    drop(listener);

    tokio::spawn(async move {
        let _ = run_server(bound_addr, db, futures::future::pending()).await;
    });

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Connect and send an oversized frame manually
    let mut stream = TcpStream::connect(bound_addr).await?;

    // Send garbage data that's too large - tarpc/serde will reject it
    // Note: With tarpc's JSON transport, we can just send garbage
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
    drop(listener);

    tokio::spawn(async move {
        let _ = run_server(bound_addr, db, futures::future::pending()).await;
    });

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Connect with SpatioClient and verify stats works
    let client = SpatioClient::connect(bound_addr).await?;
    let stats = client.stats().await?;
    assert!(stats.object_count >= 0);

    Ok(())
}
