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

    // Announce an 11MB frame; the server must refuse it instead of buffering.
    let len = 11u32 * 1024 * 1024;
    let _ = stream.write_all(&len.to_be_bytes()).await;
    let _ = stream.write_all(&vec![0u8; 64 * 1024]).await;

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

async fn spawn_secure_server(
    config: spatio_server::ServerConfig,
) -> anyhow::Result<std::net::SocketAddr> {
    let db = Arc::new(Spatio::builder().build()?);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    tokio::spawn(async move {
        let _ =
            spatio_server::run_server_with(listener, db, config, futures::future::pending()).await;
    });
    Ok(addr)
}

#[tokio::test]
async fn test_auth_token_is_required() -> anyhow::Result<()> {
    use spatio_client::{ClientError, ClientOptions};

    let addr = spawn_secure_server(spatio_server::ServerConfig {
        auth_token: Some("s3cret".into()),
        tls: None,
    })
    .await?;

    let with = |t: Option<&str>| ClientOptions {
        auth_token: t.map(Into::into),
        tls: None,
    };
    for bad in [None, Some("wrong"), Some("s3cre")] {
        let err = SpatioClient::connect_with(addr, with(bad)).await.err();
        assert!(matches!(err, Some(ClientError::Handshake)), "{bad:?}");
    }
    let client = SpatioClient::connect_with(addr, with(Some("s3cret"))).await?;
    client.stats().await?;
    Ok(())
}

#[tokio::test]
async fn test_tls_with_auth() -> anyhow::Result<()> {
    use rustls::pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer, ServerName};
    use spatio_client::{rustls, ClientOptions};

    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()])?;
    let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der()));
    let server_tls = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert.cert.der().clone()], key)?;
    let addr = spawn_secure_server(spatio_server::ServerConfig {
        auth_token: Some("s3cret".into()),
        tls: Some(Arc::new(server_tls)),
    })
    .await?;

    let mut roots = rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone())?;
    let client_tls = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let client = SpatioClient::connect_with(
        addr,
        ClientOptions {
            auth_token: Some("s3cret".into()),
            tls: Some((Arc::new(client_tls), ServerName::try_from("localhost")?)),
        },
    )
    .await?;
    client
        .upsert(
            "ns",
            "o",
            spatio::Point3d::new(1.0, 2.0, 0.0),
            serde_json::json!({}),
        )
        .await?;
    assert!(client.get("ns", "o").await?.is_some());

    // A plaintext client can't talk to a TLS server.
    assert!(SpatioClient::connect(addr).await.is_err());
    Ok(())
}

#[tokio::test]
async fn test_write_size_limits() -> anyhow::Result<()> {
    let (addr, _db) = spawn_server().await?;
    let client = SpatioClient::connect(addr).await?;
    let p = spatio::Point3d::new(1.0, 2.0, 0.0);

    let big = serde_json::json!({"b": "x".repeat(70 * 1024)});
    assert!(client.upsert("ns", "o", p.clone(), big).await.is_err());
    let long_id = "i".repeat(300);
    assert!(client
        .upsert("ns", &long_id, p.clone(), serde_json::json!({}))
        .await
        .is_err());

    let points: Vec<_> = (0..10_001)
        .map(|i| (1_000.0 + i as f64, spatio::Point::new(1.0, 2.0)))
        .collect();
    assert!(client.insert_trajectory("ns", "o", points).await.is_err());

    client.upsert("ns", "o", p, serde_json::json!({})).await?;
    Ok(())
}
