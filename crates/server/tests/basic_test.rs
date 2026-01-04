use futures::{SinkExt, StreamExt};
use spatio::{Point3d, Spatio};
use spatio_server::protocol::{Command, ResponsePayload, ResponseStatus};
use spatio_server::{run_server, SBPClientCodec};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

#[tokio::test]
async fn test_rpc_lifecycle() -> anyhow::Result<()> {
    tracing_subscriber::fmt::try_init().ok();
    // Start server in background
    let addr: SocketAddr = "127.0.0.1:0".parse()?;
    let db = Arc::new(Spatio::builder().build()?);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    let bound_addr = listener.local_addr()?;
    drop(listener); // We'll re-bind in run_server, but this is a race condition risk.
                    // Better: let run_server take the listener or use a retry.

    // Actually, let's just use a fixed port for simplicity in test if possible, or refactor run_server.
    let server_db = db.clone();
    tokio::spawn(async move {
        let _ = run_server(bound_addr, server_db, Box::pin(futures::future::pending())).await;
    });

    // Wait for server to start
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Connect client
    let stream = TcpStream::connect(bound_addr).await?;
    let mut framed = Framed::new(stream, SBPClientCodec);

    // Send UPSERT
    let upsert = Command::Upsert {
        namespace: "test_ns".into(),
        id: "obj1".into(),
        point: Point3d::new(1.0, 2.0, 3.0),
        metadata: serde_json::to_vec(&serde_json::json!({"key": "val"})).unwrap(),
        opts: None,
    };
    framed.send(upsert).await?;

    let resp = framed.next().await;
    match resp {
        Some(Ok((status, payload))) => {
            assert!(matches!(status, ResponseStatus::Ok));
            assert!(matches!(payload, ResponsePayload::Ok));
        }
        Some(Err(e)) => panic!("Received error: {}", e),
        None => panic!("Connection closed by server"),
    }

    // Send GET
    let get = Command::Get {
        namespace: "test_ns".into(),
        id: "obj1".into(),
    };
    framed.send(get).await?;

    if let Some(Ok((status, payload))) = framed.next().await {
        assert!(matches!(status, ResponseStatus::Ok));
        if let ResponsePayload::Object { id, .. } = payload {
            assert_eq!(id, "obj1");
        } else {
            panic!("Unexpected payload: {:?}", payload);
        }
    } else {
        panic!("Failed to receive GET response");
    }

    Ok(())
}

#[tokio::test]
async fn test_trajectory_rpc() -> anyhow::Result<()> {
    tracing_subscriber::fmt::try_init().ok();
    let addr: SocketAddr = "127.0.0.1:0".parse()?;
    let db = Arc::new(Spatio::builder().build()?);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let bound_addr = listener.local_addr()?;
    drop(listener);

    let server_db = db.clone();
    tokio::spawn(async move {
        let _ = run_server(bound_addr, server_db, Box::pin(futures::future::pending())).await;
    });

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let stream = TcpStream::connect(bound_addr).await?;
    let mut framed = Framed::new(stream, SBPClientCodec);

    // Send InsertTrajectory
    let tp = spatio_types::point::TemporalPoint {
        point: spatio_types::geo::Point::new(10.0, 10.0),
        timestamp: std::time::SystemTime::now(),
    };

    let insert = Command::InsertTrajectory {
        namespace: "traj_ns".into(),
        id: "truck1".into(),
        trajectory: vec![tp.clone()],
    };
    framed.send(insert).await?;

    let resp = framed.next().await;
    assert!(matches!(resp.unwrap().unwrap().1, ResponsePayload::Ok));

    // Send QueryTrajectory
    let query = Command::QueryTrajectory {
        namespace: "traj_ns".into(),
        id: "truck1".into(),
        start_time: tp.timestamp - std::time::Duration::from_secs(60),
        end_time: tp.timestamp + std::time::Duration::from_secs(60),
        limit: 10,
    };
    framed.send(query).await?;

    let resp = framed.next().await;
    if let ResponsePayload::Trajectory(updates) = resp.unwrap().unwrap().1 {
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].position.x(), 10.0);
    } else {
        panic!("Expected Trajectory payload");
    }

    Ok(())
}
