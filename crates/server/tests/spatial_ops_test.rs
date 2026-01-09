use futures::{SinkExt, StreamExt};
use spatio::{Point3d, Spatio};
use spatio_server::rpc::{Command, ResponsePayload, ResponseStatus};
use spatio_server::{RpcClientCodec, run_server};
use spatio_types::geo::Polygon;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

#[tokio::test]
async fn test_spatial_rpc_commands() -> anyhow::Result<()> {
    tracing_subscriber::fmt::try_init().ok();
    // Setup server
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

    // Connect client
    let stream = TcpStream::connect(bound_addr).await?;
    let mut framed = Framed::new(stream, RpcClientCodec);

    let namespace = "spatial_test";

    // 1. Insert Data
    // A: (-74.0, 40.7) matches NYC
    // B: (-73.0, 40.7) (Longitude +1 degree)
    // C: (-74.0, 41.7) (Latitude +1 degree)
    let p1 = Point3d::new(-74.0, 40.7, 0.0);
    let p2 = Point3d::new(-73.0, 40.7, 0.0);
    let p3 = Point3d::new(-74.0, 41.7, 0.0);

    framed
        .send(Command::Upsert {
            namespace: namespace.into(),
            id: "p1".into(),
            point: p1.clone(),
            metadata: vec![],
            opts: None,
        })
        .await?;
    assert!(matches!(
        framed.next().await.unwrap()?,
        (ResponseStatus::Ok, ResponsePayload::Ok)
    ));

    framed
        .send(Command::Upsert {
            namespace: namespace.into(),
            id: "p2".into(),
            point: p2.clone(),
            metadata: vec![],
            opts: None,
        })
        .await?;
    assert!(matches!(
        framed.next().await.unwrap()?,
        (ResponseStatus::Ok, ResponsePayload::Ok)
    ));

    framed
        .send(Command::Upsert {
            namespace: namespace.into(),
            id: "p3".into(),
            point: p3.clone(),
            metadata: vec![],
            opts: None,
        })
        .await?;
    assert!(matches!(
        framed.next().await.unwrap()?,
        (ResponseStatus::Ok, ResponsePayload::Ok)
    ));

    // 2. Test Distance (between p1 and p2)
    framed
        .send(Command::Distance {
            namespace: namespace.into(),
            id1: "p1".into(),
            id2: "p2".into(),
            metric: None, // Default Haversine
        })
        .await?;

    if let Some(Ok((status, payload))) = framed.next().await {
        assert!(matches!(status, ResponseStatus::Ok));
        if let ResponsePayload::OptionalDistance(Some(d)) = payload {
            // Distance ~84km at 40N
            assert!(
                d > 80_000.0 && d < 90_000.0,
                "Distance {} not in expected range",
                d
            );
        } else {
            panic!("Expected OptionalDistance(Some(f64)), got {:?}", payload);
        }
    } else {
        panic!("Distance failed response");
    }

    // 3. Test DistanceTo (p1 to origin 0,0)
    framed
        .send(Command::DistanceTo {
            namespace: namespace.into(),
            id: "p1".into(),
            point: Point3d::new(0.0, 0.0, 0.0),
            metric: None,
        })
        .await?;
    if let Some(Ok((status, payload))) = framed.next().await {
        assert!(matches!(status, ResponseStatus::Ok));
        if let ResponsePayload::OptionalDistance(Some(d)) = payload {
            assert!(d > 1_000_000.0);
        } else {
            panic!("Expected OptionalDistance(Some(f64)), got {:?}", payload);
        }
    } else {
        panic!("DistanceTo failed response");
    }

    // 4. Test BoundingBox
    framed
        .send(Command::BoundingBox {
            namespace: namespace.into(),
        })
        .await?;
    if let Some(Ok((status, payload))) = framed.next().await {
        assert!(matches!(status, ResponseStatus::Ok));
        if let ResponsePayload::BoundingBox(bbox) = payload {
            assert!((bbox.min_x() - -74.0).abs() < 1e-6);
            assert!((bbox.max_x() - -73.0).abs() < 1e-6);
            assert!((bbox.min_y() - 40.7).abs() < 1e-6);
            assert!((bbox.max_y() - 41.7).abs() < 1e-6);
        } else {
            panic!("Expected BoundingBox, got {:?}", payload);
        }
    } else {
        panic!("BoundingBox failed response");
    }

    // 5. Test ConvexHull
    framed
        .send(Command::ConvexHull {
            namespace: namespace.into(),
        })
        .await?;
    if let Some(Ok((status, payload))) = framed.next().await {
        assert!(matches!(status, ResponseStatus::Ok));
        if let ResponsePayload::Polygon(poly) = payload {
            // With 3 points, hull should be triangle (4 coords closed)
            if poly.exterior().coords().count() < 3 {
                panic!(
                    "Convex Hull should have at least 3 points, got {}",
                    poly.exterior().coords().count()
                );
            }
        } else {
            panic!("Expected Polygon, got {:?}", payload);
        }
    } else {
        panic!("ConvexHull failed response");
    }

    // 6. Test Contains (formerly QueryPolygon)
    // Create polygon surrounding p1 (-74.0, 40.7) only
    // Box from (-74.1, 40.6) to (-73.9, 40.8)
    let poly = Polygon::from_coords(
        &[
            (-74.1, 40.6),
            (-73.9, 40.6),
            (-73.9, 40.8),
            (-74.1, 40.8),
            (-74.1, 40.6),
        ],
        vec![],
    );

    framed
        .send(Command::Contains {
            namespace: namespace.into(),
            polygon: poly,
            limit: 10,
        })
        .await?;
    if let Some(Ok((status, payload))) = framed.next().await {
        assert!(matches!(status, ResponseStatus::Ok));
        if let ResponsePayload::ObjectList(list) = payload {
            assert_eq!(list.len(), 1);
            assert_eq!(list[0].0, "p1");
        } else {
            panic!("Expected ObjectList, got {:?}", payload);
        }
    } else {
        panic!("Contains failed response");
    }

    Ok(())
}
