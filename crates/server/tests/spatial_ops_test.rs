use spatio::{Point3d, Spatio};
use spatio_client::SpatioClient;
use spatio_server::run_server;
use spatio_types::geo::Polygon;
use std::sync::Arc;

#[tokio::test]
async fn test_spatial_rpc_commands() -> anyhow::Result<()> {
    tracing_subscriber::fmt::try_init().ok();
    // Setup server
    let _addr: std::net::SocketAddr = "127.0.0.1:0".parse()?;
    let db = Arc::new(Spatio::builder().build()?);
    let server_db = db.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let bound_addr = listener.local_addr()?;

    tokio::spawn(async move {
        let _ = run_server(listener, server_db, futures::future::pending()).await;
    });

    // Give server a moment to bind (since the listener was dropped, it has to rebind)
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Connect client
    let client = SpatioClient::connect(bound_addr).await?;

    let namespace = "spatial_test";

    // 1. Insert Data
    let p1 = Point3d::new(-74.0, 40.7, 0.0);
    let p2 = Point3d::new(-73.0, 40.7, 0.0);
    let p3 = Point3d::new(-74.0, 41.7, 0.0);

    client
        .upsert(namespace, "p1", p1.clone(), serde_json::json!({}), None)
        .await?;
    client
        .upsert(namespace, "p2", p2.clone(), serde_json::json!({}), None)
        .await?;
    client
        .upsert(namespace, "p3", p3.clone(), serde_json::json!({}), None)
        .await?;

    // 2. Test Distance (between p1 and p2)
    // Default metric is Haversine
    let dist = client.distance(namespace, "p1", "p2", None).await?;
    if let Some(d) = dist {
        // Distance ~84km at 40N.
        // Wait, longitude change of 1 degree at 40N is ~85km.
        assert!(
            d > 80_000.0 && d < 90_000.0,
            "Distance {} not in expected range",
            d
        );
    } else {
        panic!("Expected distance, got None");
    }

    // 3. Test DistanceTo (p1 to origin 0,0)
    let dist_to = client
        .distance_to(
            namespace,
            "p1",
            spatio_types::geo::Point::new(0.0, 0.0),
            None,
        )
        .await?;
    if let Some(d) = dist_to {
        assert!(d > 1_000_000.0);
    } else {
        panic!("Expected distance_to, got None");
    }

    // 4. Test BoundingBox
    let bbox = client.bounding_box(namespace).await?;
    if let Some(bbox) = bbox {
        assert!((bbox.min_x() - -74.0).abs() < 1e-6);
        assert!((bbox.max_x() - -73.0).abs() < 1e-6);
        assert!((bbox.min_y() - 40.7).abs() < 1e-6);
        assert!((bbox.max_y() - 41.7).abs() < 1e-6);
    } else {
        panic!("Expected BoundingBox, got None");
    }

    // 5. Test ConvexHull
    let hull = client.convex_hull(namespace).await?;
    if let Some(poly) = hull {
        if poly.exterior().coords().count() < 3 {
            panic!("Convex Hull should have at least 3 points");
        }
    } else {
        panic!("Expected ConvexHull, got None");
    }

    // 6. Test Contains
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

    let contains = client.contains(namespace, poly, 10).await?;
    assert_eq!(contains.len(), 1);
    assert_eq!(contains[0].object_id, "p1");

    Ok(())
}
