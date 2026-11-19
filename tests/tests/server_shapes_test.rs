use spatio::{Point3d, Spatio};
use spatio_client::{ClientError, SpatioClient};
use spatio_server::handler::MAX_SHAPE_VERTICES;
use spatio_server::run_server;
use spatio_types::geo::{LineString, Point, Polygon};
use spatio_types::shape::Shape;
use std::sync::Arc;

async fn start() -> anyhow::Result<SpatioClient> {
    let db = Arc::new(Spatio::builder().build()?);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    tokio::spawn(async move {
        let _ = run_server(listener, db, futures::future::pending()).await;
    });
    Ok(SpatioClient::connect(addr).await?)
}

#[tokio::test]
async fn shape_rpcs_round_trip() -> anyhow::Result<()> {
    let client = start().await?;
    let ns = "zones";

    let square = Shape::polygon(Polygon::from_coords(
        &[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0)],
        vec![],
    ));
    let depot = Shape::circle(Point::new(0.5, 0.5), 1_000.0);
    let route = Shape::corridor(LineString::from_coords(&[(1.01, 0.0), (1.01, 1.0)]), 500.0);

    client.set_shape(ns, "square", square.clone()).await?;
    client.set_shape(ns, "depot", depot.clone()).await?;
    client.set_shape(ns, "route", route.clone()).await?;

    assert_eq!(client.get_shape(ns, "square").await?, Some(square.clone()));
    assert_eq!(client.get_shape(ns, "route").await?, Some(route.clone()));
    assert_eq!(client.get_shape(ns, "missing").await?, None);

    let listed = client.list_shapes(ns, None, 100).await?;
    let ids: Vec<&str> = listed.iter().map(|(id, _)| id.as_str()).collect();
    assert_eq!(ids, ["depot", "route", "square"]);
    assert_eq!(listed[1].1, route);
    let page = client.list_shapes(ns, Some("depot"), 1).await?;
    assert_eq!(page.len(), 1);
    assert_eq!(page[0].0, "route");

    let centre = Point3d::new(0.5, 0.5, 0.0);
    assert_eq!(
        client.shapes_containing(ns, centre.clone(), 10).await?,
        ["depot", "square"]
    );
    assert_eq!(client.shapes_containing(ns, centre, 1).await?.len(), 1);

    let near_route = Point3d::new(1.008, 0.5, 0.0);
    let within = client
        .shapes_within(ns, near_route.clone(), 2_000.0, 10)
        .await?;
    assert_eq!(within.len(), 2, "{within:?}");
    assert_eq!(within[0].0, "route");
    assert_eq!(within[0].1, 0.0);
    assert_eq!(within[1].0, "square");
    assert!(within[1].1 > 850.0 && within[1].1 < 950.0, "{within:?}");
    assert!(client
        .shapes_within(ns, near_route, 100.0, 10)
        .await?
        .iter()
        .all(|(id, _)| id == "route"));

    assert!(client.delete_shape(ns, "depot").await?);
    assert!(!client.delete_shape(ns, "depot").await?);
    assert_eq!(client.get_shape(ns, "depot").await?, None);
    assert_eq!(client.list_shapes(ns, None, 100).await?.len(), 2);

    Ok(())
}

#[tokio::test]
async fn shape_rpcs_reject_bad_input() -> anyhow::Result<()> {
    let client = start().await?;

    let coords: Vec<(f64, f64)> = (0..=MAX_SHAPE_VERTICES)
        .map(|i| (i as f64 * 1e-6, 0.0))
        .collect();
    let big = Shape::corridor(LineString::from_coords(&coords), 10.0);
    match client.set_shape("ns", "big", big).await {
        Err(ClientError::Server(msg)) => assert!(msg.contains("vertices"), "{msg}"),
        other => panic!("expected server error, got {other:?}"),
    }

    let long = "x".repeat(300);
    let circle = Shape::circle(Point::new(0.0, 0.0), 10.0);
    assert!(matches!(
        client.set_shape("ns", &long, circle).await,
        Err(ClientError::Server(_))
    ));
    assert!(matches!(
        client.delete_shape(&long, "id").await,
        Err(ClientError::Server(_))
    ));

    assert!(matches!(
        client
            .shapes_within("ns", Point3d::new(0.0, 0.0, 0.0), -1.0, 10)
            .await,
        Err(ClientError::Server(_))
    ));
    assert!(matches!(
        client
            .shapes_containing("ns", Point3d::new(200.0, 0.0, 0.0), 10)
            .await,
        Err(ClientError::Server(_))
    ));

    Ok(())
}
