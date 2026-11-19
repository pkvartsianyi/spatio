use spatio::{LineString, Point, Point3d, Polygon, Shape, Spatio};

fn square(lon: f64, lat: f64, d: f64) -> Shape {
    Shape::polygon(Polygon::from_coords(
        &[
            (lon, lat),
            (lon + d, lat),
            (lon + d, lat + d),
            (lon, lat + d),
            (lon, lat),
        ],
        vec![],
    ))
}

fn p(lon: f64, lat: f64) -> Point3d {
    Point3d::new(lon, lat, 0.0)
}

fn sample(db: &Spatio) {
    db.set_shape("f", "sq", square(0.0, 0.0, 0.01)).unwrap();
    db.set_shape("f", "circle", Shape::circle(Point::new(1.0, 1.0), 500.0))
        .unwrap();
    db.set_shape(
        "f",
        "road",
        Shape::corridor(LineString::from_coords(&[(2.0, 0.0), (2.0, 1.0)]), 100.0),
    )
    .unwrap();
}

#[test]
fn containing_and_within() {
    let db = Spatio::memory().unwrap();
    sample(&db);

    assert_eq!(
        db.shapes_containing("f", &p(0.005, 0.005), 10).unwrap(),
        vec!["sq"]
    );
    assert_eq!(
        db.shapes_containing("f", &p(1.003, 1.0), 10).unwrap(),
        vec!["circle"]
    );
    assert!(
        db.shapes_containing("f", &p(1.006, 1.0), 10)
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        db.shapes_containing("f", &p(2.0005, 0.5), 10).unwrap(),
        vec!["road"]
    );
    assert!(
        db.shapes_containing("f", &p(2.002, 0.5), 10)
            .unwrap()
            .is_empty()
    );
    assert!(
        db.shapes_containing("other", &p(0.005, 0.005), 10)
            .unwrap()
            .is_empty()
    );

    let near = db.shapes_within("f", &p(2.002, 0.5), 200.0, 10).unwrap();
    assert_eq!(near.len(), 1);
    assert!((near[0].1 - 122.4).abs() < 2.0, "{near:?}");
}

#[test]
fn within_is_nearest_first_and_limited() {
    let db = Spatio::memory().unwrap();
    for i in 0..5 {
        db.set_shape(
            "f",
            &format!("c{i}"),
            Shape::circle(Point::new(i as f64 * 0.01, 0.0), 10.0),
        )
        .unwrap();
    }
    let hits = db.shapes_within("f", &p(0.0, 0.0), 10_000.0, 3).unwrap();
    let ids: Vec<_> = hits.iter().map(|(id, _)| id.as_str()).collect();
    assert_eq!(ids, ["c0", "c1", "c2"]);
    assert_eq!(hits[0].1, 0.0);
}

#[test]
fn set_replaces_and_delete_removes() {
    let db = Spatio::memory().unwrap();
    db.set_shape("f", "a", square(0.0, 0.0, 0.01)).unwrap();
    db.set_shape("f", "a", square(5.0, 5.0, 0.01)).unwrap();
    assert!(
        db.shapes_containing("f", &p(0.005, 0.005), 10)
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        db.shapes_containing("f", &p(5.005, 5.005), 10).unwrap(),
        vec!["a"]
    );
    assert_eq!(
        db.get_shape("f", "a").unwrap(),
        Some(square(5.0, 5.0, 0.01))
    );

    assert!(db.delete_shape("f", "a").unwrap());
    assert!(!db.delete_shape("f", "a").unwrap());
    assert_eq!(db.get_shape("f", "a").unwrap(), None);
    assert!(db.list_shapes("f").unwrap().is_empty());
}

#[test]
fn shapes_and_objects_are_separate() {
    let db = Spatio::memory().unwrap();
    db.set_shape("f", "x", square(0.0, 0.0, 0.01)).unwrap();
    db.upsert("f", "x", p(0.005, 0.005), serde_json::json!({}), None)
        .unwrap();
    assert!(db.delete_shape("f", "x").unwrap());
    assert!(db.get("f", "x").unwrap().is_some());
}

#[test]
fn rejects_invalid_shapes() {
    let db = Spatio::memory().unwrap();
    let bad = [
        Shape::circle(Point::new(0.0, 0.0), 0.0),
        Shape::circle(Point::new(200.0, 0.0), 10.0),
        Shape::corridor(LineString::from_coords(&[(0.0, 0.0)]), 10.0),
        Shape::corridor(LineString::from_coords(&[(0.0, 0.0), (1.0, 1.0)]), f64::NAN),
        Shape::polygon(Polygon::from_coords(&[(0.0, 0.0), (1.0, 0.0)], vec![])),
    ];
    for s in bad {
        assert!(db.set_shape("f", "bad", s.clone()).is_err(), "{s:?}");
    }
    assert!(db.set_shape("f", "a|b", square(0.0, 0.0, 1.0)).is_err());
    assert!(db.shapes_within("f", &p(0.0, 0.0), -1.0, 10).is_err());
}

#[test]
fn shapes_survive_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db.log");
    {
        let db = Spatio::open(&path).unwrap();
        sample(&db);
        db.delete_shape("f", "circle").unwrap();
        db.close().unwrap();
    }
    let db = Spatio::open(&path).unwrap();
    let ids: Vec<_> = db
        .list_shapes("f")
        .unwrap()
        .into_iter()
        .map(|(id, _)| id)
        .collect();
    assert_eq!(ids, ["road", "sq"]);
    assert_eq!(
        db.shapes_containing("f", &p(0.005, 0.005), 10).unwrap(),
        vec!["sq"]
    );
}

#[test]
fn torn_tail_is_dropped_and_later_writes_survive() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db.log");
    let shapes_path = dir.path().join("db.log.shapes");
    {
        let db = Spatio::open(&path).unwrap();
        db.set_shape("f", "a", square(0.0, 0.0, 0.01)).unwrap();
        db.close().unwrap();
    }
    let mut content = std::fs::read_to_string(&shapes_path).unwrap();
    content.push_str("deadbeef|S|f|torn|{\"type\":\"cir");
    std::fs::write(&shapes_path, content).unwrap();
    {
        let db = Spatio::open(&path).unwrap();
        assert_eq!(db.list_shapes("f").unwrap().len(), 1);
        db.set_shape("f", "b", square(1.0, 1.0, 0.01)).unwrap();
        db.close().unwrap();
    }
    let db = Spatio::open(&path).unwrap();
    let ids: Vec<_> = db
        .list_shapes("f")
        .unwrap()
        .into_iter()
        .map(|(id, _)| id)
        .collect();
    assert_eq!(ids, ["a", "b"]);
}

#[test]
fn compact_rewrites_shape_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db.log");
    let shapes_path = dir.path().join("db.log.shapes");
    let db = Spatio::open(&path).unwrap();
    for _ in 0..50 {
        db.set_shape("f", "a", square(0.0, 0.0, 0.01)).unwrap();
    }
    let before = std::fs::metadata(&shapes_path).unwrap().len();
    db.compact(std::time::SystemTime::UNIX_EPOCH).unwrap();
    let after = std::fs::metadata(&shapes_path).unwrap().len();
    assert!(after * 10 < before, "{before} -> {after}");
    db.set_shape("f", "b", square(1.0, 1.0, 0.01)).unwrap();
    db.close().unwrap();
    drop(db);
    let db = Spatio::open(&path).unwrap();
    assert_eq!(db.list_shapes("f").unwrap().len(), 2);
}

#[test]
fn closed_db_rejects_shape_calls() {
    let db = Spatio::memory().unwrap();
    db.close().unwrap();
    assert!(db.set_shape("f", "a", square(0.0, 0.0, 0.01)).is_err());
    assert!(db.shapes_containing("f", &p(0.0, 0.0), 10).is_err());
}
