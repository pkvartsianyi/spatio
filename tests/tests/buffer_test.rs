use spatio::config::PersistenceConfig;
use spatio::Spatio;
use spatio_types::point::Point3d;
use std::fs;

#[test]
fn test_buffered_writes() -> anyhow::Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("spatio_buffer_test.db");
    let log_path = db_path.clone();

    let config = spatio::config::Config::default()
        .with_buffer_capacity(100) // This is read buffer
        ;

    let mut config = config;
    config.persistence = PersistenceConfig { buffer_size: 10 };

    let db = Spatio::open_with_config(&db_path, config)?;
    let namespace = "test_ns";

    for i in 0..9 {
        let id = format!("p{}", i);
        let pos = Point3d::new(i as f64, 0.0, 0.0);
        db.upsert(namespace, &id, pos, serde_json::json!({}), None)?;
    }

    let initial_metadata = fs::metadata(&log_path).ok();
    let initial_size = initial_metadata.map(|m| m.len()).unwrap_or(0);

    assert_eq!(initial_size, 0, "Log file should be empty (buffered)");

    let id = "p9";
    let pos = Point3d::new(9.0, 0.0, 0.0);
    db.upsert(namespace, id, pos, serde_json::json!({}), None)?;

    let flushed_metadata = fs::metadata(&log_path)?;
    assert!(
        flushed_metadata.len() > 0,
        "Log file should contain data after flush"
    );

    Ok(())
}
