#[cfg(test)]
mod tests {
    use spatio::DB;
    use spatio_types::point::Point3d;
    use tempfile::tempdir;

    #[test]
    fn test_db_restart_preserves_current_locations() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");

        // Create DB and insert locations
        {
            let db = DB::open(&db_path).unwrap();

            db.update_location(
                "vehicles",
                "truck_001",
                Point3d::new(-74.0, 40.0, 0.0),
                serde_json::json!({"v": 1}),
            )
            .unwrap();

            db.update_location(
                "vehicles",
                "truck_002",
                Point3d::new(-74.1, 40.1, 0.0),
                serde_json::json!({"v": 2}),
            )
            .unwrap();

            db.update_location(
                "aircraft",
                "plane_001",
                Point3d::new(-75.0, 41.0, 5000.0),
                serde_json::json!({"type": "flight"}),
            )
            .unwrap();

            // Verify before close
            let results = db
                .query_current_within_radius(
                    "vehicles",
                    &Point3d::new(-74.0, 40.0, 0.0),
                    50000.0,
                    10,
                )
                .unwrap();
            assert_eq!(results.len(), 2);
        } // DB drops here

        // Reopen DB
        {
            let db = DB::open(&db_path).unwrap();

            // Query should return recovered locations
            let vehicles = db
                .query_current_within_radius(
                    "vehicles",
                    &Point3d::new(-74.0, 40.0, 0.0),
                    50000.0,
                    10,
                )
                .unwrap();
            assert_eq!(vehicles.len(), 2, "Should recover 2 vehicles");

            let aircraft = db
                .query_current_within_radius(
                    "aircraft",
                    &Point3d::new(-75.0, 41.0, 5000.0),
                    10000.0,
                    10,
                )
                .unwrap();
            assert_eq!(aircraft.len(), 1, "Should recover 1 aircraft");

            // Verify specific object
            let truck = vehicles
                .iter()
                .find(|v| v.object_id == "truck_001")
                .unwrap();
            assert_eq!(truck.position.x(), -74.0);
            assert_eq!(truck.position.y(), 40.0);
        }
    }
}
