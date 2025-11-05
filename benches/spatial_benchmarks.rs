use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use geo::Point;
use spatio::{Config, SetOptions, Spatio, SyncMode, SyncPolicy, TemporalPoint};
use std::time::{Duration, SystemTime};

fn benchmark_basic_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("basic_operations");

    let mut db = Spatio::memory().unwrap();

    // Benchmark single insert
    group.bench_function("single_insert", |b| {
        let mut counter = 0;
        b.iter(|| {
            let key = format!("key:{}", counter);
            let value = format!("value:{}", counter);
            counter += 1;
            db.insert(black_box(&key), black_box(value.as_bytes()), None)
                .unwrap()
        })
    });

    // Benchmark single get
    db.insert("benchmark_key", b"benchmark_value", None)
        .unwrap();
    group.bench_function("single_get", |b| {
        b.iter(|| db.get(black_box("benchmark_key")).unwrap())
    });

    // Benchmark batch operations
    group.bench_function("batch_insert_100", |b| {
        let mut counter = 0;
        b.iter(|| {
            let batch_start = counter;
            db.atomic(|batch| {
                for i in 0..100 {
                    let key = format!("batch_key:{}:{}", batch_start, i);
                    let value = format!("batch_value:{}", i);
                    batch.insert(&key, value.as_bytes(), None)?;
                }
                Ok(())
            })
            .unwrap();
            counter += 100;
        })
    });

    group.finish();
}

fn benchmark_spatial_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("spatial_operations");

    let mut db = Spatio::memory().unwrap();

    // Benchmark spatial point insertion
    group.bench_function("spatial_point_insert", |b| {
        let mut counter = 0;
        b.iter(|| {
            let lat = 40.7128 + ((counter % 1000) as f64 * 0.001);
            let lon = -74.0060 + ((counter % 1000) as f64 * 0.001);
            let point = Point::new(lat, lon);
            let _key = format!("spatial:{}", counter);
            let data = format!("data:{}", counter);
            counter += 1;
            db.insert_point(
                black_box("spatial_bench"),
                black_box(&point),
                black_box(data.as_bytes()),
                None,
            )
            .unwrap()
        })
    });

    // Setup data for spatial queries
    for i in 0..1000 {
        let lat = 40.7128 + (i as f64 * 0.0001);
        let lon = -74.0060 + (i as f64 * 0.0001);
        let point = Point::new(lat, lon);
        let data = format!("query_data:{}", i);
        db.insert_point("query_bench", &point, data.as_bytes(), None)
            .unwrap();
    }

    // Benchmark nearby search
    let center = Point::new(40.7128, -74.0060);
    group.bench_function("nearby_search", |b| {
        b.iter(|| {
            db.query_within_radius(
                black_box("query_bench"),
                black_box(&center),
                black_box(1000.0),
                black_box(10),
            )
            .unwrap()
        })
    });

    group.finish();
}

fn benchmark_trajectory_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("trajectory_operations");

    let mut db = Spatio::memory().unwrap();

    // Benchmark trajectory insertion
    group.bench_function("trajectory_insert", |b| {
        let mut counter = 0;
        b.iter(|| {
            let mut trajectory = Vec::new();
            let base_lat = 40.7128;
            let base_lon = -74.0060;
            let base_time = SystemTime::UNIX_EPOCH
                + Duration::from_secs(1640995200u64 + ((counter % 10000) as u64) * 1000);

            for i in 0..100 {
                let lat = base_lat + (i as f64 * 0.0001);
                let lon = base_lon + (i as f64 * 0.0001);
                let point = Point::new(lat, lon);
                let timestamp = base_time + Duration::from_secs((i as u64) * 10);
                trajectory.push(TemporalPoint { point, timestamp });
            }

            let object_id = format!("trajectory:{}", counter);
            counter += 1;
            db.insert_trajectory(black_box(&object_id), black_box(&trajectory), None)
                .unwrap()
        })
    });

    // Setup trajectory data for querying
    let mut trajectory = Vec::new();
    let base_time = SystemTime::UNIX_EPOCH + Duration::from_secs(1640995200);
    for i in 0..1000 {
        let lat = 40.7128 + (i as f64 * 0.0001);
        let lon = -74.0060 + (i as f64 * 0.0001);
        let point = Point::new(lat, lon);
        let timestamp = base_time + Duration::from_secs(i * 10);
        trajectory.push(TemporalPoint { point, timestamp });
    }
    db.insert_trajectory("benchmark_trajectory", &trajectory, None)
        .unwrap();

    // Benchmark trajectory queries
    group.bench_function("trajectory_query", |b| {
        b.iter(|| {
            db.query_trajectory(
                black_box("benchmark_trajectory"),
                black_box(1640995200),
                black_box(1640995200 + 5000),
            )
            .unwrap()
        })
    });

    group.finish();
}

fn benchmark_concurrent_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_operations");

    let mut db = Spatio::memory().unwrap();

    // Benchmark concurrent inserts
    group.bench_function("sequential_inserts", |b| {
        let mut counter = 0u64;
        b.iter(|| {
            for thread_id in 0..10 {
                for i in 0..10 {
                    let id = counter;
                    counter += 1;
                    let key = format!("sequential:{}:{}", thread_id, i);
                    let value = format!("value:{}", id);
                    db.insert(&key, value.as_bytes(), None).unwrap();
                }
            }
        });
    });

    group.finish();
}

fn benchmark_ttl_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("ttl_operations");

    let mut db = Spatio::memory().unwrap();

    // Benchmark TTL insertion
    group.bench_function("ttl_insert", |b| {
        let mut counter = 0;
        b.iter(|| {
            let key = format!("ttl_key:{}", counter % 100000);
            let value = format!("ttl_value:{}", counter);
            let opts = SetOptions::with_ttl(Duration::from_secs(60));
            counter += 1;
            db.insert(
                black_box(&key),
                black_box(value.as_bytes()),
                black_box(Some(opts)),
            )
            .unwrap()
        })
    });

    group.finish();
}

fn benchmark_large_datasets(c: &mut Criterion) {
    let mut group = c.benchmark_group("large_datasets");
    group.sample_size(10); // Fewer samples for large datasets
    group.measurement_time(Duration::from_secs(30));

    for dataset_size in [1000, 10000, 100000].iter() {
        let mut db = Spatio::memory().unwrap();

        // Pre-populate with spatial data
        for i in 0..*dataset_size {
            let lat = 40.0 + (i as f64 * 0.00001);
            let lon = -74.0 + (i as f64 * 0.00001);
            let point = Point::new(lat, lon);
            let data = format!("data:{}", i);
            db.insert_point("large_dataset", &point, data.as_bytes(), None)
                .unwrap();
        }

        group.bench_with_input(
            BenchmarkId::new("large_dataset_query", dataset_size),
            dataset_size,
            |b, &_size| {
                let center = Point::new(40.5, -74.5);
                b.iter(|| {
                    db.query_within_radius(
                        black_box("large_dataset"),
                        black_box(&center),
                        black_box(10000.0),
                        black_box(100),
                    )
                    .unwrap()
                })
            },
        );
    }

    group.finish();
}

fn benchmark_3d_spatial_operations(c: &mut Criterion) {
    use spatio::Point3d;

    let mut group = c.benchmark_group("3d_spatial_operations");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(20));

    for dataset_size in [1000, 10000, 50000].iter() {
        let mut db = Spatio::memory().unwrap();

        // Pre-populate with 3D spatial data (aircraft/drone positions)
        for i in 0..*dataset_size {
            let lat = 40.0 + (i as f64 * 0.00001);
            let lon = -74.0 + (i as f64 * 0.00001);
            let alt = 1000.0 + ((i % 10000) as f64 * 0.5);
            let point = Point3d::new(lon, lat, alt);
            let data = format!("aircraft:{}", i);
            db.insert_point_3d("aircraft", &point, data.as_bytes(), None)
                .unwrap();
        }

        // Benchmark spherical query (3D radius search)
        group.bench_with_input(
            BenchmarkId::new("3d_sphere_query", dataset_size),
            dataset_size,
            |b, &_size| {
                let center = Point3d::new(-74.0, 40.0, 3000.0);
                b.iter(|| {
                    db.query_within_sphere_3d(
                        black_box("aircraft"),
                        black_box(&center),
                        black_box(5000.0),
                        black_box(100),
                    )
                    .unwrap()
                })
            },
        );

        // Benchmark cylindrical query (altitude-constrained radius)
        group.bench_with_input(
            BenchmarkId::new("3d_cylinder_query", dataset_size),
            dataset_size,
            |b, &_size| {
                let center = Point3d::new(-74.0, 40.0, 0.0);
                b.iter(|| {
                    db.query_within_cylinder_3d(
                        black_box("aircraft"),
                        black_box(&center),
                        black_box(10000.0),
                        black_box(2000.0),
                        black_box(4000.0),
                        black_box(100),
                    )
                    .unwrap()
                })
            },
        );

        // Benchmark 3D KNN
        group.bench_with_input(
            BenchmarkId::new("3d_knn", dataset_size),
            dataset_size,
            |b, &_size| {
                let query_point = Point3d::new(-74.0, 40.0, 3000.0);
                b.iter(|| {
                    db.knn_3d(
                        black_box("aircraft"),
                        black_box(&query_point),
                        black_box(10),
                    )
                    .unwrap()
                })
            },
        );
    }

    group.finish();
}

fn benchmark_persistence(c: &mut Criterion) {
    let mut group = c.benchmark_group("persistence");

    // Benchmark AOF operations
    group.bench_function("aof_write_operations", |b| {
        use tempfile::NamedTempFile;
        let temp_file = NamedTempFile::new().unwrap();
        let mut db = Spatio::open(temp_file.path()).unwrap();

        let mut counter = 0;
        b.iter(|| {
            let key = format!("persist_key:{}", counter);
            let value = format!("persist_value:{}", counter);
            counter += 1;
            db.insert(black_box(&key), black_box(value.as_bytes()), None)
                .unwrap();
            // Force sync to measure actual persistence cost
            db.sync().unwrap();
        })
    });

    group.bench_function("aof_write_operations_fdatasync", |b| {
        use tempfile::NamedTempFile;
        let temp_file = NamedTempFile::new().unwrap();
        let config = Config::default().with_sync_mode(SyncMode::Data);
        let mut db = Spatio::open_with_config(temp_file.path(), config).unwrap();

        let mut counter = 0;
        b.iter(|| {
            let key = format!("persist_key:fdatasync:{}", counter);
            let value = format!("persist_value:fdatasync:{}", counter);
            counter += 1;
            db.insert(black_box(&key), black_box(value.as_bytes()), None)
                .unwrap();
            db.sync().unwrap();
        })
    });

    group.bench_function("aof_sync_always_batch1", |b| {
        use tempfile::NamedTempFile;
        let temp_file = NamedTempFile::new().unwrap();
        let config = Config::default().with_sync_policy(SyncPolicy::Always);
        let mut db = Spatio::open_with_config(temp_file.path(), config).unwrap();

        let mut counter = 0;
        b.iter(|| {
            let key = format!("persist_key:always:{}", counter);
            let value = format!("persist_value:always:{}", counter);
            counter += 1;
            db.insert(black_box(&key), black_box(value.as_bytes()), None)
                .unwrap();
        });

        db.sync().unwrap();
    });

    group.bench_function("aof_sync_always_batch8_single_insert", |b| {
        use tempfile::NamedTempFile;
        let temp_file = NamedTempFile::new().unwrap();
        let config = Config::default()
            .with_sync_policy(SyncPolicy::Always)
            .with_sync_batch_size(8);
        let mut db = Spatio::open_with_config(temp_file.path(), config).unwrap();

        let mut counter = 0;
        b.iter(|| {
            let key = format!("persist_key:batch8:{}", counter);
            let value = format!("persist_value:batch8:{}", counter);
            counter += 1;
            db.insert(black_box(&key), black_box(value.as_bytes()), None)
                .unwrap();
        });

        db.sync().unwrap();
    });

    group.finish();
}

criterion_group!(
    benches,
    benchmark_basic_operations,
    benchmark_spatial_operations,
    benchmark_trajectory_operations,
    benchmark_concurrent_operations,
    benchmark_ttl_operations,
    benchmark_large_datasets,
    benchmark_3d_spatial_operations,
    benchmark_persistence
);

criterion_main!(benches);
