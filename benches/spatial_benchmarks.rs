use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use spatio::{Point, SetOptions, Spatio};
use std::sync::atomic::AtomicU64;
use std::time::Duration;

fn benchmark_basic_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("basic_operations");

    let db = Spatio::memory().unwrap();

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

    let db = Spatio::memory().unwrap();

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
            db.find_nearby(
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

    let db = Spatio::memory().unwrap();

    // Benchmark trajectory insertion
    group.bench_function("trajectory_insert", |b| {
        let mut counter = 0;
        b.iter(|| {
            let mut trajectory = Vec::new();
            let base_lat = 40.7128;
            let base_lon = -74.0060;
            let base_time = 1640995200u64 + ((counter % 10000) as u64) * 1000;

            for i in 0..100 {
                let lat = base_lat + (i as f64 * 0.0001);
                let lon = base_lon + (i as f64 * 0.0001);
                let point = Point::new(lat, lon);
                let timestamp = base_time + (i as u64) * 10;
                trajectory.push((point, timestamp));
            }

            let object_id = format!("trajectory:{}", counter);
            counter += 1;
            db.insert_trajectory(black_box(&object_id), black_box(&trajectory), None)
                .unwrap()
        })
    });

    // Setup trajectory data for querying
    let mut trajectory = Vec::new();
    for i in 0..1000 {
        let lat = 40.7128 + (i as f64 * 0.0001);
        let lon = -74.0060 + (i as f64 * 0.0001);
        let point = Point::new(lat, lon);
        let timestamp = 1640995200 + i * 10;
        trajectory.push((point, timestamp));
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

    let db = std::sync::Arc::new(Spatio::memory().unwrap());

    // Benchmark concurrent inserts
    group.bench_function("concurrent_inserts", |b| {
        let counter = std::sync::Arc::new(AtomicU64::new(0));
        b.iter(|| {
            let handles: Vec<_> = (0..10)
                .map(|thread_id| {
                    let db_clone = db.clone();
                    let counter_clone = counter.clone();
                    std::thread::spawn(move || {
                        for i in 0..10 {
                            let id =
                                counter_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                            let key = format!("concurrent:{}:{}", thread_id, i);
                            let value = format!("value:{}", id);
                            db_clone.insert(&key, value.as_bytes(), None).unwrap();
                        }
                    })
                })
                .collect();

            for handle in handles {
                handle.join().unwrap();
            }
        })
    });

    group.finish();
}

fn benchmark_ttl_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("ttl_operations");

    let db = Spatio::memory().unwrap();

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
        let db = Spatio::memory().unwrap();

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
                    db.find_nearby(
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

fn benchmark_persistence(c: &mut Criterion) {
    let mut group = c.benchmark_group("persistence");

    // Benchmark AOF operations
    group.bench_function("aof_write_operations", |b| {
        use tempfile::NamedTempFile;
        let temp_file = NamedTempFile::new().unwrap();
        let db = Spatio::open(temp_file.path()).unwrap();

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
    benchmark_persistence
);

criterion_main!(benches);
