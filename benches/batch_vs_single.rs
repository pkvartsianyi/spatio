use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use spatio::{Config, Spatio, SyncPolicy};

fn bench_single_writes(c: &mut Criterion) {
    let mut group = c.benchmark_group("single_writes");

    for num_ops in [10, 100, 1000].iter() {
        group.throughput(Throughput::Elements(*num_ops as u64));

        group.bench_with_input(BenchmarkId::new("sync_never", num_ops), num_ops, |b, &n| {
            let config = Config::default().with_sync_policy(SyncPolicy::Never);
            let mut db = Spatio::memory_with_config(config).unwrap();

            b.iter(|| {
                for i in 0..n {
                    let key = format!("key_{}", i);
                    db.insert(&key, b"value", None).unwrap();
                }
            });
        });

        group.bench_with_input(
            BenchmarkId::new("sync_always", num_ops),
            num_ops,
            |b, &n| {
                let config = Config::default()
                    .with_sync_policy(SyncPolicy::Always)
                    .with_sync_batch_size(1);
                let mut db = Spatio::memory_with_config(config).unwrap();

                b.iter(|| {
                    for i in 0..n {
                        let key = format!("key_{}", i);
                        db.insert(&key, b"value", None).unwrap();
                    }
                });
            },
        );
    }

    group.finish();
}

fn bench_batch_writes(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_writes");

    for num_ops in [10, 100, 1000].iter() {
        group.throughput(Throughput::Elements(*num_ops as u64));

        group.bench_with_input(BenchmarkId::new("sync_never", num_ops), num_ops, |b, &n| {
            let config = Config::default().with_sync_policy(SyncPolicy::Never);
            let mut db = Spatio::memory_with_config(config).unwrap();

            b.iter(|| {
                db.atomic(|batch| {
                    for i in 0..n {
                        let key = format!("key_{}", i);
                        batch.insert(&key, b"value", None)?;
                    }
                    Ok(())
                })
                .unwrap();
            });
        });

        group.bench_with_input(
            BenchmarkId::new("sync_always", num_ops),
            num_ops,
            |b, &n| {
                let config = Config::default()
                    .with_sync_policy(SyncPolicy::Always)
                    .with_sync_batch_size(1);
                let mut db = Spatio::memory_with_config(config).unwrap();

                b.iter(|| {
                    db.atomic(|batch| {
                        for i in 0..n {
                            let key = format!("key_{}", i);
                            batch.insert(&key, b"value", None)?;
                        }
                        Ok(())
                    })
                    .unwrap();
                });
            },
        );
    }

    group.finish();
}

// DISABLED: DB is now !Send + !Sync by design. Use SyncDB wrapper or actor pattern for concurrency.
// This benchmark is incompatible with the new single-threaded DB design.
// For multi-threaded benchmarks, use:
// 1. SyncDB wrapper (with 'sync' feature)
// 2. Actor pattern (recommended for async usage)
// 3. Manual Arc<RwLock<DB>> wrapper
#[allow(dead_code)]
fn bench_concurrent_operations(_c: &mut Criterion) {
    // Benchmark disabled - requires thread-safe wrapper
}

// DISABLED: DB is now !Send + !Sync by design. Use SyncDB wrapper or actor pattern for concurrency.
#[allow(dead_code)]
fn bench_read_heavy_workload(_c: &mut Criterion) {
    // Benchmark disabled - requires thread-safe wrapper
}

// DISABLED: DB is now !Send + !Sync by design. Use SyncDB wrapper or actor pattern for concurrency.
#[allow(dead_code)]
fn bench_write_heavy_workload(_c: &mut Criterion) {
    // Benchmark disabled - requires thread-safe wrapper
}

// DISABLED: DB is now !Send + !Sync by design. Use SyncDB wrapper or actor pattern for concurrency.
#[allow(dead_code)]
fn bench_mixed_workload(_c: &mut Criterion) {
    // Benchmark disabled - requires thread-safe wrapper
}

criterion_group!(
    benches,
    bench_single_writes,
    bench_batch_writes,
    // Concurrent benchmarks disabled - DB is now !Send + !Sync by design
    // bench_concurrent_operations,
    // bench_read_heavy_workload,
    // bench_write_heavy_workload,
    // bench_mixed_workload,
);
criterion_main!(benches);
