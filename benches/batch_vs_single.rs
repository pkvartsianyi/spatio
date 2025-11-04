use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use spatio::{Config, Spatio, SyncPolicy};
use std::sync::Arc;
use std::thread;

fn bench_single_writes(c: &mut Criterion) {
    let mut group = c.benchmark_group("single_writes");

    for num_ops in [10, 100, 1000].iter() {
        group.throughput(Throughput::Elements(*num_ops as u64));

        group.bench_with_input(BenchmarkId::new("sync_never", num_ops), num_ops, |b, &n| {
            let config = Config::default().with_sync_policy(SyncPolicy::Never);
            let db = Spatio::memory_with_config(config).unwrap();

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
                let db = Spatio::memory_with_config(config).unwrap();

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
            let db = Spatio::memory_with_config(config).unwrap();

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
                let db = Spatio::memory_with_config(config).unwrap();

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

fn bench_concurrent_writes(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_writes");

    for num_threads in [2, 4, 8].iter() {
        let ops_per_thread = 100;
        group.throughput(Throughput::Elements((num_threads * ops_per_thread) as u64));

        // Single writes concurrent
        group.bench_with_input(
            BenchmarkId::new("single_writes", num_threads),
            num_threads,
            |b, &n| {
                let config = Config::default().with_sync_policy(SyncPolicy::Never);
                let db = Arc::new(Spatio::memory_with_config(config).unwrap());

                b.iter(|| {
                    let handles: Vec<_> = (0..n)
                        .map(|t| {
                            let db = db.clone();
                            thread::spawn(move || {
                                for i in 0..ops_per_thread {
                                    let key = format!("thread_{}_key_{}", t, i);
                                    db.insert(&key, b"value", None).unwrap();
                                }
                            })
                        })
                        .collect();

                    for h in handles {
                        h.join().unwrap();
                    }
                });
            },
        );

        // Batch writes concurrent
        group.bench_with_input(
            BenchmarkId::new("batch_writes", num_threads),
            num_threads,
            |b, &n| {
                let config = Config::default().with_sync_policy(SyncPolicy::Never);
                let db = Arc::new(Spatio::memory_with_config(config).unwrap());

                b.iter(|| {
                    let handles: Vec<_> = (0..n)
                        .map(|t| {
                            let db = db.clone();
                            thread::spawn(move || {
                                db.atomic(|batch| {
                                    for i in 0..ops_per_thread {
                                        let key = format!("thread_{}_key_{}", t, i);
                                        batch.insert(&key, b"value", None)?;
                                    }
                                    Ok(())
                                })
                                .unwrap();
                            })
                        })
                        .collect();

                    for h in handles {
                        h.join().unwrap();
                    }
                });
            },
        );
    }

    group.finish();
}

fn bench_read_heavy_workload(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_heavy_90_10");

    // 90% reads, 10% writes
    for num_threads in [2, 4, 8].iter() {
        group.throughput(Throughput::Elements((num_threads * 1000) as u64));

        group.bench_with_input(
            BenchmarkId::new("single_writes", num_threads),
            num_threads,
            |b, &n| {
                let config = Config::default().with_sync_policy(SyncPolicy::Never);
                let db = Arc::new(Spatio::memory_with_config(config).unwrap());

                // Pre-populate
                for i in 0u32..1000 {
                    db.insert(format!("key_{}", i), b"value", None).unwrap();
                }

                b.iter(|| {
                    let handles: Vec<_> = (0..n)
                        .map(|t| {
                            let db = db.clone();
                            thread::spawn(move || {
                                for i in 0u32..1000 {
                                    if i % 10 == 0 {
                                        // 10% writes
                                        let key = format!("thread_{}_key_{}", t, i);
                                        db.insert(&key, b"value", None).unwrap();
                                    } else {
                                        // 90% reads
                                        let key = format!("key_{}", i % 1000);
                                        let _ = db.get(&key).unwrap();
                                    }
                                }
                            })
                        })
                        .collect();

                    for h in handles {
                        h.join().unwrap();
                    }
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("batch_writes", num_threads),
            num_threads,
            |b, &n| {
                let config = Config::default().with_sync_policy(SyncPolicy::Never);
                let db = Arc::new(Spatio::memory_with_config(config).unwrap());

                // Pre-populate
                for i in 0u32..1000 {
                    db.insert(format!("key_{}", i), b"value", None).unwrap();
                }

                b.iter(|| {
                    let handles: Vec<_> = (0..n)
                        .map(|t| {
                            let db = db.clone();
                            thread::spawn(move || {
                                // Collect writes for batching
                                let mut write_keys = Vec::new();

                                for i in 0u32..1000 {
                                    if i % 10 == 0 {
                                        write_keys.push(format!("thread_{}_key_{}", t, i));
                                    } else {
                                        let key = format!("key_{}", i % 1000);
                                        let _ = db.get(&key).unwrap();
                                    }
                                }

                                // Batch all writes
                                db.atomic(|batch| {
                                    for key in &write_keys {
                                        batch.insert(key, b"value", None)?;
                                    }
                                    Ok(())
                                })
                                .unwrap();
                            })
                        })
                        .collect();

                    for h in handles {
                        h.join().unwrap();
                    }
                });
            },
        );
    }

    group.finish();
}

fn bench_write_heavy_workload(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_heavy_10_90");

    // 10% reads, 90% writes
    for num_threads in [2, 4, 8].iter() {
        group.throughput(Throughput::Elements((num_threads * 1000) as u64));

        group.bench_with_input(
            BenchmarkId::new("single_writes", num_threads),
            num_threads,
            |b, &n| {
                let config = Config::default().with_sync_policy(SyncPolicy::Never);
                let db = Arc::new(Spatio::memory_with_config(config).unwrap());

                b.iter(|| {
                    let handles: Vec<_> = (0..n)
                        .map(|t| {
                            let db = db.clone();
                            thread::spawn(move || {
                                for i in 0u32..1000 {
                                    if i % 10 == 0 {
                                        // 10% reads
                                        let key =
                                            format!("thread_{}_key_{}", t, i.saturating_sub(1));
                                        let _ = db.get(&key).unwrap();
                                    } else {
                                        // 90% writes
                                        let key = format!("thread_{}_key_{}", t, i);
                                        db.insert(&key, b"value", None).unwrap();
                                    }
                                }
                            })
                        })
                        .collect();

                    for h in handles {
                        h.join().unwrap();
                    }
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("batch_writes", num_threads),
            num_threads,
            |b, &n| {
                let config = Config::default().with_sync_policy(SyncPolicy::Never);
                let db = Arc::new(Spatio::memory_with_config(config).unwrap());

                b.iter(|| {
                    let handles: Vec<_> = (0..n)
                        .map(|t| {
                            let db = db.clone();
                            thread::spawn(move || {
                                let mut write_keys = Vec::new();

                                for i in 0u32..1000 {
                                    if i % 10 == 0 {
                                        let key =
                                            format!("thread_{}_key_{}", t, i.saturating_sub(1));
                                        let _ = db.get(&key).unwrap();
                                    } else {
                                        write_keys.push(format!("thread_{}_key_{}", t, i));
                                    }
                                }

                                db.atomic(|batch| {
                                    for key in &write_keys {
                                        batch.insert(key, b"value", None)?;
                                    }
                                    Ok(())
                                })
                                .unwrap();
                            })
                        })
                        .collect();

                    for h in handles {
                        h.join().unwrap();
                    }
                });
            },
        );
    }

    group.finish();
}

fn bench_mixed_workload(c: &mut Criterion) {
    let mut group = c.benchmark_group("mixed_50_50");

    // 50% reads, 50% writes
    for num_threads in [2, 4, 8].iter() {
        group.throughput(Throughput::Elements((num_threads * 1000) as u64));

        group.bench_with_input(
            BenchmarkId::new("single_writes", num_threads),
            num_threads,
            |b, &n| {
                let config = Config::default().with_sync_policy(SyncPolicy::Never);
                let db = Arc::new(Spatio::memory_with_config(config).unwrap());

                // Pre-populate
                for i in 0..500 {
                    db.insert(format!("key_{}", i), b"value", None).unwrap();
                }

                b.iter(|| {
                    let handles: Vec<_> = (0..n)
                        .map(|t| {
                            let db = db.clone();
                            thread::spawn(move || {
                                for i in 0u32..1000 {
                                    if i % 2 == 0 {
                                        // 50% reads
                                        let key = format!("key_{}", i % 500);
                                        let _ = db.get(&key).unwrap();
                                    } else {
                                        // 50% writes
                                        let key = format!("thread_{}_key_{}", t, i);
                                        db.insert(&key, b"value", None).unwrap();
                                    }
                                }
                            })
                        })
                        .collect();

                    for h in handles {
                        h.join().unwrap();
                    }
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("batch_writes", num_threads),
            num_threads,
            |b, &n| {
                let config = Config::default().with_sync_policy(SyncPolicy::Never);
                let db = Arc::new(Spatio::memory_with_config(config).unwrap());

                // Pre-populate
                for i in 0..500 {
                    db.insert(format!("key_{}", i), b"value", None).unwrap();
                }

                b.iter(|| {
                    let handles: Vec<_> = (0..n)
                        .map(|t| {
                            let db = db.clone();
                            thread::spawn(move || {
                                let mut write_keys = Vec::new();

                                for i in 0u32..1000 {
                                    if i % 2 == 0 {
                                        let key = format!("key_{}", i % 500);
                                        let _ = db.get(&key).unwrap();
                                    } else {
                                        write_keys.push(format!("thread_{}_key_{}", t, i));
                                    }
                                }

                                db.atomic(|batch| {
                                    for key in &write_keys {
                                        batch.insert(key, b"value", None)?;
                                    }
                                    Ok(())
                                })
                                .unwrap();
                            })
                        })
                        .collect();

                    for h in handles {
                        h.join().unwrap();
                    }
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_single_writes,
    bench_batch_writes,
    bench_concurrent_writes,
    bench_read_heavy_workload,
    bench_write_heavy_workload,
    bench_mixed_workload
);
criterion_main!(benches);
