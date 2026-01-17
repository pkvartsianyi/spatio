use cpu_time::ProcessTime;
use futures::stream::{self, StreamExt};
use serde::Serialize;
use spatio_client::SpatioClient;
use spatio_types::point::Point3d;
use std::fs::File;
use std::sync::Arc;
use std::time::{Duration, Instant};
use sysinfo::{Pid, ProcessesToUpdate, System};

struct BenchConfig {
    addr: String,
    dataset_size: usize,
    concurrency: usize,
    warmup_runs: usize,
    measurement_runs: usize,
    quiet: bool,
    json_output: Option<String>,
}

impl Default for BenchConfig {
    fn default() -> Self {
        Self {
            addr: "127.0.0.1:3000".to_string(),
            dataset_size: 100_000,
            concurrency: 50,
            warmup_runs: 1,
            measurement_runs: 5,
            quiet: false,
            json_output: None,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct BenchMetrics {
    name: String,
    ops_count: usize,
    total_duration_secs: f64,
    avg_latency_ms: f64,
    cpu_percent: f64,
    memory_mb: f64,
}

impl BenchMetrics {
    fn throughput(&self) -> f64 {
        self.ops_count as f64 / self.total_duration_secs
    }

    fn print(&self, quiet: bool) {
        if quiet {
            println!("{}: {:.2} ops/s", self.name, self.throughput());
        } else {
            println!(
                "{}: {:.2} ops/s | latency: {:.2}ms | CPU: {:.1}% | mem: {:.1}MB",
                self.name,
                self.throughput(),
                self.avg_latency_ms,
                self.cpu_percent,
                self.memory_mb
            );
        }
    }
}

struct BenchRunner {
    system: System,
    process_id: Pid,
}

impl BenchRunner {
    fn new() -> Self {
        Self {
            system: System::new_all(),
            process_id: Pid::from_u32(std::process::id()),
        }
    }

    fn get_memory_mb(&mut self) -> f64 {
        self.system
            .refresh_processes(ProcessesToUpdate::Some(&[self.process_id]), true);
        if let Some(process) = self.system.process(self.process_id) {
            process.memory() as f64 / (1024.0 * 1024.0)
        } else {
            0.0
        }
    }

    async fn run<F, Fut>(&mut self, name: &str, ops_count: usize, f: F) -> BenchMetrics
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = ()>,
    {
        let cpu_start = ProcessTime::try_now().ok();
        let wall_start = Instant::now();
        let mem_before = self.get_memory_mb();

        f().await;

        let wall_elapsed = wall_start.elapsed();
        let cpu_elapsed = if let (Some(start), Ok(end)) = (cpu_start, ProcessTime::try_now()) {
            end.duration_since(start)
        } else {
            Duration::ZERO
        };
        let mem_after = self.get_memory_mb();

        let total_duration_secs = wall_elapsed.as_secs_f64();
        let cpu_percent = if total_duration_secs > 0.0 {
            (cpu_elapsed.as_secs_f64() / total_duration_secs) * 100.0
        } else {
            0.0
        };

        BenchMetrics {
            name: name.to_string(),
            ops_count,
            total_duration_secs,
            avg_latency_ms: (total_duration_secs * 1000.0) / ops_count as f64,
            cpu_percent,
            memory_mb: mem_after.max(mem_before),
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();

    let config = BenchConfig {
        addr: args
            .iter()
            .position(|a| a == "--addr")
            .and_then(|i| args.get(i + 1))
            .cloned()
            .unwrap_or_else(|| "127.0.0.1:3000".to_string()),
        dataset_size: args
            .iter()
            .position(|a| a == "-n")
            .and_then(|i| args.get(i + 1))
            .and_then(|s| s.parse().ok())
            .unwrap_or(100_000),
        concurrency: args
            .iter()
            .position(|a| a == "-c")
            .and_then(|i| args.get(i + 1))
            .and_then(|s| s.parse().ok())
            .unwrap_or(50),
        warmup_runs: 1,
        measurement_runs: args
            .iter()
            .position(|a| a == "-r")
            .and_then(|i| args.get(i + 1))
            .and_then(|s| s.parse().ok())
            .unwrap_or(5),
        quiet: args.iter().any(|arg| arg == "-q"),
        json_output: args
            .iter()
            .position(|a| a == "--json" || a == "-o")
            .and_then(|i| args.get(i + 1))
            .cloned(),
    };

    if !config.quiet {
        println!("════════════════════════════════════════════════════════════════");
        println!("  Spatio Server Benchmark");
        println!("════════════════════════════════════════════════════════════════");
        println!("  Server:      {}", config.addr);
        println!("  Dataset:     {}", config.dataset_size);
        println!("  Concurrency: {}", config.concurrency);
        println!("  Runs:        {}", config.measurement_runs);
        #[cfg(target_arch = "aarch64")]
        println!("  Platform:    ARM64");
        #[cfg(target_arch = "x86_64")]
        println!("  Platform:    x86_64");
        println!("════════════════════════════════════════════════════════════════\n");
    }

    let client = Arc::new(SpatioClient::connect(config.addr.parse()?).await?);
    let mut runner = BenchRunner::new();
    let mut all_metrics = Vec::new();

    let side_len = (config.dataset_size as f64).sqrt() as usize;
    let actual_count = side_len * side_len;

    let points: Vec<(usize, usize)> = (0..side_len)
        .flat_map(|x| (0..side_len).map(move |y| (x, y)))
        .collect();

    if !config.quiet {
        println!("Warming up...");
    }
    for _ in 0..config.warmup_runs {
        let batch_size = 1000.min(points.len());
        stream::iter(points.iter().take(batch_size))
            .map(|(x, y)| {
                let client = client.clone();
                let i = x * side_len + y;
                async move {
                    let _ = client
                        .upsert(
                            "bench",
                            &format!("obj:{}", i),
                            Point3d::new(*x as f64 * 0.01, *y as f64 * 0.01, 0.0),
                            serde_json::Value::Null,
                        )
                        .await;
                }
            })
            .buffer_unordered(config.concurrency)
            .count()
            .await;

        // Clean up
        let _ = client.delete("bench", "obj:0").await;
        // Note: Client doesn't have delete_namespace, so we just let it be or overwrite
    }

    // ══════════════ UPSERT ══════════════
    if !config.quiet {
        println!("\nUPSERT benchmark:");
    }

    for run in 0..config.measurement_runs {
        let points_ref = &points;
        let client_ref = client.clone();

        let metrics = runner
            .run("UPSERT", actual_count, || async move {
                stream::iter(points_ref)
                    .map(|(x, y)| {
                        let client = client_ref.clone();
                        let i = x * side_len + y;
                        async move {
                            let _ = client
                                .upsert(
                                    "bench",
                                    &format!("obj:{}", i),
                                    Point3d::new(*x as f64 * 0.01, *y as f64 * 0.01, 0.0),
                                    serde_json::Value::Null,
                                )
                                .await;
                        }
                    })
                    .buffer_unordered(config.concurrency)
                    .count()
                    .await;
            })
            .await;

        if !config.quiet {
            print!("  Run {}: ", run + 1);
            metrics.print(false);
        }
        all_metrics.push(metrics);
    }

    if !config.quiet {
        println!("\nGET benchmark:");
    }

    for run in 0..config.measurement_runs {
        let points_ref = &points;
        let client_ref = client.clone();

        let metrics = runner
            .run("GET", actual_count, || async move {
                stream::iter(points_ref)
                    .map(|(x, y)| {
                        let client = client_ref.clone();
                        let i = x * side_len + y;
                        async move {
                            let _ = client.get("bench", &format!("obj:{}", i)).await;
                        }
                    })
                    .buffer_unordered(config.concurrency)
                    .count()
                    .await;
            })
            .await;

        if !config.quiet {
            print!("  Run {}: ", run + 1);
            metrics.print(false);
        }
        all_metrics.push(metrics);
    }

    if !config.quiet {
        println!("\nRADIUS QUERY benchmark:");
    }
    let query_count = 1000;

    for run in 0..config.measurement_runs {
        let client_ref = client.clone();

        // Random-ish queries based on indices
        let metrics = runner
            .run("RADIUS", query_count, || async move {
                stream::iter(0..query_count)
                    .map(|i| {
                        let client = client_ref.clone();
                        // Pick a center
                        let cx = (i % side_len) as f64 * 0.01;
                        let cy = (i / side_len % side_len) as f64 * 0.01;
                        async move {
                            let _ = client
                                .query_radius("bench", Point3d::new(cx, cy, 0.0), 0.05, 100)
                                .await;
                        }
                    })
                    .buffer_unordered(config.concurrency)
                    .count()
                    .await;
            })
            .await;

        if !config.quiet {
            print!("  Run {}: ", run + 1);
            metrics.print(false);
        }
        all_metrics.push(metrics);
    }

    if !config.quiet {
        println!("\nKNN benchmark:");
    }

    for run in 0..config.measurement_runs {
        let client_ref = client.clone();

        let metrics = runner
            .run("KNN", query_count, || async move {
                stream::iter(0..query_count)
                    .map(|i| {
                        let client = client_ref.clone();
                        let cx = (i % side_len) as f64 * 0.01;
                        let cy = (i / side_len % side_len) as f64 * 0.01;
                        async move {
                            let _ = client.knn("bench", Point3d::new(cx, cy, 0.0), 10).await;
                        }
                    })
                    .buffer_unordered(config.concurrency)
                    .count()
                    .await;
            })
            .await;

        if !config.quiet {
            print!("  Run {}: ", run + 1);
            metrics.print(false);
        }
        all_metrics.push(metrics);
    }

    if !config.quiet {
        println!("\nDISTANCE benchmark:");
    }

    for run in 0..config.measurement_runs {
        let client_ref = client.clone();
        let max_idx = points.len();

        let metrics = runner
            .run("DISTANCE", query_count, || async move {
                stream::iter(0..query_count)
                    .map(|i| {
                        let client = client_ref.clone();
                        let idx1 = i % max_idx;
                        let idx2 = (i + 1) % max_idx;
                        async move {
                            let _ = client
                                .distance(
                                    "bench",
                                    &format!("obj:{}", idx1),
                                    &format!("obj:{}", idx2),
                                    Some(spatio_types::geo::DistanceMetric::Euclidean),
                                )
                                .await;
                        }
                    })
                    .buffer_unordered(config.concurrency)
                    .count()
                    .await;
            })
            .await;

        if !config.quiet {
            print!("  Run {}: ", run + 1);
            metrics.print(false);
        }
        all_metrics.push(metrics);
    }

    println!("\n════════════════════════════════════════════════════════════════\n");

    if let Some(path) = config.json_output {
        #[derive(Serialize)]
        struct Output {
            platform: String,
            timestamp: String,
            dataset_size: usize,
            concurrency: usize,
            results: Vec<BenchMetrics>,
        }

        let output = Output {
            platform: if cfg!(target_arch = "aarch64") {
                "aarch64".to_string()
            } else {
                "x86_64".to_string()
            },
            timestamp: chrono::Utc::now().to_rfc3339(),
            dataset_size: config.dataset_size,
            concurrency: config.concurrency,
            results: all_metrics,
        };

        if !config.quiet {
            print!("Writing results to {}... ", path);
        }
        let file = File::create(&path)?;
        serde_json::to_writer_pretty(file, &output)?;
        if !config.quiet {
            println!("Done.");
        }
    }

    Ok(())
}
