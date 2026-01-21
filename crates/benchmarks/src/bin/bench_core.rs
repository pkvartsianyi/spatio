use cpu_time::ProcessTime;
use serde::Serialize;
use spatio::{Point3d, Spatio};
use std::fs::File;
use std::time::{Duration, Instant};
use sysinfo::{Pid, ProcessesToUpdate, System};

struct BenchConfig {
    dataset_size: usize,
    warmup_runs: usize,
    measurement_runs: usize,
    quiet: bool,
    json_output: Option<String>,
}

impl Default for BenchConfig {
    fn default() -> Self {
        Self {
            dataset_size: 100_000,
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
    cpu_time_secs: f64,
    memory_mb: f64,
}

impl BenchMetrics {
    fn throughput(&self) -> f64 {
        self.ops_count as f64 / self.total_duration_secs
    }

    fn avg_latency_us(&self) -> f64 {
        (self.total_duration_secs * 1_000_000.0) / self.ops_count as f64
    }

    fn cpu_percent(&self) -> f64 {
        if self.total_duration_secs > 0.0 {
            (self.cpu_time_secs / self.total_duration_secs) * 100.0
        } else {
            0.0
        }
    }

    fn print(&self, quiet: bool) {
        if quiet {
            println!("{}: {:.2} ops/s", self.name, self.throughput());
        } else {
            println!(
                "{}: {:.2} ops/s | latency: {:.2}µs | CPU: {:.1}% | mem: {:.1}MB",
                self.name,
                self.throughput(),
                self.avg_latency_us(),
                self.cpu_percent(),
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

    fn run<F>(&mut self, name: &str, ops_count: usize, mut f: F) -> BenchMetrics
    where
        F: FnMut(),
    {
        let cpu_start = ProcessTime::try_now().ok();
        let wall_start = Instant::now();
        let mem_before = self.get_memory_mb();

        f();

        let wall_elapsed = wall_start.elapsed();
        let cpu_elapsed = if let (Some(start), Ok(end)) = (cpu_start, ProcessTime::try_now()) {
            end.duration_since(start)
        } else {
            Duration::ZERO
        };
        let mem_after = self.get_memory_mb();

        BenchMetrics {
            name: name.to_string(),
            ops_count,
            total_duration_secs: wall_elapsed.as_secs_f64(),
            cpu_time_secs: cpu_elapsed.as_secs_f64(),
            memory_mb: mem_after.max(mem_before),
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();

    let config = BenchConfig {
        dataset_size: args
            .iter()
            .position(|a| a == "-n")
            .and_then(|i| args.get(i + 1))
            .and_then(|s| s.parse().ok())
            .unwrap_or(100_000),
        warmup_runs: 1,
        measurement_runs: args
            .iter()
            .position(|a| a == "-r")
            .and_then(|i| args.get(i + 1))
            .and_then(|s| s.parse().ok())
            .unwrap_or(5),
        quiet: args.iter().any(|a| a == "-q"),
        json_output: args
            .iter()
            .position(|a| a == "--json" || a == "-o")
            .and_then(|i| args.get(i + 1))
            .cloned(),
    };

    if !config.quiet {
        println!("════════════════════════════════════════════════════════════════");
        println!("  Spatio Core Benchmark");
        println!("════════════════════════════════════════════════════════════════");
        println!("  Dataset size: {}", config.dataset_size);
        println!("  Warmup runs: {}", config.warmup_runs);
        println!("  Measurement runs: {}", config.measurement_runs);
        #[cfg(target_arch = "aarch64")]
        println!("  Platform: ARM64 (Raspberry Pi / Apple Silicon)");
        #[cfg(target_arch = "x86_64")]
        println!("  Platform: x86_64");
        if let Some(ref path) = config.json_output {
            println!("  Output: {}", path);
        }
        println!("════════════════════════════════════════════════════════════════\n");
    }

    let mut runner = BenchRunner::new();

    let side_len = (config.dataset_size as f64).sqrt() as usize;
    let actual_count = side_len * side_len;

    let points: Vec<(String, Point3d)> = (0..side_len)
        .flat_map(|x| {
            (0..side_len).map(move |y| {
                let id = format!("obj:{}:{}", x, y);
                let point = Point3d::new(x as f64 * 0.01, y as f64 * 0.01, 0.0);
                (id, point)
            })
        })
        .collect();

    if !config.quiet {
        println!("Warming up...");
    }

    {
        let warmup_db = Spatio::memory()?;
        for _ in 0..config.warmup_runs {
            for (id, point) in points.iter().take(1000) {
                warmup_db.upsert("bench", id, point.clone(), serde_json::Value::Null, None)?;
            }
        }
    }

    let mut all_metrics = Vec::new();

    if !config.quiet {
        println!("\nUPSERT benchmark:");
    }
    let mut upsert_metrics = Vec::new();
    for run in 0..config.measurement_runs {
        // Fresh DB for each upsert run
        let db = Spatio::memory()?;

        let metrics = runner.run("UPSERT", actual_count, || {
            for (id, point) in &points {
                let _ = db.upsert("bench", id, point.clone(), serde_json::Value::Null, None);
            }
        });

        if !config.quiet {
            print!("  Run {}: ", run + 1);
            metrics.print(false);
        }
        upsert_metrics.push(metrics.clone());
        all_metrics.push(metrics);
    }

    if !config.quiet {
        println!("\nUPDATE benchmark:");
    }
    let mut update_metrics = Vec::new();
    for run in 0..config.measurement_runs {
        let db = Spatio::memory()?;
        for (id, point) in &points {
            db.upsert("bench", id, point.clone(), serde_json::Value::Null, None)
                .unwrap();
        }

        let metrics = runner.run("UPDATE", actual_count, || {
            for (id, point) in &points {
                let new_point = Point3d::new(point.point.x() + 0.0001, point.point.y(), point.z);
                let _ = db.upsert("bench", id, new_point, serde_json::Value::Null, None);
            }
        });

        if !config.quiet {
            print!("  Run {}: ", run + 1);
            metrics.print(false);
        }
        update_metrics.push(metrics.clone());
        all_metrics.push(metrics);
    }

    let db = Spatio::memory()?;
    for (id, point) in &points {
        db.upsert("bench", id, point.clone(), serde_json::Value::Null, None)?;
    }
    if !config.quiet {
        println!("\nGET benchmark:");
    }
    let mut get_metrics = Vec::new();
    for run in 0..config.measurement_runs {
        let metrics = runner.run("GET", actual_count, || {
            for (id, _) in &points {
                let _ = db.get("bench", id);
            }
        });

        if !config.quiet {
            print!("  Run {}: ", run + 1);
            metrics.print(false);
        }
        get_metrics.push(metrics.clone());
        all_metrics.push(metrics);
    }

    if !config.quiet {
        println!("\nRADIUS QUERY benchmark:");
    }
    let query_count = 1000;
    let mut radius_metrics = Vec::new();
    for run in 0..config.measurement_runs {
        let metrics = runner.run("RADIUS", query_count, || {
            for i in 0..query_count {
                let cx = (i % side_len) as f64 * 0.01;
                let cy = (i / side_len % side_len) as f64 * 0.01;
                let center = Point3d::new(cx, cy, 0.0);
                let _ = db.query_radius("bench", &center, 0.05, 100);
            }
        });

        if !config.quiet {
            print!("  Run {}: ", run + 1);
            metrics.print(false);
        }
        radius_metrics.push(metrics.clone());
        all_metrics.push(metrics);
    }

    if !config.quiet {
        println!("\nKNN benchmark:");
    }
    let mut knn_metrics = Vec::new();
    for run in 0..config.measurement_runs {
        let metrics = runner.run("KNN", query_count, || {
            for i in 0..query_count {
                let cx = (i % side_len) as f64 * 0.01;
                let cy = (i / side_len % side_len) as f64 * 0.01;
                let center = Point3d::new(cx, cy, 0.0);
                let _ = db.knn("bench", &center, 10);
            }
        });

        if !config.quiet {
            print!("  Run {}: ", run + 1);
            metrics.print(false);
        }
        knn_metrics.push(metrics.clone());
        all_metrics.push(metrics);
    }

    if !config.quiet {
        println!("\nDISTANCE benchmark:");
    }
    let mut distance_metrics = Vec::new();
    for run in 0..config.measurement_runs {
        let metrics = runner.run("DISTANCE", query_count, || {
            for i in 0..query_count {
                let id1 = &points[i % points.len()].0;
                let id2 = &points[(i + 1) % points.len()].0;
                let _ = db.distance_between(
                    "bench",
                    id1,
                    id2,
                    spatio::compute::spatial::DistanceMetric::Euclidean,
                );
            }
        });

        if !config.quiet {
            print!("  Run {}: ", run + 1);
            metrics.print(false);
        }
        distance_metrics.push(metrics.clone());
        all_metrics.push(metrics);
    }

    println!("\n════════════════════════════════════════════════════════════════");
    println!(
        "  SUMMARY (averages across {} runs)",
        config.measurement_runs
    );
    println!("════════════════════════════════════════════════════════════════");

    fn avg_throughput(metrics: &[BenchMetrics]) -> f64 {
        metrics.iter().map(|m| m.throughput()).sum::<f64>() / metrics.len() as f64
    }

    fn avg_latency(metrics: &[BenchMetrics]) -> f64 {
        metrics.iter().map(|m| m.avg_latency_us()).sum::<f64>() / metrics.len() as f64
    }

    fn avg_cpu(metrics: &[BenchMetrics]) -> f64 {
        metrics.iter().map(|m| m.cpu_percent()).sum::<f64>() / metrics.len() as f64
    }

    println!(
        "  UPSERT:   {:>12.2} ops/s | {:>8.2}µs | CPU: {:>5.1}%",
        avg_throughput(&upsert_metrics),
        avg_latency(&upsert_metrics),
        avg_cpu(&upsert_metrics)
    );
    println!(
        "  UPDATE:   {:>12.2} ops/s | {:>8.2}µs | CPU: {:>5.1}%",
        avg_throughput(&update_metrics),
        avg_latency(&update_metrics),
        avg_cpu(&update_metrics)
    );
    println!(
        "  GET:      {:>12.2} ops/s | {:>8.2}µs | CPU: {:>5.1}%",
        avg_throughput(&get_metrics),
        avg_latency(&get_metrics),
        avg_cpu(&get_metrics)
    );
    println!(
        "  RADIUS:   {:>12.2} ops/s | {:>8.2}µs | CPU: {:>5.1}%",
        avg_throughput(&radius_metrics),
        avg_latency(&radius_metrics),
        avg_cpu(&radius_metrics)
    );
    println!(
        "  KNN:      {:>12.2} ops/s | {:>8.2}µs | CPU: {:>5.1}%",
        avg_throughput(&knn_metrics),
        avg_latency(&knn_metrics),
        avg_cpu(&knn_metrics)
    );
    println!(
        "  DISTANCE: {:>12.2} ops/s | {:>8.2}µs | CPU: {:>5.1}%",
        avg_throughput(&distance_metrics),
        avg_latency(&distance_metrics),
        avg_cpu(&distance_metrics)
    );

    println!("════════════════════════════════════════════════════════════════\n");

    if let Some(path) = config.json_output {
        #[derive(Serialize)]
        struct Output {
            platform: String,
            timestamp: String,
            dataset_size: usize,
            runs: Vec<BenchMetrics>,
        }

        let output = Output {
            platform: if cfg!(target_arch = "aarch64") {
                "aarch64".to_string()
            } else {
                "x86_64".to_string()
            },
            timestamp: chrono::Utc::now().to_rfc3339(),
            dataset_size: config.dataset_size,
            runs: all_metrics,
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
