use spatio::{LineString, Point, Point3d, Polygon, Shape, Spatio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

const SIDE: usize = 316;
const STEP: f64 = 0.01;
const RUNS: usize = 5;

struct Lcg(u64);
impl Lcg {
    fn f(&mut self) -> f64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (self.0 >> 11) as f64 / (1u64 << 53) as f64
    }
}

fn points() -> Vec<(String, Point3d)> {
    (0..SIDE)
        .flat_map(|x| {
            (0..SIDE).map(move |y| {
                (
                    format!("obj:{x}:{y}"),
                    Point3d::new(x as f64 * STEP, y as f64 * STEP, 0.0),
                )
            })
        })
        .collect()
}

fn ring(lon: f64, lat: f64, r_deg: f64, n: usize) -> Polygon {
    let mut c: Vec<(f64, f64)> = (0..n)
        .map(|i| {
            let a = i as f64 / n as f64 * std::f64::consts::TAU;
            (lon + r_deg * a.cos(), lat + r_deg * a.sin())
        })
        .collect();
    c.push(c[0]);
    Polygon::from_coords(&c, vec![])
}

fn fences(n: usize, poly_vertices: usize, seed: u64) -> Vec<(String, Shape)> {
    let extent = SIDE as f64 * STEP;
    let mut r = Lcg(seed);
    (0..n)
        .map(|i| {
            let (lon, lat) = (r.f() * extent, r.f() * extent);
            let shape = match i % 3 {
                0 => Shape::polygon(ring(lon, lat, 0.02, poly_vertices)),
                1 => Shape::circle(Point::new(lon, lat), 1500.0),
                _ => {
                    let line: Vec<(f64, f64)> = (0..10)
                        .map(|k| (lon + k as f64 * 0.01, lat + (r.f() - 0.5) * 0.01))
                        .collect();
                    Shape::corridor(LineString::from_coords(&line), 50.0)
                }
            };
            (format!("fence:{i}"), shape)
        })
        .collect()
}

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

fn bench<F: FnMut()>(ops: usize, mut setup_and_run: impl FnMut() -> F) -> f64 {
    let samples = (0..RUNS)
        .map(|_| {
            let mut f = setup_and_run();
            let t = Instant::now();
            f();
            t.elapsed().as_secs_f64() * 1e9 / ops as f64
        })
        .collect();
    median(samples)
}

fn loaded_db(pts: &[(String, Point3d)], fs: &[(String, Shape)]) -> Spatio {
    let db = Spatio::memory().unwrap();
    for (id, s) in fs {
        db.set_shape("fences", id, s.clone()).unwrap();
    }
    for (id, p) in pts {
        db.upsert("bench", id, p.clone(), serde_json::Value::Null, None)
            .unwrap();
    }
    db
}

fn main() {
    let pts = points();
    let n = pts.len();
    let moved: Vec<Point3d> = pts
        .iter()
        .map(|(_, p)| Point3d::new(p.x() + 0.003, p.y(), 0.0))
        .collect();
    let q = 10_000;
    let mut r = Lcg(7);
    let queries: Vec<Point3d> = (0..q)
        .map(|_| Point3d::new(r.f() * SIDE as f64 * STEP, r.f() * SIDE as f64 * STEP, 0.0))
        .collect();

    println!("{n} objects, in-memory, single thread unless noted, median of {RUNS} runs, ns/op\n");

    println!("1) Upsert (UPDATE) with shapes stored");
    for &nf in &[0usize, 10_000] {
        let fs = fences(nf, 32, 1);
        let ns = bench(n, || {
            let db = loaded_db(&pts, &fs);
            let moved = &moved;
            let pts = &pts;
            move || {
                for ((id, _), p) in pts.iter().zip(moved) {
                    db.upsert("bench", id, p.clone(), serde_json::Value::Null, None)
                        .unwrap();
                }
            }
        });
        println!("   fences={nf:>6}: {ns:>8.0}");
    }

    println!("\n2) Shape queries");
    for &(nf, verts) in &[
        (100usize, 32usize),
        (1_000, 32),
        (10_000, 32),
        (1_000, 1_000),
    ] {
        let fs = fences(nf, verts, 1);
        let db = loaded_db(&[], &fs);
        let c = bench(q, || {
            let db = &db;
            let queries = &queries;
            move || {
                for p in queries {
                    std::hint::black_box(db.shapes_containing("fences", p, 100).unwrap());
                }
            }
        });
        let w = bench(q, || {
            let db = &db;
            let queries = &queries;
            move || {
                for p in queries {
                    std::hint::black_box(db.shapes_within("fences", p, 500.0, 100).unwrap());
                }
            }
        });
        println!(
            "   fences={nf:>6} poly_vertices={verts:>5}: containing {c:>8.0} | within 500m {w:>8.0}"
        );
    }

    println!(
        "\n3) Concurrency: 4 threads of plain upsert, with and without a thread rewriting fences nonstop"
    );
    for writer in [false, true] {
        let ns = bench(n, || {
            let db = Arc::new(loaded_db(&pts, &fences(1_000, 32, 1)));
            let pts = Arc::new(pts.clone());
            let moved = Arc::new(moved.clone());
            move || {
                let stop = Arc::new(AtomicBool::new(false));
                let w = writer.then(|| {
                    let db = db.clone();
                    let stop = stop.clone();
                    let fs = fences(1_000, 32, 2);
                    std::thread::spawn(move || {
                        let mut i = 0;
                        while !stop.load(Ordering::Relaxed) {
                            let (id, s) = &fs[i % fs.len()];
                            db.set_shape("fences", id, s.clone()).unwrap();
                            i += 1;
                        }
                        i
                    })
                });
                let hs: Vec<_> = (0..4)
                    .map(|t| {
                        let (db, pts, moved) = (db.clone(), pts.clone(), moved.clone());
                        std::thread::spawn(move || {
                            for i in (t..pts.len()).step_by(4) {
                                db.upsert(
                                    "bench",
                                    &pts[i].0,
                                    moved[i].clone(),
                                    serde_json::Value::Null,
                                    None,
                                )
                                .unwrap();
                            }
                        })
                    })
                    .collect();
                for h in hs {
                    h.join().unwrap();
                }
                stop.store(true, Ordering::Relaxed);
                if let Some(w) = w {
                    w.join().unwrap();
                }
            }
        });
        println!("   fence writer={writer:<5}: {ns:>8.0} ns/op (wall, 4 threads)");
    }
}
