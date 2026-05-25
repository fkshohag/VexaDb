//! Run: `cargo run -p vectordb-bench --release -- [OPTIONS]`
//!
//! Prints a single human-readable benchmark report (no Criterion HTML).
//!
//! Examples:
//!   cargo run -p vectordb-bench --release
//!   cargo run -p vectordb-bench --release -- --scenario large
//!   cargo run -p vectordb-bench --release -- --count 1000000 --queries 200 --workers 8 --measure-recall

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use clap::Parser;
use rand::prelude::*;
use rayon::prelude::*;
use tempfile::tempdir;
use vectordb_core::{CollectionConfig, DistanceMetric, Vector};
use vectordb_core::distance::Distance;
use vectordb_core::hnsw::{HnswConfig, HnswIndex};
use vectordb_storage::{BulkPoint, CollectionEngine, EngineConfig};

#[derive(Parser, Debug, Clone)]
#[command(name = "vectordb-bench", about = "VectorDB performance harness")]
struct Cli {
    /// Vector dimension
    #[arg(long, default_value = "128")]
    dim: usize,

    /// Number of vectors to index
    #[arg(long, default_value = "10000")]
    count: usize,

    /// Search queries per benchmark
    #[arg(long, default_value = "1000")]
    queries: usize,

    /// top-k for search
    #[arg(long, default_value = "10")]
    top_k: usize,

    /// Skip engine (WAL + RocksDB) benchmarks
    #[arg(long)]
    core_only: bool,

    /// Concurrent search workers (parallel QPS)
    #[arg(long, default_value = "1")]
    workers: usize,

    /// Disable WAL fsync for faster ingest (still durable on clean shutdown)
    #[arg(long)]
    no_sync_wal: bool,

    /// Measure recall@k vs brute-force ground truth (slow on large N)
    #[arg(long)]
    measure_recall: bool,

    /// Sample size for recall (number of queries to grade)
    #[arg(long, default_value = "100")]
    recall_queries: usize,

    /// Preset: small | medium | large | xlarge
    #[arg(long)]
    scenario: Option<String>,

    /// Print progress every N inserted vectors
    #[arg(long, default_value = "10000")]
    progress_every: usize,

    /// Skip core (in-memory HNSW) benchmark — useful when only durable path matters
    #[arg(long)]
    skip_core: bool,
}

impl Cli {
    fn apply_scenario(mut self) -> Self {
        match self.scenario.as_deref() {
            Some("small") => {
                self.dim = 128;
                self.count = 10_000;
                self.queries = 500;
            }
            Some("medium") => {
                self.dim = 384;
                self.count = 100_000;
                self.queries = 500;
                self.workers = self.workers.max(4);
            }
            Some("large") => {
                self.dim = 768;
                self.count = 500_000;
                self.queries = 200;
                self.workers = self.workers.max(8);
            }
            Some("xlarge") => {
                self.dim = 768;
                self.count = 2_000_000;
                self.queries = 100;
                self.workers = self.workers.max(8);
                self.no_sync_wal = true;
            }
            Some(other) => {
                eprintln!(
                    "warning: unknown scenario '{other}', expected small|medium|large|xlarge"
                );
            }
            None => {}
        }
        self
    }
}

fn random_unit(dim: usize, rng: &mut impl Rng) -> Vec<f32> {
    let mut v: Vec<f32> = (0..dim).map(|_| rng.gen::<f32>() - 0.5).collect();
    let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm > 0.0 {
        for x in &mut v {
            *x /= norm;
        }
    }
    v
}

fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[idx]
}

fn report_latencies(label: &str, samples_us: &mut [f64]) {
    samples_us.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let n = samples_us.len() as f64;
    let sum: f64 = samples_us.iter().sum();
    println!(
        "  {label}: p50={:.2}ms p95={:.2}ms p99={:.2}ms avg={:.2}ms",
        percentile(samples_us, 0.50) / 1000.0,
        percentile(samples_us, 0.95) / 1000.0,
        percentile(samples_us, 0.99) / 1000.0,
        (sum / n) / 1000.0,
    );
}

fn fmt_bytes(n: u64) -> String {
    const UNITS: [&str; 6] = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
    let mut x = n as f64;
    let mut u = 0;
    while x >= 1024.0 && u < UNITS.len() - 1 {
        x /= 1024.0;
        u += 1;
    }
    format!("{x:.2} {}", UNITS[u])
}

fn approx_index_bytes(count: usize, dim: usize) -> u64 {
    // f32 vector + ~m*2 neighbour ids (avg) at ~8 bytes each + bookkeeping
    let per = (dim * 4) + (16 * 2 * 8) + 64;
    (count * per) as u64
}

fn brute_force_topk(points: &[Vec<f32>], query: &[f32], k: usize) -> Vec<usize> {
    let mut scored: Vec<(f32, usize)> = points
        .iter()
        .enumerate()
        .map(|(i, v)| (Distance::cosine_distance(v, query), i))
        .collect();
    let n = scored.len();
    if n == 0 {
        return Vec::new();
    }
    let nth = k.min(n - 1);
    scored.select_nth_unstable_by(nth, |a, b| a.0.partial_cmp(&b.0).unwrap());
    let mut top: Vec<_> = scored.into_iter().take(k).collect();
    top.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
    top.into_iter().map(|(_, i)| i).collect()
}

fn bench_core(cli: &Cli) -> Option<HnswIndex> {
    if cli.skip_core {
        return None;
    }
    println!(
        "\n## Core (in-memory HNSW, dim={}, n={})",
        cli.dim, cli.count
    );
    println!(
        "  estimated index size: ~{}",
        fmt_bytes(approx_index_bytes(cli.count, cli.dim))
    );

    let config = HnswConfig::new(DistanceMetric::Cosine, 16, 200, 64);
    let index = HnswIndex::new(cli.dim, config);
    let mut rng = rand::thread_rng();

    let t0 = Instant::now();
    let mut last_print = Instant::now();
    for i in 0..cli.count {
        index
            .insert(
                format!("p{i}"),
                Vector::new(random_unit(cli.dim, &mut rng)),
            )
            .unwrap();
        if cli.progress_every > 0
            && i > 0
            && i % cli.progress_every == 0
            && last_print.elapsed().as_secs() >= 1
        {
            let rate = (i + 1) as f64 / t0.elapsed().as_secs_f64();
            eprintln!(
                "    [core] inserted {}/{} ({:.0}/s)",
                i + 1,
                cli.count,
                rate
            );
            last_print = Instant::now();
        }
    }
    let insert_secs = t0.elapsed().as_secs_f64();
    println!(
        "  insert: {:.0} vectors/s ({:.2}s total)",
        cli.count as f64 / insert_secs,
        insert_secs
    );

    bench_search_concurrent("[core]", &index, cli);

    let a = random_unit(cli.dim, &mut rng);
    let b = random_unit(cli.dim, &mut rng);
    let t = Instant::now();
    let iters = 100_000usize;
    for _ in 0..iters {
        std::hint::black_box(Distance::cosine_distance(&a, &b));
    }
    let us = t.elapsed().as_secs_f64() * 1e6 / iters as f64;
    println!("  cosine distance (dim={}): {:.3} µs/op", cli.dim, us);

    Some(index)
}

fn bench_search_concurrent(prefix: &str, index: &HnswIndex, cli: &Cli) {
    let mut rng = rand::thread_rng();
    let queries: Vec<Vec<f32>> = (0..cli.queries)
        .map(|_| random_unit(cli.dim, &mut rng))
        .collect();

    let workers = cli.workers.max(1);
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(workers)
        .build()
        .unwrap();

    let counter = Arc::new(AtomicUsize::new(0));
    let t0 = Instant::now();
    let latencies: Vec<f64> = pool.install(|| {
        queries
            .par_iter()
            .map(|q| {
                let t = Instant::now();
                let _ = index.search(q, cli.top_k).unwrap();
                let us = t.elapsed().as_secs_f64() * 1e6;
                counter.fetch_add(1, Ordering::Relaxed);
                us
            })
            .collect()
    });
    let wall = t0.elapsed().as_secs_f64();
    let qps = cli.queries as f64 / wall;

    let mut sorted = latencies;
    println!(
        "  {prefix} search k={} workers={}: {:.0} QPS (wall {:.2}s)",
        cli.top_k, workers, qps, wall
    );
    report_latencies("search latency", &mut sorted);
}

fn bench_engine(cli: &Cli) {
    println!(
        "\n## Engine (WAL + RocksDB + HNSW, dim={}, n={}, sync_wal={})",
        cli.dim, cli.count, !cli.no_sync_wal
    );

    let dir = tempdir().unwrap();
    let mut cfg = EngineConfig::new(dir.path());
    if cli.no_sync_wal {
        cfg.sync_wal = false;
    }
    let engine = CollectionEngine::open(cfg).unwrap();
    engine
        .create_collection(CollectionConfig::new(
            "bench",
            cli.dim,
            DistanceMetric::Cosine,
        ))
        .unwrap();

    let chunk = 1_000usize;
    let total_chunks = (cli.count + chunk - 1) / chunk;
    let t0 = Instant::now();
    let mut rng = rand::thread_rng();
    let mut last_print = Instant::now();
    let mut ground_truth_pool: Vec<Vec<f32>> = if cli.measure_recall {
        Vec::with_capacity(cli.count.min(50_000))
    } else {
        Vec::new()
    };

    for ci in 0..total_chunks {
        let start = ci * chunk;
        let end = (start + chunk).min(cli.count);
        let mut points = Vec::with_capacity(end - start);
        for i in start..end {
            let v = random_unit(cli.dim, &mut rng);
            if cli.measure_recall && ground_truth_pool.len() < ground_truth_pool.capacity() {
                ground_truth_pool.push(v.clone());
            }
            points.push(BulkPoint {
                id: format!("p{i}"),
                vector: Vector::new(v),
                payload: None,
                sparse: None,
            });
        }
        engine.bulk_upsert("bench", points, 500).unwrap();
        if cli.progress_every > 0 && last_print.elapsed().as_secs() >= 1 {
            let rate = (end as f64) / t0.elapsed().as_secs_f64();
            eprintln!(
                "    [engine] inserted {}/{} ({:.0}/s)",
                end, cli.count, rate
            );
            last_print = Instant::now();
        }
    }
    let bulk_secs = t0.elapsed().as_secs_f64();
    println!(
        "  bulk_upsert: {:.0} vectors/s ({:.2}s total)",
        cli.count as f64 / bulk_secs,
        bulk_secs
    );

    let queries: Vec<Vec<f32>> = (0..cli.queries)
        .map(|_| random_unit(cli.dim, &mut rng))
        .collect();
    let workers = cli.workers.max(1);
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(workers)
        .build()
        .unwrap();
    let t0 = Instant::now();
    let latencies: Vec<f64> = pool.install(|| {
        queries
            .par_iter()
            .map(|q| {
                let t = Instant::now();
                let _ = engine
                    .search("bench", q, cli.top_k, None, vectordb_core::OutputOptions::default())
                    .unwrap();
                t.elapsed().as_secs_f64() * 1e6
            })
            .collect()
    });
    let wall = t0.elapsed().as_secs_f64();
    println!(
        "  [engine] search k={} workers={}: {:.0} QPS (wall {:.2}s)",
        cli.top_k,
        workers,
        cli.queries as f64 / wall,
        wall
    );
    let mut sorted = latencies;
    report_latencies("search latency", &mut sorted);

    if cli.measure_recall && !ground_truth_pool.is_empty() {
        report_recall(&engine, &ground_truth_pool, cli);
    }

    if cli.count >= 50_000 {
        let dir_size = du(dir.path());
        println!("  data dir on disk: {}", fmt_bytes(dir_size));
    }
}

fn report_recall(engine: &CollectionEngine, points: &[Vec<f32>], cli: &Cli) {
    let q = cli.recall_queries.min(500);
    let mut rng = rand::thread_rng();
    let queries: Vec<Vec<f32>> = (0..q).map(|_| random_unit(cli.dim, &mut rng)).collect();

    let t = Instant::now();
    let mut hits = 0usize;
    let mut total = 0usize;
    for query in &queries {
        let truth = brute_force_topk(points, query, cli.top_k);
        let got = engine
            .search("bench", query, cli.top_k, None, vectordb_core::OutputOptions::default())
            .unwrap();
        let ids: std::collections::HashSet<_> = truth.iter().copied().collect();
        for hit in got {
            if let Some(idx_str) = hit.id.strip_prefix('p') {
                if let Ok(idx) = idx_str.parse::<usize>() {
                    if ids.contains(&idx) {
                        hits += 1;
                    }
                }
            }
        }
        total += cli.top_k;
    }
    let recall = hits as f64 / total as f64;
    println!(
        "  recall@{} (vs brute-force on {} truth points, {} queries): {:.1}% ({:.2}s)",
        cli.top_k,
        points.len(),
        q,
        recall * 100.0,
        t.elapsed().as_secs_f64()
    );
}

fn du(p: &std::path::Path) -> u64 {
    fn rec(p: &std::path::Path) -> u64 {
        let md = match std::fs::metadata(p) {
            Ok(m) => m,
            Err(_) => return 0,
        };
        if md.is_file() {
            return md.len();
        }
        if !md.is_dir() {
            return 0;
        }
        let entries = match std::fs::read_dir(p) {
            Ok(e) => e,
            Err(_) => return 0,
        };
        let mut total = 0;
        for e in entries.flatten() {
            total += rec(&e.path());
        }
        total
    }
    rec(p)
}

fn main() {
    let cli = Cli::parse().apply_scenario();
    println!("VectorDB benchmark harness");
    println!(
        "config: scenario={:?} dim={} count={} queries={} top_k={} workers={} sync_wal={} recall={}",
        cli.scenario,
        cli.dim,
        cli.count,
        cli.queries,
        cli.top_k,
        cli.workers,
        !cli.no_sync_wal,
        cli.measure_recall,
    );

    let _core = bench_core(&cli);
    if !cli.core_only {
        bench_engine(&cli);
    }

    println!("\nFor detailed Criterion reports:");
    println!("  cargo bench -p vectordb-core");
    println!("  cargo bench -p vectordb-storage");
    println!("\nLive end-to-end load test (run gateway first):");
    println!("  python scripts/load_test.py --base http://127.0.0.1:8080 \\");
    println!("    --collection bench --dim 128 --count 100000 --queries 1000 --workers 16");
}
