//! Run: `cargo run -p vectordb-bench --release -- [OPTIONS]`
//!
//! Prints a single human-readable benchmark report (no Criterion HTML).

use std::time::Instant;

use clap::Parser;
use rand::prelude::*;
use tempfile::tempdir;
use vectordb_core::{CollectionConfig, DistanceMetric, Vector};
use vectordb_core::distance::Distance;
use vectordb_core::hnsw::{HnswConfig, HnswIndex};
use vectordb_storage::{BulkPoint, CollectionEngine, EngineConfig};

#[derive(Parser, Debug)]
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

fn bench_core(dim: usize, count: usize, queries: usize, top_k: usize) {
    println!("\n## Core (in-memory HNSW, dim={dim}, n={count})");

    let config = HnswConfig::new(DistanceMetric::Cosine, 16, 200, 64);
    let index = HnswIndex::new(dim, config);
    let mut rng = rand::thread_rng();

    let t0 = Instant::now();
    for i in 0..count {
        index
            .insert(format!("p{i}"), Vector::new(random_unit(dim, &mut rng)))
            .unwrap();
    }
    let insert_secs = t0.elapsed().as_secs_f64();
    println!(
        "  insert: {:.0} vectors/s ({:.2}s total)",
        count as f64 / insert_secs,
        insert_secs
    );

    let query_vec = random_unit(dim, &mut rng);
    let mut latencies = Vec::with_capacity(queries);
    for _ in 0..queries {
        let t = Instant::now();
        let _ = index.search(&query_vec, top_k).unwrap();
        latencies.push(t.elapsed().as_secs_f64() * 1e6);
    }
    let qps = queries as f64 / latencies.iter().sum::<f64>() * 1e6;
    println!("  search k={top_k}: {:.0} QPS", qps);
    report_latencies("search latency", &mut latencies);

    let a = random_unit(dim, &mut rng);
    let b = random_unit(dim, &mut rng);
    let t = Instant::now();
    let iters = 100_000usize;
    for _ in 0..iters {
        std::hint::black_box(Distance::cosine_distance(&a, &b));
    }
    let us = t.elapsed().as_secs_f64() * 1e6 / iters as f64;
    println!("  cosine distance (dim={dim}): {:.3} µs/op", us);
}

fn bench_engine(dim: usize, count: usize, queries: usize, top_k: usize) {
    println!("\n## Engine (WAL + RocksDB + HNSW, dim={dim}, n={count})");

    let dir = tempdir().unwrap();
    let engine = CollectionEngine::open(EngineConfig::new(dir.path())).unwrap();
    engine
        .create_collection(CollectionConfig::new(
            "bench",
            dim,
            DistanceMetric::Cosine,
        ))
        .unwrap();

    let mut rng = rand::thread_rng();
    let t0 = Instant::now();
    let points: Vec<BulkPoint> = (0..count)
        .map(|i| BulkPoint {
            id: format!("p{i}"),
            vector: Vector::new(random_unit(dim, &mut rng)),
            payload: None,
            sparse: None,
        })
        .collect();
    engine.bulk_upsert("bench", points, 500).unwrap();
    let bulk_secs = t0.elapsed().as_secs_f64();
    println!(
        "  bulk_upsert: {:.0} vectors/s ({:.2}s total)",
        count as f64 / bulk_secs,
        bulk_secs
    );

    let query_vec = random_unit(dim, &mut rng);
    let mut latencies = Vec::with_capacity(queries);
    for _ in 0..queries {
        let t = Instant::now();
        let _ = engine.search("bench", &query_vec, top_k, None).unwrap();
        latencies.push(t.elapsed().as_secs_f64() * 1e6);
    }
    let qps = queries as f64 / latencies.iter().sum::<f64>() * 1e6;
    println!("  search k={top_k}: {:.0} QPS", qps);
    report_latencies("search latency", &mut latencies);
}

fn main() {
    let cli = Cli::parse();
    println!("VectorDB benchmark harness");
    println!(
        "config: dim={} count={} queries={} top_k={}",
        cli.dim, cli.count, cli.queries, cli.top_k
    );

    bench_core(cli.dim, cli.count, cli.queries, cli.top_k);
    if !cli.core_only {
        bench_engine(cli.dim, cli.count, cli.queries, cli.top_k);
    }

    println!("\nFor detailed Criterion reports:");
    println!("  cargo bench -p vectordb-core");
    println!("  cargo bench -p vectordb-storage");
}
