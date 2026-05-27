//! Engine-level benchmarks: durable upsert + search (WAL + HNSW).

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use rand::prelude::*;
use tempfile::tempdir;
use vectordb_core::{CollectionConfig, DistanceMetric, Vector};
use vectordb_storage::{CollectionEngine, EngineConfig};

fn random_vector(dim: usize, rng: &mut impl Rng) -> Vec<f32> {
    let mut v: Vec<f32> = (0..dim).map(|_| rng.gen::<f32>() - 0.5).collect();
    let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm > 0.0 {
        for x in &mut v {
            *x /= norm;
        }
    }
    v
}

struct BenchEngine {
    _dir: tempfile::TempDir,
    engine: CollectionEngine,
}

fn open_engine() -> BenchEngine {
    let dir = tempdir().unwrap();
    let engine = CollectionEngine::open(EngineConfig::new(dir.path())).unwrap();
    BenchEngine { _dir: dir, engine }
}

fn prep_collection(engine: &CollectionEngine, name: &str, n: usize, dim: usize) {
    let cfg = CollectionConfig::new(name, dim, DistanceMetric::Cosine);
    engine.create_collection(cfg).unwrap();
    let mut rng = rand::thread_rng();
    for i in 0..n {
        engine
            .upsert(
                name,
                format!("p{i}"),
                Vector::new(random_vector(dim, &mut rng)),
                None,
                None,
            )
            .unwrap();
    }
}

fn bench_engine_upsert(c: &mut Criterion) {
    let mut group = c.benchmark_group("engine_upsert");
    let dim = 128;
    for n in [500usize, 2000] {
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::new("points", n), &n, |bencher, &n| {
            bencher.iter(|| {
                let bench = open_engine();
                let cfg = CollectionConfig::new("bench", dim, DistanceMetric::Cosine);
                bench.engine.create_collection(cfg).unwrap();
                let mut rng = rand::thread_rng();
                for i in 0..n {
                    black_box(
                        bench
                            .engine
                            .upsert(
                                "bench",
                                format!("p{i}"),
                                Vector::new(random_vector(dim, &mut rng)),
                                None,
                                None,
                            )
                            .unwrap(),
                    );
                }
            });
        });
    }
    group.finish();
}

fn bench_engine_search(c: &mut Criterion) {
    let mut group = c.benchmark_group("engine_search");
    let dim = 128;
    let n = 5_000;
    let bench = open_engine();
    prep_collection(&bench.engine, "bench", n, dim);
    let mut rng = rand::thread_rng();
    let query = random_vector(dim, &mut rng);
    for k in [10usize, 50] {
        group.bench_with_input(BenchmarkId::new(format!("n={n}"), format!("k={k}")), &k, |bencher, &k| {
            bencher.iter(|| {
                black_box(
                    bench
                        .engine
                        .search("bench", black_box(&query), k, None, Default::default())
                        .unwrap(),
                );
            });
        });
    }
    group.finish();
}

fn bench_bulk_upsert(c: &mut Criterion) {
    let mut group = c.benchmark_group("engine_bulk_upsert");
    let dim = 128;
    let n = 2_000;
    group.throughput(Throughput::Elements(n as u64));
    group.bench_function("bulk_2000", |bencher| {
        bencher.iter(|| {
            let bench = open_engine();
            let cfg = CollectionConfig::new("bench", dim, DistanceMetric::Cosine);
            bench.engine.create_collection(cfg).unwrap();
            let mut rng = rand::thread_rng();
            let points: Vec<_> = (0..n)
                .map(|i| vectordb_storage::BulkPoint {
                    id: format!("p{i}"),
                    vector: Vector::new(random_vector(dim, &mut rng)),
                    payload: None,
                    sparse: None,
                })
                .collect();
            black_box(bench.engine.bulk_upsert("bench", points, 500).unwrap());
        });
    });
    group.finish();
}

criterion_group!(engine_upsert, bench_engine_upsert);
criterion_group!(engine_search, bench_engine_search);
criterion_group!(engine_bulk, bench_bulk_upsert);
criterion_main!(engine_upsert, engine_search, engine_bulk);
