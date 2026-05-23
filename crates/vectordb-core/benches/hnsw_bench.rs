//! Criterion benchmarks: distance kernels, HNSW insert/search at scale.

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use rand::prelude::*;
use vectordb_core::collection::DistanceMetric;
use vectordb_core::distance::Distance;
use vectordb_core::hnsw::{HnswConfig, HnswIndex};
use vectordb_core::types::Vector;

fn random_unit_vector(dim: usize, rng: &mut impl Rng) -> Vec<f32> {
    let mut v: Vec<f32> = (0..dim).map(|_| rng.gen::<f32>() - 0.5).collect();
    let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm > 0.0 {
        for x in &mut v {
            *x /= norm;
        }
    }
    v
}

fn bench_distance(c: &mut Criterion) {
    let mut group = c.benchmark_group("distance");
    let mut rng = rand::thread_rng();
    for dim in [128usize, 384, 768, 1536] {
        let a = random_unit_vector(dim, &mut rng);
        let b = random_unit_vector(dim, &mut rng);
        group.throughput(Throughput::Elements(dim as u64));
        group.bench_with_input(BenchmarkId::new("cosine", dim), &dim, |bencher, _| {
            bencher.iter(|| black_box(Distance::cosine_distance(black_box(&a), black_box(&b))));
        });
        group.bench_with_input(BenchmarkId::new("l2_squared", dim), &dim, |bencher, _| {
            bencher.iter(|| black_box(Distance::l2_squared(black_box(&a), black_box(&b))));
        });
        group.bench_with_input(BenchmarkId::new("dot", dim), &dim, |bencher, _| {
            bencher.iter(|| black_box(Distance::dot(black_box(&a), black_box(&b))));
        });
    }
    group.finish();
}

fn build_index(n: usize, dim: usize) -> (HnswIndex, Vec<f32>) {
    let config = HnswConfig::new(DistanceMetric::Cosine, 16, 200, 64);
    let index = HnswIndex::new(dim, config);
    let mut rng = rand::thread_rng();
    for i in 0..n {
        let v = random_unit_vector(dim, &mut rng);
        index
            .insert(format!("p{i}"), Vector::new(v))
            .expect("insert");
    }
    let query = random_unit_vector(dim, &mut rng);
    (index, query)
}

fn bench_hnsw_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("hnsw_insert");
    let dim = 128;
    for n in [1_000usize, 10_000] {
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::new("vectors", n), &n, |bencher, &n| {
            let config = HnswConfig::new(DistanceMetric::Cosine, 16, 200, 64);
            let mut rng = rand::thread_rng();
            bencher.iter(|| {
                let index = HnswIndex::new(dim, config.clone());
                for i in 0..n {
                    let v = random_unit_vector(dim, &mut rng);
                    black_box(
                        index
                            .insert(format!("p{i}"), Vector::new(v))
                            .unwrap(),
                    );
                }
            });
        });
    }
    group.finish();
}

fn bench_hnsw_search(c: &mut Criterion) {
    let mut group = c.benchmark_group("hnsw_search");
    let dim = 128;
    for n in [1_000usize, 10_000] {
        let (index, query) = build_index(n, dim);
        for k in [1usize, 10, 100] {
            let id = BenchmarkId::from_parameter(format!("n={n}/k={k}"));
            group.bench_function(id, |bencher| {
                bencher.iter(|| black_box(index.search(black_box(&query), k).unwrap()));
            });
        }
    }
    group.finish();
}

criterion_group!(distance, bench_distance);
criterion_group!(hnsw_insert, bench_hnsw_insert);
criterion_group!(hnsw_search, bench_hnsw_search);
criterion_main!(distance, hnsw_insert, hnsw_search);
