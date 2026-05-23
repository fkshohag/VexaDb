use criterion::{black_box, criterion_group, criterion_main, Criterion};
use vectordb_core::collection::DistanceMetric;
use vectordb_core::hnsw::{HnswConfig, HnswIndex};
use vectordb_core::types::Vector;

fn bench_insert_search(c: &mut Criterion) {
    let config = HnswConfig::new(DistanceMetric::Cosine, 16, 100, 64);
    let index = HnswIndex::new(128, config);

    for i in 0..1000 {
        let v: Vec<f32> = (0..128).map(|j| ((i + j) as f32) * 0.001).collect();
        index
            .insert(format!("p{i}"), Vector::new(v))
            .unwrap();
    }

    let query: Vec<f32> = (0..128).map(|j| (j as f32) * 0.001).collect();

    c.bench_function("hnsw_search_k10", |b| {
        b.iter(|| {
            black_box(index.search(black_box(&query), 10).unwrap());
        });
    });
}

criterion_group!(benches, bench_insert_search);
criterion_main!(benches);
