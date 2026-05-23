use serde_json::json;
use tempfile::tempdir;
use vectordb_core::{
    CollectionConfig, DistanceMetric, Filter, PayloadFieldIndex, PayloadIndexKind, SearchMode,
    SparseVector, Vector,
};
use vectordb_storage::search::SearchParams;
use vectordb_storage::{CollectionEngine, EngineConfig};

#[test]
fn wal_survives_reopen() {
    let dir = tempdir().unwrap();
    let cfg = EngineConfig::new(dir.path());

    {
        let engine = CollectionEngine::open(cfg.clone()).unwrap();
        let coll = CollectionConfig::new("docs", 3, DistanceMetric::Cosine);
        engine.create_collection(coll).unwrap();
        engine
            .upsert(
                "docs",
                "a".into(),
                Vector::new(vec![1.0, 0.0, 0.0]),
                None,
                None,
            )
            .unwrap();
    }

    let engine = CollectionEngine::open(cfg).unwrap();
    let hits = engine.search("docs", &[1.0, 0.0, 0.0], 1, None).unwrap();
    assert_eq!(hits[0].id, "a");
}

#[test]
fn filtered_search_with_payload_indexes() {
    let dir = tempdir().unwrap();
    let cfg = EngineConfig::new(dir.path());
    let engine = CollectionEngine::open(cfg).unwrap();

    let mut coll = CollectionConfig::new("books", 3, DistanceMetric::Cosine);
    coll.payload_indexes = vec![
        PayloadFieldIndex {
            field: "category".into(),
            kind: PayloadIndexKind::Keyword,
        },
        PayloadFieldIndex {
            field: "price".into(),
            kind: PayloadIndexKind::Numeric,
        },
    ];
    engine.create_collection(coll).unwrap();

    let upsert = |id: &str, v: [f32; 3], payload: serde_json::Value| {
        engine
            .upsert(
                "books",
                id.into(),
                Vector::new(v.to_vec()),
                Some(serde_json::to_vec(&payload).unwrap()),
                None,
            )
            .unwrap();
    };
    upsert("a", [1.0, 0.0, 0.0], json!({"category": "books", "price": 20}));
    upsert("b", [0.99, 0.05, 0.0], json!({"category": "books", "price": 80}));
    upsert("c", [0.95, 0.1, 0.0], json!({"category": "movies", "price": 15}));

    let filter: Filter = serde_json::from_value(json!({
        "must": [
            {"key": "category", "match": {"value": "books"}},
            {"key": "price", "range": {"lte": 50}}
        ]
    }))
    .unwrap();

    let hits = engine
        .search("books", &[1.0, 0.0, 0.0], 5, Some(&filter))
        .unwrap();
    let ids: Vec<_> = hits.iter().map(|h| h.id.as_str()).collect();
    assert_eq!(ids, vec!["a"]);
}

#[test]
fn snapshot_create_and_list() {
    let dir = tempdir().unwrap();
    let cfg = EngineConfig::new(dir.path());
    let engine = CollectionEngine::open(cfg).unwrap();
    engine
        .create_collection(CollectionConfig::new("docs", 3, DistanceMetric::Cosine))
        .unwrap();
    engine
        .upsert(
            "docs",
            "a".into(),
            Vector::new(vec![1.0, 0.0, 0.0]),
            None,
            None,
        )
        .unwrap();

    let mgr = engine.snapshot_manager();
    let snap = mgr.create().unwrap();
    let listed = mgr.list().unwrap();
    assert!(listed.iter().any(|s| s.id == snap.id));

    mgr.delete(&snap.id).unwrap();
    assert!(mgr.list().unwrap().iter().all(|s| s.id != snap.id));
}

#[test]
fn hybrid_bm25_rrf_search() {
    let dir = tempdir().unwrap();
    let engine = CollectionEngine::open(EngineConfig::new(dir.path())).unwrap();

    let mut coll = CollectionConfig::new("hybrid", 3, DistanceMetric::Cosine);
    coll.bm25_text_field = Some("text".into());
    engine.create_collection(coll).unwrap();

    engine
        .upsert(
            "hybrid",
            "a".into(),
            Vector::new(vec![1.0, 0.0, 0.0]),
            Some(serde_json::to_vec(&serde_json::json!({"text": "rust database"})).unwrap()),
            None,
        )
        .unwrap();
    engine
        .upsert(
            "hybrid",
            "b".into(),
            Vector::new(vec![0.0, 1.0, 0.0]),
            Some(serde_json::to_vec(&serde_json::json!({"text": "python web"})).unwrap()),
            None,
        )
        .unwrap();

    let hits = engine
        .search_params(
            "hybrid",
            SearchParams {
                query: &[0.1, 0.9, 0.0],
                sparse_query: None,
                text_query: Some("database"),
                mode: SearchMode::HybridRrf,
                hybrid_alpha: 0.5,
                filter: None,
            },
            2,
        )
        .unwrap();
    assert!(!hits.is_empty());
}

#[test]
fn sparse_vector_search() {
    let dir = tempdir().unwrap();
    let engine = CollectionEngine::open(EngineConfig::new(dir.path())).unwrap();
    let mut coll = CollectionConfig::new("sparse", 3, DistanceMetric::Cosine);
    coll.sparse_enabled = true;
    engine.create_collection(coll).unwrap();

    engine
        .upsert(
            "sparse",
            "a".into(),
            Vector::new(vec![1.0, 0.0, 0.0]),
            None,
            Some(SparseVector::from_pairs([(10, 1.0), (20, 0.5)])),
        )
        .unwrap();
    engine
        .upsert(
            "sparse",
            "b".into(),
            Vector::new(vec![0.9, 0.1, 0.0]),
            None,
            Some(SparseVector::from_pairs([(10, 0.5), (30, 1.0)])),
        )
        .unwrap();

    let q = SparseVector::from_pairs([(10, 1.0)]);
    let hits = engine
        .search_params(
            "sparse",
            SearchParams {
                query: &[1.0, 0.0, 0.0],
                sparse_query: Some(&q),
                text_query: None,
                mode: SearchMode::Sparse,
                hybrid_alpha: 0.5,
                filter: None,
            },
            1,
        )
        .unwrap();
    assert_eq!(hits[0].id, "a");
}
