use tempfile::tempdir;
use vectordb_core::{CollectionConfig, DistanceMetric, Vector};
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
            )
            .unwrap();
    }

    let engine = CollectionEngine::open(cfg).unwrap();
    let hits = engine
        .search("docs", &[1.0, 0.0, 0.0], 1, None)
        .unwrap();
    assert_eq!(hits[0].id, "a");
}
