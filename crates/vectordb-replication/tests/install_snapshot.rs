//! End-to-end test: stream a snapshot and restore into an empty engine.

use std::sync::Arc;

use tokio_stream::StreamExt;
use vectordb_core::{CollectionConfig, DistanceMetric, Vector};
use vectordb_replication::install_snapshot::{
    create_snapshot_dir, stream_snapshot_to_peer, SnapshotReceiver,
};
use vectordb_storage::{CollectionEngine, EngineConfig};

#[tokio::test]
async fn install_snapshot_stream_populates_empty_engine() {
    let leader_dir = tempfile::tempdir().unwrap();
    let leader = Arc::new(
        CollectionEngine::open(EngineConfig::new(leader_dir.path())).expect("leader open"),
    );
    let cfg = CollectionConfig::new("docs", 4, DistanceMetric::Cosine);
    leader.create_collection(cfg).expect("create");
    for i in 0..20 {
        leader
            .upsert(
                "docs",
                format!("id-{i}"),
                Vector::new(vec![i as f32, 0.1, 0.2, 0.3]),
                None,
                None,
            )
            .expect("upsert");
    }
    assert_eq!(leader.stats("docs").unwrap().vector_count, 20);

    let snap_dir = create_snapshot_dir(leader.clone()).expect("snapshot");

    let follower_dir = tempfile::tempdir().unwrap();
    let follower = Arc::new(
        CollectionEngine::open(EngineConfig::new(follower_dir.path())).expect("follower open"),
    );
    assert!(follower.list_collections().is_empty());

    let receiver = SnapshotReceiver::new(follower.data_dir().to_path_buf());
    let mut stream = stream_snapshot_to_peer(snap_dir, 1, 10, 20, 1)
        .await
        .expect("stream");

    let mut last_meta = None;
    while let Some(chunk) = stream.next().await {
        if let Some(meta) = receiver
            .apply_chunk(&follower, chunk)
            .expect("apply chunk")
        {
            last_meta = Some(meta);
        }
    }
    assert!(last_meta.is_some(), "expected eof chunk to finish install");
    assert_eq!(follower.list_collections(), vec!["docs".to_string()]);
    assert_eq!(follower.stats("docs").unwrap().vector_count, 20);
}

#[tokio::test]
async fn restore_from_snapshot_matches_create_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Arc::new(CollectionEngine::open(EngineConfig::new(dir.path())).unwrap());
    let cfg = CollectionConfig::new("x", 2, DistanceMetric::DotProduct);
    engine.create_collection(cfg).unwrap();
    engine
        .upsert("x", "a".into(), Vector::new(vec![1.0, 0.0]), None, None)
        .unwrap();

    let snap = engine.snapshot_manager().create().unwrap();
    let empty_dir = tempfile::tempdir().unwrap();
    let empty = CollectionEngine::open(EngineConfig::new(empty_dir.path())).unwrap();
    empty
        .restore_from_snapshot(&snap.data_dir)
        .expect("restore");
    assert_eq!(empty.list_collections(), vec!["x".to_string()]);
    assert_eq!(empty.stats("x").unwrap().vector_count, 1);
}
