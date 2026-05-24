//! End-to-end test for replica bootstrap.
//!
//! Spawns a "source" data node, writes a collection + a small batch of
//! points to it, then runs `bootstrap_from_peer` against an empty engine
//! (the "fresh replica") and asserts the data was copied via the public
//! Scroll RPC.

mod harness;

use std::sync::Arc;

use vectordb_client::VectorDbClient;
use vectordb_proto::vectordb::v1::{CollectionSpec, DistanceMetric, VectorPoint};
use vectordb_replication::{RaftConfig, RaftPeer};
use vectordb_server::{bootstrap_from_peer, BootstrapConfig};
use vectordb_storage::{CollectionEngine, EngineConfig};

#[tokio::test]
async fn fresh_replica_bootstraps_from_peer_via_scroll() {
    let source_dir = tempfile::tempdir().unwrap();
    let (source_addr, _source_handle) = harness::spawn_data_node(source_dir.path(), 0).await;
    let source_endpoint = format!("http://{source_addr}");

    // 1. Populate the source node like an external client would.
    let mut client = VectorDbClient::connect(source_endpoint.clone())
        .await
        .expect("connect source");
    client
        .create_collection(CollectionSpec {
            name: "demo".into(),
            dimension: 3,
            metric: DistanceMetric::Cosine as i32,
            m: 0,
            ef_construction: 0,
            ef_search: 0,
            payload_indexes: vec![],
            sparse_enabled: false,
            bm25_text_field: String::new(),
            scalar_quantization: false,
        })
        .await
        .expect("create collection on source");

    let mut points = Vec::new();
    for i in 0..50 {
        points.push(VectorPoint {
            id: format!("pt-{i:03}"),
            values: vec![i as f32 / 50.0, (i % 7) as f32 / 7.0, 0.25],
            payload: Vec::new(),
            sparse: None,
        });
    }
    client.upsert("demo", points).await.expect("upsert source");

    // 2. Build a fresh, empty engine. This represents a brand-new replica
    //    coming online with an empty data dir.
    let replica_dir = tempfile::tempdir().unwrap();
    let engine = Arc::new(
        CollectionEngine::open(EngineConfig::new(replica_dir.path())).expect("open replica engine"),
    );
    assert!(engine.list_collections().is_empty());

    // 3. Run bootstrap. The "raft config" only needs the peer list; the
    //    bootstrap itself uses the public Scroll/Upsert RPCs, not Raft.
    let raft = RaftConfig {
        node_id: 99,
        listen: "0.0.0.0:0".into(),
        vector_endpoint: None,
        peers: vec![
            // self
            RaftPeer {
                id: 99,
                addr: "0.0.0.0:0".into(),
                grpc: None,
            },
            // source — this is what bootstrap should pick.
            RaftPeer {
                id: 1,
                addr: source_addr.to_string(),
                grpc: Some(source_endpoint.clone()),
            },
        ],
        election_timeout_ms: 750,
        heartbeat_interval_ms: 150,
    };
    let cfg = BootstrapConfig {
        enabled: true,
        page_size: 16,
        bulk_chunk_size: 16,
        max_total_secs: 30,
        peer_connect_secs: 3,
        snapshot_after: false, // skip — irrelevant for correctness, slow on tmpfs
    };
    let report = bootstrap_from_peer(&cfg, Some(&raft), None, engine.clone())
        .await
        .expect("bootstrap_from_peer");

    assert_eq!(report.collections_created, 1);
    assert_eq!(report.points_copied, 50);
    assert_eq!(
        report.source_endpoint.as_deref(),
        Some(source_endpoint.as_str())
    );

    // 4. Verify the local engine actually got the data.
    assert_eq!(engine.list_collections(), vec!["demo".to_string()]);
    let stats = engine.stats("demo").expect("stats");
    assert_eq!(stats.vector_count, 50);
    assert_eq!(stats.dimension, 3);
}

#[tokio::test]
async fn bootstrap_skips_when_local_wal_is_non_empty() {
    let source_dir = tempfile::tempdir().unwrap();
    let (source_addr, _source_handle) = harness::spawn_data_node(source_dir.path(), 0).await;
    let source_endpoint = format!("http://{source_addr}");

    // Replica that ALREADY has some data — bootstrap must NOT overwrite it.
    let replica_dir = tempfile::tempdir().unwrap();
    let engine = Arc::new(
        CollectionEngine::open(EngineConfig::new(replica_dir.path())).expect("open replica engine"),
    );
    let cfg = vectordb_core::CollectionConfig::new(
        "preexisting".to_string(),
        3,
        vectordb_core::DistanceMetric::Cosine,
    );
    engine.create_collection(cfg).expect("create existing");

    let raft = RaftConfig {
        node_id: 99,
        listen: "0.0.0.0:0".into(),
        vector_endpoint: None,
        peers: vec![RaftPeer {
            id: 1,
            addr: source_addr.to_string(),
            grpc: Some(source_endpoint),
        }],
        election_timeout_ms: 750,
        heartbeat_interval_ms: 150,
    };
    let bootstrap_cfg = BootstrapConfig {
        enabled: true,
        ..Default::default()
    };
    let report = bootstrap_from_peer(&bootstrap_cfg, Some(&raft), None, engine.clone())
        .await
        .expect("bootstrap_from_peer");

    assert_eq!(report.points_copied, 0);
    assert_eq!(report.collections_created, 0);
    assert_eq!(report.source_endpoint, None);

    // Existing collection still there, nothing new added.
    assert_eq!(engine.list_collections(), vec!["preexisting".to_string()]);
}

#[tokio::test]
async fn bootstrap_no_reachable_peer_is_warning_not_error() {
    let replica_dir = tempfile::tempdir().unwrap();
    let engine = Arc::new(
        CollectionEngine::open(EngineConfig::new(replica_dir.path())).expect("open replica engine"),
    );

    // Point at a port nobody is listening on.
    let raft = RaftConfig {
        node_id: 99,
        listen: "0.0.0.0:0".into(),
        vector_endpoint: None,
        peers: vec![RaftPeer {
            id: 1,
            addr: "127.0.0.1:1".into(),
            grpc: Some("http://127.0.0.1:1".into()),
        }],
        election_timeout_ms: 750,
        heartbeat_interval_ms: 150,
    };
    let cfg = BootstrapConfig {
        enabled: true,
        peer_connect_secs: 1,
        ..Default::default()
    };
    let report = bootstrap_from_peer(&cfg, Some(&raft), None, engine.clone())
        .await
        .expect("must not return Err — fresh single-node start is legitimate");
    assert_eq!(report.points_copied, 0);
}
