use std::sync::Arc;
use std::time::Duration;

use tempfile::tempdir;
use vectordb_core::{CollectionConfig, DistanceMetric, Vector};
use vectordb_replication::{RaftConfig, RaftNode, RaftPeer, Role};
use vectordb_storage::{CollectionEngine, EngineConfig, WalEntry};

#[tokio::test]
async fn elects_leader_and_replicates_write() {
    let d1 = tempdir().unwrap();
    let d2 = tempdir().unwrap();
    let d3 = tempdir().unwrap();

    let peers = vec![
        RaftPeer {
            id: 1,
            addr: "127.0.0.1:19101".into(),
        },
        RaftPeer {
            id: 2,
            addr: "127.0.0.1:19102".into(),
        },
        RaftPeer {
            id: 3,
            addr: "127.0.0.1:19103".into(),
        },
    ];

    let e1 = Arc::new(
        CollectionEngine::open(EngineConfig::new(d1.path())).unwrap(),
    );
    let e2 = Arc::new(
        CollectionEngine::open(EngineConfig::new(d2.path())).unwrap(),
    );
    let e3 = Arc::new(
        CollectionEngine::open(EngineConfig::new(d3.path())).unwrap(),
    );

    let n1 = RaftNode::start(
        RaftConfig {
            node_id: 1,
            listen: "127.0.0.1:19101".into(),
            peers: peers.clone(),
            election_timeout_ms: 300,
            heartbeat_interval_ms: 80,
        },
        e1.clone(),
    )
    .await
    .unwrap();

    let n2 = RaftNode::start(
        RaftConfig {
            node_id: 2,
            listen: "127.0.0.1:19102".into(),
            peers: peers.clone(),
            election_timeout_ms: 300,
            heartbeat_interval_ms: 80,
        },
        e2.clone(),
    )
    .await
    .unwrap();

    let n3 = RaftNode::start(
        RaftConfig {
            node_id: 3,
            listen: "127.0.0.1:19103".into(),
            peers: peers.clone(),
            election_timeout_ms: 300,
            heartbeat_interval_ms: 80,
        },
        e3.clone(),
    )
    .await
    .unwrap();

    let nodes = [(&n1, &e1), (&n2, &e2), (&n3, &e3)];
    let mut leader = None;
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(200)).await;
        if let Some(found) = nodes.iter().find(|(n, _)| n.is_leader()) {
            leader = Some(found);
            break;
        }
    }

    let Some((leader_node, _)) = leader else {
        panic!("no leader elected after 10s");
    };

    assert_eq!(leader_node.role(), Role::Leader);

    leader_node
        .propose(WalEntry::CreateCollection {
            config: CollectionConfig::new("docs", 3, DistanceMetric::Cosine),
        })
        .await
        .unwrap();

    leader_node
        .propose(WalEntry::Upsert {
            collection: "docs".into(),
            id: "vec-1".into(),
            vector: Vector::new(vec![1.0, 0.0, 0.0]),
            payload: None,
            sparse: None,
        })
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await;

    for engine in [&e1, &e2, &e3] {
        let hits = engine
            .search(
                "docs",
                &[1.0, 0.0, 0.0],
                1,
                None,
                vectordb_core::OutputOptions::default(),
            )
            .unwrap();
        assert_eq!(hits[0].id, "vec-1");
    }
}
