//! End-to-end test for auto-rebalance:
//!
//! 1. Stand up two real `VectorServiceImpl` instances on local ports.
//! 2. Build a `RouterService` configured with both shards.
//! 3. Bypass the router and write 50 points DIRECTLY to shard 0
//!    (simulating "we just added a new shard; old data is now on the
//!    wrong shard").
//! 4. Trigger one rebalance sweep.
//! 5. Assert the orphans landed on their correct hash-target shard.
//! 6. Run the sweep a second time → 0 moves (idempotency).

use std::net::SocketAddr;
use std::time::Duration;

use tempfile::TempDir;
use tokio::time::sleep;
use vectordb_client::{cosine_collection, VectorDbClient};
use vectordb_proto::vectordb::v1::{vector_service_server::VectorServiceServer, VectorPoint};

mod harness;
use harness::{spawn_data_node, spawn_router};

#[tokio::test]
async fn auto_rebalance_moves_orphans_and_is_idempotent() {
    let _ = tracing_subscriber::fmt::try_init();

    // Two data nodes on ephemeral ports.
    let dir_a = TempDir::new().unwrap();
    let dir_b = TempDir::new().unwrap();
    let (addr_a, _node_a) = spawn_data_node(dir_a.path(), 0).await;
    let (addr_b, _node_b) = spawn_data_node(dir_b.path(), 1).await;

    // Router covering both shards.
    let nodes = vec![
        (
            "shard-a".to_string(),
            format!("http://{addr_a}"),
            vec![0u32],
        ),
        (
            "shard-b".to_string(),
            format!("http://{addr_b}"),
            vec![1u32],
        ),
    ];
    let (router_addr, coord) = spawn_router(nodes.clone(), 2).await;

    // Wait for everyone to come up.
    for ep in [
        format!("http://{addr_a}"),
        format!("http://{addr_b}"),
        format!("http://{router_addr}"),
    ] {
        wait_healthy(&ep).await;
    }

    // Create the collection on every shard via the router.
    let mut router_client = VectorDbClient::connect(format!("http://{router_addr}"))
        .await
        .unwrap();
    router_client
        .create_collection(cosine_collection("docs", 4))
        .await
        .unwrap();

    // Seed 50 points DIRECTLY into shard A. Roughly half hash to shard B
    // (orphans) and the rest are correctly placed.
    let mut shard_a = VectorDbClient::connect(format!("http://{addr_a}"))
        .await
        .unwrap();
    let points: Vec<VectorPoint> = (0..50)
        .map(|i| VectorPoint {
            id: format!("p{i:03}"),
            values: vec![i as f32, 0.0, 0.0, 0.0],
            payload: vec![],
            sparse: None,
        })
        .collect();
    shard_a.upsert("docs", points).await.unwrap();

    // Compute expected orphan count from the same hash function the
    // coordinator uses, so the test is robust to vnode tuning.
    let expected_orphans: u64 = (0..50)
        .filter(|i| vectordb_cluster::shard_for_point(&format!("p{i:03}"), 2) == 1)
        .count() as u64;
    assert!(
        expected_orphans > 0,
        "test fixture must include orphans for the assertion to be meaningful"
    );

    // First sweep: should migrate exactly `expected_orphans`.
    let r1 = coord.run_once(false).await.unwrap();
    assert_eq!(
        r1.moved, expected_orphans,
        "first sweep should move all orphans"
    );
    assert_eq!(r1.failed, 0);

    // Second sweep: cluster is balanced now → 0 moves (idempotency).
    let r2 = coord.run_once(false).await.unwrap();
    assert_eq!(r2.moved, 0, "second sweep should be a no-op");
    assert_eq!(r2.failed, 0);
    // Each of the 50 points is now sitting on its correct shard, so the
    // sweep should see each one exactly once and count it as kept.
    assert_eq!(r2.kept, 50, "all 50 points should be correctly placed");

    // Verify shard B actually has the orphans.
    let mut shard_b = VectorDbClient::connect(format!("http://{addr_b}"))
        .await
        .unwrap();
    let mut found_b = 0;
    let mut cursor = String::new();
    loop {
        let (pts, next) = shard_b.scroll("docs", &cursor, 100).await.unwrap();
        found_b += pts.len() as u64;
        if next.is_empty() {
            break;
        }
        cursor = next;
    }
    assert_eq!(found_b, expected_orphans);

    // ...and shard A has the rest, with no overlap.
    let mut found_a = 0;
    let mut cursor = String::new();
    loop {
        let (pts, next) = shard_a.scroll("docs", &cursor, 100).await.unwrap();
        found_a += pts.len() as u64;
        if next.is_empty() {
            break;
        }
        cursor = next;
    }
    assert_eq!(found_a, 50 - expected_orphans);
}

async fn wait_healthy(endpoint: &str) {
    for _ in 0..50 {
        if let Ok(mut c) = VectorDbClient::connect(endpoint.to_string()).await {
            if c.health().await.is_ok() {
                return;
            }
        }
        sleep(Duration::from_millis(50)).await;
    }
    panic!("endpoint {endpoint} never became healthy");
}

#[allow(dead_code)]
fn _link(_: SocketAddr, _: VectorServiceServer<()>) {}
