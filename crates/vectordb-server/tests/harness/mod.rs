//! Test harness: spin up real tonic gRPC servers backed by tempdirs.

use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tonic::transport::Server;
use vectordb_proto::vectordb::v1::vector_service_server::VectorServiceServer;
use vectordb_router::{RebalanceConfig, RebalanceCoordinator, RouterService};

/// Spawn a single-shard data node listening on an ephemeral port. Returns the
/// bound address and a JoinHandle for the server task.
pub async fn spawn_data_node(data_dir: &Path, shard_id: u32) -> (SocketAddr, JoinHandle<()>) {
    use vectordb_server::test_helpers::build_data_service;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let svc = build_data_service(data_dir, shard_id, &format!("http://{addr}"))
        .await
        .expect("build data service");
    let handle = tokio::spawn(async move {
        Server::builder()
            .add_service(VectorServiceServer::new(svc))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    // Tiny grace period so the server is accepting before the caller dials.
    tokio::time::sleep(Duration::from_millis(50)).await;
    (addr, handle)
}

/// Spawn a router fronting `nodes`. Returns the router's bound address and
/// a clone of the rebalance coordinator (so tests can drive sweeps).
pub async fn spawn_router(
    nodes: Vec<(String, String, Vec<u32>)>,
    shard_count: u32,
) -> (SocketAddr, RebalanceCoordinator) {
    use vectordb_cluster::{ClusterConfig, NodeRole, NodeState};

    let cluster = ClusterConfig {
        replication_factor: 1,
        virtual_nodes_per_shard: 128,
        nodes: nodes
            .into_iter()
            .map(|(id, advertise_addr, shard_ids)| NodeState {
                id,
                advertise_addr,
                role: NodeRole::Data,
                shard_ids,
                healthy: true,
            })
            .collect(),
    };

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let svc = RouterService::with_rebalance(
        "router-test",
        &cluster,
        shard_count,
        RebalanceConfig {
            enabled: false,
            interval_secs: 60,
            page_size: 32,
            max_moves_per_sweep: 0,
            idle_skip_after: 5,
        },
    );
    let coord = svc.rebalance();
    tokio::spawn(async move {
        Server::builder()
            .add_service(VectorServiceServer::new(svc))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    (addr, coord)
}
