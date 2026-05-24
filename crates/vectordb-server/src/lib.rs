//! Library surface for `vectordb-server`. The binary at `src/main.rs`
//! consumes the same modules; integration tests use `test_helpers` to
//! stand up real services in-process.

pub mod auth_interceptor;
pub mod config;
pub mod leader;
pub mod metrics;
pub mod replica_bootstrap;
pub mod replication;
pub mod service;
pub mod tls;

pub use config::{load_config, ServerConfig};
pub use replica_bootstrap::{bootstrap_from_peer, BootstrapConfig, BootstrapReport};
pub use service::VectorServiceImpl;

/// Helpers for integration tests / harness code in this and downstream
/// crates. Always built — the cost is trivial and gating it behind a
/// feature flag breaks `cargo test` consumers.
pub mod test_helpers {
    use std::path::Path;

    use vectordb_cluster::NodeRole;
    use vectordb_storage::EngineConfig;

    use crate::config::{ClusterSection, ServerConfig, ServerSection};
    use crate::service::VectorServiceImpl;

    /// Build a single-shard `VectorServiceImpl` rooted at `data_dir`. The
    /// `vector_endpoint` MUST be the http://… form the test will dial,
    /// because the service uses it for self-routing decisions.
    pub async fn build_data_service(
        data_dir: &Path,
        shard_id: u32,
        vector_endpoint: &str,
    ) -> anyhow::Result<VectorServiceImpl> {
        let cfg = ServerConfig {
            server: ServerSection {
                listen: strip_scheme(vector_endpoint).to_string(),
                readiness_requires_leader: false,
            },
            storage: EngineConfig::new(data_dir),
            cluster: ClusterSection {
                node_id: format!("data-{shard_id}"),
                role: NodeRole::Data,
                shard_count: 1,
                shard_id,
                replication_factor: 1,
                peers: vec![],
                nodes: vec![],
                router_grpc: None,
            },
            raft: None,
            auth: vectordb_auth::AuthConfig::default(),
            tls: None,
            metrics: None,
            snapshot: crate::config::SnapshotSection::default(),
            rebalance: vectordb_router::RebalanceConfig::default(),
            topology: vectordb_router::TopologyConfig::default(),
            bootstrap: crate::config::BootstrapConfig::default(),
        };
        VectorServiceImpl::new(cfg).await
    }

    fn strip_scheme(url: &str) -> &str {
        url.trim_start_matches("http://")
            .trim_start_matches("https://")
    }
}
