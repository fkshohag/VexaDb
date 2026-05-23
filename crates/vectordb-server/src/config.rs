use std::fs;
use std::path::Path;

use serde::{Deserialize, Serialize};
use vectordb_auth::AuthConfig;
use vectordb_cluster::{ClusterConfig, NodeRole, NodeState};
use vectordb_replication::RaftConfig;
use vectordb_storage::EngineConfig;

use crate::tls::TlsConfig;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    pub server: ServerSection,
    #[serde(default)]
    pub storage: EngineConfig,
    pub cluster: ClusterSection,
    #[serde(default)]
    pub raft: Option<RaftConfig>,
    #[serde(default)]
    pub auth: AuthConfig,
    #[serde(default)]
    pub tls: Option<TlsConfig>,
    #[serde(default)]
    pub metrics: Option<MetricsSection>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerSection {
    pub listen: String,
    /// When replication is enabled, `/ready` semantics require leadership for writes.
    #[serde(default = "default_true")]
    pub readiness_requires_leader: bool,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsSection {
    /// Prometheus scrape listen address, e.g. `0.0.0.0:9090`.
    pub listen: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterSection {
    pub node_id: String,
    pub role: NodeRole,
    pub shard_count: u32,
    #[serde(default)]
    pub shard_id: u32,
    #[serde(default)]
    pub peers: Vec<String>,
    /// Remote data nodes (required for router role).
    #[serde(default)]
    pub nodes: Vec<RemoteNodeConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteNodeConfig {
    pub id: String,
    /// gRPC address, e.g. `127.0.0.1:6334` or `http://127.0.0.1:6334`
    pub grpc: String,
    #[serde(default)]
    pub shard_ids: Vec<u32>,
}

impl ServerConfig {
    pub fn default_local() -> Self {
        Self {
            server: ServerSection {
                listen: "0.0.0.0:6334".into(),
                readiness_requires_leader: true,
            },
            storage: EngineConfig::new("./data"),
            cluster: ClusterSection {
                node_id: "node-1".into(),
                role: NodeRole::AllInOne,
                shard_count: 1,
                shard_id: 0,
                peers: vec![],
                nodes: vec![],
            },
            raft: None,
            auth: AuthConfig::default(),
            tls: None,
            metrics: None,
        }
    }

    pub fn raft_enabled(&self) -> bool {
        self.raft.is_some()
    }

    pub fn is_router(&self) -> bool {
        matches!(self.cluster.role, NodeRole::Router)
    }

    pub fn vector_endpoint(&self) -> String {
        if let Some(ep) = self.raft.as_ref().and_then(|r| r.vector_endpoint.clone()) {
            return normalize_endpoint(&ep);
        }
        normalize_endpoint(&self.server.listen)
    }

    pub fn cluster_config(&self) -> ClusterConfig {
        if !self.cluster.nodes.is_empty() {
            return ClusterConfig {
                replication_factor: 1,
                virtual_nodes_per_shard: 128,
                nodes: self
                    .cluster
                    .nodes
                    .iter()
                    .map(|n| NodeState {
                        id: n.id.clone(),
                        advertise_addr: n.grpc.clone(),
                        role: NodeRole::Data,
                        shard_ids: if n.shard_ids.is_empty() {
                            vec![]
                        } else {
                            n.shard_ids.clone()
                        },
                        healthy: true,
                    })
                    .collect(),
            };
        }

        ClusterConfig {
            replication_factor: 1,
            virtual_nodes_per_shard: 128,
            nodes: vec![NodeState {
                id: self.cluster.node_id.clone(),
                advertise_addr: self.server.listen.clone(),
                role: self.cluster.role,
                shard_ids: vec![self.cluster.shard_id],
                healthy: true,
            }],
        }
    }
}

pub fn normalize_endpoint(addr: &str) -> String {
    if addr.starts_with("http://") || addr.starts_with("https://") {
        addr.to_string()
    } else {
        format!("http://{addr}")
    }
}

pub fn load_config(path: &Path) -> anyhow::Result<ServerConfig> {
    let raw = fs::read_to_string(path)?;
    let mut cfg: ServerConfig = toml::from_str(&raw)?;
    // Default vector_endpoint on raft config from server listen.
    let endpoint = cfg.vector_endpoint();
    if let Some(raft) = cfg.raft.as_mut() {
        if raft.vector_endpoint.is_none() {
            raft.vector_endpoint = Some(endpoint);
        }
    }
    Ok(cfg)
}
