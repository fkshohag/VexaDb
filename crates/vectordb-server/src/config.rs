use std::fs;
use std::path::Path;

use serde::{Deserialize, Serialize};
use vectordb_cluster::{ClusterConfig, NodeRole, NodeState};
use vectordb_storage::EngineConfig;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    pub server: ServerSection,
    pub storage: EngineConfig,
    pub cluster: ClusterSection,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerSection {
    pub listen: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterSection {
    pub node_id: String,
    pub role: NodeRole,
    pub shard_count: u32,
    pub shard_id: u32,
    pub peers: Vec<String>,
}

impl ServerConfig {
    pub fn default_local() -> Self {
        Self {
            server: ServerSection {
                listen: "0.0.0.0:6334".into(),
            },
            storage: EngineConfig::new("./data"),
            cluster: ClusterSection {
                node_id: "node-1".into(),
                role: NodeRole::AllInOne,
                shard_count: 1,
                shard_id: 0,
                peers: vec![],
            },
        }
    }

    pub fn cluster_config(&self) -> ClusterConfig {
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

pub fn load_config(path: &Path) -> anyhow::Result<ServerConfig> {
    let raw = fs::read_to_string(path)?;
    let cfg: ServerConfig = toml::from_str(&raw)?;
    Ok(cfg)
}

pub fn example_config() -> ServerConfig {
    ServerConfig::default_local()
}
