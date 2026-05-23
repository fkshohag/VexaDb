use std::collections::HashMap;

use serde::{Deserialize, Serialize};

pub type NodeId = String;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeRole {
    /// Routes requests and holds cluster metadata.
    Router,
    /// Stores shards and serves vector operations.
    Data,
    /// Combined router + data (single-node dev mode).
    AllInOne,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeState {
    pub id: NodeId,
    pub advertise_addr: String,
    pub role: NodeRole,
    pub shard_ids: Vec<u32>,
    pub healthy: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterConfig {
    pub replication_factor: usize,
    pub virtual_nodes_per_shard: u32,
    pub nodes: Vec<NodeState>,
}

impl ClusterConfig {
    pub fn single_node(node_id: impl Into<String>, addr: impl Into<String>) -> Self {
        Self {
            replication_factor: 1,
            virtual_nodes_per_shard: 128,
            nodes: vec![NodeState {
                id: node_id.into(),
                advertise_addr: addr.into(),
                role: NodeRole::AllInOne,
                shard_ids: vec![0],
                healthy: true,
            }],
        }
    }

    pub fn data_nodes(&self) -> impl Iterator<Item = &NodeState> {
        self.nodes.iter().filter(|n| {
            n.healthy
                && matches!(
                    n.role,
                    NodeRole::Data | NodeRole::AllInOne
                )
        })
    }

    pub fn by_id(&self) -> HashMap<&str, &NodeState> {
        self.nodes.iter().map(|n| (n.id.as_str(), n)).collect()
    }
}
