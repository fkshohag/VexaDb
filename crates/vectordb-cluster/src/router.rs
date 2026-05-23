use crate::membership::ClusterConfig;
use crate::ring::HashRing;

pub type ShardId = u32;

/// Routes point keys to shards and shards to physical nodes.
#[derive(Debug, Clone)]
pub struct ShardRouter {
    ring: HashRing,
    shard_to_nodes: Vec<Vec<String>>,
}

impl ShardRouter {
    pub fn from_cluster(config: &ClusterConfig, shard_count: u32) -> Self {
        let ring = HashRing::new(shard_count, config.virtual_nodes_per_shard);
        let mut shard_to_nodes = vec![Vec::new(); shard_count as usize];

        for node in config.data_nodes() {
            for &shard in &node.shard_ids {
                if (shard as usize) < shard_to_nodes.len() {
                    shard_to_nodes[shard as usize].push(node.id.clone());
                }
            }
        }

        // Auto-assign shards round-robin if not specified
        let data_node_ids: Vec<String> = config.data_nodes().map(|n| n.id.clone()).collect();
        if !data_node_ids.is_empty() {
            for shard in 0..shard_count {
                if shard_to_nodes[shard as usize].is_empty() {
                    let node = &data_node_ids[shard as usize % data_node_ids.len()];
                    shard_to_nodes[shard as usize].push(node.clone());
                }
            }
        }

        Self {
            ring,
            shard_to_nodes,
        }
    }

    pub fn shard_for_point(&self, point_id: &str) -> ShardId {
        self.ring.shard_for_key(point_id.as_bytes())
    }

    pub fn primary_node_for_shard(&self, shard: ShardId) -> Option<&str> {
        self.shard_to_nodes
            .get(shard as usize)
            .and_then(|nodes| nodes.first())
            .map(|s| s.as_str())
    }

    pub fn route_point(&self, point_id: &str) -> Option<RouteTarget> {
        let shard = self.shard_for_point(point_id);
        let node_id = self.primary_node_for_shard(shard)?;
        Some(RouteTarget { shard, node_id })
    }

    pub fn shard_count(&self) -> u32 {
        self.ring.shard_count()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteTarget<'a> {
    pub shard: ShardId,
    pub node_id: &'a str,
}
