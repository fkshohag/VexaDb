use crate::membership::ClusterConfig;
use crate::ring::HashRing;

pub type ShardId = u32;

/// gRPC endpoint for the primary owner of each shard (index = shard id).
#[derive(Debug, Clone)]
pub struct ShardRouter {
    ring: HashRing,
    shard_to_nodes: Vec<Vec<String>>,
    /// All replica endpoints per shard (RF > 1).
    shard_replicas: Vec<Vec<String>>,
    shard_endpoints: Vec<String>,
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

        let mut shard_replicas = vec![Vec::new(); shard_count as usize];
        for node in config.nodes.iter().filter(|n| {
            matches!(n.role, crate::membership::NodeRole::Data | crate::membership::NodeRole::AllInOne)
        }) {
            let endpoint = normalize_grpc_endpoint(&node.advertise_addr);
            for &shard in &node.shard_ids {
                if (shard as usize) < shard_replicas.len() {
                    shard_replicas[shard as usize].push(endpoint.clone());
                }
            }
        }
        // Nodes with empty shard_ids participate in every shard (RF replica set).
        for node in config.nodes.iter().filter(|n| {
            n.shard_ids.is_empty()
                && matches!(
                    n.role,
                    crate::membership::NodeRole::Data | crate::membership::NodeRole::AllInOne
                )
        }) {
            let endpoint = normalize_grpc_endpoint(&node.advertise_addr);
            for replicas in shard_replicas.iter_mut() {
                replicas.push(endpoint.clone());
            }
        }

        let mut shard_endpoints = vec![String::new(); shard_count as usize];
        for shard in 0..shard_count {
            if let Some(ep) = shard_replicas[shard as usize].first() {
                shard_endpoints[shard as usize] = ep.clone();
            }
        }
        for shard in 0..shard_count {
            if shard_endpoints[shard as usize].is_empty() {
                if let Some(node) = data_node_ids.get(shard as usize % data_node_ids.len().max(1)) {
                    if let Some(n) = config.nodes.iter().find(|n| &n.id == node) {
                        shard_endpoints[shard as usize] =
                            normalize_grpc_endpoint(&n.advertise_addr);
                    }
                }
            }
        }

        Self {
            ring,
            shard_to_nodes,
            shard_replicas,
            shard_endpoints,
        }
    }

    /// Replace the routable primary endpoint for a shard (after health probing).
    pub fn set_shard_primary(&mut self, shard: ShardId, endpoint: impl Into<String>) {
        let ep = endpoint.into();
        if (shard as usize) < self.shard_endpoints.len() {
            self.shard_endpoints[shard as usize] = ep;
        }
    }

    /// All configured replica gRPC URLs for a shard.
    pub fn replicas_for_shard(&self, shard: ShardId) -> &[String] {
        self.shard_replicas
            .get(shard as usize)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    /// Unique gRPC endpoints for all shards (one per shard primary).
    pub fn shard_endpoints(&self) -> impl Iterator<Item = (ShardId, &str)> {
        self.shard_endpoints
            .iter()
            .enumerate()
            .filter(|(_, ep)| !ep.is_empty())
            .map(|(i, ep)| (i as ShardId, ep.as_str()))
    }

    pub fn endpoint_for_shard(&self, shard: ShardId) -> Option<&str> {
        self.shard_endpoints
            .get(shard as usize)
            .filter(|s| !s.is_empty())
            .map(|s| s.as_str())
    }

    pub fn endpoint_for_point(&self, point_id: &str) -> Option<&str> {
        let shard = self.shard_for_point(point_id);
        self.endpoint_for_shard(shard)
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

fn normalize_grpc_endpoint(addr: &str) -> String {
    if addr.starts_with("http://") || addr.starts_with("https://") {
        addr.to_string()
    } else {
        format!("http://{addr}")
    }
}

/// Merge scored hits from multiple shards; higher score wins.
pub fn merge_top_k(
    mut hits: Vec<(String, f32)>,
    top_k: usize,
) -> Vec<(String, f32)> {
    hits.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    hits.dedup_by(|a, b| a.0 == b.0);
    hits.truncate(top_k);
    hits
}

#[cfg(test)]
mod merge_tests {
    use super::*;

    #[test]
    fn merge_keeps_best_scores() {
        let merged = merge_top_k(
            vec![
                ("a".into(), 0.9),
                ("b".into(), 0.5),
                ("c".into(), 0.95),
            ],
            2,
        );
        assert_eq!(merged.len(), 2);
        assert_eq!(merged[0].0, "c");
        assert_eq!(merged[1].0, "a");
    }
}
