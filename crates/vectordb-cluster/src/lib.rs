//! Cluster layer for horizontal scaling across data nodes.

pub mod membership;
pub mod ring;
pub mod router;

pub use membership::{ClusterConfig, NodeId, NodeRole, NodeState};
pub use ring::{shard_for_point, HashRing};
pub use router::{merge_top_k, ShardId, ShardRouter};
