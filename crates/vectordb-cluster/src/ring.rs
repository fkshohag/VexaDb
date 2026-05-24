use std::collections::BTreeMap;

use xxhash_rust::xxh64::xxh64;

/// Consistent hash ring for shard placement (production-style horizontal partitioning).
#[derive(Debug, Clone)]
pub struct HashRing {
    ring: BTreeMap<u64, u32>,
    shard_count: u32,
}

impl HashRing {
    pub fn new(shard_count: u32, virtual_nodes_per_shard: u32) -> Self {
        let mut ring = BTreeMap::new();
        for shard in 0..shard_count {
            for vnode in 0..virtual_nodes_per_shard {
                let key = format!("shard-{shard}-vnode-{vnode}");
                let hash = xxh64(key.as_bytes(), 0);
                ring.insert(hash, shard);
            }
        }
        Self { ring, shard_count }
    }

    pub fn shard_for_key(&self, key: &[u8]) -> u32 {
        if self.ring.is_empty() {
            return 0;
        }
        let hash = xxh64(key, 0);
        self.ring
            .range(hash..)
            .next()
            .or_else(|| self.ring.iter().next())
            .map(|(_, &shard)| shard)
            .unwrap_or(0)
    }

    pub fn shard_count(&self) -> u32 {
        self.shard_count
    }
}

/// Convenience: compute the shard for a point id given a shard count.
/// Uses the standard ring (128 vnodes/shard) and is therefore consistent
/// with the live router. Useful for offline tools (rebalance, audit).
pub fn shard_for_point(point_id: &str, shard_count: u32) -> u32 {
    HashRing::new(shard_count, 128).shard_for_key(point_id.as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stable_shard_assignment() {
        let ring = HashRing::new(4, 64);
        let a = ring.shard_for_key(b"user-123");
        let b = ring.shard_for_key(b"user-123");
        assert_eq!(a, b);
    }
}
