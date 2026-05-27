use std::cmp::{Ordering, Reverse};
use std::collections::{BinaryHeap, HashSet};

use parking_lot::RwLock;
use rand::Rng;
use serde::{Deserialize, Serialize};

use crate::collection::DistanceMetric;
use crate::distance::Distance;
use crate::error::{Error, Result};
use crate::types::{PointId, ScoredPoint, Vector};

const MAX_LAYERS: usize = 16;
/// 1 / ln(2) — layer generation factor for HNSW
const ML: f64 = 1.4426950408889634;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HnswConfig {
    pub m: usize,
    pub m_max0: usize,
    pub ef_construction: usize,
    pub ef_search: usize,
    pub metric: DistanceMetric,
}

impl HnswConfig {
    pub fn new(metric: DistanceMetric, m: usize, ef_construction: usize, ef_search: usize) -> Self {
        Self {
            m,
            m_max0: m * 2,
            ef_construction,
            ef_search,
            metric,
        }
    }
}

#[derive(Debug, Clone)]
struct Node {
    id: PointId,
    vector: Vector,
    /// neighbors[level] = list of internal indices
    neighbors: Vec<Vec<usize>>,
}

/// Hierarchical Navigable Small World index for approximate nearest neighbor search.
#[derive(Debug)]
pub struct HnswIndex {
    config: HnswConfig,
    dimension: usize,
    nodes: RwLock<Vec<Node>>,
    id_to_idx: RwLock<std::collections::HashMap<PointId, usize>>,
    entry_point: RwLock<Option<usize>>,
    max_layer: RwLock<usize>,
}

impl HnswIndex {
    pub fn new(dimension: usize, config: HnswConfig) -> Self {
        Self {
            config,
            dimension,
            nodes: RwLock::new(Vec::new()),
            id_to_idx: RwLock::new(std::collections::HashMap::new()),
            entry_point: RwLock::new(None),
            max_layer: RwLock::new(0),
        }
    }

    pub fn len(&self) -> usize {
        self.nodes.read().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn insert(&self, id: PointId, vector: Vector) -> Result<()> {
        if vector.dim() != self.dimension {
            return Err(Error::DimensionMismatch {
                expected: self.dimension,
                got: vector.dim(),
            });
        }

        let mut nodes = self.nodes.write();
        let mut id_to_idx = self.id_to_idx.write();

        if id_to_idx.contains_key(&id) {
            let idx = id_to_idx[&id];
            nodes[idx].vector = vector;
            return Ok(());
        }

        let level = random_level();
        let idx = nodes.len();
        nodes.push(Node {
            id: id.clone(),
            vector: vector.clone(),
            neighbors: vec![Vec::new(); level + 1],
        });
        id_to_idx.insert(id, idx);

        let ep = *self.entry_point.read();
        if ep.is_none() {
            *self.entry_point.write() = Some(idx);
            *self.max_layer.write() = level;
            return Ok(());
        }

        let mut ep = ep.unwrap();
        let max_layer = *self.max_layer.read();
        let query = nodes[idx].vector.values.clone();

        // Greedy search from top layer down to level+1
        for l in (level + 1..=max_layer).rev() {
            ep = self.search_layer_single(&nodes, &query, ep, l);
        }

        // Insert and connect from top insertable layer down to 0
        for l in (0..=level.min(max_layer)).rev() {
            let candidates =
                self.search_layer(&nodes, &query, ep, self.config.ef_construction, l);
            let m_l = if l == 0 {
                self.config.m_max0
            } else {
                self.config.m
            };
            let selected =
                select_neighbors(&nodes, &query, &candidates, m_l, self.config.metric);

            nodes[idx].neighbors[l] = selected.clone();
            for &nb in &selected {
                nodes[nb].neighbors[l].push(idx);
                let max_conn = if l == 0 {
                    self.config.m_max0
                } else {
                    self.config.m
                };
                if nodes[nb].neighbors[l].len() > max_conn {
                    let pruned = select_neighbors(
                        &nodes,
                        &nodes[nb].vector.values,
                        &nodes[nb].neighbors[l],
                        max_conn,
                        self.config.metric,
                    );
                    nodes[nb].neighbors[l] = pruned;
                }
            }
            if !candidates.is_empty() {
                ep = candidates[0];
            }
        }

        if level > max_layer {
            *self.max_layer.write() = level;
            *self.entry_point.write() = Some(idx);
        }

        Ok(())
    }

    pub fn remove(&self, id: &str) -> Result<()> {
        let mut id_to_idx = self.id_to_idx.write();
        let idx = id_to_idx
            .remove(id)
            .ok_or_else(|| Error::PointNotFound(id.to_string()))?;

        let mut nodes = self.nodes.write();
        if idx >= nodes.len() {
            return Ok(());
        }

        // Tombstone: swap-remove pattern
        let last = nodes.len() - 1;
        if idx != last {
            let moved_id = nodes[last].id.clone();
            nodes.swap(idx, last);
            id_to_idx.insert(moved_id, idx);

            // Rewire neighbors pointing to last -> idx
            for node in nodes.iter_mut() {
                for level in &mut node.neighbors {
                    for n in level.iter_mut() {
                        if *n == last {
                            *n = idx;
                        }
                    }
                }
            }
        }
        nodes.pop();

        if nodes.is_empty() {
            *self.entry_point.write() = None;
            *self.max_layer.write() = 0;
        }

        Ok(())
    }

    pub fn search(&self, query: &[f32], k: usize) -> Result<Vec<ScoredPoint>> {
        if query.len() != self.dimension {
            return Err(Error::DimensionMismatch {
                expected: self.dimension,
                got: query.len(),
            });
        }

        let nodes = self.nodes.read();
        if nodes.is_empty() {
            return Ok(Vec::new());
        }

        let ep = self
            .entry_point
            .read()
            .ok_or(Error::EmptyIndex)?;
        let max_layer = *self.max_layer.read();

        let mut ep = ep;
        for l in (1..=max_layer).rev() {
            ep = self.search_layer_single(&nodes, query, ep, l);
        }

        let ef = self.config.ef_search.max(k);
        let mut results = self.search_layer(&nodes, query, ep, ef, 0);

        results.sort_by(|a, b| {
            let da = Distance::compare(
                self.config.metric,
                query,
                &nodes[*a].vector.values,
            );
            let db = Distance::compare(
                self.config.metric,
                query,
                &nodes[*b].vector.values,
            );
            da.partial_cmp(&db).unwrap_or(Ordering::Equal)
        });
        results.truncate(k);

        Ok(results
            .into_iter()
            .map(|idx| {
                let dist = Distance::compare(
                    self.config.metric,
                    query,
                    &nodes[idx].vector.values,
                );
                ScoredPoint {
                    id: nodes[idx].id.clone(),
                    score: Distance::to_score(self.config.metric, dist),
                    ..Default::default()
                }
            })
            .collect())
    }

    pub fn get_vector(&self, id: &str) -> Option<Vector> {
        let id_to_idx = self.id_to_idx.read();
        let idx = *id_to_idx.get(id)?;
        let nodes = self.nodes.read();
        Some(nodes[idx].vector.clone())
    }

    /// All point IDs currently in the index.
    pub fn point_ids(&self) -> Vec<PointId> {
        self.id_to_idx.read().keys().cloned().collect()
    }

    /// Clone every stored id → vector pair (used for reindex / export).
    pub fn iter_points(&self) -> Vec<(PointId, Vector)> {
        let id_to_idx = self.id_to_idx.read();
        let nodes = self.nodes.read();
        id_to_idx
            .iter()
            .map(|(id, &idx)| (id.clone(), nodes[idx].vector.clone()))
            .collect()
    }

    fn search_layer_single(
        &self,
        nodes: &[Node],
        query: &[f32],
        entry: usize,
        level: usize,
    ) -> usize {
        let mut best = entry;
        let mut best_dist = Distance::compare(
            self.config.metric,
            query,
            &nodes[entry].vector.values,
        );
        loop {
            let mut changed = false;
            for &nb in &nodes[best].neighbors[level] {
                let d = Distance::compare(self.config.metric, query, &nodes[nb].vector.values);
                if d < best_dist {
                    best_dist = d;
                    best = nb;
                    changed = true;
                }
            }
            if !changed {
                break;
            }
        }
        best
    }

    fn search_layer(
        &self,
        nodes: &[Node],
        query: &[f32],
        entry: usize,
        ef: usize,
        level: usize,
    ) -> Vec<usize> {
        let mut visited = HashSet::new();
        visited.insert(entry);

        let entry_dist = Distance::compare(self.config.metric, query, &nodes[entry].vector.values);

        let mut candidates = BinaryHeap::new();
        candidates.push(Reverse(Candidate {
            idx: entry,
            dist: entry_dist,
        }));

        let mut results = BinaryHeap::new();
        results.push(Candidate {
            idx: entry,
            dist: entry_dist,
        });

        while let Some(Reverse(Candidate { idx, dist: cd })) = candidates.pop() {
            let worst = results.peek().map(|c| c.dist).unwrap_or(f32::INFINITY);
            if cd > worst && results.len() >= ef {
                break;
            }

            for &nb in &nodes[idx].neighbors[level] {
                if !visited.insert(nb) {
                    continue;
                }
                let d = Distance::compare(self.config.metric, query, &nodes[nb].vector.values);
                let worst = results.peek().map(|c| c.dist).unwrap_or(f32::INFINITY);
                if d < worst || results.len() < ef {
                    candidates.push(Reverse(Candidate { idx: nb, dist: d }));
                    results.push(Candidate { idx: nb, dist: d });
                    if results.len() > ef {
                        results.pop();
                    }
                }
            }
        }

        let mut out: Vec<usize> = results.into_iter().map(|c| c.idx).collect();
        out.sort_by(|&a, &b| {
            let da = Distance::compare(self.config.metric, query, &nodes[a].vector.values);
            let db = Distance::compare(self.config.metric, query, &nodes[b].vector.values);
            da.partial_cmp(&db).unwrap_or(Ordering::Equal)
        });
        out
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct Candidate {
    idx: usize,
    dist: f32,
}

impl Eq for Candidate {}

impl PartialOrd for Candidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.dist.partial_cmp(&other.dist)
    }
}

impl Ord for Candidate {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap_or(Ordering::Equal)
    }
}

fn random_level() -> usize {
    let mut rng = rand::thread_rng();
    let r: f64 = rng.gen();
    let level = (-r.ln() * ML).floor() as usize;
    level.min(MAX_LAYERS - 1)
}

fn select_neighbors(
    nodes: &[Node],
    query: &[f32],
    candidates: &[usize],
    m: usize,
    metric: DistanceMetric,
) -> Vec<usize> {
    let mut sorted = candidates.to_vec();
    sorted.sort_by(|&a, &b| {
        let da = Distance::compare(metric, query, &nodes[a].vector.values);
        let db = Distance::compare(metric, query, &nodes[b].vector.values);
        da.partial_cmp(&db).unwrap_or(Ordering::Equal)
    });
    sorted.truncate(m);
    sorted
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collection::DistanceMetric;

    fn unit(v: [f32; 3]) -> Vector {
        Vector::new(v.to_vec())
    }

    #[test]
    fn finds_nearest_neighbor() {
        let config = HnswConfig::new(DistanceMetric::Cosine, 8, 50, 50);
        let index = HnswIndex::new(3, config);
        index.insert("a".into(), unit([1.0, 0.0, 0.0])).unwrap();
        index.insert("b".into(), unit([0.9, 0.1, 0.0])).unwrap();
        index.insert("c".into(), unit([0.0, 1.0, 0.0])).unwrap();

        let hits = index.search(&[1.0, 0.0, 0.0], 1).unwrap();
        assert_eq!(hits[0].id, "a");
    }
}
