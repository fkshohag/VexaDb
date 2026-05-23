//! Sparse vector representation and dot-product similarity.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::types::PointId;

/// Sparse vector as sorted index → weight pairs (SPLADE / BM25-style sparse embeddings).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SparseVector {
    pub indices: Vec<u32>,
    pub values: Vec<f32>,
}

impl SparseVector {
    pub fn new(indices: Vec<u32>, values: Vec<f32>) -> Self {
        Self { indices, values }
    }

    pub fn is_empty(&self) -> bool {
        self.indices.is_empty()
    }

    /// Dot product with another sparse vector (only overlapping indices).
    pub fn dot(&self, other: &SparseVector) -> f32 {
        let mut i = 0;
        let mut j = 0;
        let mut sum = 0.0f32;
        while i < self.indices.len() && j < other.indices.len() {
            let a = self.indices[i];
            let b = other.indices[j];
            if a == b {
                sum += self.values[i] * other.values[j];
                i += 1;
                j += 1;
            } else if a < b {
                i += 1;
            } else {
                j += 1;
            }
        }
        sum
    }

    /// Build from a map (for tests and JSON import).
    pub fn from_pairs(pairs: impl IntoIterator<Item = (u32, f32)>) -> Self {
        let mut v: Vec<_> = pairs.into_iter().collect();
        v.sort_by_key(|(idx, _)| *idx);
        let (indices, values): (Vec<_>, Vec<_>) = v.into_iter().unzip();
        Self { indices, values }
    }
}

/// Inverted index for sparse vector dot-product retrieval.
#[derive(Debug, Default)]
pub struct SparseInvertedIndex {
    /// dimension index -> posting list
    postings: HashMap<u32, Vec<(PointId, f32)>>,
    /// point id -> stored sparse vector
    vectors: HashMap<PointId, SparseVector>,
}

impl SparseInvertedIndex {
    pub fn upsert(&mut self, id: PointId, vector: SparseVector) {
        if let Some(old) = self.vectors.remove(&id) {
            self.remove_from_postings(&id, &old);
        }
        if vector.is_empty() {
            return;
        }
        for (&idx, &w) in vector.indices.iter().zip(vector.values.iter()) {
            self.postings
                .entry(idx)
                .or_default()
                .push((id.clone(), w));
        }
        self.vectors.insert(id, vector);
    }

    pub fn remove(&mut self, id: &str) {
        if let Some(v) = self.vectors.remove(id) {
            self.remove_from_postings(id, &v);
        }
    }

    fn remove_from_postings(&mut self, id: &str, vector: &SparseVector) {
        for &idx in &vector.indices {
            if let Some(list) = self.postings.get_mut(&idx) {
                list.retain(|(pid, _)| pid != id);
            }
        }
    }

    pub fn len(&self) -> usize {
        self.vectors.len()
    }

    /// Top-k by sparse dot product score.
    pub fn search(&self, query: &SparseVector, k: usize) -> Vec<(PointId, f32)> {
        if query.is_empty() || k == 0 {
            return Vec::new();
        }
        let mut scores: HashMap<&str, f32> = HashMap::new();
        for (&idx, &qw) in query.indices.iter().zip(query.values.iter()) {
            if let Some(postings) = self.postings.get(&idx) {
                for (id, w) in postings {
                    *scores.entry(id.as_str()).or_insert(0.0) += qw * w;
                }
            }
        }
        let mut ranked: Vec<(PointId, f32)> = scores
            .into_iter()
            .map(|(id, score)| (id.to_string(), score))
            .collect();
        ranked.sort_by(|a, b| {
            b.1.partial_cmp(&a.1)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        ranked.truncate(k);
        ranked
    }

    pub fn get(&self, id: &str) -> Option<&SparseVector> {
        self.vectors.get(id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sparse_dot_and_search() {
        let mut idx = SparseInvertedIndex::default();
        idx.upsert(
            "a".into(),
            SparseVector::from_pairs([(1, 1.0), (5, 0.5)]),
        );
        idx.upsert(
            "b".into(),
            SparseVector::from_pairs([(1, 0.5), (2, 1.0)]),
        );
        let q = SparseVector::from_pairs([(1, 1.0), (2, 0.5)]);
        let hits = idx.search(&q, 2);
        assert_eq!(hits[0].0, "b");
        assert!(hits[0].1 > hits[1].1);
    }
}
