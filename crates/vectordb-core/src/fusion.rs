//! Score fusion for hybrid retrieval (RRF and weighted linear).

use std::collections::HashMap;

use crate::types::{PointId, ScoredPoint};

/// Reciprocal Rank Fusion (RRF) with configurable `k` (default 60).
pub fn rrf_fusion(lists: &[Vec<(PointId, f32)>], final_k: usize) -> Vec<ScoredPoint> {
    let k_param = 60.0f32;
    let mut scores: HashMap<PointId, f32> = HashMap::new();

    for list in lists {
        for (rank, (id, _)) in list.iter().enumerate() {
            let rrf = 1.0 / (k_param + (rank + 1) as f32);
            *scores.entry(id.clone()).or_insert(0.0) += rrf;
        }
    }

    let mut out: Vec<ScoredPoint> = scores
        .into_iter()
        .map(|(id, score)| ScoredPoint { id, score })
        .collect();
    out.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    out.truncate(final_k);
    out
}

/// Weighted linear fusion after min-max normalization per list.
pub fn weighted_fusion(
    dense: &[(PointId, f32)],
    sparse: &[(PointId, f32)],
    alpha: f32,
    k: usize,
) -> Vec<ScoredPoint> {
    let alpha = alpha.clamp(0.0, 1.0);
    let norm = |list: &[(PointId, f32)]| -> HashMap<PointId, f32> {
        if list.is_empty() {
            return HashMap::new();
        }
        let min = list.iter().map(|(_, s)| *s).fold(f32::INFINITY, f32::min);
        let max = list.iter().map(|(_, s)| *s).fold(f32::NEG_INFINITY, f32::max);
        let span = (max - min).max(1e-6);
        list.iter()
            .map(|(id, s)| (id.clone(), (s - min) / span))
            .collect()
    };

    let d = norm(dense);
    let s = norm(sparse);
    let mut ids: HashMap<PointId, f32> = HashMap::new();
    for (id, score) in d {
        *ids.entry(id).or_insert(0.0) += alpha * score;
    }
    for (id, score) in s {
        *ids.entry(id).or_insert(0.0) += (1.0 - alpha) * score;
    }

    let mut out: Vec<ScoredPoint> = ids
        .into_iter()
        .map(|(id, score)| ScoredPoint { id, score })
        .collect();
    out.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    out.truncate(k);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rrf_prefers_consensus() {
        let a = vec![("x".into(), 1.0), ("y".into(), 0.5)];
        let b = vec![("x".into(), 0.9), ("z".into(), 0.8)];
        let merged = rrf_fusion(&[a, b], 2);
        assert_eq!(merged[0].id, "x");
    }
}
