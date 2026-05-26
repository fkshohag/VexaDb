//! Hybrid search orchestration (dense HNSW + sparse + BM25 fusion).

use vectordb_core::{
    rrf_fusion, weighted_fusion, Bm25Index, Filter, ScoredPoint, SearchMode, SparseInvertedIndex,
    SparseVector,
};

use crate::engine::CollectionState;

/// One leg of a [`hybrid_search_multi`] call. Mirrors Milvus's `AnnRequest`:
/// a per-field top-k together with the query payload to use for that
/// field. The `field` name is informational only — VexaDb dispatches based
/// on the `query` variant (dense / sparse / text).
pub struct AnnRequest<'a> {
    pub field: String,
    pub limit: usize,
    pub query: AnnQuery<'a>,
    pub filter: Option<&'a Filter>,
}

pub enum AnnQuery<'a> {
    Dense(&'a [f32]),
    Sparse(&'a SparseVector),
    Text(&'a str),
}

/// Reranking policy applied across [`AnnRequest`] result lists.
/// `Weighted` carries one weight per request (must equal `requests.len()`).
/// `Function` is a pass-through that interleaves and dedups by best score.
pub enum Reranker {
    Rrf,
    Weighted(Vec<f32>),
    Function,
}

pub struct SearchParams<'a> {
    pub query: &'a [f32],
    pub sparse_query: Option<&'a SparseVector>,
    pub text_query: Option<&'a str>,
    pub mode: SearchMode,
    pub hybrid_alpha: f32,
    pub filter: Option<&'a Filter>,
}

impl<'a> SearchParams<'a> {
    pub fn dense(query: &'a [f32], k: usize) -> (Self, usize) {
        (
            Self {
                query,
                sparse_query: None,
                text_query: None,
                mode: SearchMode::Dense,
                hybrid_alpha: 0.5,
                filter: None,
            },
            k,
        )
    }
}

pub fn hybrid_search(
    state: &CollectionState,
    k: usize,
    params: SearchParams<'_>,
) -> Result<Vec<ScoredPoint>, vectordb_core::Error> {
    let per_path_k = (k * 2).max(k);

    let dense_hits = match params.mode {
        SearchMode::Sparse | SearchMode::Bm25 => Vec::new(),
        _ => dense_search(state, params.query, per_path_k, params.filter)?,
    };

    let sparse_hits: Vec<(String, f32)> = match params.mode {
        SearchMode::Dense | SearchMode::Bm25 => Vec::new(),
        SearchMode::Sparse | SearchMode::HybridRrf | SearchMode::HybridWeighted => {
            if let (Some(idx), Some(q)) = (&state.sparse_index, params.sparse_query) {
                idx.search(q, per_path_k)
            } else {
                Vec::new()
            }
        }
    };

    let bm25_hits: Vec<(String, f32)> = match params.mode {
        SearchMode::Dense | SearchMode::Sparse => Vec::new(),
        SearchMode::Bm25 | SearchMode::HybridRrf | SearchMode::HybridWeighted => {
            if let (Some(idx), Some(q)) = (&state.bm25_index, params.text_query) {
                idx.search(q, per_path_k)
            } else {
                Vec::new()
            }
        }
    };

    let mut results = match params.mode {
        SearchMode::Dense => dense_hits,
        SearchMode::Sparse => sparse_to_scored(sparse_hits),
        SearchMode::Bm25 => sparse_to_scored(bm25_hits),
        SearchMode::HybridRrf => {
            let lists: Vec<Vec<(String, f32)>> = vec![
                dense_hits.iter().map(|h| (h.id.clone(), h.score)).collect(),
                sparse_hits,
                bm25_hits,
            ]
            .into_iter()
            .filter(|l| !l.is_empty())
            .collect();
            if lists.is_empty() {
                Vec::new()
            } else if lists.len() == 1 {
                sparse_to_scored(lists.into_iter().next().unwrap())
            } else {
                rrf_fusion(&lists, k)
            }
        }
        SearchMode::HybridWeighted => {
            let dense_pairs: Vec<_> = dense_hits.iter().map(|h| (h.id.clone(), h.score)).collect();
            let mut lexical = sparse_hits;
            if !bm25_hits.is_empty() {
                lexical.extend(bm25_hits);
            }
            weighted_fusion(&dense_pairs, &lexical, params.hybrid_alpha, k)
        }
    };

    if let Some(filter) = params.filter.filter(|f| !f.is_empty()) {
        results.retain(|h| {
            state
                .payloads
                .get(&h.id)
                .map(|p| filter.matches(p))
                .unwrap_or(false)
        });
        results.truncate(k);
    }

    Ok(results)
}

/// Multi-vector search: run every request independently against the same
/// collection, then rerank with `reranker`. Mirrors Milvus's
/// `HybridSearch` semantics — one AnnRequest per field, per-request
/// limit, single merged result with global `limit`.
pub fn hybrid_search_multi(
    state: &CollectionState,
    requests: Vec<AnnRequest<'_>>,
    reranker: Reranker,
    limit: usize,
) -> Result<Vec<ScoredPoint>, vectordb_core::Error> {
    if requests.is_empty() {
        return Ok(Vec::new());
    }
    let mut per_leg: Vec<Vec<(String, f32)>> = Vec::with_capacity(requests.len());
    for req in &requests {
        let k = req.limit.max(1);
        let hits: Vec<(String, f32)> = match &req.query {
            AnnQuery::Dense(q) => dense_search(state, q, k, req.filter)?
                .into_iter()
                .map(|h| (h.id, h.score))
                .collect(),
            AnnQuery::Sparse(q) => state
                .sparse_index
                .as_ref()
                .map(|idx| idx.search(q, k))
                .unwrap_or_default(),
            AnnQuery::Text(t) => state
                .bm25_index
                .as_ref()
                .map(|idx| idx.search(t, k))
                .unwrap_or_default(),
        };
        per_leg.push(hits);
    }
    let merged = match reranker {
        Reranker::Rrf => {
            let non_empty: Vec<Vec<(String, f32)>> =
                per_leg.into_iter().filter(|l| !l.is_empty()).collect();
            if non_empty.is_empty() {
                Vec::new()
            } else if non_empty.len() == 1 {
                sparse_to_scored(non_empty.into_iter().next().unwrap())
            } else {
                rrf_fusion(&non_empty, limit)
            }
        }
        Reranker::Weighted(weights) => {
            // weighted_fusion only takes (dense, lexical, alpha) today;
            // generalize by min-max normalizing each leg and summing
            // weight * normalized_score so callers can blend any number
            // of legs with explicit weights.
            let mut acc: std::collections::HashMap<String, f32> = std::collections::HashMap::new();
            for (i, leg) in per_leg.iter().enumerate() {
                if leg.is_empty() {
                    continue;
                }
                let w = weights.get(i).copied().unwrap_or(1.0);
                let max = leg.iter().map(|(_, s)| *s).fold(f32::MIN, f32::max);
                let min = leg.iter().map(|(_, s)| *s).fold(f32::MAX, f32::min);
                let span = (max - min).max(f32::EPSILON);
                for (id, score) in leg {
                    let norm = (*score - min) / span;
                    *acc.entry(id.clone()).or_insert(0.0) += w * norm;
                }
            }
            let mut all: Vec<(String, f32)> = acc.into_iter().collect();
            all.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
            all.truncate(limit);
            sparse_to_scored(all)
        }
        Reranker::Function => {
            // Pass-through: dedup across legs keeping the best per-leg
            // score, then sort. The "function" itself is applied client
            // side in Milvus; on the server we keep the contract minimal.
            let _ = weighted_fusion as fn(_, _, _, _) -> _; // silence unused import
            let mut acc: std::collections::HashMap<String, f32> = std::collections::HashMap::new();
            for leg in per_leg {
                for (id, score) in leg {
                    let cur = acc.entry(id).or_insert(score);
                    if score > *cur {
                        *cur = score;
                    }
                }
            }
            let mut all: Vec<(String, f32)> = acc.into_iter().collect();
            all.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
            all.truncate(limit);
            sparse_to_scored(all)
        }
    };
    Ok(merged)
}

fn sparse_to_scored(pairs: Vec<(String, f32)>) -> Vec<ScoredPoint> {
    pairs
        .into_iter()
        .map(|(id, score)| ScoredPoint {
            id,
            score,
            ..Default::default()
        })
        .collect()
}

fn dense_search(
    state: &CollectionState,
    query: &[f32],
    k: usize,
    filter: Option<&Filter>,
) -> Result<Vec<ScoredPoint>, vectordb_core::Error> {
    use crate::engine::{
        brute_force_topk, FILTER_BRUTE_FORCE_LIMIT, FILTER_OVERSEARCH_CAP, FILTER_OVERSEARCH_FACTOR,
    };

    let Some(filter) = filter.filter(|f| !f.is_empty()) else {
        return Ok(state.index.search(query, k)?);
    };

    let indexed_candidates = state.payload_indexes.candidates_for(filter);
    if let Some(mut ids) = indexed_candidates {
        if ids.len() <= FILTER_BRUTE_FORCE_LIMIT {
            ids.retain(|id| {
                state
                    .payloads
                    .get(id)
                    .map(|p| filter.matches(p))
                    .unwrap_or(false)
            });
            return Ok(brute_force_topk(state, query, k, &ids));
        }
    }

    let mut ef = (k * FILTER_OVERSEARCH_FACTOR)
        .min(FILTER_OVERSEARCH_CAP)
        .max(k);
    loop {
        let candidates = state.index.search(query, ef)?;
        let hits: Vec<ScoredPoint> = candidates
            .into_iter()
            .filter(|h| {
                state
                    .payloads
                    .get(&h.id)
                    .map(|p| filter.matches(p))
                    .unwrap_or(false)
            })
            .take(k)
            .collect();
        if hits.len() >= k || ef >= FILTER_OVERSEARCH_CAP {
            return Ok(hits);
        }
        ef = (ef * 2).min(FILTER_OVERSEARCH_CAP);
    }
}
