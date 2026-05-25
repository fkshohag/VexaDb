//! Hybrid search orchestration (dense HNSW + sparse + BM25 fusion).

use vectordb_core::{
    rrf_fusion, weighted_fusion, Bm25Index, Filter, ScoredPoint, SearchMode, SparseInvertedIndex,
    SparseVector,
};

use crate::engine::CollectionState;

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
