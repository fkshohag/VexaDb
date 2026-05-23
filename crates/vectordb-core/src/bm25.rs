//! BM25 inverted index over a text field in JSON payloads.

use std::collections::HashMap;

use serde_json::Value;

use crate::types::PointId;

const K1: f32 = 1.2;
const B: f32 = 0.75;

/// Tokenize text into lowercase terms.
pub fn tokenize(text: &str) -> Vec<String> {
    text.to_lowercase()
        .split(|c: char| !c.is_alphanumeric())
        .filter(|t| !t.is_empty())
        .map(|s| s.to_string())
        .collect()
}

/// BM25 index for lexical search over document text.
#[derive(Debug, Default)]
pub struct Bm25Index {
    field: String,
    doc_len: HashMap<PointId, usize>,
    avgdl: f32,
    df: HashMap<String, usize>,
    postings: HashMap<String, Vec<(PointId, u32)>>,
    doc_count: usize,
}

impl Bm25Index {
    pub fn new(field: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            ..Default::default()
        }
    }

    pub fn field(&self) -> &str {
        &self.field
    }

    fn extract_text<'a>(&self, payload: &'a Value) -> Option<&'a str> {
        match payload {
            Value::Object(map) => map.get(&self.field).and_then(|v| v.as_str()),
            _ => None,
        }
    }

    pub fn upsert(&mut self, id: PointId, payload: &Value) {
        self.remove(&id);
        let Some(text) = self.extract_text(payload) else {
            return;
        };
        let terms = tokenize(text);
        if terms.is_empty() {
            return;
        }
        self.doc_len.insert(id.clone(), terms.len());
        self.doc_count += 1;
        self.avgdl = self.doc_len.values().sum::<usize>() as f32 / self.doc_count as f32;

        let mut term_freq: HashMap<String, u32> = HashMap::new();
        for t in terms {
            *term_freq.entry(t).or_insert(0) += 1;
        }
        for (term, tf) in term_freq {
            self.postings
                .entry(term.clone())
                .or_insert_with(Vec::new)
                .push((id.clone(), tf));
            *self.df.entry(term).or_insert(0) += 1;
        }
    }

    pub fn remove(&mut self, id: &str) {
        if !self.doc_len.contains_key(id) {
            return;
        }
        self.doc_len.remove(id);
        self.doc_count = self.doc_count.saturating_sub(1);
        self.avgdl = if self.doc_count > 0 {
            self.doc_len.values().sum::<usize>() as f32 / self.doc_count as f32
        } else {
            0.0
        };
        for postings in self.postings.values_mut() {
            postings.retain(|(pid, _)| pid != id);
        }
        self.df.retain(|term, _| {
            self.postings
                .get(term)
                .map(|p| !p.is_empty())
                .unwrap_or(false)
        });
    }

    pub fn len(&self) -> usize {
        self.doc_len.len()
    }

    pub fn search(&self, query: &str, k: usize) -> Vec<(PointId, f32)> {
        if self.doc_count == 0 || k == 0 {
            return Vec::new();
        }
        let terms = tokenize(query);
        let mut scores: HashMap<&str, f32> = HashMap::new();
        let n = self.doc_count as f32;

        for term in terms {
            let Some(postings) = self.postings.get(&term) else {
                continue;
            };
            let df = *self.df.get(&term).unwrap_or(&0) as f32;
            if df == 0.0 {
                continue;
            }
            let idf = ((n - df + 0.5) / (df + 0.5) + 1.0).ln();

            for (id, tf) in postings {
                let dl = *self.doc_len.get(id).unwrap_or(&0) as f32;
                let tf = *tf as f32;
                let denom = tf + K1 * (1.0 - B + B * dl / self.avgdl.max(1.0));
                let score = idf * (tf * (K1 + 1.0)) / denom.max(1e-6);
                *scores.entry(id.as_str()).or_insert(0.0) += score;
            }
        }

        let mut ranked: Vec<(PointId, f32)> = scores
            .into_iter()
            .map(|(id, s)| (id.to_string(), s))
            .collect();
        ranked.sort_by(|a, b| {
            b.1.partial_cmp(&a.1)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        ranked.truncate(k);
        ranked
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn bm25_ranks_relevant_doc() {
        let mut idx = Bm25Index::new("text");
        idx.upsert("a".into(), &json!({"text": "rust vector database"}));
        idx.upsert("b".into(), &json!({"text": "python web framework"}));
        let hits = idx.search("vector database", 2);
        assert_eq!(hits[0].0, "a");
    }
}
