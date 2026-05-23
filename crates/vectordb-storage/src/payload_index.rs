//! Per-field payload indexes used to accelerate filtered search.

use std::collections::{BTreeMap, HashMap, HashSet};

use serde_json::Value;
use vectordb_core::{
    filter::lookup, Condition, FieldOp, Filter, PayloadFieldIndex, PayloadIndexKind, PointId,
};

/// Indexes for one collection. Built from `CollectionConfig::payload_indexes`.
#[derive(Default)]
pub struct PayloadIndexes {
    keyword: HashMap<String, HashMap<String, HashSet<PointId>>>,
    numeric: HashMap<String, BTreeMap<OrderedFloat, HashSet<PointId>>>,
    boolean: HashMap<String, (HashSet<PointId>, HashSet<PointId>)>,
    fields: HashMap<String, PayloadIndexKind>,
}

impl PayloadIndexes {
    pub fn new(config: &[PayloadFieldIndex]) -> Self {
        let mut me = Self::default();
        for idx in config {
            me.fields.insert(idx.field.clone(), idx.kind);
            match idx.kind {
                PayloadIndexKind::Keyword => {
                    me.keyword.insert(idx.field.clone(), HashMap::new());
                }
                PayloadIndexKind::Numeric => {
                    me.numeric.insert(idx.field.clone(), BTreeMap::new());
                }
                PayloadIndexKind::Bool => {
                    me.boolean
                        .insert(idx.field.clone(), (HashSet::new(), HashSet::new()));
                }
            }
        }
        me
    }

    pub fn upsert(&mut self, id: &PointId, payload: &Value) {
        // Replace any prior entries for this id.
        self.remove(id);
        for (field, kind) in self.fields.clone() {
            let Some(value) = lookup(payload, &field) else {
                continue;
            };
            match kind {
                PayloadIndexKind::Keyword => {
                    let bucket = self.keyword.entry(field.clone()).or_default();
                    for token in to_string_tokens(value) {
                        bucket.entry(token).or_default().insert(id.clone());
                    }
                }
                PayloadIndexKind::Numeric => {
                    let bucket = self.numeric.entry(field.clone()).or_default();
                    for n in to_numbers(value) {
                        bucket
                            .entry(OrderedFloat(n))
                            .or_default()
                            .insert(id.clone());
                    }
                }
                PayloadIndexKind::Bool => {
                    if let Value::Bool(b) = value {
                        let bucket = self.boolean.entry(field.clone()).or_default();
                        if *b {
                            bucket.0.insert(id.clone());
                        } else {
                            bucket.1.insert(id.clone());
                        }
                    }
                }
            }
        }
    }

    pub fn remove(&mut self, id: &PointId) {
        for bucket in self.keyword.values_mut() {
            for ids in bucket.values_mut() {
                ids.remove(id);
            }
        }
        for bucket in self.numeric.values_mut() {
            for ids in bucket.values_mut() {
                ids.remove(id);
            }
        }
        for bucket in self.boolean.values_mut() {
            bucket.0.remove(id);
            bucket.1.remove(id);
        }
    }

    /// Estimate a candidate id set for the conjunctive `must` portion of a
    /// filter using indexed fields only. Returns `None` when no `must`
    /// conditions are indexed (caller should fall back to full scan / HNSW).
    pub fn candidates_for(&self, filter: &Filter) -> Option<HashSet<PointId>> {
        let mut result: Option<HashSet<PointId>> = None;
        for cond in &filter.must {
            let Condition::Field(fc) = cond else {
                continue;
            };
            let Some(kind) = self.fields.get(&fc.key) else {
                continue;
            };
            let ids = match (&fc.op, kind) {
                (FieldOp::Match { value }, PayloadIndexKind::Keyword) => self
                    .keyword
                    .get(&fc.key)
                    .and_then(|m| value_to_token(value).and_then(|t| m.get(&t)).cloned())
                    .unwrap_or_default(),
                (FieldOp::AnyOf { any }, PayloadIndexKind::Keyword) => {
                    let mut acc = HashSet::new();
                    if let Some(m) = self.keyword.get(&fc.key) {
                        for v in any {
                            if let Some(token) = value_to_token(v) {
                                if let Some(set) = m.get(&token) {
                                    acc.extend(set.iter().cloned());
                                }
                            }
                        }
                    }
                    acc
                }
                (FieldOp::Match { value }, PayloadIndexKind::Numeric) => self
                    .numeric
                    .get(&fc.key)
                    .and_then(|m| {
                        value
                            .as_f64()
                            .and_then(|n| m.get(&OrderedFloat(n)))
                            .cloned()
                    })
                    .unwrap_or_default(),
                (FieldOp::Range { gte, lte, gt, lt }, PayloadIndexKind::Numeric) => {
                    let lo = match (gte, gt) {
                        (Some(g), _) => std::ops::Bound::Included(OrderedFloat(*g)),
                        (None, Some(g)) => std::ops::Bound::Excluded(OrderedFloat(*g)),
                        _ => std::ops::Bound::Unbounded,
                    };
                    let hi = match (lte, lt) {
                        (Some(l), _) => std::ops::Bound::Included(OrderedFloat(*l)),
                        (None, Some(l)) => std::ops::Bound::Excluded(OrderedFloat(*l)),
                        _ => std::ops::Bound::Unbounded,
                    };
                    let mut acc = HashSet::new();
                    if let Some(m) = self.numeric.get(&fc.key) {
                        for (_, set) in m.range((lo, hi)) {
                            acc.extend(set.iter().cloned());
                        }
                    }
                    acc
                }
                (FieldOp::Match { value }, PayloadIndexKind::Bool) => {
                    if let (Some(b), Some(buckets)) = (value.as_bool(), self.boolean.get(&fc.key)) {
                        if b { buckets.0.clone() } else { buckets.1.clone() }
                    } else {
                        HashSet::new()
                    }
                }
                _ => continue,
            };
            result = Some(match result {
                Some(prev) => prev.intersection(&ids).cloned().collect(),
                None => ids,
            });
        }
        result
    }
}

fn to_string_tokens(value: &Value) -> Vec<String> {
    match value {
        Value::String(s) => vec![s.clone()],
        Value::Bool(b) => vec![b.to_string()],
        Value::Number(n) => vec![n.to_string()],
        Value::Array(arr) => arr.iter().flat_map(to_string_tokens).collect(),
        _ => Vec::new(),
    }
}

fn to_numbers(value: &Value) -> Vec<f64> {
    match value {
        Value::Number(n) => n.as_f64().map(|f| vec![f]).unwrap_or_default(),
        Value::Array(arr) => arr.iter().flat_map(to_numbers).collect(),
        _ => Vec::new(),
    }
}

fn value_to_token(value: &Value) -> Option<String> {
    match value {
        Value::String(s) => Some(s.clone()),
        Value::Bool(b) => Some(b.to_string()),
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

/// Total-order wrapper around `f64` for use as a `BTreeMap` key.
#[derive(Debug, Clone, Copy)]
struct OrderedFloat(pub f64);

impl PartialEq for OrderedFloat {
    fn eq(&self, other: &Self) -> bool {
        self.0.to_bits() == other.0.to_bits()
    }
}

impl Eq for OrderedFloat {}

impl PartialOrd for OrderedFloat {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrderedFloat {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.partial_cmp(&other.0).unwrap_or(std::cmp::Ordering::Equal)
    }
}
