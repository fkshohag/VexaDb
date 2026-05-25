//! Project stored payloads / vectors onto search and query results.

use serde_json::Value;
use vectordb_core::{OutputOptions, ScoredPoint};

use crate::engine::CollectionState;

pub fn attach_outputs(state: &CollectionState, hits: &mut [ScoredPoint], opts: &OutputOptions) {
    if !opts.with_payload && !opts.with_vector {
        return;
    }
    for hit in hits.iter_mut() {
        if opts.with_payload {
            hit.payload = state
                .payloads
                .get(&hit.id)
                .map(|p| project_payload(p, &opts.output_fields));
        }
        if opts.with_vector {
            hit.vector = state
                .index
                .get_vector(&hit.id)
                .map(|v| v.values.clone());
        }
    }
}

pub fn project_payload(full: &Value, fields: &[String]) -> Value {
    if fields.is_empty() {
        return full.clone();
    }
    let Some(obj) = full.as_object() else {
        return full.clone();
    };
    let mut out = serde_json::Map::new();
    for f in fields {
        if let Some(v) = obj.get(f) {
            out.insert(f.clone(), v.clone());
        }
    }
    Value::Object(out)
}
