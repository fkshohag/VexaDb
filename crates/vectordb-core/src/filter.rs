//! Payload filter DSL (Qdrant-inspired) and evaluator.
//!
//! Filters compose `must` (AND), `must_not` (NOR), and `should` (OR) lists of
//! [`Condition`]s. Each [`Condition`] is either a leaf [`FieldCondition`]
//! against a JSON path, or a nested [`Filter`] for arbitrary boolean trees.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Boolean filter expression over JSON payloads.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct Filter {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub must: Vec<Condition>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub must_not: Vec<Condition>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub should: Vec<Condition>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum Condition {
    Field(FieldCondition),
    Nested { filter: Box<Filter> },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FieldCondition {
    /// Dotted JSON path: `"author"`, `"meta.tags"`, `"price"`.
    pub key: String,
    #[serde(flatten)]
    pub op: FieldOp,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum FieldOp {
    /// Field equals a single value (string, number, bool).
    Match { value: Value },
    /// Field is in any of the given values (set membership; supports arrays).
    AnyOf { any: Vec<Value> },
    /// Numeric range (inclusive `gte`/`lte`, exclusive `gt`/`lt`).
    Range {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        gte: Option<f64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        lte: Option<f64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        gt: Option<f64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        lt: Option<f64>,
    },
    /// Field is present and not null.
    Exists { exists: bool },
}

impl Filter {
    pub fn is_empty(&self) -> bool {
        self.must.is_empty() && self.must_not.is_empty() && self.should.is_empty()
    }

    /// Evaluate this filter against a JSON payload (treats `Null` as no payload).
    pub fn matches(&self, payload: &Value) -> bool {
        if self.must.iter().any(|c| !c.matches(payload)) {
            return false;
        }
        if self.must_not.iter().any(|c| c.matches(payload)) {
            return false;
        }
        if !self.should.is_empty() && !self.should.iter().any(|c| c.matches(payload)) {
            return false;
        }
        true
    }
}

impl Condition {
    pub fn matches(&self, payload: &Value) -> bool {
        match self {
            Condition::Field(f) => f.matches(payload),
            Condition::Nested { filter } => filter.matches(payload),
        }
    }
}

impl FieldCondition {
    pub fn matches(&self, payload: &Value) -> bool {
        let actual = lookup(payload, &self.key);
        match &self.op {
            FieldOp::Match { value } => match actual {
                Some(Value::Array(arr)) => arr.iter().any(|v| values_equal(v, value)),
                Some(v) => values_equal(v, value),
                None => false,
            },
            FieldOp::AnyOf { any } => match actual {
                Some(Value::Array(arr)) => arr.iter().any(|v| any.iter().any(|x| values_equal(v, x))),
                Some(v) => any.iter().any(|x| values_equal(v, x)),
                None => false,
            },
            FieldOp::Range { gte, lte, gt, lt } => {
                let n = match actual {
                    Some(Value::Number(n)) => n.as_f64(),
                    Some(Value::Array(arr)) => arr.iter().find_map(|v| match v {
                        Value::Number(n) => n.as_f64(),
                        _ => None,
                    }),
                    _ => None,
                };
                let Some(n) = n else { return false };
                if let Some(g) = gte {
                    if n < *g {
                        return false;
                    }
                }
                if let Some(l) = lte {
                    if n > *l {
                        return false;
                    }
                }
                if let Some(g) = gt {
                    if n <= *g {
                        return false;
                    }
                }
                if let Some(l) = lt {
                    if n >= *l {
                        return false;
                    }
                }
                true
            }
            FieldOp::Exists { exists } => {
                let present = matches!(actual, Some(v) if !v.is_null());
                present == *exists
            }
        }
    }
}

fn values_equal(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => match (x.as_f64(), y.as_f64()) {
            (Some(xf), Some(yf)) => (xf - yf).abs() < f64::EPSILON,
            _ => x == y,
        },
        _ => a == b,
    }
}

/// Resolve a dotted path inside a JSON value.
pub fn lookup<'a>(value: &'a Value, key: &str) -> Option<&'a Value> {
    let mut cur = value;
    for part in key.split('.') {
        cur = cur.as_object()?.get(part)?;
    }
    Some(cur)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn payload() -> Value {
        json!({
            "category": "books",
            "price": 29.5,
            "tags": ["bestseller", "fiction"],
            "in_stock": true,
            "meta": {"author": "ada"}
        })
    }

    #[test]
    fn match_simple() {
        let f: Filter = serde_json::from_value(json!({
            "must": [{"key": "category", "match": {"value": "books"}}]
        }))
        .unwrap();
        assert!(f.matches(&payload()));
    }

    #[test]
    fn range_with_must_not() {
        let f: Filter = serde_json::from_value(json!({
            "must": [{"key": "price", "range": {"gte": 10, "lte": 50}}],
            "must_not": [{"key": "in_stock", "match": {"value": false}}]
        }))
        .unwrap();
        assert!(f.matches(&payload()));
    }

    #[test]
    fn any_of_array_field() {
        let f: Filter = serde_json::from_value(json!({
            "must": [{"key": "tags", "any_of": {"any": ["sale", "fiction"]}}]
        }))
        .unwrap();
        assert!(f.matches(&payload()));
    }

    #[test]
    fn nested_path_and_should() {
        let f: Filter = serde_json::from_value(json!({
            "should": [
                {"key": "meta.author", "match": {"value": "ada"}},
                {"key": "category", "match": {"value": "movies"}}
            ]
        }))
        .unwrap();
        assert!(f.matches(&payload()));
    }

    #[test]
    fn exists_negative() {
        let f: Filter = serde_json::from_value(json!({
            "must": [{"key": "missing", "exists": {"exists": true}}]
        }))
        .unwrap();
        assert!(!f.matches(&payload()));
    }
}
