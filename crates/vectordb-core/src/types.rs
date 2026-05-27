use serde::{Deserialize, Serialize};

/// Stable identifier for a vector point (user-supplied or server-generated).
pub type PointId = String;

/// Dense floating-point embedding.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Vector {
    pub values: Vec<f32>,
}

impl Vector {
    pub fn new(values: Vec<f32>) -> Self {
        Self { values }
    }

    pub fn dim(&self) -> usize {
        self.values.len()
    }
}

/// A neighbor returned from similarity search (or filter-only query).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct ScoredPoint {
    pub id: PointId,
    pub score: f32,
    /// JSON payload when the caller requested `with_payload`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<serde_json::Value>,
    /// Dense vector when the caller requested `with_vector`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vector: Option<Vec<f32>>,
}

/// Options controlling which fields are returned alongside search hits.
#[derive(Debug, Clone, Default)]
pub struct OutputOptions {
    /// Top-level payload keys to include. Empty + `with_payload` means all keys.
    pub output_fields: Vec<String>,
    pub with_payload: bool,
    pub with_vector: bool,
}
