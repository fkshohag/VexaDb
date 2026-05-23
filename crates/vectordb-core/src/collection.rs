use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

/// Supported similarity metrics (Pinecone-compatible naming).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum DistanceMetric {
    #[default]
    Cosine,
    Euclidean,
    DotProduct,
}

impl DistanceMetric {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Cosine => "cosine",
            Self::Euclidean => "euclidean",
            Self::DotProduct => "dot_product",
        }
    }
}

/// Configuration for a logical vector collection (namespace).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectionConfig {
    pub name: String,
    pub dimension: usize,
    pub metric: DistanceMetric,
    /// HNSW graph degree (M parameter).
    pub m: usize,
    /// Construction-time search width.
    pub ef_construction: usize,
    /// Query-time search width.
    pub ef_search: usize,
}

impl CollectionConfig {
    pub fn new(name: impl Into<String>, dimension: usize, metric: DistanceMetric) -> Self {
        Self {
            name: name.into(),
            dimension,
            metric,
            m: 16,
            ef_construction: 200,
            ef_search: 64,
        }
    }

    pub fn validate(&self) -> Result<()> {
        if self.dimension == 0 {
            return Err(Error::InvalidConfig("dimension must be > 0".into()));
        }
        if self.m < 2 {
            return Err(Error::InvalidConfig("m must be >= 2".into()));
        }
        if self.ef_construction < self.m {
            return Err(Error::InvalidConfig(
                "ef_construction must be >= m".into(),
            ));
        }
        if self.ef_search < 1 {
            return Err(Error::InvalidConfig("ef_search must be >= 1".into()));
        }
        Ok(())
    }
}
