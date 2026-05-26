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

/// Index kind for a payload field. Determines how filter conditions on this
/// field can be accelerated by the engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PayloadIndexKind {
    /// String / boolean / array-of-string equality lookups.
    Keyword,
    /// Numeric `eq` and `range` lookups (BTree).
    Numeric,
    /// Boolean lookups.
    Bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PayloadFieldIndex {
    /// Dotted JSON path (`"price"`, `"meta.author"`).
    pub field: String,
    pub kind: PayloadIndexKind,
}

/// Name of the implicit default database every cluster starts with.
/// Mirrors Milvus's `default` semantic so existing callers keep working.
pub const DEFAULT_DATABASE: &str = "default";

fn default_database() -> String {
    DEFAULT_DATABASE.to_string()
}

/// Logical database — a namespace that owns collections.
///
/// Collection names are unique *within* a database, not globally; multiple
/// databases can hold collections with the same simple name.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseConfig {
    pub name: String,
    /// User-defined properties. Stored as opaque strings; the server may
    /// interpret a small set of well-known keys (e.g. `database.replica.number`).
    #[serde(default)]
    pub properties: std::collections::BTreeMap<String, String>,
    /// Milliseconds since epoch when the database was created.
    #[serde(default)]
    pub created_at_ms: u64,
}

impl DatabaseConfig {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            properties: std::collections::BTreeMap::new(),
            created_at_ms: 0,
        }
    }
}

/// Configuration for a logical vector collection (namespace).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectionConfig {
    pub name: String,
    /// Parent database. Defaults to [`DEFAULT_DATABASE`] for back-compat with
    /// pre-database persisted collections.
    #[serde(default = "default_database")]
    pub database: String,
    pub dimension: usize,
    pub metric: DistanceMetric,
    /// HNSW graph degree (M parameter).
    pub m: usize,
    /// Construction-time search width.
    pub ef_construction: usize,
    /// Query-time search width.
    pub ef_search: usize,
    /// Optional payload fields to index for filter acceleration.
    #[serde(default)]
    pub payload_indexes: Vec<PayloadFieldIndex>,
    /// Enable sparse vector inverted index (dot-product retrieval).
    #[serde(default)]
    pub sparse_enabled: bool,
    /// BM25 over this JSON payload field (e.g. `"text"`). Enables lexical hybrid search.
    #[serde(default)]
    pub bm25_text_field: Option<String>,
    /// Optional scalar quantization for stored vectors (search still uses f32 in HNSW).
    #[serde(default)]
    pub quantization: Option<QuantizationConfig>,
    /// Opaque user-defined properties (Milvus parity: TTL, mmap.enabled, …).
    /// Always keyed by string. Server stores them but does not interpret most;
    /// some well-known keys (see `well_known_properties`) influence runtime.
    #[serde(default)]
    pub properties: std::collections::BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SearchMode {
    Dense,
    Sparse,
    Bm25,
    HybridRrf,
    HybridWeighted,
}

impl Default for SearchMode {
    fn default() -> Self {
        Self::Dense
    }
}

impl SearchMode {
    pub fn parse(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "sparse" => Self::Sparse,
            "bm25" | "lexical" => Self::Bm25,
            "hybrid_rrf" | "hybrid" => Self::HybridRrf,
            "hybrid_weighted" | "weighted" => Self::HybridWeighted,
            _ => Self::Dense,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuantizationConfig {
    #[serde(default)]
    pub scalar: bool,
}

impl CollectionConfig {
    pub fn new(name: impl Into<String>, dimension: usize, metric: DistanceMetric) -> Self {
        Self {
            name: name.into(),
            database: default_database(),
            dimension,
            metric,
            m: 16,
            ef_construction: 200,
            ef_search: 64,
            payload_indexes: Vec::new(),
            sparse_enabled: false,
            bm25_text_field: None,
            quantization: None,
            properties: std::collections::BTreeMap::new(),
        }
    }

    /// Set the parent database. Builder helper used by the gateway / SDK.
    pub fn with_database(mut self, db: impl Into<String>) -> Self {
        self.database = db.into();
        self
    }

    pub fn with_index(mut self, field: impl Into<String>, kind: PayloadIndexKind) -> Self {
        self.payload_indexes.push(PayloadFieldIndex {
            field: field.into(),
            kind,
        });
        self
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
