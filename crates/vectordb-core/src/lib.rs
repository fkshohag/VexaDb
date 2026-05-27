//! Core primitives for VectorDB: vectors, distance metrics, and HNSW indexing.

pub mod bm25;
pub mod collection;
pub mod distance;
pub mod error;
pub mod filter;
pub mod filter_expr;
pub mod fusion;
pub mod hnsw;
pub mod quantize;
pub mod simd;
pub mod sparse;
pub mod types;

pub use bm25::{tokenize, Bm25Index};
pub use collection::{
    CollectionConfig, DatabaseConfig, DistanceMetric, PayloadFieldIndex, PayloadIndexKind,
    QuantizationConfig, ResourceGroupConfig, ResourceGroupInfo, ResourceGroupLimit,
    ResourceGroupNodeFilter, ResourceGroupTransfer, SearchMode, DEFAULT_DATABASE,
    DEFAULT_PARTITION, DEFAULT_RESOURCE_GROUP, PARTITION_PAYLOAD_FIELD,
};
pub use distance::Distance;
pub use error::{Error, Result};
pub use filter::{Condition, FieldCondition, FieldOp, Filter};
pub use filter_expr::{parse_filter_expr, parse_filter_input, FilterParseError};
pub use types::OutputOptions;
pub use fusion::{rrf_fusion, weighted_fusion};
pub use hnsw::{HnswConfig, HnswIndex};
pub use quantize::ScalarQuantizer;
pub use sparse::{SparseInvertedIndex, SparseVector};
pub use types::{PointId, ScoredPoint, Vector};
