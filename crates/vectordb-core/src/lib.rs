//! Core primitives for VectorDB: vectors, distance metrics, and HNSW indexing.

pub mod collection;
pub mod distance;
pub mod error;
pub mod hnsw;
pub mod types;

pub use collection::{CollectionConfig, DistanceMetric};
pub use distance::Distance;
pub use error::{Error, Result};
pub use hnsw::{HnswConfig, HnswIndex};
pub use types::{PointId, ScoredPoint, Vector};
