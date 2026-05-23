//! Persistent storage: write-ahead log, on-disk segments, and collection engine.

pub mod engine;
pub mod segment;
pub mod wal;

pub use engine::{CollectionEngine, EngineConfig, EngineError};
pub use wal::WalEntry;
