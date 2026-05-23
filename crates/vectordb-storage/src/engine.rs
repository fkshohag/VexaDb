use std::collections::HashMap;
use std::path::{Path, PathBuf};

use parking_lot::RwLock;
use rocksdb::{Options, DB};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use vectordb_core::{
    CollectionConfig, DistanceMetric, Error as CoreError, HnswConfig, HnswIndex, PointId,
    ScoredPoint, Vector,
};

use crate::wal::{WalEntry, WriteAheadLog};

#[derive(Debug, Error)]
pub enum EngineError {
    #[error("core error: {0}")]
    Core(#[from] CoreError),
    #[error("rocksdb error: {0}")]
    Rocks(String),
    #[error("wal error: {0}")]
    Wal(#[from] crate::wal::WalError),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("collection exists: {0}")]
    CollectionExists(String),
    #[error("collection not found: {0}")]
    CollectionNotFound(String),
}

pub type Result<T> = std::result::Result<T, EngineError>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineConfig {
    pub data_dir: PathBuf,
    pub sync_wal: bool,
}

impl EngineConfig {
    pub fn new(data_dir: impl AsRef<Path>) -> Self {
        Self {
            data_dir: data_dir.as_ref().to_path_buf(),
            sync_wal: true,
        }
    }
}

struct CollectionState {
    config: CollectionConfig,
    index: HnswIndex,
    payloads: HashMap<PointId, Vec<u8>>,
}

/// Single-node collection engine: HNSW in memory + RocksDB metadata + WAL durability.
pub struct CollectionEngine {
    config: EngineConfig,
    collections: RwLock<HashMap<String, CollectionState>>,
    meta_db: DB,
    wal: RwLock<WriteAheadLog>,
}

impl CollectionEngine {
    pub fn open(config: EngineConfig) -> Result<Self> {
        std::fs::create_dir_all(&config.data_dir)?;
        let meta_path = config.data_dir.join("meta");
        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);
        let meta_db = DB::open(&db_opts, &meta_path)
            .map_err(|e| EngineError::Rocks(e.to_string()))?;

        let wal_path = config.data_dir.join("wal.log");
        let wal = WriteAheadLog::open(wal_path)?;

        let engine = Self {
            config,
            collections: RwLock::new(HashMap::new()),
            meta_db,
            wal: RwLock::new(wal),
        };

        engine.replay_wal()?;
        engine.load_collections_from_meta()?;
        Ok(engine)
    }

    fn replay_wal(&self) -> Result<()> {
        let entries = self.wal.read().replay()?;
        for entry in entries {
            match entry {
                WalEntry::Upsert {
                    collection,
                    id,
                    vector,
                    payload,
                } => {
                    self.ensure_collection_loaded(&collection)?;
                    self.apply_upsert(&collection, id, vector, payload)?;
                }
                WalEntry::Delete { collection, id } => {
                    self.ensure_collection_loaded(&collection)?;
                    self.apply_delete(&collection, &id)?;
                }
            }
        }
        Ok(())
    }

    fn load_collections_from_meta(&self) -> Result<()> {
        let iter = self.meta_db.iterator(rocksdb::IteratorMode::Start);
        for item in iter {
            let (key, value) = item.map_err(|e| EngineError::Rocks(e.to_string()))?;
            let key_str = String::from_utf8_lossy(&key);
            if !key_str.starts_with("collection:") {
                continue;
            }
            let name = key_str.trim_start_matches("collection:").to_string();
            let config: CollectionConfig =
                serde_json::from_slice(&value).map_err(|e| EngineError::Rocks(e.to_string()))?;
            self.get_or_create_state(&name, config);
        }
        Ok(())
    }

    pub fn create_collection(&self, config: CollectionConfig) -> Result<()> {
        config.validate().map_err(EngineError::Core)?;
        let key = format!("collection:{}", config.name);
        if self.meta_db.get(&key).map_err(|e| EngineError::Rocks(e.to_string()))?.is_some() {
            return Err(EngineError::CollectionExists(config.name.clone()));
        }
        let json = serde_json::to_vec(&config).map_err(|e| EngineError::Rocks(e.to_string()))?;
        self.meta_db
            .put(key, json)
            .map_err(|e| EngineError::Rocks(e.to_string()))?;
        let name = config.name.clone();
        self.get_or_create_state(&name, config);
        Ok(())
    }

    pub fn delete_collection(&self, name: &str) -> Result<()> {
        let key = format!("collection:{name}");
        self.meta_db
            .delete(key)
            .map_err(|e| EngineError::Rocks(e.to_string()))?;
        self.collections.write().remove(name);
        Ok(())
    }

    pub fn list_collections(&self) -> Vec<String> {
        self.collections.read().keys().cloned().collect()
    }

    pub fn describe_collection(&self, name: &str) -> Result<CollectionConfig> {
        let collections = self.collections.read();
        collections
            .get(name)
            .map(|s| s.config.clone())
            .ok_or_else(|| EngineError::CollectionNotFound(name.to_string()))
    }

    pub fn upsert(
        &self,
        collection: &str,
        id: String,
        vector: Vector,
        payload: Option<Vec<u8>>,
    ) -> Result<()> {
        self.ensure_collection_loaded(collection)?;
        let entry = WalEntry::Upsert {
            collection: collection.to_string(),
            id: id.clone(),
            vector: vector.clone(),
            payload: payload.clone(),
        };
        self.wal.write().append(&entry)?;
        self.apply_upsert(collection, id, vector, payload)
    }

    pub fn delete(&self, collection: &str, id: &str) -> Result<()> {
        self.ensure_collection_loaded(collection)?;
        let entry = WalEntry::Delete {
            collection: collection.to_string(),
            id: id.to_string(),
        };
        self.wal.write().append(&entry)?;
        self.apply_delete(collection, id)
    }

    pub fn search(
        &self,
        collection: &str,
        query: &[f32],
        k: usize,
        filter_ids: Option<&[String]>,
    ) -> Result<Vec<ScoredPoint>> {
        self.ensure_collection_loaded(collection)?;
        let collections = self.collections.read();
        let state = collections
            .get(collection)
            .ok_or_else(|| EngineError::CollectionNotFound(collection.to_string()))?;

        let mut results = state.index.search(query, k)?;
        if let Some(filter) = filter_ids {
            let set: std::collections::HashSet<_> = filter.iter().collect();
            results.retain(|r| set.contains(&r.id));
        }
        Ok(results)
    }

    pub fn get(&self, collection: &str, id: &str) -> Result<Option<(Vector, Option<Vec<u8>>)>> {
        self.ensure_collection_loaded(collection)?;
        let collections = self.collections.read();
        let state = collections
            .get(collection)
            .ok_or_else(|| EngineError::CollectionNotFound(collection.to_string()))?;
        let vector = state.index.get_vector(id);
        let payload = state.payloads.get(id).cloned();
        Ok(vector.map(|v| (v, payload)))
    }

    pub fn stats(&self, collection: &str) -> Result<CollectionStats> {
        self.ensure_collection_loaded(collection)?;
        let collections = self.collections.read();
        let state = collections
            .get(collection)
            .ok_or_else(|| EngineError::CollectionNotFound(collection.to_string()))?;
        Ok(CollectionStats {
            name: collection.to_string(),
            vector_count: state.index.len(),
            dimension: state.config.dimension,
            metric: state.config.metric,
        })
    }

    fn ensure_collection_loaded(&self, name: &str) -> Result<()> {
        if self.collections.read().contains_key(name) {
            return Ok(());
        }
        let key = format!("collection:{name}");
        let raw = self
            .meta_db
            .get(key)
            .map_err(|e| EngineError::Rocks(e.to_string()))?
            .ok_or_else(|| EngineError::CollectionNotFound(name.to_string()))?;
        let config: CollectionConfig =
            serde_json::from_slice(&raw).map_err(|e| EngineError::Rocks(e.to_string()))?;
        self.get_or_create_state(name, config);
        Ok(())
    }

    fn get_or_create_state(&self, name: &str, config: CollectionConfig) -> () {
        let mut collections = self.collections.write();
        collections.entry(name.to_string()).or_insert_with(|| {
            let hnsw = HnswConfig::new(
                config.metric,
                config.m,
                config.ef_construction,
                config.ef_search,
            );
            CollectionState {
                config: config.clone(),
                index: HnswIndex::new(config.dimension, hnsw),
                payloads: HashMap::new(),
            }
        });
    }

    fn apply_upsert(
        &self,
        collection: &str,
        id: String,
        vector: Vector,
        payload: Option<Vec<u8>>,
    ) -> Result<()> {
        let collections = self.collections.read();
        let state = collections
            .get(collection)
            .ok_or_else(|| EngineError::CollectionNotFound(collection.to_string()))?;
        state.index.insert(id.clone(), vector).map_err(EngineError::Core)?;
        if let Some(p) = payload {
            drop(collections);
            let mut collections = self.collections.write();
            if let Some(state) = collections.get_mut(collection) {
                state.payloads.insert(id, p);
            }
        }
        Ok(())
    }

    fn apply_delete(&self, collection: &str, id: &str) -> Result<()> {
        let mut collections = self.collections.write();
        let state = collections
            .get_mut(collection)
            .ok_or_else(|| EngineError::CollectionNotFound(collection.to_string()))?;
        state.index.remove(id).map_err(EngineError::Core)?;
        state.payloads.remove(id);
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectionStats {
    pub name: String,
    pub vector_count: usize,
    pub dimension: usize,
    pub metric: DistanceMetric,
}
