use std::collections::HashMap;
use std::path::{Path, PathBuf};

use parking_lot::RwLock;
use rocksdb::{Options, DB};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use vectordb_core::{
    Bm25Index, CollectionConfig, DistanceMetric, Error as CoreError, Filter, HnswConfig, HnswIndex,
    OutputOptions, PointId, ScalarQuantizer, ScoredPoint, SearchMode, SparseInvertedIndex,
    SparseVector, Vector,
};
use vectordb_rbac::{RbacError, RbacOp, RbacSnapshot, RbacState};

use crate::payload_index::PayloadIndexes;
use crate::snapshot::{SnapshotManager, SnapshotMeta};
use crate::wal::{BulkPoint, WalEntry, WriteAheadLog};
use crate::wal_compact::export_state_to_wal;

const RBAC_SNAPSHOT_KEY: &[u8] = b"__rbac_snapshot__";

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
    #[error("invalid payload: {0}")]
    InvalidPayload(String),
    #[error("rbac error: {0}")]
    Rbac(#[from] RbacError),
}

pub type Result<T> = std::result::Result<T, EngineError>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineConfig {
    pub data_dir: PathBuf,
    pub sync_wal: bool,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self::new("./data")
    }
}

impl EngineConfig {
    pub fn new(data_dir: impl AsRef<Path>) -> Self {
        Self {
            data_dir: data_dir.as_ref().to_path_buf(),
            sync_wal: true,
        }
    }
}

pub(crate) struct CollectionState {
    pub(crate) config: CollectionConfig,
    pub(crate) index: HnswIndex,
    pub(crate) payloads: HashMap<PointId, Value>,
    pub(crate) payload_indexes: PayloadIndexes,
    pub(crate) sparse_index: Option<SparseInvertedIndex>,
    pub(crate) bm25_index: Option<Bm25Index>,
    pub(crate) quantizer: Option<ScalarQuantizer>,
    pub(crate) quantized: HashMap<PointId, Vec<u8>>,
}

/// Single-node collection engine: HNSW in memory + RocksDB metadata + WAL durability.
pub struct CollectionEngine {
    config: EngineConfig,
    collections: RwLock<HashMap<String, CollectionState>>,
    /// Wrapped so we can reopen RocksDB after a Raft InstallSnapshot.
    meta_db: RwLock<DB>,
    wal: RwLock<WriteAheadLog>,
    /// RBAC state (users, tokens, roles, grants). Persisted as a JSON
    /// snapshot in `meta_db` and replicated through the same WAL/Raft as
    /// collection operations via [`WalEntry::Rbac`].
    rbac: RwLock<RbacState>,
}

pub(crate) const FILTER_BRUTE_FORCE_LIMIT: usize = 50_000;
pub(crate) const FILTER_OVERSEARCH_FACTOR: usize = 16;
pub(crate) const FILTER_OVERSEARCH_CAP: usize = 1024;

impl CollectionEngine {
    pub fn open(config: EngineConfig) -> Result<Self> {
        std::fs::create_dir_all(&config.data_dir)?;
        let meta_path = config.data_dir.join("meta");
        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);
        let meta_db = DB::open(&db_opts, &meta_path)
            .map_err(|e| EngineError::Rocks(e.to_string()))?;

        let wal_path = config.data_dir.join("wal.log");
        let wal = WriteAheadLog::open_with(wal_path, config.sync_wal)?;

        let engine = Self {
            config,
            collections: RwLock::new(HashMap::new()),
            meta_db: RwLock::new(meta_db),
            wal: RwLock::new(wal),
            rbac: RwLock::new(RbacState::new()),
        };

        engine.load_rbac_from_meta()?;
        engine.replay_wal()?;
        engine.load_collections_from_meta()?;
        Ok(engine)
    }

    fn load_rbac_from_meta(&self) -> Result<()> {
        let snap = self
            .meta_db
            .read()
            .get(RBAC_SNAPSHOT_KEY)
            .map_err(|e| EngineError::Rocks(e.to_string()))?;
        if let Some(bytes) = snap {
            let snap: RbacSnapshot =
                serde_json::from_slice(&bytes).map_err(|e| EngineError::Rocks(e.to_string()))?;
            self.rbac.write().restore(snap);
        }
        Ok(())
    }

    fn persist_rbac(&self) -> Result<()> {
        let snap = self.rbac.read().snapshot();
        let bytes = serde_json::to_vec(&snap).map_err(|e| EngineError::Rocks(e.to_string()))?;
        self.meta_db
            .read()
            .put(RBAC_SNAPSHOT_KEY, bytes)
            .map_err(|e| EngineError::Rocks(e.to_string()))?;
        Ok(())
    }

    /// Read-only access to the RBAC state. The caller holds a read guard;
    /// drop it promptly so writers aren't blocked.
    pub fn rbac(&self) -> parking_lot::RwLockReadGuard<'_, RbacState> {
        self.rbac.read()
    }

    /// Apply an RBAC op locally (WAL + state). Used by Raft followers on
    /// commit. Leaders should propose through Raft instead.
    pub fn commit_rbac(&self, op: RbacOp) -> Result<()> {
        let entry = WalEntry::Rbac { op };
        self.commit_entry(&entry)
    }

    fn apply_rbac(&self, op: &RbacOp) -> Result<()> {
        self.rbac.write().apply(op.clone())?;
        self.persist_rbac()?;
        Ok(())
    }

    /// Seed the built-in roles. Safe to call any number of times.
    pub fn ensure_builtin_rbac(&self, now_ms: u64) -> Result<()> {
        self.rbac.write().ensure_builtin_roles(now_ms);
        self.persist_rbac()?;
        Ok(())
    }

    pub fn data_dir(&self) -> &Path {
        &self.config.data_dir
    }

    pub fn snapshot_manager(&self) -> SnapshotManager {
        SnapshotManager::new(&self.config.data_dir)
    }

    /// Replace on-disk state from a snapshot payload directory (as produced by
    /// `SnapshotManager::create`) and reload in-memory indexes. Used by Raft
    /// `InstallSnapshot` on followers — much faster than replaying via Scroll.
    pub fn restore_from_snapshot(&self, payload_root: &std::path::Path) -> Result<()> {
        let meta_path = self.config.data_dir.join("meta");
        let placeholder_path = self.config.data_dir.join(".meta_swap_placeholder");
        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);

        // Close RocksDB before replacing `meta/` on disk (required on macOS/Windows).
        {
            let mut guard = self.meta_db.write();
            let tmp = DB::open(&db_opts, &placeholder_path)
                .map_err(|e| EngineError::Rocks(e.to_string()))?;
            let old = std::mem::replace(&mut *guard, tmp);
            drop(old);
        }

        crate::snapshot::install_payload_into_data_dir(payload_root, &self.config.data_dir)?;

        let new_db = DB::open(&db_opts, &meta_path).map_err(|e| EngineError::Rocks(e.to_string()))?;
        *self.meta_db.write() = new_db;
        let _ = std::fs::remove_dir_all(&placeholder_path);

        let wal_path = self.config.data_dir.join("wal.log");
        *self.wal.write() =
            WriteAheadLog::open_with(wal_path, self.config.sync_wal)?;

        self.collections.write().clear();
        self.replay_wal()?;
        self.load_collections_from_meta()?;
        Ok(())
    }

    fn replay_wal(&self) -> Result<()> {
        // WAL replay is the slowest part of cold start (rebuilds HNSW indexes
        // for every Upsert). Emit progress so operators can see startup
        // progress in `docker logs` instead of staring at a silent 100% CPU.
        let wal_path = self.wal.read().path().to_path_buf();
        let wal_bytes = std::fs::metadata(&wal_path).map(|m| m.len()).unwrap_or(0);

        let started = std::time::Instant::now();
        let entries = self.wal.read().replay()?;
        let total = entries.len();
        let read_elapsed = started.elapsed();
        if total > 0 {
            tracing::info!(
                wal_path = %wal_path.display(),
                wal_bytes = wal_bytes,
                entries = total,
                read_secs = read_elapsed.as_secs_f64(),
                "WAL replay: starting apply"
            );
        }

        let apply_started = std::time::Instant::now();
        // Log progress at most every ~5s and every ~10% milestone, whichever
        // comes first; cheap modulo + monotonic clock check.
        let step = (total / 20).max(1).min(50_000);
        let mut last_log = std::time::Instant::now();
        // Tolerate poisoned entries: data-mutation entries (Upsert/Delete/
        // BulkUpsert) referencing a collection that doesn't exist locally
        // would otherwise crash the process forever in a restart loop.
        // This happens when:
        //   - a Raft follower gets an Upsert before/without the matching
        //     CreateCollection (out-of-order delivery, pre-bootstrap),
        //   - rebalance/migration sent points to a destination that never
        //     received the schema,
        //   - the meta DB was lost while the WAL survived.
        // The data is unrecoverable without the schema anyway, so we log
        // and skip — startup proceeds, and a follow-up snapshot/compaction
        // drops the bad records permanently.
        let mut skipped_unknown_collection: usize = 0;
        let mut first_skipped: Option<String> = None;
        for (i, entry) in entries.iter().enumerate() {
            match self.apply_entry(entry) {
                Ok(()) => {}
                Err(EngineError::CollectionNotFound(name)) => {
                    skipped_unknown_collection += 1;
                    if first_skipped.is_none() {
                        first_skipped = Some(name.clone());
                    }
                    if skipped_unknown_collection <= 3 || skipped_unknown_collection % 1000 == 0 {
                        tracing::warn!(
                            collection = %name,
                            entry_index = i + 1,
                            skipped_total = skipped_unknown_collection,
                            "WAL replay: skipping entry for unknown collection (will be dropped on next compact_wal)"
                        );
                    }
                }
                Err(e) => return Err(e),
            }
            if total > 1000
                && (i % step == 0 || last_log.elapsed() >= std::time::Duration::from_secs(5))
            {
                tracing::info!(
                    applied = i + 1,
                    total = total,
                    pct = (i + 1) as f64 * 100.0 / total as f64,
                    elapsed_secs = apply_started.elapsed().as_secs_f64(),
                    skipped_unknown_collection = skipped_unknown_collection,
                    "WAL replay progress"
                );
                last_log = std::time::Instant::now();
            }
        }
        if total > 0 {
            tracing::info!(
                entries = total,
                total_secs = started.elapsed().as_secs_f64(),
                skipped_unknown_collection = skipped_unknown_collection,
                first_skipped = first_skipped.as_deref(),
                "WAL replay: complete"
            );
        }
        Ok(())
    }

    fn load_collections_from_meta(&self) -> Result<()> {
        let meta = self.meta_db.read();
        let iter = meta.iterator(rocksdb::IteratorMode::Start);
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
        let key = format!("collection:{}", config.name);
        if self
            .meta_db
            .read()
            .get(&key)
            .map_err(|e| EngineError::Rocks(e.to_string()))?
            .is_some()
        {
            return Err(EngineError::CollectionExists(config.name.clone()));
        }
        let entry = WalEntry::CreateCollection {
            config: config.clone(),
        };
        self.wal.write().append(&entry)?;
        self.apply_create_collection(config)
    }

    pub fn delete_collection(&self, name: &str) -> Result<()> {
        let entry = WalEntry::DeleteCollection {
            name: name.to_string(),
        };
        self.wal.write().append(&entry)?;
        self.apply_delete_collection(name)
    }

    /// Durably append and apply (Raft commit path).
    pub fn commit_entry(&self, entry: &WalEntry) -> Result<()> {
        self.wal.write().append(entry)?;
        self.apply_entry(entry)
    }

    /// Apply only (WAL replay on startup).
    pub fn apply_entry(&self, entry: &WalEntry) -> Result<()> {
        match entry {
            WalEntry::CreateCollection { config } => self.apply_create_collection(config.clone()),
            WalEntry::DeleteCollection { name } => self.apply_delete_collection(name),
            WalEntry::Upsert {
                collection,
                id,
                vector,
                payload,
                sparse,
            } => {
                self.ensure_collection_loaded(collection)?;
                self.apply_upsert(
                    collection,
                    id.clone(),
                    vector.clone(),
                    payload.clone(),
                    sparse.clone(),
                )
            }
            WalEntry::Delete { collection, id } => {
                self.ensure_collection_loaded(collection)?;
                self.apply_delete(collection, id)
            }
            WalEntry::BulkUpsert { collection, points } => {
                self.ensure_collection_loaded(collection)?;
                for p in points {
                    self.apply_upsert(
                        collection,
                        p.id.clone(),
                        p.vector.clone(),
                        p.payload.clone(),
                        p.sparse.clone(),
                    )?;
                }
                Ok(())
            }
            WalEntry::Checkpoint { .. } => Ok(()),
            WalEntry::Rbac { op } => self.apply_rbac(op),
        }
    }

    /// Idempotent apply (used by WAL replay and Raft followers): persist config
    /// to RocksDB and ensure the in-memory state exists.
    fn apply_create_collection(&self, config: CollectionConfig) -> Result<()> {
        config.validate().map_err(EngineError::Core)?;
        let key = format!("collection:{}", config.name);
        let json = serde_json::to_vec(&config).map_err(|e| EngineError::Rocks(e.to_string()))?;
        self.meta_db
            .read()
            .put(key, json)
            .map_err(|e| EngineError::Rocks(e.to_string()))?;
        let name = config.name.clone();
        self.get_or_create_state(&name, config);
        Ok(())
    }

    fn apply_delete_collection(&self, name: &str) -> Result<()> {
        let key = format!("collection:{name}");
        self.meta_db
            .read()
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
        sparse: Option<SparseVector>,
    ) -> Result<()> {
        self.ensure_collection_loaded(collection)?;
        let entry = WalEntry::Upsert {
            collection: collection.to_string(),
            id: id.clone(),
            vector: vector.clone(),
            payload: payload.clone(),
            sparse: sparse.clone(),
        };
        self.commit_entry(&entry)
    }

    pub fn delete(&self, collection: &str, id: &str) -> Result<()> {
        self.ensure_collection_loaded(collection)?;
        let entry = WalEntry::Delete {
            collection: collection.to_string(),
            id: id.to_string(),
        };
        self.commit_entry(&entry)
    }

    /// Bulk import: one WAL record per chunk (default chunk size 500).
    pub fn bulk_upsert(
        &self,
        collection: &str,
        points: Vec<BulkPoint>,
        chunk_size: usize,
    ) -> Result<u64> {
        self.ensure_collection_loaded(collection)?;
        if points.is_empty() {
            return Ok(0);
        }
        let chunk_size = chunk_size.max(1);
        let mut total = 0u64;
        for chunk in points.chunks(chunk_size) {
            let entry = WalEntry::BulkUpsert {
                collection: collection.to_string(),
                points: chunk.to_vec(),
            };
            self.commit_entry(&entry)?;
            total += chunk.len() as u64;
        }
        Ok(total)
    }

    /// Number of WAL entries currently on disk (cheap probe for auto-snapshot loops).
    pub fn replay_wal_count(&self) -> Result<usize> {
        Ok(self.wal.read().replay()?.len())
    }

    /// Rewrite WAL from current in-memory state (drops historical deletes/updates).
    pub fn compact_wal(&self) -> Result<WalCompactionStats> {
        self.ensure_all_collections_loaded()?;
        let before = self.wal.read().replay()?.len();
        let export = self.export_current_wal_state()?;
        let after = export.len();
        self.wal.write().rewrite(&export)?;
        Ok(WalCompactionStats { before, after })
    }

    /// Snapshot data dir, then compact WAL and append a checkpoint marker.
    pub fn snapshot_and_compact_wal(&self) -> Result<(SnapshotMeta, WalCompactionStats)> {
        let snap = self.snapshot_manager().create()?;
        let stats = self.compact_wal()?;
        self.wal
            .write()
            .append_checkpoint(&snap.id)?;
        Ok((snap, stats))
    }

    /// Rebuild HNSW from stored vectors (online reindex).
    pub fn reindex_collection(&self, name: &str) -> Result<u64> {
        self.ensure_collection_loaded(name)?;
        let (config, points) = {
            let collections = self.collections.read();
            let state = collections
                .get(name)
                .ok_or_else(|| EngineError::CollectionNotFound(name.to_string()))?;
            let points = state.index.iter_points();
            (state.config.clone(), points)
        };
        let hnsw = HnswConfig::new(
            config.metric,
            config.m,
            config.ef_construction,
            config.ef_search,
        );
        let new_index = HnswIndex::new(config.dimension, hnsw);
        for (id, vector) in &points {
            new_index.insert(id.clone(), vector.clone()).map_err(EngineError::Core)?;
        }
        let n = points.len() as u64;
        let mut collections = self.collections.write();
        let state = collections
            .get_mut(name)
            .ok_or_else(|| EngineError::CollectionNotFound(name.to_string()))?;
        state.index = new_index;
        Ok(n)
    }

    fn export_current_wal_state(&self) -> Result<Vec<WalEntry>> {
        let collections = self.collections.read();
        let mut export = Vec::new();
        for (name, state) in collections.iter() {
            let mut points = Vec::new();
            for (id, vector) in state.index.iter_points() {
                let payload = state
                    .payloads
                    .get(&id)
                    .and_then(|v| serde_json::to_vec(v).ok());
                let sparse = state
                    .sparse_index
                    .as_ref()
                    .and_then(|idx| idx.get(&id).cloned());
                points.push(BulkPoint {
                    id,
                    vector,
                    payload,
                    sparse,
                });
            }
            export.push((state.config.clone(), points));
        }
        Ok(export_state_to_wal(&export))
    }

    pub fn search(
        &self,
        collection: &str,
        query: &[f32],
        k: usize,
        filter: Option<&Filter>,
        output: OutputOptions,
    ) -> Result<Vec<ScoredPoint>> {
        self.search_params(
            collection,
            crate::search::SearchParams {
                query,
                sparse_query: None,
                text_query: None,
                mode: SearchMode::Dense,
                hybrid_alpha: 0.5,
                filter,
            },
            k,
            output,
        )
    }

    pub fn search_params(
        &self,
        collection: &str,
        params: crate::search::SearchParams<'_>,
        k: usize,
        output: OutputOptions,
    ) -> Result<Vec<ScoredPoint>> {
        self.ensure_collection_loaded(collection)?;
        let collections = self.collections.read();
        let state = collections
            .get(collection)
            .ok_or_else(|| EngineError::CollectionNotFound(collection.to_string()))?;
        let mut hits = crate::search::hybrid_search(state, k, params).map_err(EngineError::Core)?;
        crate::output::attach_outputs(state, &mut hits, &output);
        Ok(hits)
    }

    /// Filter-only retrieval (no ANN). Supports pagination via `offset` + `limit`.
    pub fn query(
        &self,
        collection: &str,
        filter: Option<&Filter>,
        ids: &[String],
        limit: usize,
        offset: usize,
        output: OutputOptions,
    ) -> Result<Vec<ScoredPoint>> {
        self.ensure_collection_loaded(collection)?;
        let collections = self.collections.read();
        let state = collections
            .get(collection)
            .ok_or_else(|| EngineError::CollectionNotFound(collection.to_string()))?;
        let limit = limit.max(1);
        let mut candidates: Vec<String> = if ids.is_empty() {
            let mut all: Vec<String> = state
                .index
                .iter_points()
                .into_iter()
                .map(|(id, _)| id)
                .collect();
            all.sort();
            all
        } else {
            let mut v = ids.to_vec();
            v.sort();
            v
        };

        let mut hits = Vec::new();
        let mut skipped = 0usize;
        for id in candidates {
            if !matches_filter(state, filter, &id) {
                continue;
            }
            if skipped < offset {
                skipped += 1;
                continue;
            }
            if hits.len() >= limit {
                break;
            }
            hits.push(ScoredPoint {
                id,
                score: 0.0,
                payload: None,
                vector: None,
            });
        }
        crate::output::attach_outputs(state, &mut hits, &output);
        Ok(hits)
    }

    pub fn get(
        &self,
        collection: &str,
        id: &str,
    ) -> Result<Option<(Vector, Option<Value>)>> {
        self.ensure_collection_loaded(collection)?;
        let collections = self.collections.read();
        let state = collections
            .get(collection)
            .ok_or_else(|| EngineError::CollectionNotFound(collection.to_string()))?;
        let vector = state.index.get_vector(id);
        let payload = state.payloads.get(id).cloned();
        Ok(vector.map(|v| (v, payload)))
    }

    /// Paged enumeration of every point in a collection (used by rebalance).
    /// `cursor` is opaque; pass an empty string to start. Returns up to `limit`
    /// `(id, vector, payload)` triples plus a `next_cursor` (empty when done).
    pub fn scroll(
        &self,
        collection: &str,
        cursor: &str,
        limit: usize,
    ) -> Result<(Vec<(String, Vector, Option<Value>)>, String)> {
        self.ensure_collection_loaded(collection)?;
        let collections = self.collections.read();
        let state = collections
            .get(collection)
            .ok_or_else(|| EngineError::CollectionNotFound(collection.to_string()))?;
        let mut all = state.index.iter_points();
        all.sort_by(|a, b| a.0.cmp(&b.0));
        let start = if cursor.is_empty() {
            0
        } else {
            all.binary_search_by(|(id, _)| id.as_str().cmp(cursor))
                .map(|i| i + 1)
                .unwrap_or_else(|i| i)
        };
        let end = (start + limit.max(1)).min(all.len());
        let chunk: Vec<(String, Vector, Option<Value>)> = all[start..end]
            .iter()
            .map(|(id, vec)| {
                let payload = state.payloads.get(id).cloned();
                (id.clone(), vec.clone(), payload)
            })
            .collect();
        let next = if end < all.len() {
            chunk.last().map(|(id, _, _)| id.clone()).unwrap_or_default()
        } else {
            String::new()
        };
        Ok((chunk, next))
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
            sparse_enabled: state.config.sparse_enabled,
            bm25_text_field: state.config.bm25_text_field.clone().unwrap_or_default(),
            payload_index_count: state.config.payload_indexes.len(),
            scalar_quantization: state
                .config
                .quantization
                .as_ref()
                .map(|q| q.scalar)
                .unwrap_or(false),
        })
    }

    fn ensure_all_collections_loaded(&self) -> Result<()> {
        let meta = self.meta_db.read();
        let iter = meta.iterator(rocksdb::IteratorMode::Start);
        for item in iter {
            let (key, _) = item.map_err(|e| EngineError::Rocks(e.to_string()))?;
            let key_str = String::from_utf8_lossy(&key);
            if let Some(name) = key_str.strip_prefix("collection:") {
                self.ensure_collection_loaded(name)?;
            }
        }
        Ok(())
    }

    fn ensure_collection_loaded(&self, name: &str) -> Result<()> {
        if self.collections.read().contains_key(name) {
            return Ok(());
        }
        let key = format!("collection:{name}");
        let raw = self
            .meta_db
            .read()
            .get(key)
            .map_err(|e| EngineError::Rocks(e.to_string()))?
            .ok_or_else(|| EngineError::CollectionNotFound(name.to_string()))?;
        let config: CollectionConfig =
            serde_json::from_slice(&raw).map_err(|e| EngineError::Rocks(e.to_string()))?;
        self.get_or_create_state(name, config);
        Ok(())
    }

    fn get_or_create_state(&self, name: &str, config: CollectionConfig) {
        let mut collections = self.collections.write();
        collections.entry(name.to_string()).or_insert_with(|| {
            let hnsw = HnswConfig::new(
                config.metric,
                config.m,
                config.ef_construction,
                config.ef_search,
            );
            let payload_indexes = PayloadIndexes::new(&config.payload_indexes);
            let sparse_index = if config.sparse_enabled {
                Some(SparseInvertedIndex::default())
            } else {
                None
            };
            let bm25_index = config
                .bm25_text_field
                .as_ref()
                .map(|f| Bm25Index::new(f.clone()));
            let quantizer = if config
                .quantization
                .as_ref()
                .map(|q| q.scalar)
                .unwrap_or(false)
            {
                Some(ScalarQuantizer::new(config.dimension))
            } else {
                None
            };
            CollectionState {
                config: config.clone(),
                index: HnswIndex::new(config.dimension, hnsw),
                payloads: HashMap::new(),
                payload_indexes,
                sparse_index,
                bm25_index,
                quantizer,
                quantized: HashMap::new(),
            }
        });
    }

    fn apply_upsert(
        &self,
        collection: &str,
        id: String,
        vector: Vector,
        payload: Option<Vec<u8>>,
        sparse: Option<SparseVector>,
    ) -> Result<()> {
        let payload_value = match payload {
            Some(bytes) if !bytes.is_empty() => parse_payload(&bytes)?,
            _ => Value::Null,
        };

        let mut collections = self.collections.write();
        let state = collections
            .get_mut(collection)
            .ok_or_else(|| EngineError::CollectionNotFound(collection.to_string()))?;
        if let Some(q) = &mut state.quantizer {
            q.observe(&vector.values);
            if state.config.quantization.as_ref().map(|c| c.scalar).unwrap_or(false) {
                let codes = q.encode(&vector.values);
                state.quantized.insert(id.clone(), codes);
            }
        }

        state
            .index
            .insert(id.clone(), vector)
            .map_err(EngineError::Core)?;
        state.payload_indexes.upsert(&id, &payload_value);
        if payload_value.is_null() {
            state.payloads.remove(&id);
        } else {
            if let Some(bm25) = &mut state.bm25_index {
                bm25.upsert(id.clone(), &payload_value);
            }
            state.payloads.insert(id.clone(), payload_value);
        }
        if let (Some(idx), Some(sv)) = (&mut state.sparse_index, sparse) {
            idx.upsert(id, sv);
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
        state.payload_indexes.remove(&id.to_string());
        state.quantized.remove(id);
        if let Some(idx) = &mut state.sparse_index {
            idx.remove(id);
        }
        if let Some(bm25) = &mut state.bm25_index {
            bm25.remove(id);
        }
        Ok(())
    }
}

fn parse_payload(bytes: &[u8]) -> Result<Value> {
    if let Ok(v) = serde_json::from_slice::<Value>(bytes) {
        return Ok(v);
    }
    // Fall back to interpreting the bytes as a UTF-8 string under `_raw`.
    let text = std::str::from_utf8(bytes)
        .map_err(|e| EngineError::InvalidPayload(format!("payload not utf-8: {e}")))?;
    let mut obj = serde_json::Map::new();
    obj.insert("_raw".into(), Value::String(text.to_string()));
    Ok(Value::Object(obj))
}

pub(crate) fn brute_force_topk(
    state: &CollectionState,
    query: &[f32],
    k: usize,
    ids: &std::collections::HashSet<PointId>,
) -> Vec<ScoredPoint> {
    let mut scored: Vec<ScoredPoint> = ids
        .iter()
        .filter_map(|id| {
            let vec = state.index.get_vector(id)?;
            let dist = vectordb_core::Distance::compare(state.config.metric, query, &vec.values);
            Some(ScoredPoint {
                id: id.clone(),
                score: vectordb_core::Distance::to_score(state.config.metric, dist),
                ..Default::default()
            })
        })
        .collect();
    scored.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    scored.truncate(k);
    scored
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectionStats {
    pub name: String,
    pub vector_count: usize,
    pub dimension: usize,
    pub metric: DistanceMetric,
    pub sparse_enabled: bool,
    pub bm25_text_field: String,
    pub payload_index_count: usize,
    pub scalar_quantization: bool,
}

fn matches_filter(state: &CollectionState, filter: Option<&Filter>, id: &str) -> bool {
    let Some(f) = filter.filter(|f| !f.is_empty()) else {
        return true;
    };
    state
        .payloads
        .get(id)
        .map(|p| f.matches(p))
        .unwrap_or(false)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalCompactionStats {
    pub before: usize,
    pub after: usize,
}

#[cfg(test)]
mod m4_tests {
    use super::*;
    use tempfile::tempdir;
    use vectordb_core::{CollectionConfig, DistanceMetric, Vector};

    use crate::wal::BulkPoint;

    fn test_config(name: &str) -> CollectionConfig {
        CollectionConfig::new(name, 4, DistanceMetric::Cosine)
    }

    #[test]
    fn bulk_upsert_and_wal_compact() {
        let dir = tempdir().unwrap();
        let engine = CollectionEngine::open(EngineConfig::new(dir.path())).unwrap();
        engine
            .create_collection(test_config("docs"))
            .unwrap();

        let points: Vec<BulkPoint> = (0..1200)
            .map(|i| BulkPoint {
                id: format!("p{i}"),
                vector: Vector::new(vec![i as f32, 0.0, 0.0, 0.0]),
                payload: None,
                sparse: None,
            })
            .collect();
        let n = engine.bulk_upsert("docs", points, 500).unwrap();
        assert_eq!(n, 1200);

        let stats = engine.compact_wal().unwrap();
        assert!(stats.before > stats.after);
        assert_eq!(engine.stats("docs").unwrap().vector_count, 1200);
    }

    #[test]
    fn scroll_paginates_all_points() {
        let dir = tempdir().unwrap();
        let engine = CollectionEngine::open(EngineConfig::new(dir.path())).unwrap();
        engine.create_collection(test_config("scroll-c")).unwrap();

        // Insert 100 points.
        for i in 0..100 {
            engine
                .upsert(
                    "scroll-c",
                    format!("p{i:03}"),
                    Vector::new(vec![i as f32, 0.0, 0.0, 0.0]),
                    Some(format!(r#"{{"i":{i}}}"#).into_bytes()),
                    None,
                )
                .unwrap();
        }

        // Walk pages of 33 to exercise the pagination boundary.
        let mut seen = std::collections::HashSet::new();
        let mut cursor = String::new();
        let mut iterations = 0;
        loop {
            iterations += 1;
            assert!(iterations < 20, "scroll did not terminate");
            let (rows, next) = engine.scroll("scroll-c", &cursor, 33).unwrap();
            for (id, _, payload) in &rows {
                assert!(payload.is_some(), "payload should round-trip");
                assert!(seen.insert(id.clone()), "duplicate id {id} returned");
            }
            if next.is_empty() {
                break;
            }
            cursor = next;
        }
        assert_eq!(seen.len(), 100);
    }

    #[test]
    fn reindex_preserves_search() {
        let dir = tempdir().unwrap();
        let engine = CollectionEngine::open(EngineConfig::new(dir.path())).unwrap();
        engine
            .create_collection(test_config("idx"))
            .unwrap();
        for i in 0..50 {
            engine
                .upsert(
                    "idx",
                    format!("v{i}"),
                    Vector::new(vec![i as f32, 0.0, 0.0, 0.0]),
                    None,
                    None,
                )
                .unwrap();
        }
        let before = engine
            .search("idx", &[25.0, 0.0, 0.0, 0.0], 1, None, OutputOptions::default())
            .unwrap();
        let n = engine.reindex_collection("idx").unwrap();
        assert_eq!(n, 50);
        let after = engine
            .search("idx", &[25.0, 0.0, 0.0, 0.0], 1, None, OutputOptions::default())
            .unwrap();
        assert_eq!(before[0].id, after[0].id);
    }
}
