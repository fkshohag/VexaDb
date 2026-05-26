use std::collections::HashMap;
use std::path::{Path, PathBuf};

use parking_lot::RwLock;
use rocksdb::{Options, DB};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use vectordb_core::{
    Bm25Index, CollectionConfig, DatabaseConfig, DistanceMetric, Error as CoreError, Filter,
    HnswConfig, HnswIndex, OutputOptions, PointId, ScalarQuantizer, ScoredPoint, SearchMode,
    SparseInvertedIndex, SparseVector, Vector, DEFAULT_DATABASE,
};
use vectordb_rbac::{RbacError, RbacOp, RbacSnapshot, RbacState};

use crate::payload_index::PayloadIndexes;
use crate::snapshot::{SnapshotManager, SnapshotMeta};
use crate::wal::{BulkPoint, MetaOp, WalEntry, WriteAheadLog};
use crate::wal_compact::export_state_to_wal;

const RBAC_SNAPSHOT_KEY: &[u8] = b"__rbac_snapshot__";

// ---------- name qualification helpers ------------------------------------
//
// Internally the engine identifies every collection / alias by a
// *fully-qualified name* `"<database>/<simple_name>"`. The public API still
// accepts a single string for back-compat: any input without a `/` is
// implicitly scoped to [`DEFAULT_DATABASE`]. This keeps callers that pre-date
// the multi-database refactor working unchanged while letting the gateway
// route per-database requests by simply forming `"db/name"` upstream.

/// Return a fully-qualified collection / alias name. Bare names default to
/// the implicit `default` database.
fn qname(s: &str) -> String {
    if s.contains('/') {
        s.to_string()
    } else {
        format!("{DEFAULT_DATABASE}/{s}")
    }
}

/// Split a fully-qualified name into `(database, simple_name)`. Bare names
/// fall through to the default database.
fn split_fq(fq: &str) -> (&str, &str) {
    match fq.find('/') {
        Some(i) => (&fq[..i], &fq[i + 1..]),
        None => (DEFAULT_DATABASE, fq),
    }
}

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
    #[error("alias exists: {0}")]
    AliasExists(String),
    #[error("alias not found: {0}")]
    AliasNotFound(String),
    #[error("invalid meta op: {0}")]
    InvalidMeta(String),
    #[error("database exists: {0}")]
    DatabaseExists(String),
    #[error("database not found: {0}")]
    DatabaseNotFound(String),
    #[error("database not empty: {0} (use force=true to cascade drop)")]
    DatabaseNotEmpty(String),
    #[error("partition not found: {0}")]
    PartitionNotFound(String),
    #[error("partition exists: {0}")]
    PartitionExists(String),
    #[error("resource group not found: {0}")]
    ResourceGroupNotFound(String),
    #[error("resource group exists: {0}")]
    ResourceGroupExists(String),
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
    /// alias -> collection. Both keys and values are fully-qualified
    /// (`db/name`). Persisted under `alias:<fq_alias>` keys in `meta_db` and
    /// replicated through `WalEntry::Meta` (Milvus-parity).
    aliases: RwLock<HashMap<String, String>>,
    /// Database registry: name -> config (properties, created_at_ms). The
    /// `default` database is always present (auto-seeded on first open).
    /// Persisted under `database:<name>` keys.
    databases: RwLock<HashMap<String, DatabaseConfig>>,
    /// In-memory registry of compaction jobs (id -> status). Cleared on
    /// restart; callers should never persist compaction IDs across runs.
    compactions: RwLock<HashMap<u64, CompactionStatus>>,
    /// Monotonic counter used to mint compaction IDs. Starts from
    /// `now_ms()` so IDs sort roughly chronologically and are unique
    /// across short engine restarts.
    compaction_seq: std::sync::atomic::AtomicU64,
    /// Resource group registry: name -> (config, created_at_ms). The
    /// built-in `__default_resource_group` is auto-seeded on first open.
    /// Persisted under `rg:<name>` keys.
    resource_groups: RwLock<HashMap<String, ResourceGroupEntry>>,
}

/// In-memory record for one resource group. Kept private; callers see
/// [`vectordb_core::ResourceGroupInfo`] instead.
#[derive(Debug, Clone)]
pub(crate) struct ResourceGroupEntry {
    pub config: vectordb_core::ResourceGroupConfig,
    pub created_at_ms: u64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct ResourceGroupPersisted {
    config: vectordb_core::ResourceGroupConfig,
    #[serde(default)]
    created_at_ms: u64,
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

        let seq_seed = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let engine = Self {
            config,
            collections: RwLock::new(HashMap::new()),
            meta_db: RwLock::new(meta_db),
            wal: RwLock::new(wal),
            rbac: RwLock::new(RbacState::new()),
            aliases: RwLock::new(HashMap::new()),
            databases: RwLock::new(HashMap::new()),
            compactions: RwLock::new(HashMap::new()),
            compaction_seq: std::sync::atomic::AtomicU64::new(seq_seed),
            resource_groups: RwLock::new(HashMap::new()),
        };

        engine.load_rbac_from_meta()?;
        engine.load_databases_from_meta()?;
        engine.load_resource_groups_from_meta()?;
        engine.replay_wal()?;
        engine.load_collections_from_meta()?;
        engine.load_aliases_from_meta()?;
        engine.ensure_default_database()?;
        engine.ensure_default_resource_group()?;
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

    // ---- Database registry (Milvus parity) -------------------------------

    fn load_databases_from_meta(&self) -> Result<()> {
        let meta = self.meta_db.read();
        let iter = meta.iterator(rocksdb::IteratorMode::Start);
        let mut dbs = self.databases.write();
        for item in iter {
            let (key, value) = item.map_err(|e| EngineError::Rocks(e.to_string()))?;
            let key_str = String::from_utf8_lossy(&key);
            if let Some(name) = key_str.strip_prefix("database:") {
                let cfg: DatabaseConfig = serde_json::from_slice(&value)
                    .map_err(|e| EngineError::Rocks(format!("decode db {name}: {e}")))?;
                dbs.insert(cfg.name.clone(), cfg);
            }
        }
        Ok(())
    }

    fn ensure_default_database(&self) -> Result<()> {
        if self.databases.read().contains_key(DEFAULT_DATABASE) {
            return Ok(());
        }
        let cfg = DatabaseConfig::new(DEFAULT_DATABASE);
        self.persist_database(&cfg)?;
        self.databases.write().insert(cfg.name.clone(), cfg);
        Ok(())
    }

    fn persist_database(&self, cfg: &DatabaseConfig) -> Result<()> {
        let key = format!("database:{}", cfg.name);
        let bytes = serde_json::to_vec(cfg).map_err(|e| EngineError::Rocks(e.to_string()))?;
        self.meta_db
            .read()
            .put(key, bytes)
            .map_err(|e| EngineError::Rocks(e.to_string()))
    }

    fn delete_database_meta(&self, name: &str) -> Result<()> {
        let key = format!("database:{name}");
        self.meta_db
            .read()
            .delete(key)
            .map_err(|e| EngineError::Rocks(e.to_string()))
    }

    /// Names of every database (sorted for stable output).
    pub fn list_databases(&self) -> Vec<String> {
        let mut names: Vec<String> = self.databases.read().keys().cloned().collect();
        names.sort();
        names
    }

    /// Full config for a database (including properties).
    pub fn describe_database(&self, name: &str) -> Result<DatabaseConfig> {
        self.databases
            .read()
            .get(name)
            .cloned()
            .ok_or_else(|| EngineError::DatabaseNotFound(name.to_string()))
    }

    /// True iff `db` currently exists.
    pub fn database_exists(&self, db: &str) -> bool {
        self.databases.read().contains_key(db)
    }

    // ---- Resource group registry (Milvus parity) -------------------------

    fn load_resource_groups_from_meta(&self) -> Result<()> {
        let meta = self.meta_db.read();
        let iter = meta.iterator(rocksdb::IteratorMode::Start);
        let mut rgs = self.resource_groups.write();
        for item in iter {
            let (key, value) = item.map_err(|e| EngineError::Rocks(e.to_string()))?;
            let key_str = String::from_utf8_lossy(&key);
            if let Some(name) = key_str.strip_prefix("rg:") {
                let entry: ResourceGroupPersisted = serde_json::from_slice(&value)
                    .map_err(|e| EngineError::Rocks(format!("decode rg {name}: {e}")))?;
                rgs.insert(
                    name.to_string(),
                    ResourceGroupEntry {
                        config: entry.config,
                        created_at_ms: entry.created_at_ms,
                    },
                );
            }
        }
        Ok(())
    }

    fn ensure_default_resource_group(&self) -> Result<()> {
        use vectordb_core::DEFAULT_RESOURCE_GROUP;
        if self
            .resource_groups
            .read()
            .contains_key(DEFAULT_RESOURCE_GROUP)
        {
            return Ok(());
        }
        let entry = ResourceGroupEntry {
            config: vectordb_core::ResourceGroupConfig::default(),
            created_at_ms: 0,
        };
        self.persist_resource_group(DEFAULT_RESOURCE_GROUP, &entry)?;
        self.resource_groups
            .write()
            .insert(DEFAULT_RESOURCE_GROUP.to_string(), entry);
        Ok(())
    }

    fn persist_resource_group(&self, name: &str, entry: &ResourceGroupEntry) -> Result<()> {
        let persisted = ResourceGroupPersisted {
            config: entry.config.clone(),
            created_at_ms: entry.created_at_ms,
        };
        let key = format!("rg:{name}");
        let bytes =
            serde_json::to_vec(&persisted).map_err(|e| EngineError::Rocks(e.to_string()))?;
        self.meta_db
            .read()
            .put(key, bytes)
            .map_err(|e| EngineError::Rocks(e.to_string()))
    }

    fn delete_resource_group_meta(&self, name: &str) -> Result<()> {
        let key = format!("rg:{name}");
        self.meta_db
            .read()
            .delete(key)
            .map_err(|e| EngineError::Rocks(e.to_string()))
    }

    /// Names of every resource group (sorted for stable output). Always
    /// contains [`vectordb_core::DEFAULT_RESOURCE_GROUP`].
    pub fn list_resource_groups(&self) -> Vec<String> {
        let mut names: Vec<String> = self.resource_groups.read().keys().cloned().collect();
        names.sort();
        names
    }

    /// Full info for a resource group. Returns `ResourceGroupNotFound` if
    /// the group has never been created.
    pub fn describe_resource_group(
        &self,
        name: &str,
    ) -> Result<vectordb_core::ResourceGroupInfo> {
        let rgs = self.resource_groups.read();
        let entry = rgs
            .get(name)
            .ok_or_else(|| EngineError::ResourceGroupNotFound(name.to_string()))?;
        Ok(vectordb_core::ResourceGroupInfo {
            name: name.to_string(),
            config: entry.config.clone(),
            num_available_node: 0,
            num_loaded_replica: Default::default(),
            num_incoming_node: Default::default(),
            num_outgoing_node: Default::default(),
            created_at_ms: entry.created_at_ms,
        })
    }

    fn apply_create_resource_group(
        &self,
        name: &str,
        config: vectordb_core::ResourceGroupConfig,
        created_at_ms: u64,
    ) -> Result<()> {
        let entry = ResourceGroupEntry {
            config,
            created_at_ms,
        };
        self.persist_resource_group(name, &entry)?;
        self.resource_groups
            .write()
            .insert(name.to_string(), entry);
        Ok(())
    }

    fn apply_drop_resource_group(&self, name: &str) -> Result<()> {
        self.delete_resource_group_meta(name)?;
        self.resource_groups.write().remove(name);
        Ok(())
    }

    fn apply_update_resource_group(
        &self,
        name: &str,
        config: vectordb_core::ResourceGroupConfig,
    ) -> Result<()> {
        let mut rgs = self.resource_groups.write();
        let entry = rgs
            .get_mut(name)
            .ok_or_else(|| EngineError::ResourceGroupNotFound(name.to_string()))?;
        entry.config = config;
        let snapshot = entry.clone();
        drop(rgs);
        self.persist_resource_group(name, &snapshot)?;
        Ok(())
    }

    // ---- Aliases / rename / properties (Milvus parity) --------------------

    fn load_aliases_from_meta(&self) -> Result<()> {
        let meta = self.meta_db.read();
        let iter = meta.iterator(rocksdb::IteratorMode::Start);
        let mut aliases = self.aliases.write();
        for item in iter {
            let (key, value) = item.map_err(|e| EngineError::Rocks(e.to_string()))?;
            let key_str = String::from_utf8_lossy(&key);
            if !key_str.starts_with("alias:") {
                continue;
            }
            // Both legacy `alias:<name>` (default-db) and new `alias:<db>/<name>`
            // keys are accepted; the value is stored as a fully-qualified
            // collection name to keep cross-database aliases unambiguous.
            let fq_alias = qname(key_str.trim_start_matches("alias:"));
            let raw_target = String::from_utf8_lossy(&value).to_string();
            let target = qname(&raw_target);
            aliases.insert(fq_alias, target);
        }
        Ok(())
    }

    fn persist_alias(&self, fq_alias: &str, fq_collection: &str) -> Result<()> {
        let key = format!("alias:{fq_alias}");
        self.meta_db
            .read()
            .put(key, fq_collection.as_bytes())
            .map_err(|e| EngineError::Rocks(e.to_string()))
    }

    fn delete_alias_meta(&self, fq_alias: &str) -> Result<()> {
        let key = format!("alias:{fq_alias}");
        self.meta_db
            .read()
            .delete(key)
            .map_err(|e| EngineError::Rocks(e.to_string()))
    }

    /// Apply a metadata op locally (WAL + state). Used by Raft followers
    /// on commit. Leaders should propose `WalEntry::Meta` through Raft.
    pub fn commit_meta(&self, op: MetaOp) -> Result<()> {
        // Validate before WAL append to keep WAL clean.
        self.validate_meta(&op)?;
        let entry = WalEntry::Meta { op };
        self.commit_entry(&entry)
    }

    fn validate_meta(&self, op: &MetaOp) -> Result<()> {
        let collections = self.collections.read();
        let aliases = self.aliases.read();
        let databases = self.databases.read();
        match op {
            MetaOp::RenameCollection { old, new, database } => {
                check_simple_name(new)?;
                let fq_old = format!("{database}/{old}");
                let fq_new = format!("{database}/{new}");
                if !collections.contains_key(&fq_old) {
                    return Err(EngineError::CollectionNotFound(old.clone()));
                }
                if old == new {
                    return Ok(());
                }
                if collections.contains_key(&fq_new) {
                    return Err(EngineError::CollectionExists(new.clone()));
                }
                if aliases.contains_key(&fq_new) {
                    return Err(EngineError::InvalidMeta(format!(
                        "new name {new} collides with alias in db {database}"
                    )));
                }
            }
            MetaOp::CreateAlias {
                alias,
                collection,
                database,
            } => {
                check_simple_name(alias)?;
                let fq_alias = format!("{database}/{alias}");
                let fq_target = format!("{database}/{collection}");
                if !collections.contains_key(&fq_target) {
                    return Err(EngineError::CollectionNotFound(collection.clone()));
                }
                if aliases.contains_key(&fq_alias) {
                    return Err(EngineError::AliasExists(alias.clone()));
                }
                if collections.contains_key(&fq_alias) {
                    return Err(EngineError::InvalidMeta(format!(
                        "alias {alias} collides with existing collection in db {database}"
                    )));
                }
            }
            MetaOp::AlterAlias {
                alias,
                collection,
                database,
            } => {
                let fq_alias = format!("{database}/{alias}");
                let fq_target = format!("{database}/{collection}");
                if !collections.contains_key(&fq_target) {
                    return Err(EngineError::CollectionNotFound(collection.clone()));
                }
                if !aliases.contains_key(&fq_alias) {
                    return Err(EngineError::AliasNotFound(alias.clone()));
                }
            }
            MetaOp::DropAlias { .. } => { /* idempotent */ }
            MetaOp::AlterCollectionProperties { name, database, .. } => {
                let fq = format!("{database}/{name}");
                if !collections.contains_key(&fq) {
                    return Err(EngineError::CollectionNotFound(name.clone()));
                }
            }
            MetaOp::CreateDatabase { name, .. } => {
                check_simple_name(name)?;
                if databases.contains_key(name) {
                    return Err(EngineError::DatabaseExists(name.clone()));
                }
            }
            MetaOp::DropDatabase { name, force } => {
                if name == DEFAULT_DATABASE {
                    return Err(EngineError::InvalidMeta(
                        "the built-in `default` database cannot be dropped".into(),
                    ));
                }
                if !databases.contains_key(name) {
                    return Err(EngineError::DatabaseNotFound(name.clone()));
                }
                if !force {
                    let prefix = format!("{name}/");
                    let has_collection = collections.keys().any(|k| k.starts_with(&prefix));
                    let has_alias = aliases.keys().any(|k| k.starts_with(&prefix));
                    if has_collection || has_alias {
                        return Err(EngineError::DatabaseNotEmpty(name.clone()));
                    }
                }
            }
            MetaOp::AlterDatabaseProperties { name, .. } => {
                if !databases.contains_key(name) {
                    return Err(EngineError::DatabaseNotFound(name.clone()));
                }
            }
            MetaOp::AddPayloadIndex {
                collection,
                database,
                field,
                kind,
            } => {
                let fq = format!("{database}/{collection}");
                if !collections.contains_key(&fq) {
                    return Err(EngineError::CollectionNotFound(collection.clone()));
                }
                if field.is_empty() {
                    return Err(EngineError::InvalidMeta(
                        "payload index `field` cannot be empty".into(),
                    ));
                }
                if !matches!(kind.as_str(), "keyword" | "numeric" | "bool") {
                    return Err(EngineError::InvalidMeta(format!(
                        "unknown payload index kind `{kind}` (use keyword/numeric/bool)"
                    )));
                }
            }
            MetaOp::DropPayloadIndex {
                collection,
                database,
                ..
            } => {
                let fq = format!("{database}/{collection}");
                if !collections.contains_key(&fq) {
                    return Err(EngineError::CollectionNotFound(collection.clone()));
                }
            }
            MetaOp::CreatePartition {
                collection,
                database,
                partition,
            } => {
                check_simple_name(partition)?;
                let fq = format!("{database}/{collection}");
                let state = collections
                    .get(&fq)
                    .ok_or_else(|| EngineError::CollectionNotFound(collection.clone()))?;
                if state.config.partitions.iter().any(|p| p == partition) {
                    return Err(EngineError::PartitionExists(partition.clone()));
                }
            }
            MetaOp::DropPartition {
                collection,
                database,
                partition,
            } => {
                if partition == vectordb_core::DEFAULT_PARTITION {
                    return Err(EngineError::InvalidMeta(format!(
                        "the built-in `{}` partition cannot be dropped",
                        vectordb_core::DEFAULT_PARTITION
                    )));
                }
                let fq = format!("{database}/{collection}");
                let state = collections
                    .get(&fq)
                    .ok_or_else(|| EngineError::CollectionNotFound(collection.clone()))?;
                if !state.config.partitions.iter().any(|p| p == partition) {
                    return Err(EngineError::PartitionNotFound(partition.clone()));
                }
            }
            MetaOp::CreateResourceGroup { name, .. } => {
                check_simple_name(name)?;
                if self.resource_groups.read().contains_key(name) {
                    return Err(EngineError::ResourceGroupExists(name.clone()));
                }
            }
            MetaOp::DropResourceGroup { name } => {
                if name == vectordb_core::DEFAULT_RESOURCE_GROUP {
                    return Err(EngineError::InvalidMeta(format!(
                        "the built-in `{}` resource group cannot be dropped",
                        vectordb_core::DEFAULT_RESOURCE_GROUP
                    )));
                }
                if !self.resource_groups.read().contains_key(name) {
                    return Err(EngineError::ResourceGroupNotFound(name.clone()));
                }
            }
            MetaOp::UpdateResourceGroup { name, .. } => {
                if !self.resource_groups.read().contains_key(name) {
                    return Err(EngineError::ResourceGroupNotFound(name.clone()));
                }
            }
        }
        Ok(())
    }

    fn apply_meta(&self, op: &MetaOp) -> Result<()> {
        match op {
            MetaOp::RenameCollection {
                old,
                new,
                database,
            } => self.apply_rename(database, old, new),
            MetaOp::CreateAlias {
                alias,
                collection,
                database,
            }
            | MetaOp::AlterAlias {
                alias,
                collection,
                database,
            } => {
                let fq_alias = format!("{database}/{alias}");
                let fq_target = format!("{database}/{collection}");
                self.persist_alias(&fq_alias, &fq_target)?;
                self.aliases.write().insert(fq_alias, fq_target);
                Ok(())
            }
            MetaOp::DropAlias { alias, database } => {
                let fq_alias = format!("{database}/{alias}");
                self.delete_alias_meta(&fq_alias)?;
                self.aliases.write().remove(&fq_alias);
                Ok(())
            }
            MetaOp::AlterCollectionProperties {
                name,
                database,
                set,
                unset,
            } => {
                let fq = format!("{database}/{name}");
                self.apply_alter_properties(&fq, set, unset)
            }
            MetaOp::CreateDatabase {
                name,
                properties,
                created_at_ms,
            } => self.apply_create_database(name, properties.clone(), *created_at_ms),
            MetaOp::DropDatabase { name, force } => self.apply_drop_database(name, *force),
            MetaOp::AlterDatabaseProperties { name, set, unset } => {
                self.apply_alter_database_properties(name, set, unset)
            }
            MetaOp::AddPayloadIndex {
                collection,
                database,
                field,
                kind,
            } => {
                let fq = format!("{database}/{collection}");
                self.apply_add_payload_index(&fq, field, kind)
            }
            MetaOp::DropPayloadIndex {
                collection,
                database,
                field,
            } => {
                let fq = format!("{database}/{collection}");
                self.apply_drop_payload_index(&fq, field)
            }
            MetaOp::CreatePartition {
                collection,
                database,
                partition,
            } => {
                let fq = format!("{database}/{collection}");
                self.apply_create_partition(&fq, partition)
            }
            MetaOp::DropPartition {
                collection,
                database,
                partition,
            } => {
                let fq = format!("{database}/{collection}");
                self.apply_drop_partition(&fq, partition)
            }
            MetaOp::CreateResourceGroup {
                name,
                config,
                created_at_ms,
            } => self.apply_create_resource_group(name, config.clone(), *created_at_ms),
            MetaOp::DropResourceGroup { name } => self.apply_drop_resource_group(name),
            MetaOp::UpdateResourceGroup { name, config } => {
                self.apply_update_resource_group(name, config.clone())
            }
        }
    }

    fn apply_create_database(
        &self,
        name: &str,
        properties: std::collections::BTreeMap<String, String>,
        created_at_ms: u64,
    ) -> Result<()> {
        let cfg = DatabaseConfig {
            name: name.to_string(),
            properties,
            created_at_ms,
        };
        self.persist_database(&cfg)?;
        self.databases.write().insert(name.to_string(), cfg);
        Ok(())
    }

    fn apply_drop_database(&self, name: &str, force: bool) -> Result<()> {
        // Cascade-drop child collections and aliases when force=true.
        // Build the list first to avoid holding read guards across the
        // mutating apply_* calls.
        if force {
            let prefix = format!("{name}/");
            let victim_collections: Vec<String> = self
                .collections
                .read()
                .keys()
                .filter(|k| k.starts_with(&prefix))
                .cloned()
                .collect();
            for fq in &victim_collections {
                self.apply_delete_collection(fq)?;
            }
            let victim_aliases: Vec<String> = self
                .aliases
                .read()
                .keys()
                .filter(|k| k.starts_with(&prefix))
                .cloned()
                .collect();
            for fq_alias in &victim_aliases {
                self.delete_alias_meta(fq_alias)?;
                self.aliases.write().remove(fq_alias);
            }
        }
        self.delete_database_meta(name)?;
        self.databases.write().remove(name);
        Ok(())
    }

    fn apply_alter_database_properties(
        &self,
        name: &str,
        set: &std::collections::BTreeMap<String, String>,
        unset: &[String],
    ) -> Result<()> {
        let mut dbs = self.databases.write();
        let cfg = dbs
            .get_mut(name)
            .ok_or_else(|| EngineError::DatabaseNotFound(name.to_string()))?;
        for (k, v) in set {
            cfg.properties.insert(k.clone(), v.clone());
        }
        for k in unset {
            cfg.properties.remove(k);
        }
        let clone = cfg.clone();
        drop(dbs);
        self.persist_database(&clone)
    }

    fn apply_rename(&self, database: &str, old: &str, new: &str) -> Result<()> {
        if old == new {
            return Ok(());
        }
        let fq_old = format!("{database}/{old}");
        let fq_new = format!("{database}/{new}");
        let mut collections = self.collections.write();
        let mut state = collections
            .remove(&fq_old)
            .ok_or_else(|| EngineError::CollectionNotFound(old.to_string()))?;
        state.config.name = new.to_string();
        state.config.database = database.to_string();
        let json =
            serde_json::to_vec(&state.config).map_err(|e| EngineError::Rocks(e.to_string()))?;
        let meta = self.meta_db.read();
        meta.delete(format!("collection:{fq_old}"))
            .map_err(|e| EngineError::Rocks(e.to_string()))?;
        meta.put(format!("collection:{fq_new}"), json)
            .map_err(|e| EngineError::Rocks(e.to_string()))?;
        drop(meta);
        // Rewrite any aliases that pointed to `fq_old` (always within the
        // same database since aliases never cross databases).
        let mut aliases = self.aliases.write();
        for v in aliases.values_mut() {
            if v == &fq_old {
                *v = fq_new.clone();
            }
        }
        for (alias, target) in aliases.iter() {
            if target == &fq_new {
                self.persist_alias(alias, &fq_new)?;
            }
        }
        collections.insert(fq_new, state);
        Ok(())
    }

    fn apply_alter_properties(
        &self,
        fq: &str,
        set: &std::collections::BTreeMap<String, String>,
        unset: &[String],
    ) -> Result<()> {
        let mut collections = self.collections.write();
        let state = collections
            .get_mut(fq)
            .ok_or_else(|| EngineError::CollectionNotFound(fq.to_string()))?;
        for (k, v) in set {
            state.config.properties.insert(k.clone(), v.clone());
        }
        for k in unset {
            state.config.properties.remove(k);
        }
        let json =
            serde_json::to_vec(&state.config).map_err(|e| EngineError::Rocks(e.to_string()))?;
        self.meta_db
            .read()
            .put(format!("collection:{fq}"), json)
            .map_err(|e| EngineError::Rocks(e.to_string()))?;
        Ok(())
    }

    fn apply_add_payload_index(&self, fq: &str, field: &str, kind: &str) -> Result<()> {
        use vectordb_core::PayloadIndexKind;
        let parsed_kind = match kind {
            "keyword" => PayloadIndexKind::Keyword,
            "numeric" => PayloadIndexKind::Numeric,
            "bool" => PayloadIndexKind::Bool,
            other => {
                return Err(EngineError::InvalidMeta(format!(
                    "unknown payload index kind `{other}`"
                )))
            }
        };
        let mut collections = self.collections.write();
        let state = collections
            .get_mut(fq)
            .ok_or_else(|| EngineError::CollectionNotFound(fq.to_string()))?;

        // Replace any existing entry for the same field — payload indexes
        // are uniqued per-field so callers can flip the kind via a single op.
        state.config.payload_indexes.retain(|p| p.field != field);
        state
            .config
            .payload_indexes
            .push(vectordb_core::PayloadFieldIndex {
                field: field.to_string(),
                kind: parsed_kind,
            });

        // Rebuild in-memory indexes from the new config and back-fill with
        // the payloads we already have. Cheap relative to a full reindex.
        state.payload_indexes = crate::payload_index::PayloadIndexes::new(&state.config.payload_indexes);
        let payload_pairs: Vec<(String, serde_json::Value)> = state
            .payloads
            .iter()
            .map(|(id, v)| (id.clone(), v.clone()))
            .collect();
        for (id, payload) in payload_pairs {
            state.payload_indexes.upsert(&id, &payload);
        }
        let json =
            serde_json::to_vec(&state.config).map_err(|e| EngineError::Rocks(e.to_string()))?;
        self.meta_db
            .read()
            .put(format!("collection:{fq}"), json)
            .map_err(|e| EngineError::Rocks(e.to_string()))?;
        Ok(())
    }

    fn apply_drop_payload_index(&self, fq: &str, field: &str) -> Result<()> {
        let mut collections = self.collections.write();
        let state = collections
            .get_mut(fq)
            .ok_or_else(|| EngineError::CollectionNotFound(fq.to_string()))?;
        let before = state.config.payload_indexes.len();
        state.config.payload_indexes.retain(|p| p.field != field);
        if state.config.payload_indexes.len() == before {
            // Field wasn't indexed — idempotent no-op, preserve in-memory state.
            return Ok(());
        }
        state.payload_indexes = crate::payload_index::PayloadIndexes::new(&state.config.payload_indexes);
        let payload_pairs: Vec<(String, serde_json::Value)> = state
            .payloads
            .iter()
            .map(|(id, v)| (id.clone(), v.clone()))
            .collect();
        for (id, payload) in payload_pairs {
            state.payload_indexes.upsert(&id, &payload);
        }
        let json =
            serde_json::to_vec(&state.config).map_err(|e| EngineError::Rocks(e.to_string()))?;
        self.meta_db
            .read()
            .put(format!("collection:{fq}"), json)
            .map_err(|e| EngineError::Rocks(e.to_string()))?;
        Ok(())
    }

    fn apply_create_partition(&self, fq: &str, partition: &str) -> Result<()> {
        let mut collections = self.collections.write();
        let state = collections
            .get_mut(fq)
            .ok_or_else(|| EngineError::CollectionNotFound(fq.to_string()))?;
        if state.config.partitions.iter().any(|p| p == partition) {
            return Err(EngineError::PartitionExists(partition.to_string()));
        }
        state.config.partitions.push(partition.to_string());
        let json =
            serde_json::to_vec(&state.config).map_err(|e| EngineError::Rocks(e.to_string()))?;
        self.meta_db
            .read()
            .put(format!("collection:{fq}"), json)
            .map_err(|e| EngineError::Rocks(e.to_string()))?;
        Ok(())
    }

    fn apply_drop_partition(&self, fq: &str, partition: &str) -> Result<()> {
        if partition == vectordb_core::DEFAULT_PARTITION {
            return Err(EngineError::InvalidMeta(format!(
                "the built-in `{}` partition cannot be dropped",
                vectordb_core::DEFAULT_PARTITION
            )));
        }
        let mut collections = self.collections.write();
        let state = collections
            .get_mut(fq)
            .ok_or_else(|| EngineError::CollectionNotFound(fq.to_string()))?;
        let before = state.config.partitions.len();
        state.config.partitions.retain(|p| p != partition);
        if state.config.partitions.len() == before {
            // Caller treats DropPartition as authoritative (no idempotent
            // silent success). Mirrors Milvus's error-on-missing semantics.
            return Err(EngineError::PartitionNotFound(partition.to_string()));
        }
        // Mass-delete every point whose `_partition` payload tags into this
        // partition. We collect IDs first to avoid mutating while iterating.
        let field = vectordb_core::PARTITION_PAYLOAD_FIELD;
        let victims: Vec<String> = state
            .payloads
            .iter()
            .filter_map(|(id, payload)| match payload.get(field) {
                Some(Value::String(s)) if s == partition => Some(id.clone()),
                _ => None,
            })
            .collect();
        for id in &victims {
            // Best-effort removal — if a backing index is missing the point
            // we keep going so config is still updated.
            let _ = state.index.remove(id);
            state.payloads.remove(id);
            state.payload_indexes.remove(id);
            state.quantized.remove(id);
            if let Some(idx) = &mut state.sparse_index {
                idx.remove(id);
            }
            if let Some(bm25) = &mut state.bm25_index {
                bm25.remove(id);
            }
        }
        let json =
            serde_json::to_vec(&state.config).map_err(|e| EngineError::Rocks(e.to_string()))?;
        self.meta_db
            .read()
            .put(format!("collection:{fq}"), json)
            .map_err(|e| EngineError::Rocks(e.to_string()))?;
        Ok(())
    }

    /// List partition names for a collection. Always non-empty (the
    /// default partition is auto-created on collection creation).
    pub fn list_partitions(&self, name: &str) -> Result<Vec<String>> {
        let fq = self.resolve_alias(name);
        let collections = self.collections.read();
        let state = collections
            .get(&fq)
            .ok_or_else(|| EngineError::CollectionNotFound(fq.clone()))?;
        Ok(state.config.partitions.clone())
    }

    /// Returns true iff `partition` exists in `collection`.
    pub fn has_partition(&self, collection: &str, partition: &str) -> Result<bool> {
        let fq = self.resolve_alias(collection);
        let collections = self.collections.read();
        let state = collections
            .get(&fq)
            .ok_or_else(|| EngineError::CollectionNotFound(fq.clone()))?;
        Ok(state.config.partitions.iter().any(|p| p == partition))
    }

    /// Stats for a single partition: row count + collection metadata.
    /// Returns `PartitionNotFound` if the partition is not declared.
    pub fn partition_stats(
        &self,
        collection: &str,
        partition: &str,
    ) -> Result<std::collections::BTreeMap<String, String>> {
        let fq = self.resolve_alias(collection);
        let collections = self.collections.read();
        let state = collections
            .get(&fq)
            .ok_or_else(|| EngineError::CollectionNotFound(fq.clone()))?;
        if !state.config.partitions.iter().any(|p| p == partition) {
            return Err(EngineError::PartitionNotFound(partition.to_string()));
        }
        let field = vectordb_core::PARTITION_PAYLOAD_FIELD;
        // Count points carrying the partition tag. The default partition
        // also picks up legacy / pre-partition points whose payload has no
        // `_partition` field — they're treated as belonging to `_default`.
        let row_count = state
            .payloads
            .iter()
            .filter(|(_, payload)| match payload.get(field) {
                Some(Value::String(s)) => s == partition,
                _ => partition == vectordb_core::DEFAULT_PARTITION,
            })
            .count();
        let mut out = std::collections::BTreeMap::new();
        out.insert("row_count".to_string(), row_count.to_string());
        out.insert("partition_name".to_string(), partition.to_string());
        out.insert("collection".to_string(), fq);
        Ok(out)
    }

    /// Resolve an alias to its target collection name. Input/output are FQN
    /// (`db/name`); bare names are auto-qualified to the default database.
    /// If the input is neither an alias nor a known collection it is returned
    /// untouched (qualified) so callers see a consistent FQN form.
    pub fn resolve_alias(&self, name: &str) -> String {
        let fq = qname(name);
        if self.collections.read().contains_key(&fq) {
            return fq;
        }
        if let Some(target) = self.aliases.read().get(&fq).cloned() {
            return target;
        }
        fq
    }

    /// Every alias in the cluster as `(fq_alias, fq_collection)` pairs.
    pub fn list_aliases(&self) -> Vec<(String, String)> {
        self.aliases
            .read()
            .iter()
            .map(|(a, c)| (a.clone(), c.clone()))
            .collect()
    }

    /// Aliases (FQ form) pointing to `collection` (FQ form; bare names are
    /// auto-qualified to the default database).
    pub fn aliases_for(&self, collection: &str) -> Vec<String> {
        let fq = qname(collection);
        self.aliases
            .read()
            .iter()
            .filter(|(_, target)| target.as_str() == fq.as_str())
            .map(|(a, _)| a.clone())
            .collect()
    }

    /// Resolve a single alias (FQ or bare) to its collection (FQ).
    pub fn describe_alias(&self, alias: &str) -> Result<String> {
        let fq = qname(alias);
        self.aliases
            .read()
            .get(&fq)
            .cloned()
            .ok_or_else(|| EngineError::AliasNotFound(alias.to_string()))
    }

    /// Returns the properties map for `name` (after alias resolution).
    /// Accepts FQ or bare names.
    pub fn properties(&self, name: &str) -> Result<std::collections::BTreeMap<String, String>> {
        let resolved = self.resolve_alias(name);
        let collections = self.collections.read();
        collections
            .get(&resolved)
            .map(|s| s.config.properties.clone())
            .ok_or_else(|| EngineError::CollectionNotFound(name.to_string()))
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
        self.aliases.write().clear();
        self.databases.write().clear();
        self.load_databases_from_meta()?;
        self.replay_wal()?;
        self.load_collections_from_meta()?;
        self.load_aliases_from_meta()?;
        self.ensure_default_database()?;
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
            // Either FQ (`collection:db/name`) or legacy bare (`collection:name`).
            // `qname()` normalizes the latter into `default/name`.
            let fq = qname(key_str.trim_start_matches("collection:"));
            let mut config: CollectionConfig =
                serde_json::from_slice(&value).map_err(|e| EngineError::Rocks(e.to_string()))?;
            // Make sure the in-memory config reflects the FQN we just derived
            // even when the persisted JSON predates the `database` field.
            let (db_seg, name_seg) = split_fq(&fq);
            if config.database.is_empty() {
                config.database = db_seg.to_string();
            }
            if config.name.is_empty() {
                config.name = name_seg.to_string();
            }
            self.get_or_create_state(&fq, config);
        }
        Ok(())
    }

    pub fn create_collection(&self, mut config: CollectionConfig) -> Result<()> {
        if config.database.is_empty() {
            config.database = DEFAULT_DATABASE.to_string();
        }
        check_simple_name(&config.name)?;
        if !self.database_exists(&config.database) {
            return Err(EngineError::DatabaseNotFound(config.database.clone()));
        }
        let fq = format!("{}/{}", config.database, config.name);
        let key = format!("collection:{fq}");
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
        let fq = qname(name);
        let entry = WalEntry::DeleteCollection { name: fq.clone() };
        self.wal.write().append(&entry)?;
        self.apply_delete_collection(&fq)
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
            WalEntry::DeleteCollection { name } => self.apply_delete_collection(&qname(name)),
            WalEntry::Upsert {
                collection,
                id,
                vector,
                payload,
                sparse,
            } => {
                let fq = qname(collection);
                self.ensure_collection_loaded(&fq)?;
                self.apply_upsert(
                    &fq,
                    id.clone(),
                    vector.clone(),
                    payload.clone(),
                    sparse.clone(),
                )
            }
            WalEntry::Delete { collection, id } => {
                let fq = qname(collection);
                self.ensure_collection_loaded(&fq)?;
                self.apply_delete(&fq, id)
            }
            WalEntry::BulkUpsert { collection, points } => {
                let fq = qname(collection);
                self.ensure_collection_loaded(&fq)?;
                for p in points {
                    self.apply_upsert(
                        &fq,
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
            WalEntry::Meta { op } => self.apply_meta(op),
        }
    }

    /// Idempotent apply (used by WAL replay and Raft followers): persist config
    /// to RocksDB and ensure the in-memory state exists.
    fn apply_create_collection(&self, mut config: CollectionConfig) -> Result<()> {
        if config.database.is_empty() {
            config.database = DEFAULT_DATABASE.to_string();
        }
        config.validate().map_err(EngineError::Core)?;
        let fq = format!("{}/{}", config.database, config.name);
        let key = format!("collection:{fq}");
        let json = serde_json::to_vec(&config).map_err(|e| EngineError::Rocks(e.to_string()))?;
        self.meta_db
            .read()
            .put(key, json)
            .map_err(|e| EngineError::Rocks(e.to_string()))?;
        self.get_or_create_state(&fq, config);
        Ok(())
    }

    /// Delete by FQ name (`db/name`). Bare callers should pre-qualify.
    fn apply_delete_collection(&self, fq: &str) -> Result<()> {
        let key = format!("collection:{fq}");
        self.meta_db
            .read()
            .delete(key)
            .map_err(|e| EngineError::Rocks(e.to_string()))?;
        self.collections.write().remove(fq);
        Ok(())
    }

    /// Every collection in the cluster, as fully-qualified names (`db/name`).
    pub fn list_collections(&self) -> Vec<String> {
        self.collections.read().keys().cloned().collect()
    }

    /// Simple collection names that belong to `database`.
    pub fn list_collections_in_database(&self, database: &str) -> Vec<String> {
        let prefix = format!("{database}/");
        let mut names: Vec<String> = self
            .collections
            .read()
            .keys()
            .filter_map(|k| k.strip_prefix(&prefix).map(|s| s.to_string()))
            .collect();
        names.sort();
        names
    }

    pub fn describe_collection(&self, name: &str) -> Result<CollectionConfig> {
        let fq = self.resolve_alias(name);
        let collections = self.collections.read();
        collections
            .get(&fq)
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
        self.upsert_in_partition(
            collection,
            vectordb_core::DEFAULT_PARTITION,
            id,
            vector,
            payload,
            sparse,
        )
    }

    /// Partition-aware upsert. `partition` is validated against
    /// `CollectionConfig::partitions`; when it's the default partition the
    /// payload is left untouched (back-compat for pre-partition writers),
    /// otherwise the engine injects a `_partition` tag into the payload so
    /// the membership is queryable via the existing filter DSL and
    /// `DropPartition` can find the points later.
    pub fn upsert_in_partition(
        &self,
        collection: &str,
        partition: &str,
        id: String,
        vector: Vector,
        payload: Option<Vec<u8>>,
        sparse: Option<SparseVector>,
    ) -> Result<()> {
        let fq = self.resolve_alias(collection);
        self.ensure_collection_loaded(&fq)?;
        let partition = if partition.is_empty() {
            vectordb_core::DEFAULT_PARTITION
        } else {
            partition
        };
        {
            let cols = self.collections.read();
            let state = cols
                .get(&fq)
                .ok_or_else(|| EngineError::CollectionNotFound(fq.clone()))?;
            if !state.config.partitions.iter().any(|p| p == partition) {
                return Err(EngineError::PartitionNotFound(partition.to_string()));
            }
        }
        let payload = if partition == vectordb_core::DEFAULT_PARTITION {
            payload
        } else {
            Some(inject_partition_tag(payload, partition)?)
        };
        let entry = WalEntry::Upsert {
            collection: fq,
            id,
            vector,
            payload,
            sparse,
        };
        self.commit_entry(&entry)
    }

    pub fn delete(&self, collection: &str, id: &str) -> Result<()> {
        let fq = self.resolve_alias(collection);
        self.ensure_collection_loaded(&fq)?;
        let entry = WalEntry::Delete {
            collection: fq,
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
        self.bulk_upsert_in_partition(
            collection,
            vectordb_core::DEFAULT_PARTITION,
            points,
            chunk_size,
        )
    }

    /// Partition-aware bulk import. Same back-compat rule as
    /// [`upsert_in_partition`]: payload is left untouched for the default
    /// partition, otherwise each point's payload is tagged with
    /// `_partition`. The partition is validated once up front.
    pub fn bulk_upsert_in_partition(
        &self,
        collection: &str,
        partition: &str,
        points: Vec<BulkPoint>,
        chunk_size: usize,
    ) -> Result<u64> {
        let fq = self.resolve_alias(collection);
        self.ensure_collection_loaded(&fq)?;
        if points.is_empty() {
            return Ok(0);
        }
        let partition = if partition.is_empty() {
            vectordb_core::DEFAULT_PARTITION
        } else {
            partition
        };
        {
            let cols = self.collections.read();
            let state = cols
                .get(&fq)
                .ok_or_else(|| EngineError::CollectionNotFound(fq.clone()))?;
            if !state.config.partitions.iter().any(|p| p == partition) {
                return Err(EngineError::PartitionNotFound(partition.to_string()));
            }
        }
        let needs_tag = partition != vectordb_core::DEFAULT_PARTITION;
        let chunk_size = chunk_size.max(1);
        let mut total = 0u64;
        for chunk in points.chunks(chunk_size) {
            let owned: Vec<BulkPoint> = if needs_tag {
                chunk
                    .iter()
                    .cloned()
                    .map(|mut p| {
                        p.payload = Some(inject_partition_tag(p.payload, partition)?);
                        Ok::<_, EngineError>(p)
                    })
                    .collect::<Result<Vec<_>>>()?
            } else {
                chunk.to_vec()
            };
            let entry = WalEntry::BulkUpsert {
                collection: fq.clone(),
                points: owned,
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

    /// Flush a single collection — Milvus parity. VexaDb persists writes
    /// synchronously through the WAL, so "flush" reduces to fsync'ing the
    /// active log and returning the current segment IDs / timestamp. The
    /// collection still needs to exist (returns `CollectionNotFound`).
    pub fn flush_collection(&self, name: &str) -> Result<FlushInfo> {
        let fq = self.resolve_alias(name);
        self.ensure_collection_loaded(&fq)?;
        // Force a sync regardless of `sync_wal` mode so flushed data is
        // genuinely durable when this returns.
        self.wal
            .write()
            .force_sync()
            .map_err(EngineError::Wal)?;
        let wal_entries = self.wal.read().replay()?.len();
        let ts = now_ms_engine();
        // Single in-process WAL segment today; ID is a stable hash of the
        // FQN so the client can correlate flushes for the same collection.
        let seg_id = stable_segment_id(&fq);
        Ok(FlushInfo {
            collection: fq,
            flush_ts_ms: ts,
            segment_ids: vec![seg_id],
            flushed_segment_ids: vec![seg_id],
            wal_entries,
        })
    }

    /// Trigger an explicit compaction job for `name` (Milvus parity). The
    /// underlying op is the cluster-wide `compact_wal`; the engine tracks a
    /// per-call `CompactionStatus` so callers can poll for completion via
    /// [`CollectionEngine::compaction_state`].
    pub fn compact_collection(&self, name: &str) -> Result<u64> {
        let fq = self.resolve_alias(name);
        self.ensure_collection_loaded(&fq)?;
        let id = self.next_compaction_id();
        let started = now_ms_engine();
        // Pre-register as Running so concurrent state probes see the job.
        self.compactions.write().insert(
            id,
            CompactionStatus {
                id,
                collection: fq.clone(),
                state: CompactionStateCode::Running,
                entries_before: 0,
                entries_after: 0,
                started_ms: started,
                finished_ms: 0,
                error: None,
            },
        );
        let result = self.compact_wal();
        let mut reg = self.compactions.write();
        let slot = reg.get_mut(&id).expect("just inserted");
        slot.finished_ms = now_ms_engine();
        match result {
            Ok(stats) => {
                slot.entries_before = stats.before;
                slot.entries_after = stats.after;
                slot.state = CompactionStateCode::Completed;
                Ok(id)
            }
            Err(err) => {
                slot.state = CompactionStateCode::Failed;
                slot.error = Some(err.to_string());
                Err(err)
            }
        }
    }

    /// Look up a compaction job. Returns [`EngineError::InvalidMeta`] if
    /// the ID was never minted by this engine instance.
    pub fn compaction_state(&self, id: u64) -> Result<CompactionStatus> {
        self.compactions
            .read()
            .get(&id)
            .cloned()
            .ok_or_else(|| EngineError::InvalidMeta(format!("compaction id `{id}` not found")))
    }

    /// Enumerate persistent segments for `collection` — Milvus parity.
    ///
    /// VexaDb keeps a single live WAL segment per cluster; the returned
    /// list always contains one `Growing` row for the active WAL plus one
    /// `Flushed` row per snapshot that captured this collection.
    pub fn persistent_segments(&self, name: &str) -> Result<Vec<SegmentInfo>> {
        let fq = self.resolve_alias(name);
        self.ensure_collection_loaded(&fq)?;
        let num_rows = {
            let collections = self.collections.read();
            let state = collections
                .get(&fq)
                .ok_or_else(|| EngineError::CollectionNotFound(name.to_string()))?;
            state.index.len() as u64
        };
        let seg_id = stable_segment_id(&fq);
        let mut out = vec![SegmentInfo {
            id: seg_id,
            collection: fq.clone(),
            num_rows,
            state: SegmentState::Growing,
            source: "wal".into(),
        }];
        // One synthetic Flushed segment per snapshot — `num_rows=0` since
        // VexaDb does not record per-snapshot row counts today.
        if let Ok(snaps) = self.snapshot_manager().list() {
            for snap in snaps {
                out.push(SegmentInfo {
                    id: hash_str_to_id(&snap.id),
                    collection: fq.clone(),
                    num_rows: 0,
                    state: SegmentState::Flushed,
                    source: format!("snapshot:{}", snap.id),
                });
            }
        }
        Ok(out)
    }

    fn next_compaction_id(&self) -> u64 {
        // Wrap-aware fetch_add; collisions are astronomically unlikely
        // since the counter is seeded from epoch-ms at engine open.
        self.compaction_seq
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }

    /// Rebuild HNSW from stored vectors (online reindex).
    pub fn reindex_collection(&self, name: &str) -> Result<u64> {
        let fq = self.resolve_alias(name);
        self.ensure_collection_loaded(&fq)?;
        let (config, points) = {
            let collections = self.collections.read();
            let state = collections
                .get(&fq)
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
            .get_mut(&fq)
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
        let fq = self.resolve_alias(collection);
        self.ensure_collection_loaded(&fq)?;
        let collections = self.collections.read();
        let state = collections
            .get(&fq)
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
        let fq = self.resolve_alias(collection);
        self.ensure_collection_loaded(&fq)?;
        let collections = self.collections.read();
        let state = collections
            .get(&fq)
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
        let fq = self.resolve_alias(collection);
        self.ensure_collection_loaded(&fq)?;
        let collections = self.collections.read();
        let state = collections
            .get(&fq)
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
        let fq = self.resolve_alias(collection);
        self.ensure_collection_loaded(&fq)?;
        let collections = self.collections.read();
        let state = collections
            .get(&fq)
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
        let fq = self.resolve_alias(collection);
        self.ensure_collection_loaded(&fq)?;
        let collections = self.collections.read();
        let state = collections
            .get(&fq)
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
                let fq = qname(name);
                self.ensure_collection_loaded(&fq)?;
            }
        }
        Ok(())
    }

    /// `fq` must already be fully qualified (`db/name`).
    fn ensure_collection_loaded(&self, fq: &str) -> Result<()> {
        if self.collections.read().contains_key(fq) {
            return Ok(());
        }
        let key = format!("collection:{fq}");
        let raw = self
            .meta_db
            .read()
            .get(key)
            .map_err(|e| EngineError::Rocks(e.to_string()))?
            .ok_or_else(|| EngineError::CollectionNotFound(fq.to_string()))?;
        let mut config: CollectionConfig =
            serde_json::from_slice(&raw).map_err(|e| EngineError::Rocks(e.to_string()))?;
        let (db_seg, name_seg) = split_fq(fq);
        if config.database.is_empty() {
            config.database = db_seg.to_string();
        }
        if config.name.is_empty() {
            config.name = name_seg.to_string();
        }
        self.get_or_create_state(fq, config);
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

/// Reject simple names that would collide with the FQN parser (database/name
/// separator) or that are otherwise empty. Applied to user-supplied
/// collection / alias / database names, never to FQNs.
fn check_simple_name(name: &str) -> Result<()> {
    if name.is_empty() {
        return Err(EngineError::InvalidMeta("empty name".into()));
    }
    if name.contains('/') {
        return Err(EngineError::InvalidMeta(format!(
            "name {name:?} must not contain '/' (used as database separator)"
        )));
    }
    Ok(())
}

/// Inject the `_partition` tag into the JSON payload, creating an object
/// payload if none exists or wrapping non-object payloads under `_raw`.
/// Returns the canonical JSON bytes; only called by partition-aware
/// upsert paths.
fn inject_partition_tag(payload: Option<Vec<u8>>, partition: &str) -> Result<Vec<u8>> {
    let field = vectordb_core::PARTITION_PAYLOAD_FIELD;
    let mut value = match payload {
        Some(bytes) if !bytes.is_empty() => match serde_json::from_slice::<Value>(&bytes) {
            Ok(v) => v,
            Err(_) => {
                // Bytes weren't valid JSON; preserve them under `_raw` so
                // we don't silently drop user data.
                let raw = String::from_utf8_lossy(&bytes).to_string();
                serde_json::json!({ "_raw": raw })
            }
        },
        _ => Value::Object(serde_json::Map::new()),
    };
    if !value.is_object() {
        value = serde_json::json!({ "_raw": value });
    }
    if let Some(obj) = value.as_object_mut() {
        obj.insert(field.to_string(), Value::String(partition.to_string()));
    }
    serde_json::to_vec(&value).map_err(|e| EngineError::InvalidPayload(e.to_string()))
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

fn now_ms_engine() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Deterministic 64-bit ID derived from a fully-qualified name. Stable
/// across restarts so the same collection always reports the same
/// "segment ID" to external observers.
fn stable_segment_id(fq: &str) -> u64 {
    // Lift the high bit so IDs can also slot into `int64` clients without
    // becoming negative.
    hash_str_to_id(fq)
}

fn hash_str_to_id(s: &str) -> u64 {
    let mut h: u64 = 1469598103934665603; // FNV-1a 64 offset basis
    for b in s.as_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(1099511628211);
    }
    h >> 1
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

/// Result of a per-collection [`CollectionEngine::flush_collection`] call.
///
/// Mirrors the shape Milvus's FlushTask reports: a list of segment IDs and
/// a flush timestamp (here: epoch ms). VexaDb has a single WAL segment
/// today, so `segment_ids` returns at most one entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlushInfo {
    pub collection: String,
    pub flush_ts_ms: u64,
    pub segment_ids: Vec<u64>,
    pub flushed_segment_ids: Vec<u64>,
    pub wal_entries: usize,
}

/// Persistent-segment row reported by [`CollectionEngine::persistent_segments`].
///
/// VexaDb does not partition data internally; one segment per (collection,
/// WAL file) is reported plus one entry per snapshot referencing it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentInfo {
    pub id: u64,
    pub collection: String,
    pub num_rows: u64,
    pub state: SegmentState,
    pub source: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SegmentState {
    Growing,
    Sealed,
    Flushed,
}

/// State of a compaction job tracked by the engine. Cleared once the
/// engine restarts; callers should treat completed jobs as terminal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompactionStateCode {
    Running,
    Completed,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactionStatus {
    pub id: u64,
    pub collection: String,
    pub state: CompactionStateCode,
    pub entries_before: usize,
    pub entries_after: usize,
    pub started_ms: u64,
    pub finished_ms: u64,
    pub error: Option<String>,
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

    // ---- Database management (Milvus parity) ------------------------------

    #[test]
    fn default_database_is_seeded_on_open() {
        let dir = tempdir().unwrap();
        let engine = CollectionEngine::open(EngineConfig::new(dir.path())).unwrap();
        let names = engine.list_databases();
        assert_eq!(names, vec!["default".to_string()]);
        let cfg = engine.describe_database("default").unwrap();
        assert_eq!(cfg.name, "default");
    }

    #[test]
    fn create_describe_drop_database() {
        let dir = tempdir().unwrap();
        let engine = CollectionEngine::open(EngineConfig::new(dir.path())).unwrap();

        // Create with one property.
        let mut props = std::collections::BTreeMap::new();
        props.insert("replica.number".to_string(), "2".to_string());
        engine
            .commit_meta(MetaOp::CreateDatabase {
                name: "analytics".into(),
                properties: props,
                created_at_ms: 42,
            })
            .unwrap();

        // List and describe.
        let dbs = engine.list_databases();
        assert!(dbs.contains(&"analytics".to_string()));
        let cfg = engine.describe_database("analytics").unwrap();
        assert_eq!(cfg.properties.get("replica.number"), Some(&"2".to_string()));
        assert_eq!(cfg.created_at_ms, 42);

        // Drop.
        engine
            .commit_meta(MetaOp::DropDatabase {
                name: "analytics".into(),
                force: false,
            })
            .unwrap();
        assert!(engine.describe_database("analytics").is_err());
    }

    #[test]
    fn cannot_drop_default_database() {
        let dir = tempdir().unwrap();
        let engine = CollectionEngine::open(EngineConfig::new(dir.path())).unwrap();
        let err = engine
            .commit_meta(MetaOp::DropDatabase {
                name: "default".into(),
                force: false,
            })
            .unwrap_err();
        assert!(matches!(err, EngineError::InvalidMeta(_)));
    }

    #[test]
    fn drop_database_requires_force_when_non_empty() {
        let dir = tempdir().unwrap();
        let engine = CollectionEngine::open(EngineConfig::new(dir.path())).unwrap();
        engine
            .commit_meta(MetaOp::CreateDatabase {
                name: "ws".into(),
                properties: Default::default(),
                created_at_ms: 0,
            })
            .unwrap();
        let mut cfg = test_config("docs");
        cfg.database = "ws".into();
        engine.create_collection(cfg).unwrap();

        let err = engine
            .commit_meta(MetaOp::DropDatabase {
                name: "ws".into(),
                force: false,
            })
            .unwrap_err();
        assert!(matches!(err, EngineError::DatabaseNotEmpty(_)));

        engine
            .commit_meta(MetaOp::DropDatabase {
                name: "ws".into(),
                force: true,
            })
            .unwrap();
        // Cascade should have removed the collection too.
        assert!(!engine
            .list_collections()
            .iter()
            .any(|n| n.starts_with("ws/")));
    }

    #[test]
    fn collections_are_per_database() {
        let dir = tempdir().unwrap();
        let engine = CollectionEngine::open(EngineConfig::new(dir.path())).unwrap();
        engine
            .commit_meta(MetaOp::CreateDatabase {
                name: "team_a".into(),
                properties: Default::default(),
                created_at_ms: 0,
            })
            .unwrap();
        engine
            .commit_meta(MetaOp::CreateDatabase {
                name: "team_b".into(),
                properties: Default::default(),
                created_at_ms: 0,
            })
            .unwrap();

        // Same simple name in two databases.
        let mut a = test_config("articles");
        a.database = "team_a".into();
        let mut b = test_config("articles");
        b.database = "team_b".into();
        engine.create_collection(a).unwrap();
        engine.create_collection(b).unwrap();

        assert_eq!(
            engine.list_collections_in_database("team_a"),
            vec!["articles".to_string()]
        );
        assert_eq!(
            engine.list_collections_in_database("team_b"),
            vec!["articles".to_string()]
        );

        // Upserts target the FQN; engine writes into the correct namespace.
        engine
            .upsert(
                "team_a/articles",
                "x".into(),
                Vector::new(vec![1.0, 0.0, 0.0, 0.0]),
                None,
                None,
            )
            .unwrap();
        assert_eq!(engine.stats("team_a/articles").unwrap().vector_count, 1);
        assert_eq!(engine.stats("team_b/articles").unwrap().vector_count, 0);
    }

    #[test]
    fn alter_and_drop_database_properties() {
        let dir = tempdir().unwrap();
        let engine = CollectionEngine::open(EngineConfig::new(dir.path())).unwrap();
        engine
            .commit_meta(MetaOp::CreateDatabase {
                name: "billing".into(),
                properties: Default::default(),
                created_at_ms: 0,
            })
            .unwrap();

        let mut set = std::collections::BTreeMap::new();
        set.insert("ttl".into(), "30d".into());
        set.insert("tier".into(), "hot".into());
        engine
            .commit_meta(MetaOp::AlterDatabaseProperties {
                name: "billing".into(),
                set,
                unset: vec![],
            })
            .unwrap();
        let cfg = engine.describe_database("billing").unwrap();
        assert_eq!(cfg.properties.get("ttl"), Some(&"30d".to_string()));
        assert_eq!(cfg.properties.get("tier"), Some(&"hot".to_string()));

        engine
            .commit_meta(MetaOp::AlterDatabaseProperties {
                name: "billing".into(),
                set: Default::default(),
                unset: vec!["tier".into()],
            })
            .unwrap();
        let cfg = engine.describe_database("billing").unwrap();
        assert_eq!(cfg.properties.get("ttl"), Some(&"30d".to_string()));
        assert!(cfg.properties.get("tier").is_none());
    }

    #[test]
    fn legacy_bare_collection_is_loaded_into_default_db() {
        // Verify backward-compat: collections persisted before the database
        // refactor used `collection:<name>` keys; the engine should resurrect
        // them under the implicit `default` database.
        let dir = tempdir().unwrap();
        {
            // Hand-write a legacy key directly into the meta DB so we don't
            // depend on a pre-database engine binary being available.
            let engine = CollectionEngine::open(EngineConfig::new(dir.path())).unwrap();
            let cfg = serde_json::to_vec(&serde_json::json!({
                "name": "legacy",
                "dimension": 4,
                "metric": "cosine",
                "m": 16,
                "ef_construction": 200,
                "ef_search": 64,
            }))
            .unwrap();
            // Reach into the internal rocksdb handle. The intent is to
            // simulate an old key format; we drop the engine right after.
            engine
                .meta_db
                .read()
                .put(b"collection:legacy", &cfg)
                .unwrap();
        }
        let engine = CollectionEngine::open(EngineConfig::new(dir.path())).unwrap();
        let listed = engine.list_collections_in_database("default");
        assert!(listed.contains(&"legacy".to_string()));
    }

    // ---- Index / segment / compaction management (Milvus parity) ----------

    #[test]
    fn add_and_drop_payload_index_updates_config_and_persists() {
        let dir = tempdir().unwrap();
        let engine = CollectionEngine::open(EngineConfig::new(dir.path())).unwrap();
        engine.create_collection(test_config("docs")).unwrap();

        engine
            .commit_meta(MetaOp::AddPayloadIndex {
                collection: "docs".into(),
                database: "default".into(),
                field: "category".into(),
                kind: "keyword".into(),
            })
            .unwrap();

        // Persisted: reopen and verify.
        drop(engine);
        let engine = CollectionEngine::open(EngineConfig::new(dir.path())).unwrap();
        let stats = engine.stats("docs").unwrap();
        assert_eq!(stats.payload_index_count, 1);

        // Drop should bring the count back to zero.
        engine
            .commit_meta(MetaOp::DropPayloadIndex {
                collection: "docs".into(),
                database: "default".into(),
                field: "category".into(),
            })
            .unwrap();
        assert_eq!(engine.stats("docs").unwrap().payload_index_count, 0);
    }

    #[test]
    fn add_payload_index_rejects_unknown_kind() {
        let dir = tempdir().unwrap();
        let engine = CollectionEngine::open(EngineConfig::new(dir.path())).unwrap();
        engine.create_collection(test_config("docs")).unwrap();
        let err = engine
            .commit_meta(MetaOp::AddPayloadIndex {
                collection: "docs".into(),
                database: "default".into(),
                field: "f".into(),
                kind: "tree".into(),
            })
            .unwrap_err();
        assert!(matches!(err, EngineError::InvalidMeta(_)));
    }

    #[test]
    fn drop_payload_index_idempotent_when_missing() {
        let dir = tempdir().unwrap();
        let engine = CollectionEngine::open(EngineConfig::new(dir.path())).unwrap();
        engine.create_collection(test_config("docs")).unwrap();
        // Idempotent: no panic, no error.
        engine
            .commit_meta(MetaOp::DropPayloadIndex {
                collection: "docs".into(),
                database: "default".into(),
                field: "missing".into(),
            })
            .unwrap();
    }

    #[test]
    fn flush_collection_returns_segments_and_ts() {
        let dir = tempdir().unwrap();
        let engine = CollectionEngine::open(EngineConfig::new(dir.path())).unwrap();
        engine.create_collection(test_config("docs")).unwrap();

        let info = engine.flush_collection("docs").unwrap();
        assert_eq!(info.collection, "default/docs");
        assert_eq!(info.segment_ids.len(), 1);
        assert_eq!(info.flushed_segment_ids, info.segment_ids);
        assert!(info.flush_ts_ms > 0);
    }

    #[test]
    fn compact_collection_tracks_state() {
        let dir = tempdir().unwrap();
        let engine = CollectionEngine::open(EngineConfig::new(dir.path())).unwrap();
        engine.create_collection(test_config("docs")).unwrap();

        let id = engine.compact_collection("docs").unwrap();
        let s = engine.compaction_state(id).unwrap();
        assert_eq!(s.state, CompactionStateCode::Completed);
        assert_eq!(s.collection, "default/docs");
        assert!(s.finished_ms >= s.started_ms);

        // Unknown id -> error.
        assert!(engine.compaction_state(0).is_err());
    }

    #[test]
    fn persistent_segments_reports_growing_segment() {
        let dir = tempdir().unwrap();
        let engine = CollectionEngine::open(EngineConfig::new(dir.path())).unwrap();
        engine.create_collection(test_config("docs")).unwrap();
        engine
            .upsert(
                "docs",
                "x".into(),
                Vector::new(vec![1.0, 0.0, 0.0, 0.0]),
                None,
                None,
            )
            .unwrap();
        let segs = engine.persistent_segments("docs").unwrap();
        assert!(!segs.is_empty());
        let growing = &segs[0];
        assert_eq!(growing.state, SegmentState::Growing);
        assert_eq!(growing.num_rows, 1);
        assert!(growing.id > 0);
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

    // ---- Partition management (Milvus parity) ------------------------------

    fn open_engine_with_collection(name: &str) -> (tempfile::TempDir, CollectionEngine) {
        let dir = tempdir().unwrap();
        let engine = CollectionEngine::open(EngineConfig::new(dir.path())).unwrap();
        engine.create_collection(test_config(name)).unwrap();
        (dir, engine)
    }

    #[test]
    fn new_collection_has_default_partition() {
        let (_dir, engine) = open_engine_with_collection("parts");
        let names = engine.list_partitions("parts").unwrap();
        assert_eq!(names, vec![vectordb_core::DEFAULT_PARTITION.to_string()]);
        assert!(engine
            .has_partition("parts", vectordb_core::DEFAULT_PARTITION)
            .unwrap());
        assert!(!engine.has_partition("parts", "nope").unwrap());
    }

    #[test]
    fn create_and_drop_partition_round_trips() {
        let (_dir, engine) = open_engine_with_collection("parts");

        engine
            .commit_meta(MetaOp::CreatePartition {
                collection: "parts".into(),
                database: vectordb_core::DEFAULT_DATABASE.into(),
                partition: "hot".into(),
            })
            .unwrap();
        assert!(engine.has_partition("parts", "hot").unwrap());

        // Duplicate create fails.
        let err = engine
            .commit_meta(MetaOp::CreatePartition {
                collection: "parts".into(),
                database: vectordb_core::DEFAULT_DATABASE.into(),
                partition: "hot".into(),
            })
            .unwrap_err();
        assert!(matches!(err, EngineError::PartitionExists(_)));

        engine
            .commit_meta(MetaOp::DropPartition {
                collection: "parts".into(),
                database: vectordb_core::DEFAULT_DATABASE.into(),
                partition: "hot".into(),
            })
            .unwrap();
        assert!(!engine.has_partition("parts", "hot").unwrap());

        // Dropping the default partition is rejected.
        let err = engine
            .commit_meta(MetaOp::DropPartition {
                collection: "parts".into(),
                database: vectordb_core::DEFAULT_DATABASE.into(),
                partition: vectordb_core::DEFAULT_PARTITION.into(),
            })
            .unwrap_err();
        assert!(matches!(err, EngineError::InvalidMeta(_)));
    }

    #[test]
    fn upsert_in_partition_tags_payload_and_drop_removes_points() {
        let (_dir, engine) = open_engine_with_collection("parts");
        engine
            .commit_meta(MetaOp::CreatePartition {
                collection: "parts".into(),
                database: vectordb_core::DEFAULT_DATABASE.into(),
                partition: "hot".into(),
            })
            .unwrap();

        // 1 point in _default + 2 points in hot.
        engine
            .upsert(
                "parts",
                "d1".into(),
                Vector::new(vec![1.0, 0.0, 0.0, 0.0]),
                Some(br#"{"x":1}"#.to_vec()),
                None,
            )
            .unwrap();
        for (i, id) in ["h1", "h2"].iter().enumerate() {
            engine
                .upsert_in_partition(
                    "parts",
                    "hot",
                    id.to_string(),
                    Vector::new(vec![i as f32, 1.0, 0.0, 0.0]),
                    Some(format!(r#"{{"x":{i}}}"#).into_bytes()),
                    None,
                )
                .unwrap();
        }
        assert_eq!(engine.stats("parts").unwrap().vector_count, 3);

        let stats_default = engine
            .partition_stats("parts", vectordb_core::DEFAULT_PARTITION)
            .unwrap();
        assert_eq!(stats_default.get("row_count").unwrap(), "1");
        let stats_hot = engine.partition_stats("parts", "hot").unwrap();
        assert_eq!(stats_hot.get("row_count").unwrap(), "2");

        engine
            .commit_meta(MetaOp::DropPartition {
                collection: "parts".into(),
                database: vectordb_core::DEFAULT_DATABASE.into(),
                partition: "hot".into(),
            })
            .unwrap();
        assert_eq!(engine.stats("parts").unwrap().vector_count, 1);
        assert!(matches!(
            engine.partition_stats("parts", "hot").unwrap_err(),
            EngineError::PartitionNotFound(_)
        ));
    }

    #[test]
    fn upsert_in_unknown_partition_errors() {
        let (_dir, engine) = open_engine_with_collection("parts");
        let err = engine
            .upsert_in_partition(
                "parts",
                "nope",
                "x".into(),
                Vector::new(vec![0.0, 0.0, 0.0, 0.0]),
                None,
                None,
            )
            .unwrap_err();
        assert!(matches!(err, EngineError::PartitionNotFound(_)));
    }

    // ---- Resource group management (Milvus parity) ------------------------

    #[test]
    fn default_resource_group_is_seeded_on_open() {
        let dir = tempdir().unwrap();
        let engine = CollectionEngine::open(EngineConfig::new(dir.path())).unwrap();
        let names = engine.list_resource_groups();
        assert_eq!(
            names,
            vec![vectordb_core::DEFAULT_RESOURCE_GROUP.to_string()]
        );
        let info = engine
            .describe_resource_group(vectordb_core::DEFAULT_RESOURCE_GROUP)
            .unwrap();
        assert_eq!(info.name, vectordb_core::DEFAULT_RESOURCE_GROUP);
    }

    #[test]
    fn create_describe_update_drop_resource_group() {
        let dir = tempdir().unwrap();
        let engine = CollectionEngine::open(EngineConfig::new(dir.path())).unwrap();
        let mut cfg = vectordb_core::ResourceGroupConfig::default();
        cfg.requests = vectordb_core::ResourceGroupLimit { node_num: 2 };
        cfg.limits = vectordb_core::ResourceGroupLimit { node_num: 4 };
        engine
            .commit_meta(MetaOp::CreateResourceGroup {
                name: "hot".into(),
                config: cfg.clone(),
                created_at_ms: 12345,
            })
            .unwrap();

        // Duplicate create fails.
        let err = engine
            .commit_meta(MetaOp::CreateResourceGroup {
                name: "hot".into(),
                config: cfg.clone(),
                created_at_ms: 12345,
            })
            .unwrap_err();
        assert!(matches!(err, EngineError::ResourceGroupExists(_)));

        let names = engine.list_resource_groups();
        assert!(names.contains(&"hot".to_string()));

        let info = engine.describe_resource_group("hot").unwrap();
        assert_eq!(info.config.requests.node_num, 2);
        assert_eq!(info.config.limits.node_num, 4);
        assert_eq!(info.created_at_ms, 12345);

        let mut cfg2 = cfg.clone();
        cfg2.limits = vectordb_core::ResourceGroupLimit { node_num: 8 };
        engine
            .commit_meta(MetaOp::UpdateResourceGroup {
                name: "hot".into(),
                config: cfg2,
            })
            .unwrap();
        let info2 = engine.describe_resource_group("hot").unwrap();
        assert_eq!(info2.config.limits.node_num, 8);

        engine
            .commit_meta(MetaOp::DropResourceGroup {
                name: "hot".into(),
            })
            .unwrap();
        assert!(matches!(
            engine.describe_resource_group("hot").unwrap_err(),
            EngineError::ResourceGroupNotFound(_)
        ));

        // Cannot drop default.
        let err = engine
            .commit_meta(MetaOp::DropResourceGroup {
                name: vectordb_core::DEFAULT_RESOURCE_GROUP.into(),
            })
            .unwrap_err();
        assert!(matches!(err, EngineError::InvalidMeta(_)));
    }

    #[test]
    fn update_unknown_resource_group_errors() {
        let dir = tempdir().unwrap();
        let engine = CollectionEngine::open(EngineConfig::new(dir.path())).unwrap();
        let err = engine
            .commit_meta(MetaOp::UpdateResourceGroup {
                name: "ghost".into(),
                config: vectordb_core::ResourceGroupConfig::default(),
            })
            .unwrap_err();
        assert!(matches!(err, EngineError::ResourceGroupNotFound(_)));
    }
}
