use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use vectordb_core::{SparseVector, Vector};

#[derive(Debug, Error)]
pub enum WalError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("serialization error: {0}")]
    Serde(#[from] bincode::Error),
}

pub type Result<T> = std::result::Result<T, WalError>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WalEntry {
    CreateCollection {
        config: vectordb_core::CollectionConfig,
    },
    DeleteCollection {
        name: String,
    },
    Upsert {
        collection: String,
        id: String,
        vector: Vector,
        #[serde(default)]
        payload: Option<Vec<u8>>,
        #[serde(default)]
        sparse: Option<SparseVector>,
    },
    Delete {
        collection: String,
        id: String,
    },
    /// Batch upsert for bulk import (single WAL record).
    BulkUpsert {
        collection: String,
        points: Vec<BulkPoint>,
    },
    /// Marker written after snapshot + compaction.
    Checkpoint {
        snapshot_id: String,
    },
    /// Mutate the replicated RBAC state (users, tokens, roles, grants).
    ///
    /// Appended last so bincode's variant-index encoding stays
    /// backwards-compatible with WAL files written before RBAC existed.
    Rbac { op: vectordb_rbac::RbacOp },
    /// Replicated collection-metadata mutations (rename, aliases, properties).
    /// Appended after `Rbac` for the same compat reason.
    Meta { op: MetaOp },
}

/// Replicated metadata operation. Applied through Raft so all replicas
/// converge.
///
/// Collection-scoped variants implicitly operate within the request's active
/// database. Database management lives at the bottom — new variants are
/// appended to keep bincode's variant-index encoding wire-compatible with
/// older WAL files.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MetaOp {
    /// Rename an existing collection. Fails if `new_name` already exists or
    /// collides with an existing alias in the same database.
    RenameCollection {
        old: String,
        new: String,
        #[serde(default = "default_database")]
        database: String,
    },
    /// Create a new alias pointing to `collection`. Fails if `alias` exists
    /// or collides with a collection name in the same database.
    CreateAlias {
        alias: String,
        collection: String,
        #[serde(default = "default_database")]
        database: String,
    },
    /// Drop an alias (no-op if missing).
    DropAlias {
        alias: String,
        #[serde(default = "default_database")]
        database: String,
    },
    /// Reassign an alias to a different collection.
    AlterAlias {
        alias: String,
        collection: String,
        #[serde(default = "default_database")]
        database: String,
    },
    /// Merge `set` into the collection's properties, then remove `unset` keys.
    AlterCollectionProperties {
        name: String,
        #[serde(default = "default_database")]
        database: String,
        #[serde(default)]
        set: std::collections::BTreeMap<String, String>,
        #[serde(default)]
        unset: Vec<String>,
    },
    // ---- Database management (Milvus-parity) -----------------------------
    /// Create a new database. Idempotent only if `name` does not already
    /// exist — otherwise fails so callers see a clean conflict.
    CreateDatabase {
        name: String,
        #[serde(default)]
        properties: std::collections::BTreeMap<String, String>,
        #[serde(default)]
        created_at_ms: u64,
    },
    /// Drop a database. `force=false` requires the database to be empty
    /// (no collections, no aliases). `force=true` cascade-drops every
    /// collection and alias inside it. The built-in `default` database
    /// cannot be dropped.
    DropDatabase {
        name: String,
        #[serde(default)]
        force: bool,
    },
    /// Merge `set` into the database's properties, then remove `unset` keys.
    AlterDatabaseProperties {
        name: String,
        #[serde(default)]
        set: std::collections::BTreeMap<String, String>,
        #[serde(default)]
        unset: Vec<String>,
    },
    // ---- Index management (Milvus-parity) -------------------------------
    /// Attach a payload (scalar) index to a field. Replaces any existing
    /// entry for the same field — payload indexes are uniqued per-field.
    AddPayloadIndex {
        collection: String,
        #[serde(default = "default_database")]
        database: String,
        field: String,
        kind: String,
    },
    /// Detach a payload index from a field. No-op when the field has no
    /// index, so callers can safely retry.
    DropPayloadIndex {
        collection: String,
        #[serde(default = "default_database")]
        database: String,
        field: String,
    },
    // ---- Partition management (Milvus-parity) ---------------------------
    /// Create a new logical partition inside a collection. Partitions live
    /// in `CollectionConfig::partitions` and gate upserts via the
    /// `_partition` payload field.
    CreatePartition {
        collection: String,
        #[serde(default = "default_database")]
        database: String,
        partition: String,
    },
    /// Drop a partition. Removes the partition name from the config and
    /// deletes every point whose `_partition` payload equals `partition`.
    /// Dropping [`vectordb_core::DEFAULT_PARTITION`] is rejected.
    DropPartition {
        collection: String,
        #[serde(default = "default_database")]
        database: String,
        partition: String,
    },
    // ---- Resource group management (Milvus-parity) ----------------------
    /// Create a new resource group. Names are unique cluster-wide; the
    /// built-in `__default_resource_group` cannot be re-created.
    CreateResourceGroup {
        name: String,
        #[serde(default)]
        config: vectordb_core::ResourceGroupConfig,
        #[serde(default)]
        created_at_ms: u64,
    },
    /// Drop a resource group. Rejected for the built-in default group.
    DropResourceGroup { name: String },
    /// Replace the config of an existing resource group. The previous
    /// config is overwritten in full (Milvus semantics).
    UpdateResourceGroup {
        name: String,
        config: vectordb_core::ResourceGroupConfig,
    },
}

fn default_database() -> String {
    vectordb_core::DEFAULT_DATABASE.to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BulkPoint {
    pub id: String,
    pub vector: Vector,
    #[serde(default)]
    pub payload: Option<Vec<u8>>,
    #[serde(default)]
    pub sparse: Option<SparseVector>,
}

/// Append-only write-ahead log for crash recovery.
///
/// `fsync_on_append=true` makes every committed write survive an uncontrolled
/// OS / VM crash at the cost of throughput. With `fsync_on_append=false` we
/// only flush to the kernel page cache; data can be lost on hard reboot.
pub struct WriteAheadLog {
    path: PathBuf,
    writer: BufWriter<File>,
    fsync_on_append: bool,
}

impl WriteAheadLog {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with(path, true)
    }

    pub fn open_with(path: impl AsRef<Path>, fsync_on_append: bool) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)?;
        Ok(Self {
            path,
            writer: BufWriter::new(file),
            fsync_on_append,
        })
    }

    pub fn append(&mut self, entry: &WalEntry) -> Result<()> {
        let bytes = bincode::serialize(entry)?;
        let len = bytes.len() as u32;
        self.writer.write_all(&len.to_le_bytes())?;
        self.writer.write_all(&bytes)?;
        self.writer.flush()?;
        if self.fsync_on_append {
            // sync_data() fsyncs file contents but skips metadata updates that
            // don't affect durability — cheaper than sync_all() on most filesystems.
            self.writer.get_ref().sync_data()?;
        }
        Ok(())
    }

    pub fn fsync_on_append(&self) -> bool {
        self.fsync_on_append
    }

    pub fn replay(&self) -> Result<Vec<WalEntry>> {
        let file = File::open(&self.path)?;
        let mut reader = BufReader::new(file);
        let mut entries = Vec::new();
        loop {
            let mut len_buf = [0u8; 4];
            match reader.read_exact(&mut len_buf) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            }
            let len = u32::from_le_bytes(len_buf) as usize;
            let mut buf = vec![0u8; len];
            reader.read_exact(&mut buf)?;
            entries.push(bincode::deserialize(&buf)?);
        }
        Ok(entries)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Replace the WAL file with a compacted sequence of entries.
    pub fn rewrite(&mut self, entries: &[WalEntry]) -> Result<()> {
        let path = self.path.clone();
        drop(std::mem::replace(
            &mut self.writer,
            BufWriter::new(
                OpenOptions::new()
                    .write(true)
                    .truncate(true)
                    .create(true)
                    .open(&path)?,
            ),
        ));
        for entry in entries {
            self.append(entry)?;
        }
        // After a full rewrite, fsync regardless of mode — the truncate+rewrite
        // would otherwise leave a torn file across a crash window.
        self.writer.get_ref().sync_data()?;
        Ok(())
    }

    pub fn append_checkpoint(&mut self, snapshot_id: impl Into<String>) -> Result<()> {
        self.append(&WalEntry::Checkpoint {
            snapshot_id: snapshot_id.into(),
        })
    }

    /// Flush buffered data and force an `fsync` regardless of the
    /// `fsync_on_append` mode. Used by [`crate::engine::CollectionEngine::flush_collection`]
    /// to honour Milvus's strict durability contract on Flush.
    pub fn force_sync(&mut self) -> Result<()> {
        self.writer.flush()?;
        self.writer.get_ref().sync_data()?;
        Ok(())
    }
}

#[cfg(test)]
mod fsync_tests {
    use super::*;
    use tempfile::tempdir;
    use vectordb_core::Vector;

    #[test]
    fn append_with_fsync_is_replayable() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal.log");
        let mut wal = WriteAheadLog::open_with(&path, true).unwrap();
        assert!(wal.fsync_on_append());
        for i in 0..5 {
            wal.append(&WalEntry::Upsert {
                collection: "c".into(),
                id: format!("p{i}"),
                vector: Vector::new(vec![i as f32]),
                payload: None,
                sparse: None,
            })
            .unwrap();
        }
        // Drop without explicit shutdown — durable writes survive.
        drop(wal);
        let reopened = WriteAheadLog::open_with(&path, true).unwrap();
        let entries = reopened.replay().unwrap();
        assert_eq!(entries.len(), 5);
    }

    #[test]
    fn fsync_disabled_still_works() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal.log");
        let mut wal = WriteAheadLog::open_with(&path, false).unwrap();
        assert!(!wal.fsync_on_append());
        wal.append(&WalEntry::DeleteCollection { name: "x".into() })
            .unwrap();
        drop(wal);
        let entries = WriteAheadLog::open_with(&path, false)
            .unwrap()
            .replay()
            .unwrap();
        assert_eq!(entries.len(), 1);
    }
}
