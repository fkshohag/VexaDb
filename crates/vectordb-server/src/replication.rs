use std::sync::Arc;

use vectordb_core::{CollectionConfig, SparseVector};
use vectordb_rbac::RbacOp;
use vectordb_replication::RaftNode;
use vectordb_storage::{BulkPoint, CollectionEngine, WalEntry};

pub struct ReplicatedEngine {
    pub engine: Arc<CollectionEngine>,
    pub raft: Arc<RaftNode>,
}

impl ReplicatedEngine {
    pub fn ensure_leader(&self) -> Result<(), String> {
        if self.raft.is_leader() {
            Ok(())
        } else {
            Err(format!(
                "not leader; current leader={:?} role={:?}",
                self.raft.leader_id(),
                self.raft.role()
            ))
        }
    }

    pub async fn create_collection(&self, config: CollectionConfig) -> anyhow::Result<()> {
        self.ensure_leader().map_err(anyhow::Error::msg)?;
        self.raft
            .propose(WalEntry::CreateCollection { config })
            .await
    }

    pub async fn delete_collection(&self, name: &str) -> anyhow::Result<()> {
        self.ensure_leader().map_err(anyhow::Error::msg)?;
        self.raft
            .propose(WalEntry::DeleteCollection {
                name: name.to_string(),
            })
            .await
    }

    pub async fn upsert(
        &self,
        collection: &str,
        id: String,
        vector: vectordb_core::Vector,
        payload: Option<Vec<u8>>,
        sparse: Option<SparseVector>,
    ) -> anyhow::Result<()> {
        self.ensure_leader().map_err(anyhow::Error::msg)?;
        self.raft
            .propose(WalEntry::Upsert {
                collection: collection.to_string(),
                id,
                vector,
                payload,
                sparse,
            })
            .await
    }

    pub async fn delete(&self, collection: &str, id: &str) -> anyhow::Result<()> {
        self.ensure_leader().map_err(anyhow::Error::msg)?;
        self.raft
            .propose(WalEntry::Delete {
                collection: collection.to_string(),
                id: id.to_string(),
            })
            .await
    }

    pub async fn bulk_upsert(
        &self,
        collection: &str,
        points: Vec<BulkPoint>,
        chunk_size: usize,
    ) -> anyhow::Result<u64> {
        self.ensure_leader().map_err(anyhow::Error::msg)?;
        let chunk_size = chunk_size.max(1);
        let mut total = 0u64;
        for chunk in points.chunks(chunk_size) {
            self.raft
                .propose(WalEntry::BulkUpsert {
                    collection: collection.to_string(),
                    points: chunk.to_vec(),
                })
                .await?;
            total += chunk.len() as u64;
        }
        Ok(total)
    }

    pub async fn apply_rbac(&self, op: RbacOp) -> anyhow::Result<()> {
        self.ensure_leader().map_err(anyhow::Error::msg)?;
        self.raft.propose(WalEntry::Rbac { op }).await
    }
}
