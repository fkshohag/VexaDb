use tonic::{metadata::MetadataValue, Status};
use vectordb_auth::METADATA_LEADER;
use vectordb_replication::RaftNode;

/// Attach leader endpoint metadata to a `failed_precondition` status when known.
pub fn map_raft_err(e: anyhow::Error, raft: Option<&RaftNode>) -> Status {
    let msg = e.to_string();
    if msg.contains("not leader") {
        let mut status = Status::failed_precondition(msg);
        if let Some(node) = raft {
            if let Some(ep) = node.leader_vector_endpoint() {
                if let Ok(val) = MetadataValue::try_from(ep) {
                    status.metadata_mut().insert(METADATA_LEADER, val);
                }
            }
        }
        status
    } else {
        Status::internal(msg)
    }
}
