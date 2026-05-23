use tonic::{metadata::MetadataValue, Status};
use vectordb_auth::METADATA_LEADER;
use vectordb_replication::RaftNode;

/// Attach leader endpoint metadata to a `failed_precondition` status when known.
///
/// Accepts any displayable error so that callers passing `anyhow::Error`,
/// `String`, or any other `Display` type can convert without extra adapters.
pub fn map_raft_err<E: std::fmt::Display>(e: E, raft: Option<&RaftNode>) -> Status {
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
