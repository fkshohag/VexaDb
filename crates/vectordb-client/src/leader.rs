use tonic::Status;
use vectordb_auth::METADATA_LEADER;

/// Read `x-vectordb-leader` from a gRPC status (leader redirect hint).
pub fn leader_from_status(status: &Status) -> Option<String> {
    status
        .metadata()
        .get(METADATA_LEADER)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
}

pub fn is_not_leader(status: &Status) -> bool {
    status.code() == tonic::Code::FailedPrecondition
        && status.message().contains("not leader")
}
