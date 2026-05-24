use std::sync::Arc;

use tonic::{Request, Response, Status, Streaming};
use vectordb_proto::raft::v1::{
    raft_service_server::RaftService, AppendEntriesRequest, AppendEntriesResponse,
    InstallSnapshotChunk, InstallSnapshotResponse, RequestVoteRequest, RequestVoteResponse,
};

use crate::node::RaftNode;

pub struct RaftServiceImpl {
    node: Arc<RaftNode>,
}

impl RaftServiceImpl {
    pub fn new(node: Arc<RaftNode>) -> Self {
        Self { node }
    }
}

#[tonic::async_trait]
impl RaftService for RaftServiceImpl {
    async fn request_vote(
        &self,
        request: Request<RequestVoteRequest>,
    ) -> Result<Response<RequestVoteResponse>, Status> {
        Ok(Response::new(
            self.node.handle_request_vote(request.into_inner()),
        ))
    }

    async fn append_entries(
        &self,
        request: Request<AppendEntriesRequest>,
    ) -> Result<Response<AppendEntriesResponse>, Status> {
        Ok(Response::new(
            self.node.handle_append_entries(request.into_inner()),
        ))
    }

    async fn install_snapshot(
        &self,
        request: Request<Streaming<InstallSnapshotChunk>>,
    ) -> Result<Response<InstallSnapshotResponse>, Status> {
        let mut stream = request.into_inner();
        let mut last_resp = InstallSnapshotResponse {
            term: self.node.current_term(),
            success: true,
        };
        while let Some(msg) = stream.message().await? {
            last_resp = self.node.handle_install_snapshot_chunk(msg)?;
        }
        Ok(Response::new(last_resp))
    }
}
