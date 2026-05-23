pub mod vectordb {
    pub mod v1 {
        include!(concat!(env!("OUT_DIR"), "/vectordb.v1.rs"));
    }
}

pub mod raft {
    pub mod v1 {
        include!(concat!(env!("OUT_DIR"), "/vectordb.raft.v1.rs"));
    }
}

pub use vectordb::v1::vector_service_client::VectorServiceClient;
pub use vectordb::v1::vector_service_server::{VectorService, VectorServiceServer};
pub use raft::v1::raft_service_client::RaftServiceClient;
pub use raft::v1::raft_service_server::{RaftService, RaftServiceServer};
