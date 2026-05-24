//! Raft InstallSnapshot: stream on-disk shard snapshots between replicas.

use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Status;
use vectordb_proto::raft::v1::InstallSnapshotChunk;
use vectordb_storage::{CollectionEngine, EngineError};

pub const CHUNK_SIZE: usize = 256 * 1024;
const STAGING_DIR: &str = ".raft_install_staging";

/// In-progress snapshot receive on a follower.
#[derive(Debug)]
pub struct SnapshotReceiveState {
    pub term: u64,
    pub leader_id: u64,
    pub last_included_index: u64,
    pub last_included_term: u64,
    pub staging: PathBuf,
}

pub struct SnapshotReceiver {
    data_dir: PathBuf,
    state: Mutex<Option<SnapshotReceiveState>>,
}

impl SnapshotReceiver {
    pub fn new(data_dir: PathBuf) -> Self {
        Self {
            data_dir,
            state: Mutex::new(None),
        }
    }

    pub fn staging_root(&self) -> PathBuf {
        self.data_dir.join(STAGING_DIR)
    }

    /// Apply one streamed chunk. Returns metadata when the full snapshot
    /// has been installed into the engine.
    pub fn apply_chunk(
        &self,
        engine: &CollectionEngine,
        chunk: InstallSnapshotChunk,
    ) -> Result<Option<SnapshotReceiveState>, Status> {
        if chunk.eof && chunk.name.is_empty() {
            return self.finish(engine);
        }

        let mut guard = self.state.lock();
        if guard.is_none() {
            let staging = self.staging_root();
            if staging.exists() {
                std::fs::remove_dir_all(&staging).map_err(io_status)?;
            }
            std::fs::create_dir_all(&staging).map_err(io_status)?;
            *guard = Some(SnapshotReceiveState {
                term: chunk.term,
                leader_id: chunk.leader_id,
                last_included_index: chunk.last_included_index,
                last_included_term: chunk.last_included_term,
                staging,
            });
        }

        let st = guard.as_ref().expect("just initialized");
        if chunk.term < st.term {
            return Err(Status::failed_precondition("stale snapshot term"));
        }
        if chunk.name.is_empty() {
            return Ok(None);
        }

        let path = st.staging.join(&chunk.name);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(io_status)?;
        }
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(&path)
            .map_err(io_status)?;
        file.seek(SeekFrom::Start(chunk.offset))
            .map_err(io_status)?;
        file.write_all(&chunk.data).map_err(io_status)?;

        Ok(None)
    }

    fn finish(&self, engine: &CollectionEngine) -> Result<Option<SnapshotReceiveState>, Status> {
        let st = self.state.lock().take().ok_or_else(|| {
            Status::failed_precondition("no snapshot transfer in progress")
        })?;

        engine
            .restore_from_snapshot(&st.staging)
            .map_err(|e| Status::internal(e.to_string()))?;

        if self.staging_root().exists() {
            let _ = std::fs::remove_dir_all(self.staging_root());
        }

        Ok(Some(st))
    }
}

fn io_status(e: std::io::Error) -> Status {
    Status::internal(e.to_string())
}

/// Build a client stream of `InstallSnapshotChunk` messages from a snapshot
/// directory on disk.
pub async fn stream_snapshot_to_peer(
    snapshot_root: PathBuf,
    term: u64,
    leader_id: u64,
    last_included_index: u64,
    last_included_term: u64,
) -> Result<ReceiverStream<InstallSnapshotChunk>, EngineError> {
    let files = vectordb_storage::snapshot::collect_payload_files(&snapshot_root)?;
    let (tx, rx) = mpsc::channel(16);

    tokio::spawn(async move {
        for (abs_path, rel_name) in files {
            if stream_one_file(
                &tx,
                &abs_path,
                &rel_name,
                term,
                leader_id,
                last_included_index,
                last_included_term,
            )
            .await
            .is_err()
            {
                return;
            }
        }
        let _ = tx
            .send(InstallSnapshotChunk {
                term,
                leader_id,
                last_included_index,
                last_included_term,
                name: String::new(),
                offset: 0,
                data: Vec::new(),
                done: false,
                eof: true,
            })
            .await;
    });

    Ok(ReceiverStream::new(rx))
}

async fn stream_one_file(
    tx: &mpsc::Sender<InstallSnapshotChunk>,
    path: &Path,
    name: &str,
    term: u64,
    leader_id: u64,
    last_included_index: u64,
    last_included_term: u64,
) -> Result<(), ()> {
    let mut file = std::fs::File::open(path).map_err(|_| ())?;
    let mut offset = 0u64;
    let mut buf = vec![0u8; CHUNK_SIZE];
    loop {
        let n = file.read(&mut buf).map_err(|_| ())?;
        if n == 0 {
            break;
        }
        if tx
            .send(InstallSnapshotChunk {
                term,
                leader_id,
                last_included_index,
                last_included_term,
                name: name.to_string(),
                offset,
                data: buf[..n].to_vec(),
                done: false,
                eof: false,
            })
            .await
            .is_err()
        {
            return Err(());
        }
        offset += n as u64;
    }
    if tx
        .send(InstallSnapshotChunk {
            term,
            leader_id,
            last_included_index,
            last_included_term,
            name: name.to_string(),
            offset,
            data: Vec::new(),
            done: true,
            eof: false,
        })
        .await
        .is_err()
    {
        return Err(());
    }
    Ok(())
}

/// Create a fresh on-disk snapshot and return its directory path.
pub fn create_snapshot_dir(engine: Arc<CollectionEngine>) -> Result<PathBuf, EngineError> {
    let snap = engine.snapshot_manager().create()?;
    Ok(snap.data_dir)
}
