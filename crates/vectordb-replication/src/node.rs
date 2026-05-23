use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;
use vectordb_proto::raft::v1::{
    raft_service_server::RaftServiceServer, AppendEntriesRequest, AppendEntriesResponse,
    LogRecord, RequestVoteRequest, RequestVoteResponse,
};
use vectordb_storage::{CollectionEngine, WalEntry};

use crate::rpc::RaftServiceImpl;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftPeer {
    pub id: u64,
    /// Raft RPC listen address (host:port).
    pub addr: String,
    /// Vector API gRPC endpoint for this peer, e.g. `http://127.0.0.1:6334`.
    #[serde(default)]
    pub grpc: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftConfig {
    pub node_id: u64,
    pub listen: String,
    /// This node's Vector API endpoint (for leader discovery).
    #[serde(default)]
    pub vector_endpoint: Option<String>,
    pub peers: Vec<RaftPeer>,
    #[serde(default = "default_election_ms")]
    pub election_timeout_ms: u64,
    #[serde(default = "default_heartbeat_ms")]
    pub heartbeat_interval_ms: u64,
}

fn default_election_ms() -> u64 {
    750
}
fn default_heartbeat_ms() -> u64 {
    150
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            node_id: 1,
            listen: "0.0.0.0:7334".into(),
            peers: vec![],
            election_timeout_ms: default_election_ms(),
            heartbeat_interval_ms: default_heartbeat_ms(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug, Clone)]
struct LogEntry {
    term: u64,
    command: WalEntry,
}

#[derive(Debug)]
struct RaftState {
    role: Role,
    current_term: u64,
    voted_for: Option<u64>,
    leader_id: Option<u64>,
    log: Vec<LogEntry>,
    commit_index: usize,
    last_applied: usize,
    next_index: HashMap<u64, usize>,
    match_index: HashMap<u64, usize>,
}

pub struct RaftNode {
    config: RaftConfig,
    state: Arc<RwLock<RaftState>>,
    engine: Arc<CollectionEngine>,
    election_notify: Arc<Notify>,
}

impl RaftNode {
    pub async fn start(
        config: RaftConfig,
        engine: Arc<CollectionEngine>,
    ) -> anyhow::Result<Arc<Self>> {
        let state = Arc::new(RwLock::new(RaftState {
            role: Role::Follower,
            current_term: 0,
            voted_for: None,
            leader_id: None,
            log: Vec::new(),
            commit_index: 0,
            last_applied: 0,
            next_index: HashMap::new(),
            match_index: HashMap::new(),
        }));

        let election_notify = Arc::new(Notify::new());

        let node = Arc::new(Self {
            config: config.clone(),
            state: state.clone(),
            engine,
            election_notify: election_notify.clone(),
        });

        let service = RaftServiceImpl::new(node.clone());
        let addr = config.listen.parse()?;
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(RaftServiceServer::new(service))
                .serve(addr)
                .await
                .ok();
        });

        let bg_node = node.clone();
        tokio::spawn(async move {
            bg_node.run_background().await;
        });

        Ok(node)
    }

    pub fn is_leader(&self) -> bool {
        self.state.read().role == Role::Leader
    }

    pub fn role(&self) -> Role {
        self.state.read().role
    }

    pub fn leader_id(&self) -> Option<u64> {
        self.state.read().leader_id
    }

    /// gRPC endpoint for the current Raft leader's Vector API (`http://host:port`).
    pub fn leader_vector_endpoint(&self) -> Option<String> {
        if self.is_leader() {
            return self.config.vector_endpoint.clone().or_else(|| {
                self.config
                    .peers
                    .iter()
                    .find(|p| p.id == self.config.node_id)
                    .and_then(|p| p.grpc.clone())
            });
        }
        let leader_id = self.leader_id()?;
        self.config
            .peers
            .iter()
            .find(|p| p.id == leader_id)
            .and_then(|p| p.grpc.clone())
    }

    pub fn is_ready_for_writes(&self) -> bool {
        self.is_leader()
    }

    pub async fn propose(&self, entry: WalEntry) -> anyhow::Result<()> {
        if !self.is_leader() {
            anyhow::bail!("not leader (role={:?})", self.role());
        }
        {
            let mut s = self.state.write();
            let term = s.current_term;
            s.log.push(LogEntry {
                term,
                command: entry,
            });
        }
        self.replicate_to_majority().await
    }

    pub(crate) fn handle_request_vote(
        &self,
        req: RequestVoteRequest,
    ) -> RequestVoteResponse {
        let mut s = self.state.write();
        if req.term > s.current_term {
            s.current_term = req.term;
            s.voted_for = None;
            s.role = Role::Follower;
            s.leader_id = None;
        }

        let last_index = s.log.len() as u64;
        let last_term = s.log.last().map(|e| e.term).unwrap_or(0);
        let log_ok = req.last_log_term > last_term
            || (req.last_log_term == last_term && req.last_log_index >= last_index);

        let mut vote_granted = false;
        if req.term == s.current_term
            && (s.voted_for.is_none() || s.voted_for == Some(req.candidate_id))
            && log_ok
        {
            s.voted_for = Some(req.candidate_id);
            vote_granted = true;
        }

        RequestVoteResponse {
            term: s.current_term,
            vote_granted,
        }
    }

    pub(crate) fn handle_append_entries(
        &self,
        req: AppendEntriesRequest,
    ) -> AppendEntriesResponse {
        self.election_notify.notify_one();
        let mut s = self.state.write();

        if req.term < s.current_term {
            return AppendEntriesResponse {
                term: s.current_term,
                success: false,
                match_index: s.log.len() as u64,
            };
        }

        s.current_term = req.term;
        s.role = Role::Follower;
        s.leader_id = Some(req.leader_id);
        s.voted_for = Some(req.leader_id);

        if req.prev_log_index > 0 {
            if req.prev_log_index as usize > s.log.len() {
                return AppendEntriesResponse {
                    term: s.current_term,
                    success: false,
                    match_index: s.log.len() as u64,
                };
            }
            let local_term = s.log[req.prev_log_index as usize - 1].term;
            if local_term != req.prev_log_term {
                return AppendEntriesResponse {
                    term: s.current_term,
                    success: false,
                    match_index: (req.prev_log_index.saturating_sub(1)),
                };
            }
        }

        let mut index = req.prev_log_index as usize;
        for rec in req.entries {
            index += 1;
            let cmd: WalEntry = bincode::deserialize(&rec.payload).unwrap_or(WalEntry::DeleteCollection {
                name: "__bad__".into(),
            });
            let entry = LogEntry {
                term: rec.term,
                command: cmd,
            };
            if index <= s.log.len() {
                s.log[index - 1] = entry;
            } else if index == s.log.len() + 1 {
                s.log.push(entry);
            }
        }

        if req.leader_commit as usize > s.commit_index {
            s.commit_index = (req.leader_commit as usize).min(s.log.len());
        }

        let match_index = s.log.len() as u64;
        let commit = s.commit_index;
        drop(s);

        let node = self.clone_arc();
        tokio::spawn(async move {
            let _ = node.apply_through(commit).await;
        });

        AppendEntriesResponse {
            term: self.state.read().current_term,
            success: true,
            match_index,
        }
    }

    fn clone_arc(&self) -> Arc<Self> {
        Arc::new(Self {
            config: self.config.clone(),
            state: self.state.clone(),
            engine: self.engine.clone(),
            election_notify: self.election_notify.clone(),
        })
    }

    async fn apply_through(&self, commit: usize) -> anyhow::Result<()> {
        loop {
            let entry = {
                let mut s = self.state.write();
                if s.last_applied >= commit {
                    break;
                }
                s.last_applied += 1;
                s.log[s.last_applied - 1].command.clone()
            };
            if matches!(
                entry,
                WalEntry::DeleteCollection { ref name } if name == "__bad__"
            ) {
                continue;
            }
            self.engine
                .commit_entry(&entry)
                .map_err(|e| anyhow::anyhow!("{e}"))?;
        }
        Ok(())
    }

    async fn run_background(self: Arc<Self>) {
        loop {
            let timeout = Duration::from_millis(self.config.election_timeout_ms);
            tokio::select! {
                _ = tokio::time::sleep(timeout) => {
                    if self.state.read().role != Role::Leader {
                        let _ = self.start_election().await;
                    }
                }
                _ = self.election_notify.notified() => {}
            }

            if self.is_leader() {
                let _ = self.send_heartbeats().await;
                tokio::time::sleep(Duration::from_millis(self.config.heartbeat_interval_ms)).await;
            }
        }
    }

    async fn start_election(self: &Arc<Self>) -> anyhow::Result<()> {
        let (term, last_index, last_term) = {
            let mut s = self.state.write();
            s.current_term += 1;
            s.role = Role::Candidate;
            s.voted_for = Some(self.config.node_id);
            s.leader_id = None;
            let last_index = s.log.len() as u64;
            let last_term = s.log.last().map(|e| e.term).unwrap_or(0);
            (s.current_term, last_index, last_term)
        };

        let mut votes = 1usize;
        let quorum = self.quorum();

        for peer in &self.config.peers {
            if peer.id == self.config.node_id {
                continue;
            }
            let Ok(mut client) = raft_client(&peer.addr).await else {
                continue;
            };
            let Ok(resp) = client
                .request_vote(RequestVoteRequest {
                    term,
                    candidate_id: self.config.node_id,
                    last_log_index: last_index,
                    last_log_term: last_term,
                })
                .await
            else {
                continue;
            };
            let resp = resp.into_inner();

            if resp.term > term {
                let mut s = self.state.write();
                s.current_term = resp.term;
                s.role = Role::Follower;
                return Ok(());
            }
            if resp.vote_granted {
                votes += 1;
            }
        }

        if votes >= quorum {
            self.become_leader().await?;
        } else {
            self.state.write().role = Role::Follower;
        }
        Ok(())
    }

    async fn become_leader(&self) -> anyhow::Result<()> {
        {
            let mut s = self.state.write();
            s.role = Role::Leader;
            s.leader_id = Some(self.config.node_id);
            let len = s.log.len() + 1;
            s.next_index.clear();
            s.match_index.clear();
            for peer in &self.config.peers {
                if peer.id != self.config.node_id {
                    s.next_index.insert(peer.id, len);
                    s.match_index.insert(peer.id, 0);
                }
            }
        }
        self.replicate_to_majority().await
    }

    async fn send_heartbeats(&self) -> anyhow::Result<()> {
        self.replicate_to_majority().await
    }

    async fn replicate_to_majority(&self) -> anyhow::Result<()> {
        if !self.is_leader() {
            return Ok(());
        }

        let mut acks = 1usize;
        let quorum = self.quorum();
        let (term, leader_id, mut commit, log_len) = {
            let s = self.state.read();
            (
                s.current_term,
                self.config.node_id,
                s.commit_index as u64,
                s.log.len(),
            )
        };

        for peer in self.config.peers.clone() {
            if peer.id == self.config.node_id {
                continue;
            }
            let next = {
                let s = self.state.read();
                *s.next_index.get(&peer.id).unwrap_or(&1)
            };

            let (prev_index, prev_term, entries) = {
                let s = self.state.read();
                let prev_index = next.saturating_sub(1);
                let prev_term = if prev_index == 0 {
                    0
                } else {
                    s.log[prev_index - 1].term
                };
                let entries: Vec<LogRecord> = s
                    .log
                    .iter()
                    .skip(next.saturating_sub(1))
                    .map(|e| LogRecord {
                        term: e.term,
                        index: 0,
                        payload: bincode::serialize(&e.command).unwrap_or_default(),
                    })
                    .collect();
                (prev_index as u64, prev_term, entries)
            };

            let Ok(mut client) = raft_client(&peer.addr).await else {
                continue;
            };

            let Ok(resp) = client
                .append_entries(AppendEntriesRequest {
                    term,
                    leader_id,
                    prev_log_index: prev_index,
                    prev_log_term: prev_term,
                    entries,
                    leader_commit: commit,
                })
                .await
            else {
                continue;
            };

            let inner = resp.into_inner();
            if inner.term > term {
                let mut s = self.state.write();
                s.current_term = inner.term;
                s.role = Role::Follower;
                return Ok(());
            }
            if inner.success {
                let mut s = self.state.write();
                s.match_index.insert(peer.id, inner.match_index as usize);
                if let Some(ni) = s.next_index.get_mut(&peer.id) {
                    *ni = inner.match_index as usize + 1;
                }
                acks += 1;

                for i in (s.commit_index + 1)..=log_len {
                    let replicated = s
                        .match_index
                        .values()
                        .filter(|&&m| m >= i)
                        .count()
                        + 1;
                    if replicated >= quorum && s.log[i - 1].term == s.current_term {
                        s.commit_index = i;
                        commit = i as u64;
                    }
                }
            }
        }

        let commit_usize = self.state.read().commit_index;
        self.apply_through(commit_usize).await?;

        if acks >= quorum {
            Ok(())
        } else {
            Err(anyhow::anyhow!("failed to reach quorum ({acks}/{quorum})"))
        }
    }

    fn quorum(&self) -> usize {
        let n = self.config.peers.len().max(1);
        n / 2 + 1
    }
}

async fn raft_client(
    addr: &str,
) -> anyhow::Result<vectordb_proto::RaftServiceClient<tonic::transport::Channel>> {
    let url = if addr.starts_with("http") {
        addr.to_string()
    } else {
        format!("http://{addr}")
    };
    Ok(vectordb_proto::RaftServiceClient::connect(url).await?)
}
