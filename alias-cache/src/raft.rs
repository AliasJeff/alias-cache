//! OpenRaft type configuration and an in-memory storage vertical slice.

use crate::error::{Error, Result as CacheResult};
use crate::protocol::{Frame, Request};
use crate::state_machine::{ClientRequestId, Operation, StateMachine};
use openraft::error::{RPCError, ReplicationClosed, StreamingError, Unreachable};
use openraft::network::{Backoff, RPCOption};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, VoteRequest, VoteResponse,
};
use openraft::storage::{
    EntryResponder, IOFlushed, LogState, RaftLogStorage, RaftSnapshotBuilder, RaftStateMachine,
};
use openraft::{
    EntryPayload, OptionalSend, RaftLogReader, RaftNetworkFactory, RaftNetworkV2, RaftTypeConfig,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;
use std::fs::{File, OpenOptions};
use std::future::Future;
use std::io;
use std::io::{Read, Seek, SeekFrom, Write};
use std::ops::{Bound, RangeBounds};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, watch};
use tokio::time::interval;
use tokio::time::timeout;

pub type NodeId = u64;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Node {
    pub addr: String,
}

impl Default for Node {
    fn default() -> Self {
        Self {
            addr: "mem://default".to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum RaftRequest {
    Apply(Operation),
}

impl std::fmt::Display for RaftRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum RaftResponse {
    Applied(Frame),
    Noop,
}

static NEXT_CLIENT_REQUEST_ID: AtomicU64 = AtomicU64::new(1);

pub fn new_client_request_id() -> ClientRequestId {
    (u128::from(current_time_millis()) << 64)
        | u128::from(NEXT_CLIENT_REQUEST_ID.fetch_add(1, Ordering::Relaxed))
}

openraft::declare_raft_types!(
    pub TypeConfig:
        D = RaftRequest,
        R = RaftResponse,
        NodeId = NodeId,
        Node = Node,
);

pub type Entry = <TypeConfig as RaftTypeConfig>::Entry;
pub type LogId = openraft::alias::LogIdOf<TypeConfig>;
pub type Vote = openraft::alias::VoteOf<TypeConfig>;
pub type StoredMembership = openraft::alias::StoredMembershipOf<TypeConfig>;

#[derive(Debug, Clone, Serialize, Deserialize)]
enum NetworkRequest {
    Append(AppendEntriesRequest<TypeConfig>),
    Vote(VoteRequest<TypeConfig>),
    PreVote(VoteRequest<TypeConfig>),
    Client {
        request_id: ClientRequestId,
        request: Request,
    },
    Transaction {
        request_id: ClientRequestId,
        requests: Vec<Request>,
    },
    Snapshot {
        vote: Vote,
        snapshot: Vec<u8>,
        meta: openraft::SnapshotMeta<<TypeConfig as RaftTypeConfig>::LeaderId, NodeId, Node>,
    },
}

#[derive(Debug, Serialize, Deserialize)]
enum NetworkResponse {
    Append(Result<AppendEntriesResponse<TypeConfig>, String>),
    Vote(Result<VoteResponse<TypeConfig>, String>),
    Snapshot(Result<SnapshotResponse<TypeConfig>, String>),
    Client(Result<Frame, String>),
}

const NETWORK_MAX_FRAME: usize = 64 * 1024 * 1024;
const RAFT_SNAPSHOT_PAYLOAD_VERSION: u16 = 2;

#[derive(Debug, Serialize, Deserialize)]
struct RaftSnapshotPayload {
    version: u16,
    state: StateMachine,
    pending_operations: Vec<Operation>,
}

fn encode_raft_snapshot(
    state: &StateMachine,
    pending_operations: Vec<Operation>,
) -> io::Result<Vec<u8>> {
    bincode::serialize(&RaftSnapshotPayload {
        version: RAFT_SNAPSHOT_PAYLOAD_VERSION,
        state: state.clone(),
        pending_operations,
    })
    .map_err(io::Error::other)
}

fn decode_raft_snapshot(data: &[u8]) -> io::Result<(StateMachine, Vec<Operation>)> {
    let payload: RaftSnapshotPayload = bincode::deserialize(data).map_err(io::Error::other)?;
    if payload.version != RAFT_SNAPSHOT_PAYLOAD_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "unsupported raft snapshot payload version {}",
                payload.version
            ),
        ));
    }
    Ok((payload.state, payload.pending_operations))
}

const RAFT_LOG_MAGIC: [u8; 4] = *b"ARLG";
const RAFT_LOG_VERSION: u16 = 1;
const RAFT_LOG_HEADER_LEN: usize = 4 + 2 + 1 + 8;
const RAFT_LOG_CHECKSUM_LEN: usize = 4;

#[derive(Debug)]
pub struct FileLogStore {
    path: PathBuf,
    file: File,
    inner: Arc<Mutex<FileLog>>,
}

#[derive(Debug, Default)]
struct FileLog {
    entries: Vec<Entry>,
    vote: Option<Vote>,
    last_purged_log_id: Option<LogId>,
    committed: Option<LogId>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum FileRecord {
    Entry(Entry),
    Vote(Vote),
    Committed(Option<LogId>),
    Purged(LogId),
}

#[derive(Debug, Clone)]
pub struct FileLogReader {
    inner: Arc<Mutex<FileLog>>,
}

impl FileLogStore {
    pub fn open(path: impl AsRef<Path>) -> CacheResult<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|source| Error::io("create raft log directory", source))?;
        }
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&path)
            .map_err(|source| Error::io("open raft log", source))?;
        let inner = load_file_log(&mut file)?;
        Ok(Self {
            path,
            file,
            inner: Arc::new(Mutex::new(inner)),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn append_record(&mut self, file_record: &FileRecord) -> CacheResult<()> {
        let payload = bincode::serialize(file_record)
            .map_err(|error| Error::server(format!("serialize raft log record: {error}")))?;
        let mut record =
            Vec::with_capacity(RAFT_LOG_HEADER_LEN + payload.len() + RAFT_LOG_CHECKSUM_LEN);
        record.extend_from_slice(&RAFT_LOG_MAGIC);
        record.extend_from_slice(&RAFT_LOG_VERSION.to_le_bytes());
        record.push(file_record.kind());
        record.extend_from_slice(&(payload.len() as u64).to_le_bytes());
        record.extend_from_slice(&payload);
        record.extend_from_slice(&checksum(&record).to_le_bytes());
        self.file
            .write_all(&record)
            .map_err(|source| Error::io("append raft log entry", source))?;
        self.file
            .sync_data()
            .map_err(|source| Error::io("sync raft log entry", source))
    }
}

fn load_file_log(file: &mut File) -> CacheResult<FileLog> {
    file.seek(SeekFrom::Start(0))
        .map_err(|source| Error::io("seek raft log", source))?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)
        .map_err(|source| Error::io("read raft log", source))?;
    if bytes.is_empty() {
        return Ok(FileLog::default());
    }
    let mut offset = 0;
    let mut state = FileLog::default();
    while offset < bytes.len() {
        let start = offset;
        if bytes.len() - offset < RAFT_LOG_HEADER_LEN {
            return Err(Error::server("raft log header is truncated"));
        }
        if bytes[offset..offset + 4] != RAFT_LOG_MAGIC {
            return Err(Error::server("raft log magic is invalid"));
        }
        offset += 4;
        let version = u16::from_le_bytes(bytes[offset..offset + 2].try_into().unwrap());
        offset += 2;
        if version != RAFT_LOG_VERSION {
            return Err(Error::server("raft log version is unsupported"));
        }
        let kind = bytes[offset];
        offset += 1;
        let length = u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap());
        let length = usize::try_from(length)
            .map_err(|_| Error::server("raft log payload length overflows"))?;
        offset += 8;
        let end = offset
            .checked_add(length)
            .and_then(|value| value.checked_add(RAFT_LOG_CHECKSUM_LEN))
            .ok_or_else(|| Error::server("raft log record size overflows"))?;
        if end > bytes.len() {
            return Err(Error::server("raft log record is truncated"));
        }
        let payload_end = offset + length;
        let expected = u32::from_le_bytes(bytes[payload_end..end].try_into().unwrap());
        if checksum(&bytes[start..payload_end]) != expected {
            return Err(Error::server("raft log checksum mismatch"));
        }
        let record: FileRecord = bincode::deserialize(&bytes[offset..payload_end])
            .map_err(|error| Error::server(format!("raft log payload is invalid: {error}")))?;
        if kind != record.kind() {
            return Err(Error::server("raft log record kind is invalid"));
        }
        match record {
            FileRecord::Entry(entry) => state.entries.push(entry),
            FileRecord::Vote(vote) => state.vote = Some(vote),
            FileRecord::Committed(committed) => state.committed = committed,
            FileRecord::Purged(log_id) => {
                state
                    .entries
                    .retain(|entry| entry.log_id.index > log_id.index);
                state.last_purged_log_id = Some(log_id);
            }
        }
        offset = end;
    }
    Ok(state)
}

impl FileRecord {
    fn kind(&self) -> u8 {
        match self {
            Self::Entry(_) => 1,
            Self::Vote(_) => 2,
            Self::Committed(_) => 3,
            Self::Purged(_) => 4,
        }
    }
}

fn checksum(bytes: &[u8]) -> u32 {
    let mut hash = 2_166_136_261_u32;
    for byte in bytes {
        hash ^= u32::from(*byte);
        hash = hash.wrapping_mul(16_777_619);
    }
    hash
}

#[derive(Debug, Default)]
pub struct MemLogStore {
    inner: Arc<Mutex<MemLog>>,
}

#[derive(Debug, Default)]
struct MemLog {
    entries: Vec<Entry>,
    vote: Option<Vote>,
    last_purged_log_id: Option<LogId>,
    committed: Option<LogId>,
}

#[derive(Debug, Clone)]
pub struct MemLogReader {
    inner: Arc<Mutex<MemLog>>,
}

impl MemLogStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl MemLog {
    fn first_index(&self) -> u64 {
        self.entries
            .first()
            .map(|entry| entry.log_id.index)
            .unwrap_or_else(|| {
                self.last_purged_log_id
                    .map(|log_id| log_id.index + 1)
                    .unwrap_or(0)
            })
    }

    fn last_log_id(&self) -> Option<LogId> {
        self.entries
            .last()
            .map(|entry| entry.log_id)
            .or(self.last_purged_log_id)
    }
}

impl RaftLogReader<TypeConfig> for MemLogReader {
    async fn try_get_log_entries<RB>(&mut self, range: RB) -> Result<Vec<Entry>, io::Error>
    where
        RB: RangeBounds<u64> + Clone + Debug + OptionalSend,
    {
        let start = match range.start_bound() {
            Bound::Included(index) => *index,
            Bound::Excluded(index) => index.saturating_add(1),
            Bound::Unbounded => self.inner.lock().await.first_index(),
        };
        let end = match range.end_bound() {
            Bound::Included(index) => index.saturating_add(1),
            Bound::Excluded(index) => *index,
            Bound::Unbounded => u64::MAX,
        };

        let inner = self.inner.lock().await;
        Ok(inner
            .entries
            .iter()
            .filter(|entry| entry.log_id.index >= start && entry.log_id.index < end)
            .cloned()
            .collect())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote>, io::Error> {
        Ok(self.inner.lock().await.vote)
    }
}

impl RaftLogReader<TypeConfig> for FileLogReader {
    async fn try_get_log_entries<RB>(&mut self, range: RB) -> Result<Vec<Entry>, io::Error>
    where
        RB: RangeBounds<u64> + Clone + Debug + OptionalSend,
    {
        let start = match range.start_bound() {
            Bound::Included(index) => *index,
            Bound::Excluded(index) => index.saturating_add(1),
            Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            Bound::Included(index) => index.saturating_add(1),
            Bound::Excluded(index) => *index,
            Bound::Unbounded => u64::MAX,
        };
        let inner = self.inner.lock().await;
        Ok(inner
            .entries
            .iter()
            .filter(|entry| entry.log_id.index >= start && entry.log_id.index < end)
            .cloned()
            .collect())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote>, io::Error> {
        Ok(self.inner.lock().await.vote)
    }
}

impl RaftLogStorage<TypeConfig> for FileLogStore {
    type LogReader = FileLogReader;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, io::Error> {
        let inner = self.inner.lock().await;
        Ok(LogState {
            last_purged_log_id: inner.last_purged_log_id,
            last_log_id: inner
                .entries
                .last()
                .map(|entry| entry.log_id)
                .or(inner.last_purged_log_id),
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        FileLogReader {
            inner: Arc::clone(&self.inner),
        }
    }

    async fn save_vote(&mut self, vote: &Vote) -> Result<(), io::Error> {
        self.inner.lock().await.vote = Some(*vote);
        self.append_record(&FileRecord::Vote(*vote))
            .map_err(|error| io::Error::other(error.to_string()))?;
        Ok(())
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: IOFlushed<TypeConfig>,
    ) -> Result<(), io::Error>
    where
        I: IntoIterator<Item = Entry> + Send,
    {
        let entries: Vec<Entry> = entries.into_iter().collect();
        for entry in &entries {
            self.append_record(&FileRecord::Entry(entry.clone()))
                .map_err(|error| io::Error::other(error.to_string()))?;
        }
        self.inner.lock().await.entries.extend(entries);
        callback.io_completed(Ok(()));
        Ok(())
    }

    async fn truncate_after(&mut self, last_log_id: Option<LogId>) -> Result<(), io::Error> {
        let mut inner = self.inner.lock().await;
        if let Some(last_log_id) = last_log_id {
            inner
                .entries
                .retain(|entry| entry.log_id.index <= last_log_id.index);
        } else {
            inner.entries.clear();
        }
        rewrite_file(&mut self.file, &inner).map_err(|error| io::Error::other(error.to_string()))
    }

    async fn purge(&mut self, log_id: LogId) -> Result<(), io::Error> {
        let mut inner = self.inner.lock().await;
        inner
            .entries
            .retain(|entry| entry.log_id.index > log_id.index);
        inner.last_purged_log_id = Some(log_id);
        rewrite_file(&mut self.file, &inner)
            .map_err(|error| io::Error::other(error.to_string()))?;
        drop(inner);
        self.append_record(&FileRecord::Purged(log_id))
            .map_err(|error| io::Error::other(error.to_string()))
    }

    async fn save_committed(&mut self, committed: Option<LogId>) -> Result<(), io::Error> {
        self.inner.lock().await.committed = committed;
        self.append_record(&FileRecord::Committed(committed))
            .map_err(|error| io::Error::other(error.to_string()))?;
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId>, io::Error> {
        Ok(self.inner.lock().await.committed)
    }
}

fn rewrite_file(file: &mut File, state: &FileLog) -> CacheResult<()> {
    file.set_len(0)
        .map_err(|source| Error::io("truncate raft log", source))?;
    file.seek(SeekFrom::Start(0))
        .map_err(|source| Error::io("seek raft log for rewrite", source))?;
    let mut records = Vec::with_capacity(
        state.entries.len()
            + usize::from(state.vote.is_some())
            + usize::from(state.committed.is_some())
            + usize::from(state.last_purged_log_id.is_some()),
    );
    if let Some(vote) = state.vote {
        records.push(FileRecord::Vote(vote));
    }
    if let Some(log_id) = state.last_purged_log_id {
        records.push(FileRecord::Purged(log_id));
    }
    records.extend(state.entries.iter().cloned().map(FileRecord::Entry));
    if state.committed.is_some() {
        records.push(FileRecord::Committed(state.committed));
    }
    for record_value in records {
        let payload = bincode::serialize(&record_value)
            .map_err(|error| Error::server(format!("serialize raft log record: {error}")))?;
        let mut record =
            Vec::with_capacity(RAFT_LOG_HEADER_LEN + payload.len() + RAFT_LOG_CHECKSUM_LEN);
        record.extend_from_slice(&RAFT_LOG_MAGIC);
        record.extend_from_slice(&RAFT_LOG_VERSION.to_le_bytes());
        record.push(record_value.kind());
        record.extend_from_slice(&(payload.len() as u64).to_le_bytes());
        record.extend_from_slice(&payload);
        record.extend_from_slice(&checksum(&record).to_le_bytes());
        file.write_all(&record)
            .map_err(|source| Error::io("rewrite raft log", source))?;
    }
    file.sync_all()
        .map_err(|source| Error::io("sync rewritten raft log", source))
}

impl RaftLogStorage<TypeConfig> for MemLogStore {
    type LogReader = MemLogReader;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, io::Error> {
        let inner = self.inner.lock().await;
        Ok(LogState {
            last_purged_log_id: inner.last_purged_log_id,
            last_log_id: inner.last_log_id(),
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        MemLogReader {
            inner: Arc::clone(&self.inner),
        }
    }

    async fn save_vote(&mut self, vote: &Vote) -> Result<(), io::Error> {
        self.inner.lock().await.vote = Some(*vote);
        Ok(())
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: IOFlushed<TypeConfig>,
    ) -> Result<(), io::Error>
    where
        I: IntoIterator<Item = Entry> + Send,
    {
        self.inner.lock().await.entries.extend(entries);
        callback.io_completed(Ok(()));
        Ok(())
    }

    async fn truncate_after(&mut self, last_log_id: Option<LogId>) -> Result<(), io::Error> {
        if let Some(last_log_id) = last_log_id {
            self.inner
                .lock()
                .await
                .entries
                .retain(|entry| entry.log_id.index <= last_log_id.index);
        } else {
            self.inner.lock().await.entries.clear();
        }
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId) -> Result<(), io::Error> {
        let mut inner = self.inner.lock().await;
        inner
            .entries
            .retain(|entry| entry.log_id.index > log_id.index);
        inner.last_purged_log_id = Some(log_id);
        Ok(())
    }

    async fn save_committed(&mut self, committed: Option<LogId>) -> Result<(), io::Error> {
        self.inner.lock().await.committed = committed;
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId>, io::Error> {
        Ok(self.inner.lock().await.committed)
    }
}

#[derive(Debug, Clone, Default)]
pub struct MemStateMachine {
    inner: Arc<Mutex<MemState>>,
}

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq)]
enum SnapshotState {
    #[default]
    End,
    Start,
    Tail,
}

#[derive(Debug, Default)]
struct MemState {
    state: StateMachine,
    last_applied: Option<LogId>,
    membership: StoredMembership,
    snapshot: Option<StoredSnapshot>,
    snapshot_state: SnapshotState,
    snapshot_queue: Vec<Operation>,
}

#[derive(Debug, Clone)]
struct StoredSnapshot {
    data: Vec<u8>,
    last_log_id: Option<LogId>,
    membership: StoredMembership,
}

impl MemStateMachine {
    pub async fn state(&self) -> StateMachine {
        self.inner.lock().await.state.clone()
    }

    pub async fn state_with_applied_index(&self) -> (StateMachine, Option<LogId>) {
        let inner = self.inner.lock().await;
        (inner.state.clone(), inner.last_applied)
    }
}

pub async fn execute_raft_request(raft: &RaftHandle, request: Request) -> CacheResult<Frame> {
    raft.ensure_linearizable(openraft::ReadPolicy::ReadIndex)
        .await
        .map_err(|error| Error::server(format!("raft read barrier failed: {error}")))?;
    let (operation, response) = raft
        .with_state_machine(move |state_machine| {
            Box::pin(async move { state_machine.state().await.prepare_request(request) })
        })
        .await
        .map_err(|error| Error::server(format!("read raft state machine: {error}")))??;
    let Some(operation) = operation else {
        return Ok(response);
    };

    let result = raft
        .client_write(RaftRequest::Apply(operation))
        .await
        .map_err(|error| {
            if let Some(forward) = error.forward_to_leader() {
                if let Some(node) = &forward.leader_node {
                    return Error::server(format!(
                        "not leader; retry at raft leader {}",
                        node.addr
                    ));
                }
                return Error::server("not leader; raft leader is not known");
            }
            Error::server(format!("raft write failed: {error}"))
        })?;
    match result.data {
        RaftResponse::Applied(response) => Ok(response),
        RaftResponse::Noop => Err(Error::server("raft write produced no response")),
    }
}

#[derive(Debug, PartialEq)]
pub enum ClientRequestOutcome {
    Applied(Frame),
    Redirect(String),
}

pub async fn execute_client_request(
    raft: &RaftHandle,
    request: Request,
) -> Result<ClientRequestOutcome, String> {
    execute_client_request_with_id(raft, new_client_request_id(), request).await
}

pub async fn execute_client_request_with_id(
    raft: &RaftHandle,
    request_id: ClientRequestId,
    request: Request,
) -> Result<ClientRequestOutcome, String> {
    let write_time = current_time_millis();
    let (operation, response) = raft
        .with_state_machine(move |state_machine| {
            Box::pin(async move {
                let state = state_machine.state().await;
                state.prepare_request_at_with_id(request, write_time, Some(request_id))
            })
        })
        .await
        .map_err(|error| format!("read raft state machine: {error}"))?
        .map_err(|error| error.to_string())?;
    let Some(operation) = operation else {
        raft.ensure_linearizable(openraft::ReadPolicy::ReadIndex)
            .await
            .map_err(|error| format!("raft read barrier failed: {error}"))?;
        return Ok(ClientRequestOutcome::Applied(response));
    };
    let result = raft.client_write(RaftRequest::Apply(operation)).await;
    match result {
        Ok(response) => match response.data {
            RaftResponse::Applied(response) => Ok(ClientRequestOutcome::Applied(response)),
            RaftResponse::Noop => Err("raft write produced no response".to_string()),
        },
        Err(error) => {
            if let Some(forward) = error.forward_to_leader() {
                if let Some(node) = &forward.leader_node {
                    return Ok(ClientRequestOutcome::Redirect(node.addr.clone()));
                }
                return Err("raft leader is not known".to_string());
            }
            Err(format!("raft write failed: {error}"))
        }
    }
}

pub async fn execute_transaction_request(
    raft: &RaftHandle,
    requests: Vec<Request>,
) -> Result<ClientRequestOutcome, String> {
    execute_transaction_request_with_id(raft, new_client_request_id(), requests).await
}

pub async fn execute_transaction_request_with_id(
    raft: &RaftHandle,
    request_id: ClientRequestId,
    requests: Vec<Request>,
) -> Result<ClientRequestOutcome, String> {
    let write_time = current_time_millis();
    let operation = raft
        .with_state_machine(move |state_machine| {
            Box::pin(async move {
                let state = state_machine.state().await;
                state
                    .prepare_transaction_with_id(&requests, write_time, Some(request_id))
                    .map(|(operation, _)| operation)
            })
        })
        .await
        .map_err(|error| format!("read raft state machine: {error}"))?
        .map_err(|error| error.to_string())?;
    match raft.client_write(RaftRequest::Apply(operation)).await {
        Ok(result) => match result.data {
            RaftResponse::Applied(response) => Ok(ClientRequestOutcome::Applied(response)),
            RaftResponse::Noop => Err("transaction produced no response".to_string()),
        },
        Err(error) => {
            if let Some(forward) = error.forward_to_leader() {
                if let Some(node) = &forward.leader_node {
                    return Ok(ClientRequestOutcome::Redirect(node.addr.clone()));
                }
                return Err("raft leader is not known".to_string());
            }
            Err(format!("transaction failed: {error}"))
        }
    }
}

pub async fn advance_raft_time(raft: &RaftHandle) -> Result<(), String> {
    let write_time = current_time_millis();
    let operation = raft
        .with_state_machine(move |state_machine| {
            Box::pin(async move {
                let state = state_machine.state().await;
                state.prepare_time_advance(write_time)
            })
        })
        .await
        .map_err(|error| format!("read raft state machine: {error}"))?;
    raft.client_write(RaftRequest::Apply(operation))
        .await
        .map_err(|error| format!("advance raft logical time: {error}"))?;
    Ok(())
}

pub async fn run_leader_time_worker(
    raft: Arc<RaftHandle>,
    node_id: NodeId,
    shutdown: watch::Receiver<bool>,
    period: Duration,
) {
    let mut ticker = interval(period);
    let mut shutdown = shutdown;
    loop {
        tokio::select! {
            _ = ticker.tick() => {
                if raft.current_leader().await != Some(node_id) {
                    continue;
                }
                if let Err(error) = advance_raft_time(&raft).await {
                    tracing::debug!(node_id, %error, "leader time advancement skipped");
                }
            }
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return;
                }
            }
        }
    }
}

pub async fn run_expiration_worker(
    raft: Arc<RaftHandle>,
    node_id: NodeId,
    shutdown: watch::Receiver<bool>,
    period: Duration,
) {
    let mut ticker = interval(period);
    let mut shutdown = shutdown;
    loop {
        tokio::select! {
            _ = ticker.tick() => {
                if raft.current_leader().await != Some(node_id) {
                    continue;
                }
                let candidates = match raft
                    .with_state_machine(|state_machine| {
                        Box::pin(async move { state_machine.state().await.expired_entries() })
                    })
                    .await
                {
                    Ok(candidates) => candidates,
                    Err(error) => {
                        tracing::debug!(node_id, %error, "expiration scan skipped");
                        continue;
                    }
                };
                for (key, expire_at) in candidates {
                    let operation = Operation::DeleteIfExpired { key, expire_at };
                    if let Err(error) = raft.client_write(RaftRequest::Apply(operation)).await {
                        tracing::debug!(node_id, %error, "expiration cleanup skipped");
                        break;
                    }
                }
            }
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return;
                }
            }
        }
    }
}

fn current_time_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

impl RaftStateMachine<TypeConfig> for MemStateMachine {
    type SnapshotData = std::io::Cursor<Vec<u8>>;
    type SnapshotBuilder = Self;

    async fn applied_state(&mut self) -> Result<(Option<LogId>, StoredMembership), io::Error> {
        let inner = self.inner.lock().await;
        Ok((inner.last_applied, inner.membership.clone()))
    }

    async fn apply<Strm>(&mut self, mut entries: Strm) -> Result<(), io::Error>
    where
        Strm: tokio_stream::Stream<Item = Result<EntryResponder<TypeConfig>, io::Error>>
            + Unpin
            + OptionalSend,
    {
        while let Some((entry, responder)) = tokio_stream::StreamExt::try_next(&mut entries).await?
        {
            let mut inner = self.inner.lock().await;
            let snapshot_operation = match &entry.payload {
                EntryPayload::Normal(RaftRequest::Apply(operation)) => Some(operation.clone()),
                EntryPayload::Blank | EntryPayload::Membership(_) => None,
            };
            if let EntryPayload::Normal(RaftRequest::Apply(operation)) = &entry.payload {
                inner.state.advance_logical_time(operation.write_time());
            }
            inner.last_applied = Some(entry.log_id);
            let response = match entry.payload {
                EntryPayload::Blank => RaftResponse::Noop,
                EntryPayload::Normal(RaftRequest::Apply(operation)) => {
                    let response = match inner.state.apply(operation) {
                        crate::state_machine::ApplyResult::Response(response) => response,
                        crate::state_machine::ApplyResult::NoResponse => Frame::Null,
                    };
                    RaftResponse::Applied(response)
                }
                EntryPayload::Membership(membership) => {
                    inner.membership = StoredMembership::new(Some(entry.log_id), membership);
                    RaftResponse::Noop
                }
            };
            if inner.snapshot_state == SnapshotState::Start {
                if let Some(operation) = snapshot_operation {
                    inner.snapshot_queue.push(operation);
                }
            }
            drop(inner);
            if let Some(responder) = responder {
                responder.send(response);
            }
        }
        Ok(())
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn try_create_snapshot_builder(&mut self, force: bool) -> Option<Self::SnapshotBuilder> {
        let inner = self.inner.lock().await;
        (force || inner.snapshot_state == SnapshotState::End).then(|| self.clone())
    }

    async fn install_snapshot(
        &mut self,
        meta: &openraft::SnapshotMeta<<TypeConfig as RaftTypeConfig>::LeaderId, NodeId, Node>,
        mut snapshot: Self::SnapshotData,
    ) -> Result<(), io::Error> {
        let mut data = Vec::new();
        std::io::Read::read_to_end(&mut snapshot, &mut data)?;
        let (mut state, pending_operations) = decode_raft_snapshot(&data)?;
        for operation in pending_operations {
            state.advance_logical_time(operation.write_time());
            let _ = state.apply(operation);
        }
        let mut inner = self.inner.lock().await;
        inner.state = state;
        inner.last_applied = meta.last_log_id;
        inner.membership = meta.last_membership.clone();
        inner.snapshot_state = SnapshotState::End;
        inner.snapshot_queue.clear();
        inner.snapshot = Some(StoredSnapshot {
            data,
            last_log_id: meta.last_log_id,
            membership: meta.last_membership.clone(),
        });
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<
        Option<
            openraft::Snapshot<
                <TypeConfig as RaftTypeConfig>::LeaderId,
                NodeId,
                Node,
                Self::SnapshotData,
            >,
        >,
        io::Error,
    > {
        let inner = self.inner.lock().await;
        Ok(inner.snapshot.clone().map(|snapshot| openraft::Snapshot {
            meta: openraft::SnapshotMeta {
                last_log_id: snapshot.last_log_id,
                last_membership: snapshot.membership,
            },
            snapshot: std::io::Cursor::new(snapshot.data),
        }))
    }
}

impl RaftSnapshotBuilder<TypeConfig> for MemStateMachine {
    type SnapshotData = std::io::Cursor<Vec<u8>>;

    async fn build_snapshot(
        &mut self,
    ) -> Result<
        openraft::Snapshot<
            <TypeConfig as RaftTypeConfig>::LeaderId,
            NodeId,
            Node,
            Self::SnapshotData,
        >,
        io::Error,
    > {
        let base_state = {
            let mut inner = self.inner.lock().await;
            if inner.snapshot_state != SnapshotState::End {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "raft snapshot is already in progress",
                ));
            }
            inner.snapshot_state = SnapshotState::Start;
            inner.state.clone()
        };

        let result = self.build_snapshot_from_base(base_state).await;
        if result.is_err() {
            self.inner.lock().await.snapshot_state = SnapshotState::End;
        }
        result
    }
}

impl MemStateMachine {
    async fn build_snapshot_from_base(
        &self,
        base_state: StateMachine,
    ) -> Result<
        openraft::Snapshot<
            <TypeConfig as RaftTypeConfig>::LeaderId,
            NodeId,
            Node,
            std::io::Cursor<Vec<u8>>,
        >,
        io::Error,
    > {
        let (pending_operations, last_log_id, membership) = {
            let mut inner = self.inner.lock().await;
            inner.snapshot_state = SnapshotState::Tail;
            (
                std::mem::take(&mut inner.snapshot_queue),
                inner.last_applied,
                inner.membership.clone(),
            )
        };
        let data = encode_raft_snapshot(&base_state, pending_operations)?;
        let mut inner = self.inner.lock().await;
        inner.snapshot = Some(StoredSnapshot {
            data: data.clone(),
            last_log_id,
            membership: membership.clone(),
        });
        inner.snapshot_state = SnapshotState::End;
        Ok(openraft::Snapshot {
            meta: openraft::SnapshotMeta {
                last_log_id,
                last_membership: membership,
            },
            snapshot: std::io::Cursor::new(data),
        })
    }
}

pub type SharedMemStateMachine = Arc<Mutex<MemStateMachine>>;

pub type RaftHandle = openraft::Raft<TypeConfig, MemStateMachine>;
pub type PeerMap = Arc<Mutex<BTreeMap<NodeId, RaftHandle>>>;

#[derive(Clone)]
pub struct MemNetworkFactory {
    peers: PeerMap,
}

#[derive(Clone)]
pub struct MemNetwork {
    target: NodeId,
    peers: PeerMap,
}

impl MemNetworkFactory {
    fn new(peers: PeerMap) -> Self {
        Self { peers }
    }
}

impl RaftNetworkFactory<TypeConfig> for MemNetworkFactory {
    type Network = MemNetwork;

    async fn new_client(&mut self, target: NodeId, _node: &Node) -> Self::Network {
        MemNetwork {
            target,
            peers: Arc::clone(&self.peers),
        }
    }
}

impl MemNetwork {
    async fn target(&self) -> Result<RaftHandle, RPCError<TypeConfig>> {
        self.peers
            .lock()
            .await
            .get(&self.target)
            .cloned()
            .ok_or_else(|| {
                RPCError::Unreachable(Unreachable::from_string(format!(
                    "raft node {} is not registered",
                    self.target
                )))
            })
    }
}

#[derive(Clone)]
pub struct TcpNetworkControl {
    blocked_links: Arc<Mutex<BTreeSet<(NodeId, NodeId)>>>,
}

impl TcpNetworkControl {
    pub async fn block_link(&self, from: NodeId, to: NodeId) {
        self.blocked_links.lock().await.insert((from, to));
    }

    pub async fn unblock_link(&self, from: NodeId, to: NodeId) {
        self.blocked_links.lock().await.remove(&(from, to));
    }

    pub async fn block_bidirectional(&self, first: NodeId, second: NodeId) {
        let mut blocked_links = self.blocked_links.lock().await;
        blocked_links.insert((first, second));
        blocked_links.insert((second, first));
    }

    pub async fn unblock_bidirectional(&self, first: NodeId, second: NodeId) {
        let mut blocked_links = self.blocked_links.lock().await;
        blocked_links.remove(&(first, second));
        blocked_links.remove(&(second, first));
    }

    async fn is_blocked(&self, from: NodeId, to: NodeId) -> bool {
        self.blocked_links.lock().await.contains(&(from, to))
    }
}

impl Default for TcpNetworkControl {
    fn default() -> Self {
        Self {
            blocked_links: Arc::new(Mutex::new(BTreeSet::new())),
        }
    }
}

#[derive(Clone)]
pub struct TcpNetworkFactory {
    source: NodeId,
    control: TcpNetworkControl,
}

#[derive(Clone)]
pub struct TcpNetwork {
    source: NodeId,
    target: NodeId,
    address: String,
    control: TcpNetworkControl,
}

impl TcpNetworkFactory {
    fn new(source: NodeId, control: TcpNetworkControl) -> Self {
        Self { source, control }
    }
}

impl RaftNetworkFactory<TypeConfig> for TcpNetworkFactory {
    type Network = TcpNetwork;

    async fn new_client(&mut self, target: NodeId, node: &Node) -> Self::Network {
        TcpNetwork {
            source: self.source,
            target,
            address: node.addr.clone(),
            control: self.control.clone(),
        }
    }
}

impl TcpNetwork {
    async fn send(
        &self,
        request: NetworkRequest,
        option: &RPCOption,
    ) -> Result<NetworkResponse, RPCError<TypeConfig>> {
        if self.control.is_blocked(self.source, self.target).await {
            return Err(RPCError::Unreachable(Unreachable::from_string(format!(
                "raft network link {} -> {} is blocked",
                self.source, self.target
            ))));
        }
        send_rpc(&self.address, request, option).await
    }
}

async fn connect(address: &str, option: &RPCOption) -> Result<TcpStream, RPCError<TypeConfig>> {
    timeout(option.soft_ttl(), TcpStream::connect(address))
        .await
        .map_err(|error| RPCError::Unreachable(Unreachable::from_string(error.to_string())))?
        .map_err(|error| RPCError::Unreachable(Unreachable::from_string(error.to_string())))
}

async fn send_rpc(
    address: &str,
    request: NetworkRequest,
    option: &RPCOption,
) -> Result<NetworkResponse, RPCError<TypeConfig>> {
    let mut stream = connect(address, option).await?;
    let payload = bincode::serialize(&request).map_err(|error| {
        RPCError::Network(openraft::error::NetworkError::from_string(
            error.to_string(),
        ))
    })?;
    if payload.len() > NETWORK_MAX_FRAME {
        return Err(RPCError::Network(
            openraft::error::NetworkError::from_string(
                "raft rpc request exceeds maximum frame size",
            ),
        ));
    }
    let length = u32::try_from(payload.len()).map_err(|_| {
        RPCError::Network(openraft::error::NetworkError::from_string(
            "raft rpc request length overflows u32",
        ))
    })?;
    stream
        .write_all(&length.to_be_bytes())
        .await
        .map_err(|error| RPCError::Unreachable(Unreachable::from_string(error.to_string())))?;
    stream
        .write_all(&payload)
        .await
        .map_err(|error| RPCError::Unreachable(Unreachable::from_string(error.to_string())))?;
    stream
        .flush()
        .await
        .map_err(|error| RPCError::Unreachable(Unreachable::from_string(error.to_string())))?;

    let mut header = [0_u8; 4];
    timeout(option.soft_ttl(), stream.read_exact(&mut header))
        .await
        .map_err(|error| RPCError::Unreachable(Unreachable::from_string(error.to_string())))?
        .map_err(|error| RPCError::Unreachable(Unreachable::from_string(error.to_string())))?;
    let length = u32::from_be_bytes(header) as usize;
    if length > NETWORK_MAX_FRAME {
        return Err(RPCError::Network(
            openraft::error::NetworkError::from_string(
                "raft rpc response exceeds maximum frame size",
            ),
        ));
    }
    let mut payload = vec![0_u8; length];
    timeout(option.soft_ttl(), stream.read_exact(&mut payload))
        .await
        .map_err(|error| RPCError::Unreachable(Unreachable::from_string(error.to_string())))?
        .map_err(|error| RPCError::Unreachable(Unreachable::from_string(error.to_string())))?;
    bincode::deserialize(&payload).map_err(|error| {
        RPCError::Network(openraft::error::NetworkError::from_string(
            error.to_string(),
        ))
    })
}

pub async fn send_client_request(
    address: &str,
    request: Request,
    option: &RPCOption,
) -> CacheResult<Frame> {
    send_client_request_with_id(address, new_client_request_id(), request, option).await
}

pub async fn send_client_request_with_id(
    address: &str,
    request_id: ClientRequestId,
    request: Request,
    option: &RPCOption,
) -> CacheResult<Frame> {
    let response = send_rpc(
        address,
        NetworkRequest::Client {
            request_id,
            request,
        },
        option,
    )
    .await
    .map_err(|error| Error::server(format!("send client request: {error}")))?;
    match response {
        NetworkResponse::Client(Ok(frame)) => Ok(frame),
        NetworkResponse::Client(Err(error)) => Err(Error::server(error)),
        _ => Err(Error::server("unexpected client rpc response")),
    }
}

pub async fn send_transaction_request(
    address: &str,
    requests: Vec<Request>,
    option: &RPCOption,
) -> CacheResult<Frame> {
    send_transaction_request_with_id(address, new_client_request_id(), requests, option).await
}

pub async fn send_transaction_request_with_id(
    address: &str,
    request_id: ClientRequestId,
    requests: Vec<Request>,
    option: &RPCOption,
) -> CacheResult<Frame> {
    let response = send_rpc(
        address,
        NetworkRequest::Transaction {
            request_id,
            requests,
        },
        option,
    )
    .await
    .map_err(|error| Error::server(format!("send transaction request: {error}")))?;
    match response {
        NetworkResponse::Client(Ok(frame)) => Ok(frame),
        NetworkResponse::Client(Err(error)) => Err(Error::server(error)),
        _ => Err(Error::server("unexpected transaction rpc response")),
    }
}

fn rpc_response_error(error: String) -> RPCError<TypeConfig> {
    RPCError::Network(openraft::error::NetworkError::from_string(error))
}

impl RaftNetworkV2<TypeConfig> for TcpNetwork {
    type SnapshotData = std::io::Cursor<Vec<u8>>;

    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<TypeConfig>, RPCError<TypeConfig>> {
        match self.send(NetworkRequest::Append(rpc), &option).await? {
            NetworkResponse::Append(response) => response.map_err(rpc_response_error),
            _ => Err(rpc_response_error(
                "unexpected raft rpc response".to_string(),
            )),
        }
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<VoteResponse<TypeConfig>, RPCError<TypeConfig>> {
        match self.send(NetworkRequest::Vote(rpc), &option).await? {
            NetworkResponse::Vote(response) => response.map_err(rpc_response_error),
            _ => Err(rpc_response_error(
                "unexpected raft rpc response".to_string(),
            )),
        }
    }

    async fn pre_vote(
        &mut self,
        rpc: VoteRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<VoteResponse<TypeConfig>, RPCError<TypeConfig>> {
        match self.send(NetworkRequest::PreVote(rpc), &option).await? {
            NetworkResponse::Vote(response) => response.map_err(rpc_response_error),
            _ => Err(rpc_response_error(
                "unexpected raft rpc response".to_string(),
            )),
        }
    }

    async fn full_snapshot(
        &mut self,
        vote: Vote,
        mut snapshot: openraft::alias::SnapshotOf<TypeConfig, Self::SnapshotData>,
        cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        option: RPCOption,
    ) -> Result<SnapshotResponse<TypeConfig>, StreamingError<TypeConfig>> {
        let mut data = Vec::new();
        tokio::select! {
            closed = cancel => return Err(StreamingError::Closed(closed)),
            result = async {
                std::io::Read::read_to_end(&mut snapshot.snapshot, &mut data)
            } => {
                result.map_err(|error| StreamingError::Network(
                    openraft::error::NetworkError::from_string(error.to_string())
                ))?;
            }
        }
        let response = self
            .send(
                NetworkRequest::Snapshot {
                    vote,
                    snapshot: data,
                    meta: snapshot.meta,
                },
                &option,
            )
            .await
            .map_err(StreamingError::from)?;
        match response {
            NetworkResponse::Snapshot(response) => response.map_err(|error| {
                StreamingError::Network(openraft::error::NetworkError::from_string(error))
            }),
            _ => Err(StreamingError::Network(
                openraft::error::NetworkError::from_string("unexpected raft rpc response"),
            )),
        }
    }

    fn backoff(&self) -> Option<Backoff> {
        Some(Backoff::new(std::iter::repeat(Duration::from_millis(25))))
    }
}

pub async fn serve_raft_rpc(
    listener: TcpListener,
    raft: RaftHandle,
    shutdown: tokio::sync::watch::Receiver<bool>,
) -> CacheResult<()> {
    let shutdown = Arc::new(shutdown);
    loop {
        tokio::select! {
            result = listener.accept() => {
                let (stream, _) = result.map_err(|error| Error::io("accept raft rpc connection", error))?;
                let raft = raft.clone();
                let shutdown = shutdown.clone();
                tokio::spawn(async move {
                    if let Err(error) = handle_raft_rpc(stream, raft, shutdown).await {
                        tracing::debug!(%error, "raft rpc connection failed");
                    }
                });
            }
            result = wait_for_raft_shutdown(shutdown.clone()) => {
                result?;
                return Ok(());
            }
        }
    }
}

async fn wait_for_raft_shutdown(
    shutdown: Arc<tokio::sync::watch::Receiver<bool>>,
) -> CacheResult<()> {
    let mut receiver = shutdown.as_ref().clone();
    receiver
        .wait_for(|value| *value)
        .await
        .map(|_| ())
        .map_err(|_| Error::server("raft shutdown signal was closed"))
}

async fn handle_raft_rpc(
    mut stream: TcpStream,
    raft: RaftHandle,
    shutdown: Arc<tokio::sync::watch::Receiver<bool>>,
) -> CacheResult<()> {
    let request = tokio::select! {
        result = read_network_message(&mut stream) => result?,
        result = wait_for_raft_shutdown(shutdown) => {
            result?;
            return Ok(());
        }
    };
    let response = match request {
        NetworkRequest::Append(request) => NetworkResponse::Append(
            raft.append_entries(request)
                .await
                .map_err(|error| error.to_string()),
        ),
        NetworkRequest::Vote(request) => {
            NetworkResponse::Vote(raft.vote(request).await.map_err(|error| error.to_string()))
        }
        NetworkRequest::PreVote(request) => NetworkResponse::Vote(
            raft.pre_vote(request)
                .await
                .map_err(|error| error.to_string()),
        ),
        NetworkRequest::Client {
            request_id,
            request,
        } => {
            let response = execute_client_request_with_id(&raft, request_id, request)
                .await
                .map(|outcome| match outcome {
                    ClientRequestOutcome::Applied(frame) => frame,
                    ClientRequestOutcome::Redirect(address) => {
                        // Client forwarding is intentionally one hop. The edge server
                        // resolves the next leader and owns the retry boundary.
                        Frame::error(format!("MOVED {address}"))
                    }
                });
            NetworkResponse::Client(response)
        }
        NetworkRequest::Transaction {
            request_id,
            requests,
        } => {
            let response = execute_transaction_request_with_id(&raft, request_id, requests)
                .await
                .map(|outcome| match outcome {
                    ClientRequestOutcome::Applied(frame) => frame,
                    ClientRequestOutcome::Redirect(address) => {
                        Frame::error(format!("MOVED {address}"))
                    }
                });
            NetworkResponse::Client(response)
        }
        NetworkRequest::Snapshot {
            vote,
            snapshot,
            meta,
        } => {
            let response = raft
                .install_full_snapshot(
                    vote,
                    openraft::Snapshot {
                        meta,
                        snapshot: std::io::Cursor::new(snapshot),
                    },
                )
                .await
                .map_err(|error| error.to_string());
            NetworkResponse::Snapshot(response)
        }
    };
    write_network_message(&mut stream, &response).await
}

async fn read_network_message<T: for<'de> Deserialize<'de>>(
    stream: &mut TcpStream,
) -> CacheResult<T> {
    let mut header = [0_u8; 4];
    stream
        .read_exact(&mut header)
        .await
        .map_err(|error| Error::io("read raft rpc frame header", error))?;
    let length = u32::from_be_bytes(header) as usize;
    if length > NETWORK_MAX_FRAME {
        return Err(Error::protocol("raft rpc frame exceeds maximum size"));
    }
    let mut payload = vec![0_u8; length];
    stream
        .read_exact(&mut payload)
        .await
        .map_err(|error| Error::io("read raft rpc frame", error))?;
    bincode::deserialize(&payload)
        .map_err(|error| Error::server(format!("decode raft rpc frame: {error}")))
}

async fn write_network_message<T: Serialize>(
    stream: &mut TcpStream,
    message: &T,
) -> CacheResult<()> {
    let payload = bincode::serialize(message)
        .map_err(|error| Error::server(format!("encode raft rpc frame: {error}")))?;
    let length = u32::try_from(payload.len())
        .map_err(|_| Error::server("raft rpc frame length overflows u32"))?;
    if payload.len() > NETWORK_MAX_FRAME {
        return Err(Error::protocol("raft rpc frame exceeds maximum size"));
    }
    stream
        .write_all(&length.to_be_bytes())
        .await
        .map_err(|error| Error::io("write raft rpc frame header", error))?;
    stream
        .write_all(&payload)
        .await
        .map_err(|error| Error::io("write raft rpc frame", error))?;
    stream
        .flush()
        .await
        .map_err(|error| Error::io("flush raft rpc frame", error))
}

impl RaftNetworkV2<TypeConfig> for MemNetwork {
    type SnapshotData = std::io::Cursor<Vec<u8>>;

    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<TypeConfig>, RPCError<TypeConfig>> {
        self.target()
            .await?
            .append_entries(rpc)
            .await
            .map_err(|error| {
                RPCError::Network(openraft::error::NetworkError::from_string(
                    error.to_string(),
                ))
            })
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<VoteResponse<TypeConfig>, RPCError<TypeConfig>> {
        self.target().await?.vote(rpc).await.map_err(|error| {
            RPCError::Network(openraft::error::NetworkError::from_string(
                error.to_string(),
            ))
        })
    }

    async fn full_snapshot(
        &mut self,
        vote: Vote,
        mut snapshot: openraft::alias::SnapshotOf<TypeConfig, Self::SnapshotData>,
        cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        _option: RPCOption,
    ) -> Result<SnapshotResponse<TypeConfig>, StreamingError<TypeConfig>> {
        let target = self.target().await.map_err(StreamingError::from)?;
        let mut data = Vec::new();
        tokio::select! {
            closed = cancel => return Err(StreamingError::Closed(closed)),
            result = async {
                std::io::Read::read_to_end(&mut snapshot.snapshot, &mut data)
            } => {
                result.map_err(|error| StreamingError::Network(
                    openraft::error::NetworkError::from_string(error.to_string())
                ))?;
            }
        }

        target
            .install_full_snapshot(
                vote,
                openraft::Snapshot {
                    meta: snapshot.meta,
                    snapshot: std::io::Cursor::new(data),
                },
            )
            .await
            .map_err(|error| {
                StreamingError::Network(openraft::error::NetworkError::from_string(
                    error.to_string(),
                ))
            })
    }

    fn backoff(&self) -> Option<Backoff> {
        Some(Backoff::new(std::iter::repeat(Duration::from_millis(25))))
    }
}

pub async fn new_mem_raft(
    node_id: NodeId,
    peers: PeerMap,
) -> Result<RaftHandle, openraft::error::Fatal<TypeConfig>> {
    let config = openraft::Config {
        cluster_name: "alias-cache-test".to_string(),
        heartbeat_interval: 50,
        election_timeout_min: 150,
        election_timeout_max: 300,
        enable_tick: true,
        ..Default::default()
    }
    .validate()
    .map_err(|_| openraft::error::Fatal::Panicked)?;
    let raft = openraft::Raft::new(
        node_id,
        Arc::new(config),
        MemNetworkFactory::new(Arc::clone(&peers)),
        MemLogStore::default(),
        MemStateMachine::default(),
    )
    .await?;
    peers.lock().await.insert(node_id, raft.clone());
    Ok(raft)
}

pub async fn new_file_raft(
    node_id: NodeId,
    data_dir: impl AsRef<Path>,
    peers: PeerMap,
) -> Result<RaftHandle, openraft::error::Fatal<TypeConfig>> {
    let config = openraft::Config {
        cluster_name: "alias-cache".to_string(),
        heartbeat_interval: 50,
        election_timeout_min: 150,
        election_timeout_max: 300,
        enable_tick: true,
        ..Default::default()
    }
    .validate()
    .map_err(|_| openraft::error::Fatal::Panicked)?;
    let data_dir = data_dir.as_ref();
    std::fs::create_dir_all(data_dir).map_err(|_| openraft::error::Fatal::Panicked)?;
    let log_store = FileLogStore::open(data_dir.join("raft").join("log"))
        .map_err(|_| openraft::error::Fatal::Panicked)?;
    let raft = openraft::Raft::new(
        node_id,
        Arc::new(config),
        MemNetworkFactory::new(Arc::clone(&peers)),
        log_store,
        MemStateMachine::default(),
    )
    .await?;
    peers.lock().await.insert(node_id, raft.clone());
    Ok(raft)
}

pub async fn new_tcp_raft(
    node_id: NodeId,
    data_dir: impl AsRef<Path>,
    nodes: BTreeMap<NodeId, Node>,
) -> Result<RaftHandle, openraft::error::Fatal<TypeConfig>> {
    new_tcp_raft_inner(node_id, data_dir, nodes, true, TcpNetworkControl::default()).await
}

pub async fn new_tcp_raft_with_control(
    node_id: NodeId,
    data_dir: impl AsRef<Path>,
    nodes: BTreeMap<NodeId, Node>,
    control: TcpNetworkControl,
) -> Result<RaftHandle, openraft::error::Fatal<TypeConfig>> {
    new_tcp_raft_inner(node_id, data_dir, nodes, true, control).await
}

pub async fn new_tcp_raft_joiner(
    node_id: NodeId,
    data_dir: impl AsRef<Path>,
    nodes: BTreeMap<NodeId, Node>,
) -> Result<RaftHandle, openraft::error::Fatal<TypeConfig>> {
    new_tcp_raft_inner(
        node_id,
        data_dir,
        nodes,
        false,
        TcpNetworkControl::default(),
    )
    .await
}

pub async fn new_tcp_raft_joiner_with_control(
    node_id: NodeId,
    data_dir: impl AsRef<Path>,
    nodes: BTreeMap<NodeId, Node>,
    control: TcpNetworkControl,
) -> Result<RaftHandle, openraft::error::Fatal<TypeConfig>> {
    new_tcp_raft_inner(node_id, data_dir, nodes, false, control).await
}

async fn new_tcp_raft_inner(
    node_id: NodeId,
    data_dir: impl AsRef<Path>,
    nodes: BTreeMap<NodeId, Node>,
    bootstrap: bool,
    control: TcpNetworkControl,
) -> Result<RaftHandle, openraft::error::Fatal<TypeConfig>> {
    let config = openraft::Config {
        cluster_name: "alias-cache".to_string(),
        heartbeat_interval: 50,
        election_timeout_min: 150,
        election_timeout_max: 300,
        snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(64),
        enable_tick: bootstrap,
        ..Default::default()
    }
    .validate()
    .map_err(|_| openraft::error::Fatal::Panicked)?;
    let data_dir = data_dir.as_ref();
    std::fs::create_dir_all(data_dir).map_err(|_| openraft::error::Fatal::Panicked)?;
    let log_store = FileLogStore::open(data_dir.join("raft").join("log"))
        .map_err(|_| openraft::error::Fatal::Panicked)?;
    let raft = openraft::Raft::new(
        node_id,
        Arc::new(config),
        TcpNetworkFactory::new(node_id, control),
        log_store,
        MemStateMachine::default(),
    )
    .await?;
    if bootstrap
        && !raft
            .is_initialized()
            .await
            .map_err(|_| openraft::error::Fatal::Panicked)?
    {
        initialize_if_pristine(&raft, nodes).await?;
    }
    Ok(raft)
}

async fn initialize_if_pristine(
    raft: &RaftHandle,
    nodes: BTreeMap<NodeId, Node>,
) -> Result<(), openraft::error::Fatal<TypeConfig>> {
    match raft.initialize(nodes).await {
        Ok(())
        | Err(openraft::error::RaftError::APIError(
            openraft::error::InitializeError::NotAllowed(_),
        )) => Ok(()),
        Err(_) => Err(openraft::error::Fatal::Panicked),
    }
}

pub async fn new_tcp_raft_with_bootstrap(
    node_id: NodeId,
    data_dir: impl AsRef<Path>,
    nodes: BTreeMap<NodeId, Node>,
) -> CacheResult<RaftHandle> {
    let raft = new_tcp_raft(node_id, data_dir, nodes)
        .await
        .map_err(|error| Error::server(format!("open tcp raft node: {error}")))?;
    raft.wait(None)
        .current_leader(node_id, "tcp raft leader election")
        .await
        .map_err(|error| Error::server(format!("wait for tcp raft leader: {error}")))?;
    Ok(raft)
}

pub async fn new_tcp_raft_for_server(
    node_id: NodeId,
    data_dir: impl AsRef<Path>,
    nodes: BTreeMap<NodeId, Node>,
    bootstrap: bool,
) -> CacheResult<RaftHandle> {
    let raft = if bootstrap {
        new_tcp_raft(node_id, data_dir, nodes)
            .await
            .map_err(|error| Error::server(format!("open bootstrap raft node: {error}")))?
    } else {
        new_tcp_raft_joiner(node_id, data_dir, nodes)
            .await
            .map_err(|error| Error::server(format!("open raft joiner: {error}")))?
    };
    Ok(raft)
}

pub fn enable_raft_runtime(raft: &RaftHandle) {
    raft.runtime_config().tick(true);
    raft.runtime_config().elect(true);
}

async fn wait_for_membership_commit(
    raft: &RaftHandle,
    context: impl Into<String>,
) -> CacheResult<()> {
    let context = context.into();
    raft.wait(None)
        .metrics(
            |metrics| metrics.membership_config == metrics.committed_membership_config,
            context.clone(),
        )
        .await
        .map_err(|error| Error::server(format!("{context}: {error}")))?;
    Ok(())
}

pub async fn add_raft_learner(raft: &RaftHandle, node_id: NodeId, node: Node) -> CacheResult<()> {
    wait_for_membership_commit(raft, format!("wait before adding raft learner {node_id}")).await?;
    raft.add_learner(node_id, node, false)
        .await
        .map_err(|error| Error::server(format!("add raft learner {node_id}: {error}")))?;
    Ok(())
}

/// Move a learner to a new network address without retaining a stale replication stream.
///
/// Voters must be drained before their address is changed so an old and a new endpoint
/// cannot both participate in quorum formation.
pub async fn update_raft_node(raft: &RaftHandle, node_id: NodeId, node: Node) -> CacheResult<()> {
    let membership = raft
        .wait(None)
        .metrics(
            |_| true,
            format!("read raft node {node_id} membership before address update"),
        )
        .await
        .map_err(|error| {
            Error::server(format!(
                "read raft node {node_id} membership before address update: {error}"
            ))
        })?
        .membership_config;
    if membership.membership().get_node(&node_id).is_none() {
        return Err(Error::server(format!(
            "raft node {node_id} is not in membership"
        )));
    }
    if membership.voter_ids().any(|voter_id| voter_id == node_id) {
        return Err(Error::invalid_config(format!(
            "raft voter {node_id} must be drained before address update"
        )));
    }

    let expected_node = node.clone();
    wait_for_membership_commit(
        raft,
        format!("wait before removing raft node {node_id} for address update"),
    )
    .await?;
    raft.change_membership(
        openraft::ChangeMembers::RemoveNodes(BTreeSet::from([node_id])),
        false,
    )
    .await
    .map_err(|error| Error::server(format!("remove raft node {node_id}: {error}")))?;
    wait_for_membership_commit(
        raft,
        format!("wait after removing raft node {node_id} for address update"),
    )
    .await?;
    raft.add_learner(node_id, node, false)
        .await
        .map_err(|error| Error::server(format!("re-add raft learner {node_id}: {error}")))?;
    raft.wait(None)
        .metrics(
            |metrics| {
                metrics.membership_config.membership().get_node(&node_id) == Some(&expected_node)
                    && metrics
                        .committed_membership_config
                        .membership()
                        .get_node(&node_id)
                        == Some(&expected_node)
            },
            format!("wait for raft node {node_id} address update"),
        )
        .await
        .map_err(|error| {
            Error::server(format!(
                "wait for raft node {node_id} address update: {error}"
            ))
        })?;
    Ok(())
}

pub async fn wait_for_raft_replication(raft: &RaftHandle, node_id: NodeId) -> CacheResult<()> {
    raft.wait(None)
        .metrics(
            |metrics| {
                let Some(target) = metrics.last_log_index else {
                    return false;
                };
                if metrics
                    .membership_config
                    .membership()
                    .get_node(&node_id)
                    .is_none()
                {
                    return false;
                }
                metrics
                    .replication
                    .as_ref()
                    .and_then(|replication| replication.get(&node_id))
                    .and_then(|matched| matched.as_ref())
                    .is_some_and(|matched| matched.index >= target)
            },
            format!("wait for raft learner {node_id} replication"),
        )
        .await
        .map_err(|error| Error::server(format!("wait for raft learner {node_id}: {error}")))?;
    Ok(())
}

pub async fn trigger_raft_snapshot(raft: &RaftHandle) -> CacheResult<()> {
    raft.trigger()
        .snapshot()
        .await
        .map_err(|error| Error::server(format!("trigger raft snapshot: {error}")))?;
    Ok(())
}

pub async fn purge_raft_log(raft: &RaftHandle, upto: u64) -> CacheResult<()> {
    raft.trigger()
        .purge_log(upto)
        .await
        .map_err(|error| Error::server(format!("purge raft log through {upto}: {error}")))?;
    Ok(())
}

pub async fn promote_raft_learner(raft: &RaftHandle, node_id: NodeId) -> CacheResult<()> {
    wait_for_membership_commit(
        raft,
        format!("wait before promoting raft learner {node_id}"),
    )
    .await?;
    let mut voters = raft.voter_ids().collect::<Vec<_>>();
    if !voters.contains(&node_id) {
        voters.push(node_id);
    }
    voters.sort_unstable();
    raft.change_membership(voters, false)
        .await
        .map_err(|error| Error::server(format!("promote raft learner {node_id}: {error}")))?;
    Ok(())
}

pub async fn remove_raft_voter(raft: &RaftHandle, node_id: NodeId) -> CacheResult<()> {
    wait_for_membership_commit(raft, format!("wait before removing raft voter {node_id}")).await?;
    let current_voters = raft.voter_ids().collect::<Vec<_>>();
    if !current_voters.contains(&node_id) {
        return Err(Error::server(format!("raft node {node_id} is not a voter")));
    }
    let mut voters = current_voters
        .into_iter()
        .filter(|voter_id| *voter_id != node_id)
        .collect::<Vec<_>>();
    voters.sort_unstable();
    if voters.is_empty() {
        return Err(Error::invalid_config(
            "refuse to remove the last raft voter",
        ));
    }
    raft.change_membership(voters, false)
        .await
        .map_err(|error| Error::server(format!("remove raft voter {node_id}: {error}")))?;
    Ok(())
}

pub async fn drain_raft_voter(raft: &RaftHandle, node_id: NodeId) -> CacheResult<()> {
    wait_for_membership_commit(raft, format!("wait before draining raft voter {node_id}")).await?;
    let voters = raft.voter_ids().collect::<Vec<_>>();
    if !voters.contains(&node_id) {
        return Err(Error::server(format!("raft node {node_id} is not a voter")));
    }
    let remaining = voters
        .into_iter()
        .filter(|voter_id| *voter_id != node_id)
        .collect::<Vec<_>>();
    if remaining.is_empty() {
        return Err(Error::invalid_config("refuse to drain the last raft voter"));
    }
    raft.change_membership(remaining, true)
        .await
        .map_err(|error| Error::server(format!("drain raft voter {node_id}: {error}")))?;
    raft.wait(None)
        .metrics(
            |metrics| {
                !metrics
                    .membership_config
                    .voter_ids()
                    .any(|id| id == node_id)
                    && metrics
                        .membership_config
                        .membership()
                        .get_node(&node_id)
                        .is_some()
            },
            format!("wait for raft voter {node_id} drain"),
        )
        .await
        .map_err(|error| Error::server(format!("wait for raft voter {node_id} drain: {error}")))?;
    Ok(())
}

pub async fn wait_for_leader(raft: &RaftHandle, node_id: NodeId) -> CacheResult<()> {
    raft.wait(None)
        .current_leader(node_id, "tcp raft leader election")
        .await
        .map_err(|error| Error::server(format!("wait for tcp raft leader: {error}")))?;
    Ok(())
}

pub async fn restart_tcp_raft(
    node_id: NodeId,
    data_dir: impl AsRef<Path>,
    nodes: BTreeMap<NodeId, Node>,
) -> CacheResult<RaftHandle> {
    restart_tcp_raft_with_control(node_id, data_dir, nodes, TcpNetworkControl::default()).await
}

pub async fn restart_tcp_raft_with_control(
    node_id: NodeId,
    data_dir: impl AsRef<Path>,
    nodes: BTreeMap<NodeId, Node>,
    control: TcpNetworkControl,
) -> CacheResult<RaftHandle> {
    new_tcp_raft_joiner_with_control(node_id, data_dir, nodes, control)
        .await
        .map_err(|error| Error::server(format!("restart tcp raft node: {error}")))
}

pub async fn open_single_node(
    node_id: NodeId,
    data_dir: impl AsRef<Path>,
    raft_addr: impl Into<String>,
) -> CacheResult<RaftHandle> {
    let peers = Arc::new(Mutex::new(BTreeMap::new()));
    let raft = new_file_raft(node_id, data_dir, Arc::clone(&peers))
        .await
        .map_err(|error| Error::server(format!("open raft node: {error}")))?;
    if !raft
        .is_initialized()
        .await
        .map_err(|error| Error::server(format!("check raft initialization: {error}")))?
    {
        raft.initialize(BTreeMap::from([(
            node_id,
            Node {
                addr: raft_addr.into(),
            },
        )]))
        .await
        .map_err(|error| Error::server(format!("initialize raft cluster: {error}")))?;
    }
    raft.wait(None)
        .current_leader(node_id, "single-node raft leader election")
        .await
        .map_err(|error| Error::server(format!("wait for raft leader: {error}")))?;
    Ok(raft)
}

#[cfg(test)]
mod storage_suite {
    use super::*;
    use openraft::StorageError;
    use openraft::testing::log::{StoreBuilder, Suite};
    use openraft::type_config::TypeConfigExt;
    #[derive(Debug, Clone, Copy, Default)]
    struct Builder;

    impl StoreBuilder<TypeConfig, MemLogStore, MemStateMachine> for Builder {
        async fn build(
            &self,
        ) -> Result<((), MemLogStore, MemStateMachine), StorageError<TypeConfig>> {
            Ok(((), MemLogStore::default(), MemStateMachine::default()))
        }
    }

    #[derive(Debug)]
    struct FileGuard {
        path: std::path::PathBuf,
    }

    impl Drop for FileGuard {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.path);
        }
    }

    #[derive(Debug, Clone, Copy, Default)]
    struct FileBuilder;

    impl StoreBuilder<TypeConfig, FileLogStore, MemStateMachine, FileGuard> for FileBuilder {
        async fn build(
            &self,
        ) -> Result<(FileGuard, FileLogStore, MemStateMachine), StorageError<TypeConfig>> {
            let path = std::env::temp_dir().join(format!(
                "alias-cache-raft-suite-{}-{}.log",
                std::process::id(),
                unique_test_id()
            ));
            let _ = std::fs::remove_file(&path);
            let store = FileLogStore::open(&path)
                .map_err(|error| StorageError::read(TypeConfig::err_from_error(&error)))?;
            Ok((FileGuard { path }, store, MemStateMachine::default()))
        }
    }

    fn unique_test_id() -> u128 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock should be after epoch")
            .as_nanos()
    }

    #[tokio::test]
    async fn memory_storage_satisfies_openraft_contract() {
        Suite::<TypeConfig, MemLogStore, MemStateMachine, Builder, ()>::test_all(Builder)
            .await
            .expect("memory storage should satisfy OpenRaft storage contract");
    }

    #[tokio::test]
    async fn file_storage_satisfies_openraft_contract() {
        Suite::<TypeConfig, FileLogStore, MemStateMachine, FileBuilder, FileGuard>::test_all(
            FileBuilder,
        )
        .await
        .expect("file storage should satisfy OpenRaft storage contract");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use openraft::vote::RaftLeaderId;
    use std::collections::BTreeMap;
    use tokio::time::{Duration, timeout};

    fn entry(index: u64, payload: EntryPayload<RaftRequest, NodeId, Node>) -> Entry {
        Entry {
            log_id: openraft::LogId::new(
                <TypeConfig as RaftTypeConfig>::LeaderId::new(1, 1),
                index,
            ),
            payload,
        }
    }

    #[tokio::test]
    async fn memory_log_store_supports_append_read_and_truncate() {
        let mut store = MemLogStore::default();
        let first = entry(1, EntryPayload::Blank);
        let second = entry(
            2,
            EntryPayload::Normal(RaftRequest::Apply(Operation::Set {
                key: Bytes::from_static(b"key"),
                value: Bytes::from_static(b"value"),
            })),
        );

        store
            .append(vec![first.clone(), second.clone()], IOFlushed::noop())
            .await
            .expect("append should succeed");
        let mut reader = store.get_log_reader().await;
        assert_eq!(
            reader
                .try_get_log_entries(1..=2)
                .await
                .expect("read should succeed"),
            vec![first, second]
        );

        store
            .truncate_after(Some(openraft::LogId::new(
                <TypeConfig as RaftTypeConfig>::LeaderId::new(1, 1),
                1,
            )))
            .await
            .expect("truncate should succeed");
        assert_eq!(
            store
                .get_log_state()
                .await
                .expect("state should be readable")
                .last_log_id
                .expect("entry should remain")
                .index,
            1
        );
    }

    #[test]
    fn file_log_store_recovers_entries_after_reopen() {
        let path = std::env::temp_dir().join(format!(
            "alias-cache-file-log-reopen-{}-{}.log",
            std::process::id(),
            unique_test_id()
        ));
        let first = entry(1, EntryPayload::Blank);
        let second = entry(
            2,
            EntryPayload::Normal(RaftRequest::Apply(Operation::Set {
                key: Bytes::from_static(b"key"),
                value: Bytes::from_static(b"value"),
            })),
        );
        {
            let mut store = FileLogStore::open(&path).expect("file log should open");
            let runtime = tokio::runtime::Runtime::new().expect("runtime should start");
            runtime
                .block_on(store.append(vec![first.clone(), second.clone()], IOFlushed::noop()))
                .expect("entries should append");
        }
        let mut reopened = FileLogStore::open(&path).expect("file log should reopen");
        let runtime = tokio::runtime::Runtime::new().expect("runtime should start");
        let mut reader = runtime.block_on(reopened.get_log_reader());
        let entries = runtime
            .block_on(reader.try_get_log_entries(1..=2))
            .expect("entries should recover");
        assert_eq!(entries, vec![first, second]);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn file_log_store_recovers_metadata_after_reopen() {
        let path = std::env::temp_dir().join(format!(
            "alias-cache-file-log-metadata-{}-{}.log",
            std::process::id(),
            unique_test_id()
        ));
        let log_id = openraft::LogId::new(<TypeConfig as RaftTypeConfig>::LeaderId::new(2, 1), 7);
        let vote = Vote::new_committed(3, 1);
        {
            let mut store = FileLogStore::open(&path).expect("file log should open");
            let runtime = tokio::runtime::Runtime::new().expect("runtime should start");
            runtime
                .block_on(store.save_vote(&vote))
                .expect("vote should persist");
            runtime
                .block_on(store.save_committed(Some(log_id)))
                .expect("commit should persist");
            runtime
                .block_on(store.purge(log_id))
                .expect("purge should persist");
        }

        let mut reopened = FileLogStore::open(&path).expect("file log should reopen");
        let runtime = tokio::runtime::Runtime::new().expect("runtime should start");
        let mut reader = runtime.block_on(reopened.get_log_reader());
        assert_eq!(
            runtime
                .block_on(reader.read_vote())
                .expect("vote should recover"),
            Some(vote)
        );
        assert_eq!(
            runtime
                .block_on(reopened.read_committed())
                .expect("commit should recover"),
            Some(log_id)
        );
        assert_eq!(
            runtime
                .block_on(reopened.get_log_state())
                .expect("state should recover")
                .last_purged_log_id,
            Some(log_id)
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn file_log_store_rejects_corrupt_record() {
        let path = std::env::temp_dir().join(format!(
            "alias-cache-file-log-corrupt-{}-{}.log",
            std::process::id(),
            unique_test_id()
        ));
        {
            let mut store = FileLogStore::open(&path).expect("file log should open");
            let runtime = tokio::runtime::Runtime::new().expect("runtime should start");
            runtime
                .block_on(store.append(vec![entry(1, EntryPayload::Blank)], IOFlushed::noop()))
                .expect("entry should append");
        }
        let mut bytes = std::fs::read(&path).expect("log should be readable");
        bytes[RAFT_LOG_HEADER_LEN] ^= 1;
        std::fs::write(&path, bytes).expect("corrupt log should be writable");
        assert!(FileLogStore::open(&path).is_err());
        let _ = std::fs::remove_file(path);
    }

    fn unique_test_id() -> u128 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock should be after epoch")
            .as_nanos()
    }

    #[tokio::test]
    async fn memory_state_machine_applies_operations_and_snapshots() {
        let mut state_machine = MemStateMachine::default();
        let request = RaftRequest::Apply(Operation::SetWithExpiration {
            key: Bytes::from_static(b"key"),
            value: Bytes::from_static(b"value"),
            expire_at: Some(2_000),
            write_time: 1_000,
            only_if_exists: false,
            only_if_missing: false,
            keep_ttl: false,
            return_previous: false,
        });
        let RaftRequest::Apply(operation) = request;
        let snapshot_log_id =
            openraft::LogId::new(<TypeConfig as RaftTypeConfig>::LeaderId::new(1, 1), 1);
        {
            let mut inner = state_machine.inner.lock().await;
            inner.state.apply(operation);
            inner.last_applied = Some(snapshot_log_id);
        }
        let expected_snapshot_state = state_machine.state().await;

        let snapshot = state_machine
            .build_snapshot()
            .await
            .expect("snapshot should succeed");
        assert_eq!(snapshot.meta.last_log_id, Some(snapshot_log_id));
        let later_log_id =
            openraft::LogId::new(<TypeConfig as RaftTypeConfig>::LeaderId::new(1, 1), 2);
        {
            let mut inner = state_machine.inner.lock().await;
            inner.state.apply(Operation::Set {
                key: Bytes::from_static(b"later-key"),
                value: Bytes::from_static(b"later-value"),
            });
            inner.last_applied = Some(later_log_id);
        }
        let current_snapshot = state_machine
            .get_current_snapshot()
            .await
            .expect("current snapshot should be readable")
            .expect("current snapshot should exist");
        assert_eq!(current_snapshot.meta.last_log_id, Some(snapshot_log_id));
        let mut restored = MemStateMachine::default();
        restored
            .install_snapshot(&current_snapshot.meta, current_snapshot.snapshot)
            .await
            .expect("snapshot install should succeed");
        assert_eq!(restored.state().await, expected_snapshot_state);
        assert_eq!(
            restored
                .state()
                .await
                .snapshot()
                .get(&Bytes::from_static(b"later-key")),
            None
        );
        assert_eq!(restored.state().await.logical_time(), 1_000);
        assert_eq!(
            restored
                .state()
                .await
                .execute_request(crate::protocol::Request::Command(vec![
                    Bytes::from_static(b"PTTL"),
                    Bytes::from_static(b"key"),
                ]))
                .unwrap()
                .1,
            crate::protocol::Frame::Integer(1_000)
        );
    }

    #[tokio::test]
    async fn raft_snapshot_rejects_unsupported_payload_version() {
        let state_machine = MemStateMachine::default();
        let snapshot_log_id =
            openraft::LogId::new(<TypeConfig as RaftTypeConfig>::LeaderId::new(1, 1), 1);
        let meta = openraft::SnapshotMeta {
            last_log_id: Some(snapshot_log_id),
            last_membership: StoredMembership::default(),
        };
        let payload = bincode::serialize(&RaftSnapshotPayload {
            version: RAFT_SNAPSHOT_PAYLOAD_VERSION + 1,
            state: StateMachine::default(),
            pending_operations: Vec::new(),
        })
        .expect("snapshot payload should encode");
        let mut restored = state_machine;
        let error = restored
            .install_snapshot(&meta, std::io::Cursor::new(payload))
            .await
            .expect_err("unknown snapshot payload version should be rejected");
        assert!(
            error
                .to_string()
                .contains("unsupported raft snapshot payload version")
        );
    }

    #[tokio::test]
    async fn raft_snapshot_replays_operations_collected_during_start() {
        let state_machine = MemStateMachine::default();
        let base_state = {
            let mut inner = state_machine.inner.lock().await;
            inner.state.advance_logical_time(1_000);
            inner.state.apply(Operation::Set {
                key: Bytes::from_static(b"base"),
                value: Bytes::from_static(b"value"),
            });
            inner.snapshot_state = SnapshotState::Start;
            inner.snapshot_queue.push(Operation::SetWithExpiration {
                key: Bytes::from_static(b"pending"),
                value: Bytes::from_static(b"value"),
                expire_at: Some(3_000),
                write_time: 2_000,
                only_if_exists: false,
                only_if_missing: false,
                keep_ttl: false,
                return_previous: false,
            });
            inner.state.clone()
        };

        let snapshot = state_machine
            .build_snapshot_from_base(base_state)
            .await
            .expect("snapshot with pending operations should build");
        let mut restored = MemStateMachine::default();
        restored
            .install_snapshot(&snapshot.meta, snapshot.snapshot)
            .await
            .expect("snapshot with pending operations should install");
        let mut restored_state = restored.state().await;
        assert_eq!(
            restored_state.snapshot().get(&Bytes::from_static(b"base")),
            Some(&crate::state_machine::Value::String(Bytes::from_static(
                b"value"
            )))
        );
        assert_eq!(
            restored_state
                .snapshot()
                .get(&Bytes::from_static(b"pending")),
            Some(&crate::state_machine::Value::String(Bytes::from_static(
                b"value"
            )))
        );
        assert_eq!(restored_state.logical_time(), 2_000);
        assert_eq!(
            restored_state
                .execute_request(crate::protocol::Request::Command(vec![
                    Bytes::from_static(b"PTTL"),
                    Bytes::from_static(b"pending"),
                ]))
                .unwrap()
                .1,
            crate::protocol::Frame::Integer(1_000)
        );
    }

    #[tokio::test]
    async fn three_node_cluster_replicates_write_and_shuts_down() {
        let peers = Arc::new(Mutex::new(BTreeMap::new()));
        let node1 = timeout(Duration::from_secs(3), new_mem_raft(1, Arc::clone(&peers)))
            .await
            .expect("node 1 startup should finish")
            .expect("node 1 should start");
        let node2 = timeout(Duration::from_secs(3), new_mem_raft(2, Arc::clone(&peers)))
            .await
            .expect("node 2 startup should finish")
            .expect("node 2 should start");
        let node3 = timeout(Duration::from_secs(3), new_mem_raft(3, Arc::clone(&peers)))
            .await
            .expect("node 3 startup should finish")
            .expect("node 3 should start");

        let members = BTreeMap::from([
            (
                1,
                Node {
                    addr: "mem://1".into(),
                },
            ),
            (
                2,
                Node {
                    addr: "mem://2".into(),
                },
            ),
            (
                3,
                Node {
                    addr: "mem://3".into(),
                },
            ),
        ]);
        timeout(Duration::from_secs(3), node1.initialize(members))
            .await
            .expect("cluster initialization should finish")
            .expect("cluster should initialize");

        let leader = timeout(
            Duration::from_secs(3),
            node1.wait(None).current_leader(1, "node 1 should lead"),
        )
        .await
        .expect("leader election should finish")
        .expect("leader wait should succeed");
        assert_eq!(leader.current_leader, Some(1));

        timeout(
            Duration::from_secs(3),
            node1.client_write(RaftRequest::Apply(Operation::Set {
                key: Bytes::from_static(b"cluster-key"),
                value: Bytes::from_static(b"cluster-value"),
            })),
        )
        .await
        .expect("cluster write should finish")
        .expect("cluster write should succeed");

        for node in [&node1, &node2, &node3] {
            timeout(
                Duration::from_secs(3),
                node.wait(None)
                    .applied_index_at_least(Some(2), "write should apply"),
            )
            .await
            .expect("replication should finish")
            .expect("apply wait should succeed");
        }

        for node in [node1, node2, node3] {
            timeout(Duration::from_secs(3), node.shutdown())
                .await
                .expect("shutdown should finish")
                .expect("shutdown should succeed");
        }
    }

    #[tokio::test]
    async fn tcp_network_replicates_three_node_cluster() {
        let root = std::env::temp_dir().join(format!(
            "alias-cache-tcp-cluster-{}-{}",
            std::process::id(),
            unique_test_id()
        ));
        let addresses: [std::net::SocketAddr; 3] = [
            "127.0.0.1:0".parse().unwrap(),
            "127.0.0.1:0".parse().unwrap(),
            "127.0.0.1:0".parse().unwrap(),
        ];
        let mut listeners = Vec::new();
        let mut nodes = BTreeMap::new();
        for (index, address) in addresses.iter().enumerate() {
            let listener = TcpListener::bind(address)
                .await
                .expect("rpc listener should bind");
            let actual = listener.local_addr().expect("rpc address should resolve");
            listeners.push(listener);
            nodes.insert(
                (index + 1) as NodeId,
                Node {
                    addr: actual.to_string(),
                },
            );
        }

        let mut rafts = Vec::new();
        let mut rpc_tasks = Vec::new();
        for (index, listener) in listeners.into_iter().enumerate() {
            let raft = if index == 0 {
                new_tcp_raft(
                    (index + 1) as NodeId,
                    root.join(format!("node-{}", index + 1)),
                    nodes.clone(),
                )
                .await
            } else {
                new_tcp_raft_joiner(
                    (index + 1) as NodeId,
                    root.join(format!("node-{}", index + 1)),
                    nodes.clone(),
                )
                .await
            }
            .expect("tcp raft node should start");
            let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
            let rpc_raft = raft.clone();
            rpc_tasks.push((
                shutdown_tx,
                tokio::spawn(async move {
                    serve_raft_rpc(listener, rpc_raft, shutdown_rx)
                        .await
                        .expect("rpc server should stop cleanly");
                }),
            ));
            rafts.push(raft);
        }

        if !rafts[0]
            .is_initialized()
            .await
            .expect("state should be readable")
        {
            rafts[0]
                .initialize(nodes)
                .await
                .expect("tcp cluster should initialize");
        }
        timeout(
            Duration::from_secs(5),
            rafts[0].wait(None).current_leader(1, "node 1 should lead"),
        )
        .await
        .expect("leader election should finish")
        .expect("leader election should succeed");

        timeout(Duration::from_secs(5), async {
            loop {
                match rafts[0]
                    .client_write(RaftRequest::Apply(Operation::Set {
                        key: Bytes::from_static(b"tcp-key"),
                        value: Bytes::from_static(b"tcp-value"),
                    }))
                    .await
                {
                    Ok(_) => break,
                    Err(openraft::error::RaftError::APIError(
                        openraft::error::ClientWriteError::ForwardToLeader(_),
                    )) => tokio::time::sleep(Duration::from_millis(25)).await,
                    Err(error) => panic!("tcp write failed: {error}"),
                }
            }
        })
        .await
        .expect("tcp write should finish");

        for raft in &rafts {
            timeout(
                Duration::from_secs(5),
                raft.wait(None)
                    .applied_index_at_least(Some(2), "tcp write should apply"),
            )
            .await
            .expect("tcp replication should finish")
            .expect("tcp apply wait should succeed");
        }

        for raft in rafts {
            raft.shutdown().await.expect("raft should shut down");
        }
        for (shutdown_tx, task) in rpc_tasks {
            shutdown_tx.send(true).expect("rpc shutdown should send");
            task.await.expect("rpc task should join");
        }
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn tcp_cluster_replicates_ttl_expiration() {
        let root = std::env::temp_dir().join(format!(
            "alias-cache-tcp-ttl-{}-{}",
            std::process::id(),
            unique_test_id()
        ));
        let mut listeners = Vec::new();
        let mut nodes = BTreeMap::new();
        for index in 0..3 {
            let listener = TcpListener::bind("127.0.0.1:0")
                .await
                .expect("rpc listener should bind");
            let address = listener.local_addr().expect("rpc address should resolve");
            listeners.push(listener);
            nodes.insert(
                (index + 1) as NodeId,
                Node {
                    addr: address.to_string(),
                },
            );
        }

        let mut rafts = Vec::new();
        let mut rpc_tasks = Vec::new();
        for (index, listener) in listeners.into_iter().enumerate() {
            let raft = if index == 0 {
                new_tcp_raft(1, root.join("node-1"), nodes.clone()).await
            } else {
                new_tcp_raft_joiner(
                    (index + 1) as NodeId,
                    root.join(format!("node-{}", index + 1)),
                    nodes.clone(),
                )
                .await
            }
            .expect("raft node should start");
            let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
            let rpc_raft = raft.clone();
            rpc_tasks.push((
                shutdown_tx,
                tokio::spawn(async move {
                    serve_raft_rpc(listener, rpc_raft, shutdown_rx)
                        .await
                        .expect("rpc server should stop");
                }),
            ));
            rafts.push(raft);
        }
        initialize_if_pristine(&rafts[0], nodes.clone())
            .await
            .expect("cluster should initialize");
        for raft in &rafts[1..] {
            enable_raft_runtime(raft);
        }
        timeout(
            Duration::from_secs(5),
            rafts[0].wait(None).current_leader(1, "node 1 should lead"),
        )
        .await
        .expect("leader election should finish")
        .expect("leader election should succeed");

        let request = Request::Command(vec![
            Bytes::from_static(b"SET"),
            Bytes::from_static(b"ttl-key"),
            Bytes::from_static(b"ttl-value"),
            Bytes::from_static(b"PX"),
            Bytes::from_static(b"50"),
        ]);
        let response = timeout(Duration::from_secs(5), async {
            loop {
                match execute_client_request(&rafts[0], request.clone()).await {
                    Ok(response) => break response,
                    Err(error) if error.contains("leader is not known") => {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                    Err(error) => panic!("ttl write should be handled: {error}"),
                }
            }
        })
        .await
        .expect("ttl write should finish");
        assert_eq!(
            response,
            ClientRequestOutcome::Applied(Frame::simple_string("OK"))
        );
        for raft in &rafts {
            timeout(
                Duration::from_secs(5),
                raft.wait(None)
                    .applied_index_at_least(Some(2), "ttl write should apply"),
            )
            .await
            .expect("ttl write should replicate")
            .expect("ttl write apply wait should succeed");
        }

        tokio::time::sleep(Duration::from_millis(75)).await;
        advance_raft_time(&rafts[0])
            .await
            .expect("logical time advance should be committed");
        for raft in &rafts[1..] {
            timeout(
                Duration::from_secs(5),
                raft.wait(None)
                    .applied_index_at_least(Some(3), "time advance should apply"),
            )
            .await
            .expect("time advance should replicate")
            .expect("time advance apply wait should succeed");
        }
        for raft in &rafts {
            let response = raft
                .with_state_machine(|state_machine| {
                    Box::pin(async move {
                        state_machine
                            .state()
                            .await
                            .prepare_request(Request::Command(vec![
                                Bytes::from_static(b"GET"),
                                Bytes::from_static(b"ttl-key"),
                            ]))
                    })
                })
                .await
                .expect("ttl state should be readable")
                .expect("ttl read should be valid")
                .1;
            assert_eq!(response, Frame::Null);
        }

        for raft in rafts {
            raft.shutdown().await.expect("raft should stop");
        }
        for (shutdown_tx, task) in rpc_tasks {
            shutdown_tx.send(true).expect("rpc shutdown should send");
            task.await.expect("rpc task should stop");
        }
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn leader_time_worker_advances_only_on_leader_and_stops() {
        let root = std::env::temp_dir().join(format!(
            "alias-cache-leader-time-worker-{}-{}",
            std::process::id(),
            unique_test_id()
        ));
        let peers = Arc::new(Mutex::new(BTreeMap::new()));
        let raft = new_mem_raft(1, Arc::clone(&peers))
            .await
            .expect("raft node should start");
        raft.initialize(BTreeMap::from([(
            1,
            Node {
                addr: "mem://1".into(),
            },
        )]))
        .await
        .expect("single node should initialize");
        timeout(
            Duration::from_secs(3),
            raft.wait(None).current_leader(1, "single node should lead"),
        )
        .await
        .expect("leader election should finish")
        .expect("leader election should succeed");

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let worker = tokio::spawn(run_leader_time_worker(
            Arc::new(raft.clone()),
            1,
            shutdown_rx,
            Duration::from_millis(5),
        ));
        timeout(
            Duration::from_secs(3),
            raft.wait(None).metrics(
                |metrics| metrics.last_log_index.is_some_and(|index| index >= 2),
                "time worker should append",
            ),
        )
        .await
        .expect("time worker should append")
        .expect("time worker metrics wait should succeed");

        shutdown_tx.send(true).expect("worker shutdown should send");
        timeout(Duration::from_secs(3), worker)
            .await
            .expect("worker should stop")
            .expect("worker task should join");
        raft.shutdown().await.expect("raft should shut down");
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn expiration_worker_removes_expired_key_after_replicated_time_advance() {
        let peers = Arc::new(Mutex::new(BTreeMap::new()));
        let raft = new_mem_raft(1, Arc::clone(&peers))
            .await
            .expect("raft node should start");
        raft.initialize(BTreeMap::from([(
            1,
            Node {
                addr: "mem://1".into(),
            },
        )]))
        .await
        .expect("single node should initialize");
        timeout(
            Duration::from_secs(3),
            raft.wait(None).current_leader(1, "single node should lead"),
        )
        .await
        .expect("leader election should finish")
        .expect("leader election should succeed");

        let request = Request::Command(vec![
            Bytes::from_static(b"SET"),
            Bytes::from_static(b"cleanup-key"),
            Bytes::from_static(b"value"),
            Bytes::from_static(b"PX"),
            Bytes::from_static(b"10"),
        ]);
        execute_client_request(&raft, request)
            .await
            .expect("ttl write should succeed");
        tokio::time::sleep(Duration::from_millis(20)).await;

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let time_worker = tokio::spawn(run_leader_time_worker(
            Arc::new(raft.clone()),
            1,
            shutdown_rx.clone(),
            Duration::from_millis(5),
        ));
        let worker = tokio::spawn(run_expiration_worker(
            Arc::new(raft.clone()),
            1,
            shutdown_rx,
            Duration::from_millis(5),
        ));
        timeout(
            Duration::from_secs(3),
            raft.wait(None).metrics(
                |metrics| metrics.last_log_index.is_some_and(|index| index >= 4),
                "expiration worker should advance and clean",
            ),
        )
        .await
        .expect("expiration worker should append cleanup")
        .expect("expiration worker metrics wait should succeed");
        let state = raft
            .with_state_machine(|state_machine| {
                Box::pin(async move { state_machine.state().await })
            })
            .await
            .expect("state should be readable");
        assert!(
            !state
                .snapshot()
                .contains_key(&Bytes::from_static(b"cleanup-key"))
        );

        shutdown_tx.send(true).expect("worker shutdown should send");
        timeout(Duration::from_secs(3), time_worker)
            .await
            .expect("time worker should stop")
            .expect("time worker task should join");
        timeout(Duration::from_secs(3), worker)
            .await
            .expect("worker should stop")
            .expect("worker task should join");
        raft.shutdown().await.expect("raft should shut down");
    }

    #[tokio::test]
    async fn tcp_cluster_adds_promotes_and_removes_learner() {
        let root = std::env::temp_dir().join(format!(
            "alias-cache-tcp-membership-{}-{}",
            std::process::id(),
            unique_test_id()
        ));
        let mut listeners = Vec::new();
        let mut nodes = BTreeMap::new();
        for index in 0..4 {
            let listener = TcpListener::bind("127.0.0.1:0")
                .await
                .expect("rpc listener should bind");
            let address = listener.local_addr().expect("rpc address should resolve");
            listeners.push(listener);
            nodes.insert(
                (index + 1) as NodeId,
                Node {
                    addr: address.to_string(),
                },
            );
        }
        let initial_nodes = nodes
            .iter()
            .take(3)
            .map(|(id, node)| (*id, node.clone()))
            .collect::<BTreeMap<_, _>>();

        let mut rafts = Vec::new();
        let mut rpc_tasks = Vec::new();
        for (index, listener) in listeners.into_iter().enumerate() {
            let raft = if index == 0 {
                new_tcp_raft(1, root.join("node-1"), initial_nodes.clone()).await
            } else if index < 3 {
                new_tcp_raft_joiner(
                    (index + 1) as NodeId,
                    root.join(format!("node-{}", index + 1)),
                    initial_nodes.clone(),
                )
                .await
            } else {
                new_tcp_raft_joiner(4, root.join("node-4"), BTreeMap::new()).await
            }
            .expect("tcp raft node should start");
            let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
            let rpc_raft = raft.clone();
            rpc_tasks.push((
                shutdown_tx,
                tokio::spawn(async move {
                    serve_raft_rpc(listener, rpc_raft, shutdown_rx)
                        .await
                        .expect("rpc server should stop cleanly");
                }),
            ));
            rafts.push(raft);
        }

        for raft in &rafts[1..3] {
            enable_raft_runtime(raft);
        }
        timeout(
            Duration::from_secs(5),
            rafts[0].wait(None).current_leader(1, "node 1 should lead"),
        )
        .await
        .expect("leader election should finish")
        .expect("leader election should succeed");

        timeout(Duration::from_secs(5), async {
            loop {
                match rafts[0]
                    .client_write(RaftRequest::Apply(Operation::Set {
                        key: Bytes::from_static(b"membership-key"),
                        value: Bytes::from_static(b"membership-value"),
                    }))
                    .await
                {
                    Ok(_) => break,
                    Err(openraft::error::RaftError::APIError(
                        openraft::error::ClientWriteError::ForwardToLeader(_),
                    )) => tokio::time::sleep(Duration::from_millis(25)).await,
                    Err(error) => panic!("initial write failed: {error}"),
                }
            }
        })
        .await
        .expect("initial write should finish");

        add_raft_learner(&rafts[0], 4, nodes[&4].clone())
            .await
            .expect("learner should be added");
        let before_catch_up_index = rafts[0]
            .wait(None)
            .metrics(
                |metrics| metrics.last_log_index.is_some(),
                "leader should have a log before catch-up",
            )
            .await
            .expect("leader metrics should be readable")
            .last_log_index
            .expect("leader should have a log before catch-up");
        for index in 0..16 {
            rafts[0]
                .client_write(RaftRequest::Apply(Operation::Set {
                    key: Bytes::from(format!("membership-key-{index}")),
                    value: Bytes::from(format!("membership-value-{index}")),
                }))
                .await
                .expect("writes during learner catch-up should succeed");
        }
        let after_writes_index = rafts[0]
            .wait(None)
            .metrics(
                |metrics| {
                    metrics
                        .last_log_index
                        .is_some_and(|index| index > before_catch_up_index)
                },
                "leader should append catch-up writes",
            )
            .await
            .expect("leader metrics after catch-up writes should be readable")
            .last_log_index
            .expect("leader should have a log after catch-up writes");
        assert!(after_writes_index > before_catch_up_index);
        wait_for_raft_replication(&rafts[0], 4)
            .await
            .expect("learner should catch up");
        rafts[3]
            .wait(None)
            .applied_index_at_least(
                Some(after_writes_index),
                "learner should apply catch-up writes",
            )
            .await
            .expect("learner should apply catch-up writes");
        let learner_state = rafts[3]
            .with_state_machine(|state_machine| {
                Box::pin(async move { state_machine.state().await })
            })
            .await
            .expect("learner state should be readable");
        assert_eq!(
            learner_state
                .snapshot()
                .get(&Bytes::from_static(b"membership-key-15")),
            Some(&crate::state_machine::Value::String(Bytes::from_static(
                b"membership-value-15"
            )))
        );
        promote_raft_learner(&rafts[0], 4)
            .await
            .expect("learner should be promoted");
        rafts[3]
            .wait(None)
            .metrics(
                |metrics| metrics.membership_config.voter_ids().any(|id| id == 4),
                "node 4 should become voter",
            )
            .await
            .expect("promotion should reach node 4");

        remove_raft_voter(&rafts[0], 3)
            .await
            .expect("node 3 should be removed");
        rafts[0]
            .wait(None)
            .metrics(
                |metrics| metrics.membership_config.voter_ids().eq([1, 2, 4]),
                "node 3 should be removed from voters",
            )
            .await
            .expect("removal should commit");

        for raft in &rafts {
            raft.shutdown().await.expect("raft should shut down");
        }
        for (shutdown_tx, task) in rpc_tasks {
            shutdown_tx.send(true).expect("rpc shutdown should send");
            task.await.expect("rpc task should join");
        }
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn tcp_membership_change_survives_target_restart_and_leader_loss() {
        let root = std::env::temp_dir().join(format!(
            "alias-cache-tcp-membership-recovery-{}-{}",
            std::process::id(),
            unique_test_id()
        ));
        let mut listeners = Vec::new();
        let mut nodes = BTreeMap::new();
        for index in 0..4 {
            let listener = TcpListener::bind("127.0.0.1:0")
                .await
                .expect("rpc listener should bind");
            let address = listener.local_addr().expect("rpc address should resolve");
            listeners.push(listener);
            nodes.insert(
                (index + 1) as NodeId,
                Node {
                    addr: address.to_string(),
                },
            );
        }
        let initial_nodes = nodes
            .iter()
            .take(3)
            .map(|(id, node)| (*id, node.clone()))
            .collect::<BTreeMap<_, _>>();

        let mut rafts = Vec::new();
        let mut rpc_tasks = Vec::new();
        for (index, listener) in listeners.into_iter().enumerate() {
            let raft = if index == 0 {
                new_tcp_raft(1, root.join("node-1"), initial_nodes.clone()).await
            } else if index < 3 {
                new_tcp_raft_joiner(
                    (index + 1) as NodeId,
                    root.join(format!("node-{}", index + 1)),
                    initial_nodes.clone(),
                )
                .await
            } else {
                new_tcp_raft_joiner(4, root.join("node-4"), BTreeMap::new()).await
            }
            .expect("tcp raft node should start");
            let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
            let rpc_raft = raft.clone();
            rpc_tasks.push((
                shutdown_tx,
                tokio::spawn(async move {
                    serve_raft_rpc(listener, rpc_raft, shutdown_rx)
                        .await
                        .expect("rpc server should stop");
                }),
            ));
            rafts.push(raft);
        }

        initialize_if_pristine(&rafts[0], initial_nodes.clone())
            .await
            .expect("cluster should initialize");
        for raft in &rafts[1..3] {
            enable_raft_runtime(raft);
        }
        timeout(
            Duration::from_secs(5),
            rafts[0].wait(None).current_leader(1, "node 1 should lead"),
        )
        .await
        .expect("leader election should finish")
        .expect("leader election should succeed");
        rafts[0]
            .wait(None)
            .metrics(
                |metrics| {
                    metrics
                        .committed_membership_config
                        .voter_ids()
                        .eq([1, 2, 3])
                },
                "initial membership should be committed",
            )
            .await
            .expect("initial membership commit wait should finish");

        add_raft_learner(&rafts[0], 4, nodes[&4].clone())
            .await
            .expect("learner should be added");
        enable_raft_runtime(&rafts[3]);
        timeout(
            Duration::from_secs(8),
            wait_for_raft_replication(&rafts[0], 4),
        )
        .await
        .expect("learner replication should finish")
        .expect("learner should catch up");
        add_raft_learner(&rafts[0], 4, nodes[&4].clone())
            .await
            .expect("re-adding an existing learner should be accepted");
        wait_for_raft_replication(&rafts[0], 4)
            .await
            .expect("re-added learner should remain caught up");

        let target = rafts.pop().expect("target raft should exist");
        target.shutdown().await.expect("target raft should stop");
        let (target_shutdown, target_task) = rpc_tasks.pop().expect("target rpc should exist");
        target_shutdown
            .send(true)
            .expect("target rpc shutdown should send");
        target_task.await.expect("target rpc task should stop");

        let restarted = restart_tcp_raft(4, root.join("node-4"), nodes.clone())
            .await
            .expect("target raft should restart");
        let listener = TcpListener::bind(nodes[&4].addr.clone())
            .await
            .expect("restarted target listener should bind");
        let (restart_shutdown_tx, restart_shutdown_rx) = tokio::sync::watch::channel(false);
        let rpc_raft = restarted.clone();
        let restart_task = tokio::spawn(async move {
            serve_raft_rpc(listener, rpc_raft, restart_shutdown_rx)
                .await
                .expect("restarted rpc server should stop");
        });
        enable_raft_runtime(&restarted);
        rafts.push(restarted);
        rpc_tasks.push((restart_shutdown_tx, restart_task));

        wait_for_raft_replication(&rafts[0], 4)
            .await
            .expect("restarted learner should catch up");
        add_raft_learner(&rafts[0], 4, nodes[&4].clone())
            .await
            .expect("re-adding a restarted learner should be accepted");
        wait_for_raft_replication(&rafts[0], 4)
            .await
            .expect("restarted learner should remain caught up");

        let old_leader = rafts.remove(0);
        old_leader.shutdown().await.expect("old leader should stop");
        let (leader_shutdown, leader_task) = rpc_tasks.remove(0);
        leader_shutdown
            .send(true)
            .expect("old leader rpc shutdown should send");
        leader_task.await.expect("old leader rpc task should stop");

        timeout(Duration::from_secs(8), async {
            loop {
                let node2_leader = rafts[0].current_leader().await;
                let node3_leader = rafts[1].current_leader().await;
                if node2_leader == node3_leader && matches!(node2_leader, Some(2 | 3)) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("remaining voters should elect a leader");

        let leader_index = if rafts[0].current_leader().await == Some(2) {
            0
        } else {
            1
        };
        promote_raft_learner(&rafts[leader_index], 4)
            .await
            .expect("new leader should promote the restarted learner");
        promote_raft_learner(&rafts[leader_index], 4)
            .await
            .expect("repeating promotion should be accepted");

        for raft in &rafts {
            timeout(
                Duration::from_secs(8),
                raft.wait(None).metrics(
                    |metrics| metrics.membership_config.voter_ids().eq([1, 2, 3, 4]),
                    "promoted learner should be a voter",
                ),
            )
            .await
            .expect("promotion should reach every live node")
            .expect("promotion metrics wait should succeed");
        }

        let response = timeout(Duration::from_secs(8), async {
            loop {
                match rafts[leader_index]
                    .client_write(RaftRequest::Apply(Operation::Set {
                        key: Bytes::from_static(b"membership-recovery-key"),
                        value: Bytes::from_static(b"membership-recovery-value"),
                    }))
                    .await
                {
                    Ok(response) => break response,
                    Err(openraft::error::RaftError::APIError(
                        openraft::error::ClientWriteError::ForwardToLeader(_),
                    )) => tokio::time::sleep(Duration::from_millis(20)).await,
                    Err(error) => panic!("write after membership recovery failed: {error}"),
                }
            }
        })
        .await
        .expect("write after membership recovery should finish");
        assert!(matches!(
            response.data,
            RaftResponse::Applied(Frame::SimpleString(_))
        ));

        for raft in rafts {
            raft.shutdown().await.expect("raft node should stop");
        }
        for (shutdown_tx, task) in rpc_tasks {
            shutdown_tx.send(true).expect("rpc shutdown should send");
            task.await.expect("rpc task should join");
        }
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn tcp_raft_node_address_update_rebuilds_replication_route() {
        let root = std::env::temp_dir().join(format!(
            "alias-cache-tcp-membership-address-{}-{}",
            std::process::id(),
            unique_test_id()
        ));
        let mut listeners = Vec::new();
        let mut nodes = BTreeMap::new();
        for index in 0..4 {
            let listener = TcpListener::bind("127.0.0.1:0")
                .await
                .expect("rpc listener should bind");
            let address = listener.local_addr().expect("rpc address should resolve");
            listeners.push(listener);
            nodes.insert(
                (index + 1) as NodeId,
                Node {
                    addr: address.to_string(),
                },
            );
        }
        let initial_nodes = nodes
            .iter()
            .take(3)
            .map(|(id, node)| (*id, node.clone()))
            .collect::<BTreeMap<_, _>>();

        let mut rafts = Vec::new();
        let mut rpc_tasks = Vec::new();
        for (index, listener) in listeners.into_iter().enumerate() {
            let raft = if index == 0 {
                new_tcp_raft(1, root.join("node-1"), initial_nodes.clone()).await
            } else if index < 3 {
                new_tcp_raft_joiner(
                    (index + 1) as NodeId,
                    root.join(format!("node-{}", index + 1)),
                    initial_nodes.clone(),
                )
                .await
            } else {
                new_tcp_raft_joiner(4, root.join("node-4"), BTreeMap::new()).await
            }
            .expect("tcp raft node should start");
            let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
            let rpc_raft = raft.clone();
            rpc_tasks.push((
                shutdown_tx,
                tokio::spawn(async move {
                    serve_raft_rpc(listener, rpc_raft, shutdown_rx)
                        .await
                        .expect("rpc server should stop");
                }),
            ));
            rafts.push(raft);
        }

        initialize_if_pristine(&rafts[0], initial_nodes.clone())
            .await
            .expect("cluster should initialize");
        for raft in &rafts[1..3] {
            enable_raft_runtime(raft);
        }
        timeout(
            Duration::from_secs(5),
            rafts[0].wait(None).current_leader(1, "node 1 should lead"),
        )
        .await
        .expect("leader election should finish")
        .expect("leader election should succeed");
        rafts[0]
            .wait(None)
            .metrics(
                |metrics| {
                    metrics
                        .committed_membership_config
                        .voter_ids()
                        .eq([1, 2, 3])
                },
                "initial membership should be committed",
            )
            .await
            .expect("initial membership commit wait should finish");

        add_raft_learner(&rafts[0], 4, nodes[&4].clone())
            .await
            .expect("learner should be added");
        enable_raft_runtime(&rafts[3]);
        timeout(
            Duration::from_secs(8),
            wait_for_raft_replication(&rafts[0], 4),
        )
        .await
        .expect("learner replication should finish")
        .expect("learner should catch up");

        let new_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("new rpc listener should bind");
        let new_address = new_listener
            .local_addr()
            .expect("new rpc address should resolve")
            .to_string();
        let (new_shutdown_tx, new_shutdown_rx) = tokio::sync::watch::channel(false);
        let rpc_raft = rafts[3].clone();
        let new_task = tokio::spawn(async move {
            serve_raft_rpc(new_listener, rpc_raft, new_shutdown_rx)
                .await
                .expect("new rpc server should stop");
        });

        timeout(
            Duration::from_secs(8),
            update_raft_node(
                &rafts[0],
                4,
                Node {
                    addr: new_address.clone(),
                },
            ),
        )
        .await
        .expect("learner address update should finish")
        .expect("learner address should update");
        for raft in &rafts[..3] {
            timeout(
                Duration::from_secs(5),
                raft.wait(None).metrics(
                    |metrics| {
                        metrics
                            .membership_config
                            .membership()
                            .get_node(&4)
                            .is_some_and(|node| node.addr == new_address)
                    },
                    "updated learner address should replicate",
                ),
            )
            .await
            .expect("updated address should reach voters")
            .expect("updated address metrics wait should succeed");
        }

        let (old_target_shutdown, old_target_task) = rpc_tasks.swap_remove(3);
        old_target_shutdown
            .send(true)
            .expect("old target rpc shutdown should send");
        old_target_task
            .await
            .expect("old target rpc task should stop");

        let response = timeout(Duration::from_secs(8), async {
            loop {
                match rafts[0]
                    .client_write(RaftRequest::Apply(Operation::Set {
                        key: Bytes::from_static(b"address-update-key"),
                        value: Bytes::from_static(b"address-update-value"),
                    }))
                    .await
                {
                    Ok(response) => break response,
                    Err(openraft::error::RaftError::APIError(
                        openraft::error::ClientWriteError::ForwardToLeader(_),
                    )) => tokio::time::sleep(Duration::from_millis(20)).await,
                    Err(error) => panic!("write after address update failed: {error}"),
                }
            }
        })
        .await
        .expect("write after address update should finish");
        let final_log_index = response.log_id.index;
        timeout(
            Duration::from_secs(8),
            wait_for_raft_replication(&rafts[0], 4),
        )
        .await
        .expect("updated learner replication should finish")
        .expect("learner should replicate through the new address");
        rafts[3]
            .wait(None)
            .applied_index_at_least(Some(final_log_index), "updated learner should apply write")
            .await
            .expect("updated learner should apply write");
        let state = rafts[3]
            .with_state_machine(|state_machine| {
                Box::pin(async move { state_machine.state().await })
            })
            .await
            .expect("updated learner state should be readable");
        assert_eq!(
            state
                .snapshot()
                .get(&Bytes::from_static(b"address-update-key")),
            Some(&crate::state_machine::Value::String(Bytes::from_static(
                b"address-update-value"
            )))
        );

        for raft in rafts {
            raft.shutdown().await.expect("raft node should stop");
        }
        for (shutdown_tx, task) in rpc_tasks {
            shutdown_tx.send(true).expect("rpc shutdown should send");
            task.await.expect("rpc task should join");
        }
        new_shutdown_tx
            .send(true)
            .expect("new target rpc shutdown should send");
        new_task.await.expect("new target rpc task should join");
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn raft_membership_refuses_to_remove_last_voter() {
        let peers = Arc::new(Mutex::new(BTreeMap::new()));
        let raft = new_mem_raft(1, Arc::clone(&peers))
            .await
            .expect("raft node should start");
        raft.initialize(BTreeMap::from([(
            1,
            Node {
                addr: "mem://1".into(),
            },
        )]))
        .await
        .expect("single-node cluster should initialize");
        raft.wait(None)
            .current_leader(1, "single node should lead")
            .await
            .expect("single node should elect itself");

        let error = remove_raft_voter(&raft, 1)
            .await
            .expect_err("last voter removal should fail");
        assert!(error.to_string().contains("last raft voter"));
        let error = drain_raft_voter(&raft, 1)
            .await
            .expect_err("last voter drain should fail");
        assert!(error.to_string().contains("last raft voter"));
        let error = update_raft_node(
            &raft,
            1,
            Node {
                addr: "mem://new-address".into(),
            },
        )
        .await
        .expect_err("voter address update should require a drain");
        assert!(error.to_string().contains("must be drained"));
        raft.shutdown().await.expect("raft should shut down");
    }

    #[tokio::test]
    async fn mem_cluster_self_drain_stops_before_remaining_voters_write() {
        let peers = Arc::new(Mutex::new(BTreeMap::new()));
        let node1 = new_mem_raft(1, Arc::clone(&peers))
            .await
            .expect("node 1 should start");
        let node2 = new_mem_raft(2, Arc::clone(&peers))
            .await
            .expect("node 2 should start");
        let node3 = new_mem_raft(3, Arc::clone(&peers))
            .await
            .expect("node 3 should start");
        node1
            .initialize(BTreeMap::from([
                (
                    1,
                    Node {
                        addr: "mem://1".into(),
                    },
                ),
                (
                    2,
                    Node {
                        addr: "mem://2".into(),
                    },
                ),
                (
                    3,
                    Node {
                        addr: "mem://3".into(),
                    },
                ),
            ]))
            .await
            .expect("cluster should initialize");
        timeout(
            Duration::from_secs(3),
            node1.wait(None).current_leader(1, "node 1 should lead"),
        )
        .await
        .expect("leader election should finish")
        .expect("leader election should succeed");
        node1
            .wait(None)
            .metrics(
                |metrics| {
                    metrics
                        .committed_membership_config
                        .voter_ids()
                        .eq([1, 2, 3])
                },
                "initial membership should be committed",
            )
            .await
            .expect("initial membership should commit");

        drain_raft_voter(&node1, 1)
            .await
            .expect("local voter should drain");
        node1
            .wait(None)
            .metrics(
                |metrics| {
                    !metrics.membership_config.voter_ids().any(|id| id == 1)
                        && metrics
                            .membership_config
                            .membership()
                            .learner_ids()
                            .any(|id| id == 1)
                },
                "local node should become a learner",
            )
            .await
            .expect("local drain should commit");
        node1
            .shutdown()
            .await
            .expect("drained node should stop before leaving");

        timeout(Duration::from_secs(5), async {
            loop {
                let leader = node2.current_leader().await;
                if matches!(leader, Some(2 | 3)) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("remaining voters should elect a leader");
        let leader = if node2.current_leader().await == Some(2) {
            &node2
        } else {
            &node3
        };
        let response = leader
            .client_write(RaftRequest::Apply(Operation::Set {
                key: Bytes::from_static(b"self-drain-key"),
                value: Bytes::from_static(b"self-drain-value"),
            }))
            .await
            .expect("remaining voters should accept writes");
        assert!(matches!(
            response.data,
            RaftResponse::Applied(Frame::SimpleString(_))
        ));

        node2.shutdown().await.expect("node 2 should stop");
        node3.shutdown().await.expect("node 3 should stop");
    }

    #[tokio::test]
    async fn tcp_cluster_drains_voter_and_retains_it_as_learner() {
        let root = std::env::temp_dir().join(format!(
            "alias-cache-tcp-drain-{}-{}",
            std::process::id(),
            unique_test_id()
        ));
        let mut listeners = Vec::new();
        let mut nodes = BTreeMap::new();
        for index in 0..3 {
            let listener = TcpListener::bind("127.0.0.1:0")
                .await
                .expect("rpc listener should bind");
            let address = listener.local_addr().expect("rpc address should resolve");
            listeners.push(listener);
            nodes.insert(
                (index + 1) as NodeId,
                Node {
                    addr: address.to_string(),
                },
            );
        }

        let mut rafts = Vec::new();
        let mut rpc_tasks = Vec::new();
        for (index, listener) in listeners.into_iter().enumerate() {
            let raft = if index == 0 {
                new_tcp_raft(1, root.join("node-1"), nodes.clone()).await
            } else {
                new_tcp_raft_joiner(
                    (index + 1) as NodeId,
                    root.join(format!("node-{}", index + 1)),
                    nodes.clone(),
                )
                .await
            }
            .expect("raft node should start");
            let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
            let rpc_raft = raft.clone();
            rpc_tasks.push((
                shutdown_tx,
                tokio::spawn(async move {
                    serve_raft_rpc(listener, rpc_raft, shutdown_rx)
                        .await
                        .expect("rpc server should stop");
                }),
            ));
            rafts.push(raft);
        }
        initialize_if_pristine(&rafts[0], nodes.clone())
            .await
            .expect("cluster should initialize");
        for raft in &rafts[1..] {
            enable_raft_runtime(raft);
        }
        timeout(
            Duration::from_secs(5),
            rafts[0].wait(None).current_leader(1, "node 1 should lead"),
        )
        .await
        .expect("leader election should finish")
        .expect("leader election should succeed");
        rafts[0]
            .wait(None)
            .metrics(
                |metrics| {
                    metrics
                        .committed_membership_config
                        .voter_ids()
                        .eq([1, 2, 3])
                },
                "initial membership should be committed",
            )
            .await
            .expect("initial membership should commit");

        drain_raft_voter(&rafts[0], 3)
            .await
            .expect("node 3 should drain");
        rafts[0]
            .wait(None)
            .metrics(
                |metrics| {
                    metrics.membership_config.voter_ids().eq([1, 2])
                        && metrics
                            .membership_config
                            .membership()
                            .learner_ids()
                            .any(|id| id == 3)
                },
                "node 3 should be retained as learner",
            )
            .await
            .expect("drain membership should commit");
        rafts[2]
            .wait(None)
            .metrics(
                |metrics| {
                    !metrics.membership_config.voter_ids().any(|id| id == 3)
                        && metrics
                            .membership_config
                            .membership()
                            .learner_ids()
                            .any(|id| id == 3)
                },
                "drained node should observe learner membership",
            )
            .await
            .expect("drained node should observe membership");
        let error = remove_raft_voter(&rafts[0], 3)
            .await
            .expect_err("removing a drained learner as voter should fail");
        assert!(error.to_string().contains("is not a voter"));

        let response = timeout(Duration::from_secs(5), async {
            loop {
                match rafts[0]
                    .client_write(RaftRequest::Apply(Operation::Set {
                        key: Bytes::from_static(b"drain-key"),
                        value: Bytes::from_static(b"drain-value"),
                    }))
                    .await
                {
                    Ok(response) => break response,
                    Err(openraft::error::RaftError::APIError(
                        openraft::error::ClientWriteError::ForwardToLeader(_),
                    )) => tokio::time::sleep(Duration::from_millis(10)).await,
                    Err(error) => panic!("write after drain should succeed: {error}"),
                }
            }
        })
        .await
        .expect("write after drain should finish");
        assert!(matches!(
            response.data,
            RaftResponse::Applied(Frame::SimpleString(_))
        ));

        let drained_outcome = execute_client_request(
            &rafts[2],
            Request::Command(vec![
                Bytes::from_static(b"SET"),
                Bytes::from_static(b"drained-local-key"),
                Bytes::from_static(b"must-forward"),
            ]),
        )
        .await
        .expect("drained node request should be handled");
        assert!(matches!(drained_outcome, ClientRequestOutcome::Redirect(_)));

        for raft in rafts {
            raft.shutdown().await.expect("raft node should stop");
        }
        for (shutdown_tx, task) in rpc_tasks {
            shutdown_tx.send(true).expect("rpc shutdown should send");
            task.await.expect("rpc task should join");
        }
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn tcp_client_request_returns_applied_response() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("rpc listener should bind");
        let address = listener.local_addr().expect("rpc address should resolve");
        let raft = new_mem_raft(1, Arc::new(Mutex::new(BTreeMap::new())))
            .await
            .expect("raft should start");
        raft.initialize(BTreeMap::from([(
            1,
            Node {
                addr: address.to_string(),
            },
        )]))
        .await
        .expect("raft should initialize");
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let rpc_raft = raft.clone();
        let task = tokio::spawn(async move {
            serve_raft_rpc(listener, rpc_raft, shutdown_rx)
                .await
                .expect("rpc server should stop");
        });
        timeout(
            Duration::from_secs(3),
            raft.wait(None).current_leader(1, "node should lead"),
        )
        .await
        .expect("leader election should finish")
        .expect("leader election should succeed");

        let response = send_client_request(
            &address.to_string(),
            Request::Command(vec![
                Bytes::from_static(b"SET"),
                Bytes::from_static(b"rpc-key"),
                Bytes::from_static(b"rpc-value"),
            ]),
            &RPCOption::new(Duration::from_secs(1)),
        )
        .await
        .expect("client rpc should succeed");
        assert_eq!(response, Frame::simple_string("OK"));

        raft.shutdown().await.expect("raft should stop");
        shutdown_tx.send(true).expect("rpc shutdown should send");
        task.await.expect("rpc task should join");
    }

    #[tokio::test]
    async fn follower_client_rpc_returns_redirect_to_leader() {
        let root = std::env::temp_dir().join(format!(
            "alias-cache-follower-rpc-{}-{}",
            std::process::id(),
            unique_test_id()
        ));
        let mut listeners = Vec::new();
        let mut nodes = BTreeMap::new();
        for index in 0..3 {
            let listener = TcpListener::bind("127.0.0.1:0")
                .await
                .expect("rpc listener should bind");
            let address = listener.local_addr().expect("rpc address should resolve");
            listeners.push(listener);
            nodes.insert(
                (index + 1) as NodeId,
                Node {
                    addr: address.to_string(),
                },
            );
        }

        let mut rafts = Vec::new();
        let mut rpc_tasks = Vec::new();
        for (index, listener) in listeners.into_iter().enumerate() {
            let raft = if index == 0 {
                new_tcp_raft(1, root.join("node-1"), nodes.clone()).await
            } else {
                new_tcp_raft_joiner(
                    (index + 1) as NodeId,
                    root.join(format!("node-{}", index + 1)),
                    nodes.clone(),
                )
                .await
            }
            .expect("raft node should start");
            let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
            let rpc_raft = raft.clone();
            rpc_tasks.push((
                shutdown_tx,
                tokio::spawn(async move {
                    serve_raft_rpc(listener, rpc_raft, shutdown_rx)
                        .await
                        .expect("rpc server should stop");
                }),
            ));
            rafts.push(raft);
        }
        initialize_if_pristine(&rafts[0], nodes.clone())
            .await
            .expect("cluster should initialize");
        for raft in &rafts[1..] {
            enable_raft_runtime(raft);
        }
        timeout(
            Duration::from_secs(5),
            rafts[0]
                .wait(None)
                .current_leader(1, "leader should be elected"),
        )
        .await
        .expect("leader election should finish")
        .expect("leader election should succeed");
        timeout(Duration::from_secs(5), async {
            loop {
                if rafts[1].current_leader().await == Some(1) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("follower should observe leader");

        let outcome = execute_client_request(
            &rafts[1],
            Request::Command(vec![
                Bytes::from_static(b"SET"),
                Bytes::from_static(b"follower-key"),
                Bytes::from_static(b"follower-value"),
            ]),
        )
        .await
        .expect("follower request should be handled");
        let address = match outcome {
            ClientRequestOutcome::Redirect(address) => address,
            ClientRequestOutcome::Applied(_) => {
                panic!("follower should not apply a write locally")
            }
        };
        assert_eq!(address, nodes.get(&1).unwrap().addr);
        let response = send_client_request(
            &address,
            Request::Command(vec![
                Bytes::from_static(b"SET"),
                Bytes::from_static(b"follower-key"),
                Bytes::from_static(b"follower-value"),
            ]),
            &RPCOption::new(Duration::from_secs(1)),
        )
        .await
        .expect("leader forwarding should succeed");
        assert_eq!(response, Frame::simple_string("OK"));

        for raft in rafts {
            raft.shutdown().await.expect("raft should stop");
        }
        for (shutdown_tx, task) in rpc_tasks {
            shutdown_tx.send(true).expect("rpc shutdown should send");
            task.await.expect("rpc task should join");
        }
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn tcp_cluster_elects_new_leader_after_old_leader_shutdown() {
        let root = std::env::temp_dir().join(format!(
            "alias-cache-tcp-failover-{}-{}",
            std::process::id(),
            unique_test_id()
        ));
        let mut listeners = Vec::new();
        let mut nodes = BTreeMap::new();
        for index in 0..3 {
            let listener = TcpListener::bind("127.0.0.1:0")
                .await
                .expect("rpc listener should bind");
            let address = listener.local_addr().expect("rpc address should resolve");
            listeners.push(listener);
            nodes.insert(
                (index + 1) as NodeId,
                Node {
                    addr: address.to_string(),
                },
            );
        }

        let mut rafts = Vec::new();
        let mut rpc_tasks = Vec::new();
        for (index, listener) in listeners.into_iter().enumerate() {
            let raft = if index == 0 {
                new_tcp_raft(1, root.join("node-1"), nodes.clone()).await
            } else {
                new_tcp_raft_joiner(
                    (index + 1) as NodeId,
                    root.join(format!("node-{}", index + 1)),
                    nodes.clone(),
                )
                .await
            }
            .expect("raft node should start");
            let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
            let rpc_raft = raft.clone();
            rpc_tasks.push((
                shutdown_tx,
                tokio::spawn(async move {
                    serve_raft_rpc(listener, rpc_raft, shutdown_rx)
                        .await
                        .expect("rpc server should stop");
                }),
            ));
            rafts.push(raft);
        }
        initialize_if_pristine(&rafts[0], nodes.clone())
            .await
            .expect("cluster should initialize");
        for raft in &rafts[1..] {
            enable_raft_runtime(raft);
        }
        timeout(
            Duration::from_secs(5),
            rafts[0]
                .wait(None)
                .current_leader(1, "initial leader should be elected"),
        )
        .await
        .expect("initial election should finish")
        .expect("initial election should succeed");
        timeout(Duration::from_secs(5), async {
            loop {
                if rafts[1].current_leader().await == Some(1)
                    && rafts[2].current_leader().await == Some(1)
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("followers should observe initial leader");

        rafts[0].shutdown().await.expect("old leader should stop");
        let (leader_shutdown, leader_task) = rpc_tasks.remove(0);
        leader_shutdown
            .send(true)
            .expect("leader rpc shutdown should send");
        leader_task.await.expect("leader rpc task should stop");

        timeout(Duration::from_secs(8), async {
            loop {
                let node2_leader = rafts[1].current_leader().await;
                let node3_leader = rafts[2].current_leader().await;
                if node2_leader == Some(2) && node3_leader == Some(2) {
                    break;
                }
                if node2_leader == Some(3) && node3_leader == Some(3) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("new leader should be elected");

        let new_leader = rafts[1]
            .current_leader()
            .await
            .expect("new leader should be known");
        let leader_index = match new_leader {
            2 => 1,
            3 => 2,
            _ => panic!("unexpected leader {new_leader}"),
        };
        let response = timeout(Duration::from_secs(5), async {
            loop {
                match rafts[leader_index]
                    .client_write(RaftRequest::Apply(Operation::Set {
                        key: Bytes::from_static(b"failover-key"),
                        value: Bytes::from_static(b"failover-value"),
                    }))
                    .await
                {
                    Ok(response) => break response,
                    Err(openraft::error::RaftError::APIError(
                        openraft::error::ClientWriteError::ForwardToLeader(_),
                    )) => tokio::time::sleep(Duration::from_millis(25)).await,
                    Err(error) => panic!("failover write should succeed: {error}"),
                }
            }
        })
        .await
        .expect("failover write should finish");
        assert!(matches!(
            response.data,
            RaftResponse::Applied(Frame::SimpleString(_))
        ));

        for raft in rafts.iter().skip(1) {
            timeout(
                Duration::from_secs(5),
                raft.wait(None)
                    .applied_index_at_least(Some(2), "failover write should apply"),
            )
            .await
            .expect("failover replication should finish")
            .expect("failover replication should succeed");
        }

        for raft in rafts.into_iter().skip(1) {
            raft.shutdown().await.expect("raft node should stop");
        }
        for (shutdown_tx, task) in rpc_tasks {
            shutdown_tx.send(true).expect("rpc shutdown should send");
            task.await.expect("rpc task should join");
        }
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn tcp_minority_partition_separates_read_write_and_redirect_errors() {
        let root = std::env::temp_dir().join(format!(
            "alias-cache-tcp-minority-partition-{}-{}",
            std::process::id(),
            unique_test_id()
        ));
        let control = TcpNetworkControl::default();
        let mut listeners = Vec::new();
        let mut nodes = BTreeMap::new();
        for index in 0..3 {
            let listener = TcpListener::bind("127.0.0.1:0")
                .await
                .expect("rpc listener should bind");
            let address = listener.local_addr().expect("rpc address should resolve");
            listeners.push(listener);
            nodes.insert(
                (index + 1) as NodeId,
                Node {
                    addr: address.to_string(),
                },
            );
        }

        let mut rafts = Vec::new();
        let mut rpc_tasks = Vec::new();
        for (index, listener) in listeners.into_iter().enumerate() {
            let raft = if index == 0 {
                new_tcp_raft_with_control(1, root.join("node-1"), nodes.clone(), control.clone())
                    .await
            } else {
                new_tcp_raft_joiner_with_control(
                    (index + 1) as NodeId,
                    root.join(format!("node-{}", index + 1)),
                    nodes.clone(),
                    control.clone(),
                )
                .await
            }
            .expect("tcp raft node should start");
            let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
            let rpc_raft = raft.clone();
            rpc_tasks.push((
                shutdown_tx,
                tokio::spawn(async move {
                    serve_raft_rpc(listener, rpc_raft, shutdown_rx)
                        .await
                        .expect("rpc server should stop");
                }),
            ));
            rafts.push(raft);
        }

        initialize_if_pristine(&rafts[0], nodes.clone())
            .await
            .expect("cluster should initialize");
        for raft in &rafts[1..] {
            enable_raft_runtime(raft);
        }
        timeout(
            Duration::from_secs(5),
            rafts[0].wait(None).current_leader(1, "node 1 should lead"),
        )
        .await
        .expect("initial leader election should finish")
        .expect("initial leader election should succeed");
        rafts[0]
            .wait(None)
            .metrics(
                |metrics| {
                    metrics
                        .committed_membership_config
                        .voter_ids()
                        .eq([1, 2, 3])
                },
                "initial membership should be committed",
            )
            .await
            .expect("initial membership commit wait should succeed");

        let initial_write = execute_client_request(
            &rafts[0],
            Request::Command(vec![
                Bytes::from_static(b"SET"),
                Bytes::from_static(b"partition-key"),
                Bytes::from_static(b"initial-value"),
            ]),
        )
        .await
        .expect("initial write should be handled");
        assert_eq!(
            initial_write,
            ClientRequestOutcome::Applied(Frame::simple_string("OK"))
        );
        for raft in &rafts {
            timeout(
                Duration::from_secs(5),
                raft.wait(None)
                    .applied_index_at_least(Some(2), "initial write should apply"),
            )
            .await
            .expect("initial write replication should finish")
            .expect("initial write apply wait should succeed");
        }

        control.block_bidirectional(1, 2).await;
        control.block_bidirectional(1, 3).await;

        let majority_leader = timeout(Duration::from_secs(8), async {
            loop {
                let node2_leader = rafts[1].current_leader().await;
                let node3_leader = rafts[2].current_leader().await;
                if node2_leader == node3_leader && matches!(node2_leader, Some(2 | 3)) {
                    break node2_leader.expect("majority leader should be known");
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("majority should elect a new leader");
        let majority_index = match majority_leader {
            2 => 1,
            3 => 2,
            leader => panic!("unexpected majority leader {leader}"),
        };
        let majority_follower_index = if majority_index == 1 { 2 } else { 1 };

        let majority_read = timeout(
            Duration::from_secs(5),
            execute_client_request(
                &rafts[majority_index],
                Request::Command(vec![
                    Bytes::from_static(b"GET"),
                    Bytes::from_static(b"partition-key"),
                ]),
            ),
        )
        .await
        .expect("majority read should finish")
        .expect("majority read should succeed");
        assert_eq!(
            majority_read,
            ClientRequestOutcome::Applied(Frame::bulk_string("initial-value"))
        );

        let majority_write = timeout(
            Duration::from_secs(5),
            execute_client_request(
                &rafts[majority_index],
                Request::Command(vec![
                    Bytes::from_static(b"SET"),
                    Bytes::from_static(b"majority-key"),
                    Bytes::from_static(b"majority-value"),
                ]),
            ),
        )
        .await
        .expect("majority write should finish")
        .expect("majority write should succeed");
        assert_eq!(
            majority_write,
            ClientRequestOutcome::Applied(Frame::simple_string("OK"))
        );

        let majority_redirect = timeout(
            Duration::from_secs(5),
            execute_client_request(
                &rafts[majority_follower_index],
                Request::Command(vec![
                    Bytes::from_static(b"SET"),
                    Bytes::from_static(b"redirect-key"),
                    Bytes::from_static(b"redirect-value"),
                ]),
            ),
        )
        .await
        .expect("majority follower request should finish")
        .expect("majority follower request should be classified");
        assert_eq!(
            majority_redirect,
            ClientRequestOutcome::Redirect(nodes[&majority_leader].addr.clone())
        );

        let minority_read_error = timeout(
            Duration::from_secs(5),
            execute_client_request(
                &rafts[0],
                Request::Command(vec![
                    Bytes::from_static(b"GET"),
                    Bytes::from_static(b"partition-key"),
                ]),
            ),
        )
        .await
        .expect("minority read should finish")
        .expect_err("minority read must not return a linearizable value");
        assert!(minority_read_error.contains("read barrier failed"));
        assert!(minority_read_error.to_ascii_lowercase().contains("quorum"));

        let minority_write_error = timeout(
            Duration::from_secs(5),
            execute_client_request(
                &rafts[0],
                Request::Command(vec![
                    Bytes::from_static(b"SET"),
                    Bytes::from_static(b"minority-key"),
                    Bytes::from_static(b"minority-value"),
                ]),
            ),
        )
        .await
        .expect("minority write should finish")
        .expect_err("minority write must not be acknowledged");
        assert_eq!(minority_write_error, "raft leader is not known");

        let minority_state = rafts[0]
            .with_state_machine(|state_machine| {
                Box::pin(async move { state_machine.state().await })
            })
            .await
            .expect("minority state should be readable locally");
        assert_eq!(
            minority_state
                .snapshot()
                .get(&Bytes::from_static(b"minority-key")),
            None
        );

        control.unblock_bidirectional(1, 2).await;
        control.unblock_bidirectional(1, 3).await;
        timeout(Duration::from_secs(5), async {
            loop {
                if rafts[0].current_leader().await == Some(majority_leader) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("old leader should learn the majority leader after recovery");

        let request_id = new_client_request_id();
        let rediscovery_request = Request::Command(vec![
            Bytes::from_static(b"SET"),
            Bytes::from_static(b"rediscovery-key"),
            Bytes::from_static(b"first-value"),
        ]);
        let old_leader_response = send_client_request_with_id(
            &nodes[&1].addr,
            request_id,
            rediscovery_request.clone(),
            &RPCOption::new(Duration::from_secs(1)),
        )
        .await
        .expect("old leader redirect should be returned as a RESP error");
        assert_eq!(
            old_leader_response,
            Frame::error(format!("MOVED {}", nodes[&majority_leader].addr))
        );

        let response = send_client_request_with_id(
            &nodes[&majority_leader].addr,
            request_id,
            rediscovery_request,
            &RPCOption::new(Duration::from_secs(1)),
        )
        .await
        .expect("rediscovered leader should accept the request");
        assert_eq!(response, Frame::simple_string("OK"));

        let duplicate_response = send_client_request_with_id(
            &nodes[&majority_leader].addr,
            request_id,
            Request::Command(vec![
                Bytes::from_static(b"SET"),
                Bytes::from_static(b"rediscovery-key"),
                Bytes::from_static(b"second-value"),
            ]),
            &RPCOption::new(Duration::from_secs(1)),
        )
        .await
        .expect("duplicate request should return the cached response");
        assert_eq!(duplicate_response, Frame::simple_string("OK"));
        let value = execute_client_request(
            &rafts[majority_index],
            Request::Command(vec![
                Bytes::from_static(b"GET"),
                Bytes::from_static(b"rediscovery-key"),
            ]),
        )
        .await
        .expect("rediscovery value read should succeed");
        assert_eq!(
            value,
            ClientRequestOutcome::Applied(Frame::bulk_string("first-value"))
        );

        for raft in rafts {
            raft.shutdown().await.expect("raft node should stop");
        }
        for (shutdown_tx, task) in rpc_tasks {
            shutdown_tx.send(true).expect("rpc shutdown should send");
            task.await.expect("rpc task should join");
        }
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn tcp_old_leader_conflict_is_overwritten_after_partition_recovery() {
        let root = std::env::temp_dir().join(format!(
            "alias-cache-tcp-old-leader-recovery-{}-{}",
            std::process::id(),
            unique_test_id()
        ));
        let control = TcpNetworkControl::default();
        let mut listeners = Vec::new();
        let mut nodes = BTreeMap::new();
        for index in 0..3 {
            let listener = TcpListener::bind("127.0.0.1:0")
                .await
                .expect("rpc listener should bind");
            let address = listener.local_addr().expect("rpc address should resolve");
            listeners.push(listener);
            nodes.insert(
                (index + 1) as NodeId,
                Node {
                    addr: address.to_string(),
                },
            );
        }

        let mut rafts = Vec::new();
        let mut rpc_tasks = Vec::new();
        for (index, listener) in listeners.into_iter().enumerate() {
            let raft = if index == 0 {
                new_tcp_raft_with_control(1, root.join("node-1"), nodes.clone(), control.clone())
                    .await
            } else {
                new_tcp_raft_joiner_with_control(
                    (index + 1) as NodeId,
                    root.join(format!("node-{}", index + 1)),
                    nodes.clone(),
                    control.clone(),
                )
                .await
            }
            .expect("tcp raft node should start");
            let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
            let rpc_raft = raft.clone();
            rpc_tasks.push((
                shutdown_tx,
                tokio::spawn(async move {
                    serve_raft_rpc(listener, rpc_raft, shutdown_rx)
                        .await
                        .expect("rpc server should stop");
                }),
            ));
            rafts.push(raft);
        }

        initialize_if_pristine(&rafts[0], nodes.clone())
            .await
            .expect("cluster should initialize");
        for raft in &rafts[1..] {
            enable_raft_runtime(raft);
        }
        timeout(
            Duration::from_secs(5),
            rafts[0].wait(None).current_leader(1, "node 1 should lead"),
        )
        .await
        .expect("initial leader election should finish")
        .expect("initial leader election should succeed");
        rafts[0]
            .wait(None)
            .metrics(
                |metrics| {
                    metrics
                        .committed_membership_config
                        .voter_ids()
                        .eq([1, 2, 3])
                },
                "initial membership should be committed",
            )
            .await
            .expect("initial membership commit wait should succeed");

        let initial_write = rafts[0]
            .client_write(RaftRequest::Apply(Operation::Set {
                key: Bytes::from_static(b"recovery-base-key"),
                value: Bytes::from_static(b"recovery-base-value"),
            }))
            .await
            .expect("initial write should commit");
        let base_log_index = initial_write.log_id.index;
        for raft in &rafts {
            timeout(
                Duration::from_secs(5),
                raft.wait(None)
                    .applied_index_at_least(Some(base_log_index), "base write should apply"),
            )
            .await
            .expect("base write replication should finish")
            .expect("base write apply wait should succeed");
        }

        rafts[1].runtime_config().elect(false);
        rafts[2].runtime_config().elect(false);
        control.block_bidirectional(1, 2).await;
        control.block_bidirectional(1, 3).await;

        let before_conflict_index = rafts[0]
            .wait(None)
            .metrics(
                |metrics| metrics.last_log_index.is_some(),
                "old leader should expose its current log index",
            )
            .await
            .expect("old leader metrics should be readable")
            .last_log_index
            .expect("old leader log index should exist");
        // OpenRaft keeps a leader-side client write pending while it waits for quorum.
        // Observe the local append, then cancel only the caller so the uncommitted entry
        // remains available for the conflict-overwrite check.
        let stale_write = {
            let raft = rafts[0].clone();
            tokio::spawn(async move {
                raft.client_write(RaftRequest::Apply(Operation::Set {
                    key: Bytes::from_static(b"recovery-conflict-key"),
                    value: Bytes::from_static(b"stale-value"),
                }))
                .await
            })
        };
        let stale_log_index = timeout(
            Duration::from_secs(5),
            rafts[0].wait(None).metrics(
                |metrics| {
                    metrics
                        .last_log_index
                        .is_some_and(|index| index > before_conflict_index)
                },
                "old leader should append an uncommitted conflict",
            ),
        )
        .await
        .expect("old leader conflict append should finish")
        .expect("old leader should append the conflict")
        .last_log_index
        .expect("stale log index should exist");
        stale_write.abort();
        let _ = stale_write.await;
        assert!(stale_log_index > before_conflict_index);

        rafts[1].runtime_config().elect(true);
        rafts[2].runtime_config().elect(true);
        let majority_leader = timeout(Duration::from_secs(8), async {
            loop {
                let node2_leader = rafts[1].current_leader().await;
                let node3_leader = rafts[2].current_leader().await;
                if node2_leader == node3_leader && matches!(node2_leader, Some(2 | 3)) {
                    break node2_leader.expect("majority leader should be known");
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("majority should elect a new leader");
        let majority_index = match majority_leader {
            2 => 1,
            3 => 2,
            leader => panic!("unexpected majority leader {leader}"),
        };

        let committed_conflict = timeout(Duration::from_secs(5), async {
            loop {
                match rafts[majority_index]
                    .client_write(RaftRequest::Apply(Operation::Set {
                        key: Bytes::from_static(b"recovery-conflict-key"),
                        value: Bytes::from_static(b"fresh-value"),
                    }))
                    .await
                {
                    Ok(response) => break response,
                    Err(openraft::error::RaftError::APIError(
                        openraft::error::ClientWriteError::ForwardToLeader(_),
                    )) => tokio::time::sleep(Duration::from_millis(20)).await,
                    Err(error) => panic!("majority conflict write failed: {error}"),
                }
            }
        })
        .await
        .expect("majority conflict write should finish");
        let committed_log_index = committed_conflict.log_id.index;
        assert!(committed_log_index >= stale_log_index);
        assert!(matches!(
            committed_conflict.data,
            RaftResponse::Applied(Frame::SimpleString(_))
        ));

        control.unblock_bidirectional(1, 2).await;
        control.unblock_bidirectional(1, 3).await;
        timeout(Duration::from_secs(12), async {
            loop {
                let mut states = Vec::new();
                for raft in &rafts {
                    let (state, applied) = raft
                        .with_state_machine(|state_machine| {
                            Box::pin(async move { state_machine.state_with_applied_index().await })
                        })
                        .await
                        .expect("recovered state should be readable");
                    if applied.is_none_or(|log_id| log_id.index < committed_log_index) {
                        states.clear();
                        break;
                    }
                    states.push(state);
                }
                if states.len() == rafts.len() && states.windows(2).all(|pair| pair[0] == pair[1]) {
                    break states;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("all nodes should converge after partition recovery");

        let expected_state = rafts[majority_index]
            .with_state_machine(|state_machine| {
                Box::pin(async move { state_machine.state().await })
            })
            .await
            .expect("expected state should be readable");
        assert_eq!(
            expected_state
                .snapshot()
                .get(&Bytes::from_static(b"recovery-conflict-key")),
            Some(&crate::state_machine::Value::String(Bytes::from_static(
                b"fresh-value"
            )))
        );
        for raft in &rafts {
            let state = raft
                .with_state_machine(|state_machine| {
                    Box::pin(async move { state_machine.state().await })
                })
                .await
                .expect("final state should be readable");
            assert_eq!(state, expected_state);
            assert_ne!(
                state
                    .snapshot()
                    .get(&Bytes::from_static(b"recovery-conflict-key")),
                Some(&crate::state_machine::Value::String(Bytes::from_static(
                    b"stale-value"
                )))
            );
        }

        for raft in rafts {
            raft.shutdown().await.expect("raft node should stop");
        }
        for (shutdown_tx, task) in rpc_tasks {
            shutdown_tx.send(true).expect("rpc shutdown should send");
            task.await.expect("rpc task should join");
        }
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn committed_expired_key_does_not_reappear_after_leader_failover() {
        let root = std::env::temp_dir().join(format!(
            "alias-cache-tcp-ttl-failover-{}-{}",
            std::process::id(),
            unique_test_id()
        ));
        let mut listeners = Vec::new();
        let mut nodes = BTreeMap::new();
        for index in 0..3 {
            let listener = TcpListener::bind("127.0.0.1:0")
                .await
                .expect("rpc listener should bind");
            let address = listener.local_addr().expect("rpc address should resolve");
            listeners.push(listener);
            nodes.insert(
                (index + 1) as NodeId,
                Node {
                    addr: address.to_string(),
                },
            );
        }

        let mut rafts = Vec::new();
        let mut rpc_tasks = Vec::new();
        for (index, listener) in listeners.into_iter().enumerate() {
            let raft = if index == 0 {
                new_tcp_raft(1, root.join("node-1"), nodes.clone()).await
            } else {
                new_tcp_raft_joiner(
                    (index + 1) as NodeId,
                    root.join(format!("node-{}", index + 1)),
                    nodes.clone(),
                )
                .await
            }
            .expect("raft node should start");
            let (shutdown_tx, shutdown_rx) = watch::channel(false);
            let rpc_raft = raft.clone();
            rpc_tasks.push((
                shutdown_tx,
                tokio::spawn(async move {
                    serve_raft_rpc(listener, rpc_raft, shutdown_rx)
                        .await
                        .expect("rpc server should stop");
                }),
            ));
            rafts.push(raft);
        }
        initialize_if_pristine(&rafts[0], nodes.clone())
            .await
            .expect("cluster should initialize");
        for raft in &rafts[1..] {
            enable_raft_runtime(raft);
        }
        timeout(
            Duration::from_secs(5),
            rafts[0].wait(None).current_leader(1, "node 1 should lead"),
        )
        .await
        .expect("leader election should finish")
        .expect("leader election should succeed");

        let request = Request::Command(vec![
            Bytes::from_static(b"SET"),
            Bytes::from_static(b"failover-key"),
            Bytes::from_static(b"value"),
            Bytes::from_static(b"PX"),
            Bytes::from_static(b"25"),
        ]);
        let response = timeout(Duration::from_secs(5), async {
            loop {
                match execute_client_request(&rafts[0], request.clone()).await {
                    Ok(response) => break response,
                    Err(error) if error.contains("leader is not known") => {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                    Err(error) => panic!("ttl write should succeed: {error}"),
                }
            }
        })
        .await
        .expect("ttl write should finish");
        assert_eq!(
            response,
            ClientRequestOutcome::Applied(Frame::simple_string("OK"))
        );
        for raft in &rafts {
            timeout(
                Duration::from_secs(5),
                raft.wait(None)
                    .applied_index_at_least(Some(2), "ttl write should replicate"),
            )
            .await
            .expect("ttl write should replicate")
            .expect("ttl write apply wait should succeed");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        advance_raft_time(&rafts[0])
            .await
            .expect("time advance should succeed");
        for raft in &rafts {
            timeout(
                Duration::from_secs(5),
                raft.wait(None)
                    .applied_index_at_least(Some(3), "time advance should replicate"),
            )
            .await
            .expect("time advance should replicate")
            .expect("time advance apply wait should succeed");
        }

        rafts[0].shutdown().await.expect("old leader should stop");
        let (leader_shutdown, leader_task) = rpc_tasks.remove(0);
        leader_shutdown
            .send(true)
            .expect("leader rpc shutdown should send");
        leader_task.await.expect("leader rpc task should stop");
        timeout(Duration::from_secs(8), async {
            loop {
                if rafts[1].current_leader().await == Some(2)
                    || rafts[2].current_leader().await == Some(2)
                {
                    break;
                }
                if rafts[1].current_leader().await == Some(3)
                    || rafts[2].current_leader().await == Some(3)
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("new leader should be elected");
        let new_leader = if rafts[1].current_leader().await == Some(2) {
            &rafts[1]
        } else if rafts[1].current_leader().await == Some(3) {
            &rafts[2]
        } else if rafts[2].current_leader().await == Some(2) {
            &rafts[1]
        } else {
            &rafts[2]
        };
        let response = timeout(Duration::from_secs(5), async {
            loop {
                match execute_client_request(
                    new_leader,
                    Request::Command(vec![
                        Bytes::from_static(b"GET"),
                        Bytes::from_static(b"failover-key"),
                    ]),
                )
                .await
                {
                    Ok(response) => break response,
                    Err(error)
                        if error.contains("not enough for a quorum")
                            || error.contains("leader is not known") =>
                    {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                    Err(error) => panic!("read after failover should succeed: {error}"),
                }
            }
        })
        .await
        .expect("read after failover should finish");
        assert_eq!(response, ClientRequestOutcome::Applied(Frame::Null));

        for raft in rafts.into_iter().skip(1) {
            raft.shutdown().await.expect("raft node should stop");
        }
        for (shutdown_tx, task) in rpc_tasks {
            shutdown_tx.send(true).expect("rpc shutdown should send");
            task.await.expect("rpc task should stop");
        }
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn tcp_network_recovers_state_after_node_restart() {
        let root = std::env::temp_dir().join(format!(
            "alias-cache-tcp-restart-{}-{}",
            std::process::id(),
            unique_test_id()
        ));
        let mut listeners = Vec::new();
        let mut nodes = BTreeMap::new();
        for index in 0..3 {
            let listener = TcpListener::bind("127.0.0.1:0")
                .await
                .expect("rpc listener should bind");
            let address = listener.local_addr().expect("rpc address should resolve");
            listeners.push(listener);
            nodes.insert(
                (index + 1) as NodeId,
                Node {
                    addr: address.to_string(),
                },
            );
        }

        let mut rafts = Vec::new();
        let mut rpc_tasks = Vec::new();
        for (index, listener) in listeners.into_iter().enumerate() {
            let raft = if index == 0 {
                new_tcp_raft(1, root.join("node-1"), nodes.clone()).await
            } else {
                new_tcp_raft_joiner(
                    (index + 1) as NodeId,
                    root.join(format!("node-{}", index + 1)),
                    nodes.clone(),
                )
                .await
            }
            .expect("raft node should start");
            let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
            let rpc_raft = raft.clone();
            rpc_tasks.push((
                shutdown_tx,
                tokio::spawn(async move {
                    serve_raft_rpc(listener, rpc_raft, shutdown_rx)
                        .await
                        .expect("rpc server should stop");
                }),
            ));
            rafts.push(raft);
        }

        initialize_if_pristine(&rafts[0], nodes.clone())
            .await
            .expect("cluster should initialize or already be running");
        timeout(
            Duration::from_secs(5),
            rafts[0]
                .wait(None)
                .current_leader(1, "leader should be elected"),
        )
        .await
        .expect("leader election should finish")
        .expect("leader election should succeed");
        timeout(Duration::from_secs(5), async {
            loop {
                match rafts[0]
                    .client_write(RaftRequest::Apply(Operation::Set {
                        key: Bytes::from_static(b"restart-key"),
                        value: Bytes::from_static(b"restart-value"),
                    }))
                    .await
                {
                    Ok(_) => break,
                    Err(openraft::error::RaftError::APIError(
                        openraft::error::ClientWriteError::ForwardToLeader(_),
                    )) => tokio::time::sleep(Duration::from_millis(25)).await,
                    Err(error) => panic!("write should succeed: {error}"),
                }
            }
        })
        .await
        .expect("write should finish");
        for raft in &rafts {
            timeout(
                Duration::from_secs(5),
                raft.wait(None)
                    .applied_index_at_least(Some(2), "write should replicate"),
            )
            .await
            .expect("replication should finish")
            .expect("replication should succeed");
        }

        let restarted_data_dir = root.join("node-3");
        rafts
            .pop()
            .unwrap()
            .shutdown()
            .await
            .expect("node should stop");
        let (shutdown_tx, shutdown_task) = rpc_tasks.pop().unwrap();
        shutdown_tx.send(true).expect("rpc shutdown should send");
        shutdown_task.await.expect("rpc task should stop");
        let restarted = restart_tcp_raft(3, &restarted_data_dir, nodes.clone())
            .await
            .expect("node should restart");
        let listener = TcpListener::bind(nodes.get(&3).unwrap().addr.clone())
            .await
            .expect("restart listener should bind");
        let (restart_shutdown_tx, restart_shutdown_rx) = tokio::sync::watch::channel(false);
        let rpc_raft = restarted.clone();
        let restart_task = tokio::spawn(async move {
            serve_raft_rpc(listener, rpc_raft, restart_shutdown_rx)
                .await
                .expect("restart rpc server should stop");
        });
        timeout(
            Duration::from_secs(5),
            restarted
                .wait(None)
                .applied_index_at_least(Some(2), "restarted node should apply state"),
        )
        .await
        .expect("restart recovery should finish")
        .expect("restart recovery should succeed");
        let state = restarted
            .with_state_machine(|state_machine| {
                Box::pin(async move { state_machine.state().await })
            })
            .await
            .expect("restarted state machine should be readable");
        assert_eq!(
            state.snapshot().get(&Bytes::from_static(b"restart-key")),
            Some(&crate::state_machine::Value::String(Bytes::from_static(
                b"restart-value"
            )))
        );

        restarted
            .shutdown()
            .await
            .expect("restarted node should stop");
        restart_shutdown_tx
            .send(true)
            .expect("restart rpc shutdown should send");
        restart_task.await.expect("restart rpc task should join");
        for raft in rafts {
            raft.shutdown().await.expect("raft node should stop");
        }
        for (shutdown_tx, task) in rpc_tasks {
            shutdown_tx.send(true).expect("rpc shutdown should send");
            task.await.expect("rpc task should join");
        }
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn tcp_network_installs_snapshot_on_lagging_node() {
        let root = std::env::temp_dir().join(format!(
            "alias-cache-tcp-snapshot-{}-{}",
            std::process::id(),
            unique_test_id()
        ));
        let mut listeners = Vec::new();
        let mut nodes = BTreeMap::new();
        for index in 0..3 {
            let listener = TcpListener::bind("127.0.0.1:0")
                .await
                .expect("rpc listener should bind");
            let address = listener.local_addr().expect("rpc address should resolve");
            listeners.push(listener);
            nodes.insert(
                (index + 1) as NodeId,
                Node {
                    addr: address.to_string(),
                },
            );
        }

        let mut rafts = Vec::new();
        let mut rpc_tasks = Vec::new();
        for (index, listener) in listeners.into_iter().enumerate() {
            let raft = if index == 0 {
                new_tcp_raft(1, root.join("node-1"), nodes.clone()).await
            } else {
                new_tcp_raft_joiner(
                    (index + 1) as NodeId,
                    root.join(format!("node-{}", index + 1)),
                    nodes.clone(),
                )
                .await
            }
            .expect("raft node should start");
            let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
            let rpc_raft = raft.clone();
            rpc_tasks.push((
                shutdown_tx,
                tokio::spawn(async move {
                    serve_raft_rpc(listener, rpc_raft, shutdown_rx)
                        .await
                        .expect("rpc server should stop");
                }),
            ));
            rafts.push(raft);
        }

        initialize_if_pristine(&rafts[0], nodes.clone())
            .await
            .expect("cluster should initialize");
        for raft in &rafts[1..] {
            enable_raft_runtime(raft);
        }
        timeout(
            Duration::from_secs(5),
            rafts[0].wait(None).current_leader(1, "node 1 should lead"),
        )
        .await
        .expect("leader election should finish")
        .expect("leader election should succeed");

        let (node3_shutdown, node3_task) = rpc_tasks.pop().unwrap();
        rafts[2].shutdown().await.expect("lagging node should stop");
        node3_shutdown
            .send(true)
            .expect("lagging rpc shutdown should send");
        node3_task.await.expect("lagging rpc task should stop");

        for index in 0..80_u8 {
            let key = format!("snapshot-key-{index:03}");
            timeout(Duration::from_secs(5), async {
                loop {
                    match rafts[0]
                        .client_write(RaftRequest::Apply(Operation::Set {
                            key: Bytes::from(key.as_bytes().to_vec()),
                            value: Bytes::from_static(b"snapshot-value"),
                        }))
                        .await
                    {
                        Ok(_) => break,
                        Err(openraft::error::RaftError::APIError(
                            openraft::error::ClientWriteError::ForwardToLeader(_),
                        )) => tokio::time::sleep(Duration::from_millis(10)).await,
                        Err(error) => panic!("snapshot workload write failed: {error}"),
                    }
                }
            })
            .await
            .expect("snapshot workload write should finish");
        }

        timeout(
            Duration::from_secs(10),
            rafts[0].wait(None).metrics(
                |metrics| metrics.snapshot.is_some(),
                "leader should build a snapshot",
            ),
        )
        .await
        .expect("snapshot should be built")
        .expect("snapshot metrics should become available");
        let snapshot_index = rafts[0]
            .wait(None)
            .metrics(
                |metrics| metrics.snapshot.is_some(),
                "leader snapshot index should exist",
            )
            .await
            .expect("leader snapshot metrics should be available")
            .snapshot
            .expect("leader snapshot index should exist")
            .index;
        purge_raft_log(&rafts[0], snapshot_index)
            .await
            .expect("leader log purge should be triggered");

        let final_response = timeout(Duration::from_secs(5), async {
            loop {
                match rafts[0]
                    .client_write(RaftRequest::Apply(Operation::Set {
                        key: Bytes::from_static(b"post-snapshot-key"),
                        value: Bytes::from_static(b"post-snapshot-value"),
                    }))
                    .await
                {
                    Ok(response) => break response,
                    Err(openraft::error::RaftError::APIError(
                        openraft::error::ClientWriteError::ForwardToLeader(_),
                    )) => tokio::time::sleep(Duration::from_millis(10)).await,
                    Err(error) => panic!("post-snapshot write failed: {error}"),
                }
            }
        })
        .await
        .expect("post-snapshot write should finish");
        let final_log_index = final_response.log_id.index;
        assert!(matches!(
            final_response.data,
            RaftResponse::Applied(Frame::SimpleString(_))
        ));

        let restarted = restart_tcp_raft(3, root.join("node-3"), nodes.clone())
            .await
            .expect("lagging node should restart");
        let listener = TcpListener::bind(nodes.get(&3).unwrap().addr.clone())
            .await
            .expect("lagging listener should rebind");
        let (restart_shutdown_tx, restart_shutdown_rx) = tokio::sync::watch::channel(false);
        let rpc_raft = restarted.clone();
        let restart_task = tokio::spawn(async move {
            serve_raft_rpc(listener, rpc_raft, restart_shutdown_rx)
                .await
                .expect("restart rpc server should stop");
        });
        enable_raft_runtime(&restarted);

        timeout(Duration::from_secs(15), async {
            loop {
                let (state, applied) = restarted
                    .with_state_machine(|state_machine| {
                        Box::pin(async move { state_machine.state_with_applied_index().await })
                    })
                    .await
                    .expect("snapshot follower state should be readable");
                if applied.is_some_and(|log_id| log_id.index >= final_log_index)
                    && state
                        .snapshot()
                        .get(&Bytes::from_static(b"post-snapshot-key"))
                        == Some(&crate::state_machine::Value::String(Bytes::from_static(
                            b"post-snapshot-value",
                        )))
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("snapshot follower state should contain final write");

        restarted
            .shutdown()
            .await
            .expect("restarted node should stop");
        restart_shutdown_tx
            .send(true)
            .expect("restart rpc shutdown should send");
        restart_task.await.expect("restart rpc task should join");
        for raft in rafts.into_iter().take(2) {
            raft.shutdown().await.expect("raft node should stop");
        }
        for (shutdown_tx, task) in rpc_tasks {
            shutdown_tx.send(true).expect("rpc shutdown should send");
            task.await.expect("rpc task should join");
        }
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn file_raft_recovers_committed_state_after_restart() {
        let data_dir = std::env::temp_dir().join(format!(
            "alias-cache-file-raft-restart-{}-{}",
            std::process::id(),
            unique_test_id()
        ));
        let peers = Arc::new(Mutex::new(BTreeMap::new()));
        let node = timeout(
            Duration::from_secs(3),
            new_file_raft(1, &data_dir, Arc::clone(&peers)),
        )
        .await
        .expect("initial startup should finish")
        .expect("initial node should start");
        timeout(
            Duration::from_secs(3),
            node.initialize(BTreeMap::from([(
                1,
                Node {
                    addr: "file://1".into(),
                },
            )])),
        )
        .await
        .expect("initialization should finish")
        .expect("initialization should succeed");
        timeout(
            Duration::from_secs(3),
            node.wait(None)
                .current_leader(1, "node should become leader"),
        )
        .await
        .expect("leader election should finish")
        .expect("leader election should succeed");
        timeout(
            Duration::from_secs(3),
            node.client_write(RaftRequest::Apply(Operation::Set {
                key: Bytes::from_static(b"restart-key"),
                value: Bytes::from_static(b"restart-value"),
            })),
        )
        .await
        .expect("write should finish")
        .expect("write should succeed");
        timeout(Duration::from_secs(3), node.shutdown())
            .await
            .expect("initial shutdown should finish")
            .expect("initial shutdown should succeed");

        let peers = Arc::new(Mutex::new(BTreeMap::new()));
        let restarted = timeout(
            Duration::from_secs(3),
            new_file_raft(1, &data_dir, Arc::clone(&peers)),
        )
        .await
        .expect("restart should finish")
        .expect("restarted node should start");
        let metrics = timeout(
            Duration::from_secs(3),
            restarted
                .wait(None)
                .applied_index_at_least(Some(2), "state should recover"),
        )
        .await
        .expect("recovery should finish")
        .expect("recovery should succeed");
        assert!(metrics.last_applied.is_some());
        let state = restarted
            .with_state_machine(|state_machine| {
                Box::pin(async move { state_machine.state().await })
            })
            .await
            .expect("state machine should be readable");
        assert_eq!(
            state.snapshot().get(&Bytes::from_static(b"restart-key")),
            Some(&crate::state_machine::Value::String(Bytes::from_static(
                b"restart-value"
            )))
        );
        timeout(Duration::from_secs(3), restarted.shutdown())
            .await
            .expect("restart shutdown should finish")
            .expect("restart shutdown should succeed");
        let _ = std::fs::remove_dir_all(data_dir);
    }

    #[tokio::test]
    async fn file_raft_preserves_ttl_across_restart() {
        let data_dir = std::env::temp_dir().join(format!(
            "alias-cache-file-raft-ttl-restart-{}-{}",
            std::process::id(),
            unique_test_id()
        ));
        let peers = Arc::new(Mutex::new(BTreeMap::new()));
        let node = new_file_raft(1, &data_dir, Arc::clone(&peers))
            .await
            .expect("initial node should start");
        node.initialize(BTreeMap::from([(
            1,
            Node {
                addr: "file://1".into(),
            },
        )]))
        .await
        .expect("single node should initialize");
        timeout(
            Duration::from_secs(3),
            node.wait(None)
                .current_leader(1, "node should become leader"),
        )
        .await
        .expect("leader election should finish")
        .expect("leader election should succeed");

        node.client_write(RaftRequest::Apply(Operation::SetWithExpiration {
            key: Bytes::from_static(b"restart-ttl-key"),
            value: Bytes::from_static(b"restart-ttl-value"),
            expire_at: Some(2_000),
            write_time: 1_000,
            only_if_exists: false,
            only_if_missing: false,
            keep_ttl: false,
            return_previous: false,
        }))
        .await
        .expect("ttl write should succeed");
        node.client_write(RaftRequest::Apply(Operation::AdvanceTime {
            write_time: 1_500,
        }))
        .await
        .expect("time advance should succeed");
        node.shutdown().await.expect("initial node should stop");

        let peers = Arc::new(Mutex::new(BTreeMap::new()));
        let restarted = new_file_raft(1, &data_dir, Arc::clone(&peers))
            .await
            .expect("restarted node should start");
        timeout(
            Duration::from_secs(3),
            restarted
                .wait(None)
                .applied_index_at_least(Some(3), "ttl state should recover"),
        )
        .await
        .expect("ttl state recovery should finish")
        .expect("ttl state recovery should succeed");
        let state = restarted
            .with_state_machine(|state_machine| {
                Box::pin(async move { state_machine.state().await })
            })
            .await
            .expect("state should be readable");
        assert_eq!(state.logical_time(), 1_500);
        assert_eq!(
            state
                .prepare_request(Request::Command(vec![
                    Bytes::from_static(b"PTTL"),
                    Bytes::from_static(b"restart-ttl-key"),
                ]))
                .unwrap()
                .1,
            Frame::Integer(500)
        );
        restarted
            .shutdown()
            .await
            .expect("restarted node should stop");
        let _ = std::fs::remove_dir_all(data_dir);
    }

    #[tokio::test]
    async fn tcp_learner_joins_after_snapshot_purge() {
        let root = std::env::temp_dir().join(format!(
            "alias-cache-tcp-learner-snapshot-{}-{}",
            std::process::id(),
            unique_test_id()
        ));
        let mut listeners = Vec::new();
        let mut nodes = BTreeMap::new();
        for index in 0..4 {
            let listener = TcpListener::bind("127.0.0.1:0")
                .await
                .expect("rpc listener should bind");
            let address = listener.local_addr().expect("rpc address should resolve");
            listeners.push(listener);
            nodes.insert(
                (index + 1) as NodeId,
                Node {
                    addr: address.to_string(),
                },
            );
        }
        let initial_nodes = nodes
            .iter()
            .take(3)
            .map(|(id, node)| (*id, node.clone()))
            .collect::<BTreeMap<_, _>>();

        let mut rafts = Vec::new();
        let mut rpc_tasks = Vec::new();
        for (index, listener) in listeners.into_iter().enumerate() {
            let raft = if index == 0 {
                new_tcp_raft(1, root.join("node-1"), initial_nodes.clone()).await
            } else if index < 3 {
                new_tcp_raft_joiner(
                    (index + 1) as NodeId,
                    root.join(format!("node-{}", index + 1)),
                    initial_nodes.clone(),
                )
                .await
            } else {
                new_tcp_raft_joiner(4, root.join("node-4"), BTreeMap::new()).await
            }
            .expect("tcp raft node should start");
            let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
            let rpc_raft = raft.clone();
            rpc_tasks.push((
                shutdown_tx,
                tokio::spawn(async move {
                    serve_raft_rpc(listener, rpc_raft, shutdown_rx)
                        .await
                        .expect("rpc server should stop");
                }),
            ));
            rafts.push(raft);
        }

        initialize_if_pristine(&rafts[0], initial_nodes.clone())
            .await
            .expect("cluster should initialize");
        for raft in &rafts[1..] {
            enable_raft_runtime(raft);
        }
        timeout(
            Duration::from_secs(5),
            rafts[0].wait(None).current_leader(1, "node 1 should lead"),
        )
        .await
        .expect("leader election should finish")
        .expect("leader election should succeed");

        for index in 0..96_u16 {
            timeout(Duration::from_secs(5), async {
                loop {
                    match rafts[0]
                        .client_write(RaftRequest::Apply(Operation::Set {
                            key: Bytes::from(format!("learner-snapshot-key-{index:03}")),
                            value: Bytes::from(format!("learner-snapshot-value-{index:03}")),
                        }))
                        .await
                    {
                        Ok(_) => break,
                        Err(openraft::error::RaftError::APIError(
                            openraft::error::ClientWriteError::ForwardToLeader(_),
                        )) => tokio::time::sleep(Duration::from_millis(10)).await,
                        Err(error) => panic!("snapshot workload write failed: {error}"),
                    }
                }
            })
            .await
            .expect("snapshot workload write should finish");
        }
        timeout(
            Duration::from_secs(10),
            rafts[0].wait(None).metrics(
                |metrics| metrics.snapshot.is_some(),
                "leader should build a snapshot",
            ),
        )
        .await
        .expect("snapshot should be built")
        .expect("snapshot metrics should become available");
        let snapshot_index = rafts[0]
            .wait(None)
            .metrics(
                |metrics| metrics.snapshot.is_some(),
                "leader snapshot index should exist",
            )
            .await
            .expect("leader snapshot metrics should be available")
            .snapshot
            .expect("leader snapshot index should exist")
            .index;
        purge_raft_log(&rafts[0], snapshot_index)
            .await
            .expect("leader log purge should be triggered");

        add_raft_learner(&rafts[0], 4, nodes[&4].clone())
            .await
            .expect("learner should be added after snapshot purge");
        wait_for_raft_replication(&rafts[0], 4)
            .await
            .expect("learner should catch up from snapshot");
        let final_response = timeout(Duration::from_secs(5), async {
            loop {
                match rafts[0]
                    .client_write(RaftRequest::Apply(Operation::Set {
                        key: Bytes::from_static(b"learner-post-snapshot-key"),
                        value: Bytes::from_static(b"learner-post-snapshot-value"),
                    }))
                    .await
                {
                    Ok(response) => break response,
                    Err(openraft::error::RaftError::APIError(
                        openraft::error::ClientWriteError::ForwardToLeader(_),
                    )) => tokio::time::sleep(Duration::from_millis(10)).await,
                    Err(error) => panic!("post-snapshot write failed: {error}"),
                }
            }
        })
        .await
        .expect("post-snapshot write should finish");
        let final_log_index = final_response.log_id.index;
        rafts[3]
            .wait(None)
            .metrics(
                |metrics| {
                    metrics
                        .last_applied
                        .is_some_and(|log_id| log_id.index >= final_log_index)
                },
                "learner should apply snapshot and trailing log",
            )
            .await
            .expect("learner should apply snapshot and trailing log");
        timeout(Duration::from_secs(10), async {
            loop {
                let learner_state = rafts[3]
                    .with_state_machine(|state_machine| {
                        Box::pin(async move { state_machine.state().await })
                    })
                    .await
                    .expect("learner state should be readable");
                if learner_state
                    .snapshot()
                    .get(&Bytes::from_static(b"learner-post-snapshot-key"))
                    == Some(&crate::state_machine::Value::String(Bytes::from_static(
                        b"learner-post-snapshot-value",
                    )))
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("learner should apply snapshot state");

        for raft in rafts {
            raft.shutdown().await.expect("raft node should stop");
        }
        for (shutdown_tx, task) in rpc_tasks {
            shutdown_tx.send(true).expect("rpc shutdown should send");
            task.await.expect("rpc task should join");
        }
        let _ = std::fs::remove_dir_all(root);
    }
}
