//! OpenRaft type configuration and an in-memory storage vertical slice.

use crate::error::{Error, Result as CacheResult};
use crate::state_machine::{Operation, StateMachine};
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
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::fs::{File, OpenOptions};
use std::future::Future;
use std::io;
use std::io::{Read, Seek, SeekFrom, Write};
use std::ops::{Bound, RangeBounds};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

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
    Applied,
    Noop,
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

#[derive(Debug, Default)]
struct MemState {
    state: StateMachine,
    last_applied: Option<LogId>,
    membership: StoredMembership,
    snapshot: Option<Vec<u8>>,
}

impl MemStateMachine {
    pub async fn state(&self) -> StateMachine {
        self.inner.lock().await.state.clone()
    }
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
            inner.last_applied = Some(entry.log_id);
            let response = match entry.payload {
                EntryPayload::Blank => RaftResponse::Noop,
                EntryPayload::Normal(RaftRequest::Apply(operation)) => {
                    let _ = inner.state.apply(operation);
                    RaftResponse::Applied
                }
                EntryPayload::Membership(membership) => {
                    inner.membership = StoredMembership::new(Some(entry.log_id), membership);
                    RaftResponse::Noop
                }
            };
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

    async fn try_create_snapshot_builder(&mut self, _force: bool) -> Option<Self::SnapshotBuilder> {
        Some(self.clone())
    }

    async fn install_snapshot(
        &mut self,
        meta: &openraft::SnapshotMeta<<TypeConfig as RaftTypeConfig>::LeaderId, NodeId, Node>,
        mut snapshot: Self::SnapshotData,
    ) -> Result<(), io::Error> {
        use std::io::Read;

        let mut data = Vec::new();
        snapshot.read_to_end(&mut data)?;
        let state = bincode::deserialize(&data).map_err(io::Error::other)?;
        let mut inner = self.inner.lock().await;
        inner.state = state;
        inner.last_applied = meta.last_log_id;
        inner.membership = meta.last_membership.clone();
        inner.snapshot = Some(data);
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
        Ok(inner.snapshot.clone().map(|data| openraft::Snapshot {
            meta: openraft::SnapshotMeta {
                last_log_id: inner.last_applied,
                last_membership: inner.membership.clone(),
            },
            snapshot: std::io::Cursor::new(data),
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
        let mut inner = self.inner.lock().await;
        let data = bincode::serialize(&inner.state).map_err(io::Error::other)?;
        inner.snapshot = Some(data.clone());
        Ok(openraft::Snapshot {
            meta: openraft::SnapshotMeta {
                last_log_id: inner.last_applied,
                last_membership: inner.membership.clone(),
            },
            snapshot: std::io::Cursor::new(data),
        })
    }
}

pub type SharedMemStateMachine = Arc<Mutex<MemStateMachine>>;

type RaftHandle = openraft::Raft<TypeConfig, MemStateMachine>;
type PeerMap = Arc<Mutex<BTreeMap<NodeId, RaftHandle>>>;

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
                use std::io::Read;
                snapshot.snapshot.read_to_end(&mut data)
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
        let request = RaftRequest::Apply(Operation::Set {
            key: Bytes::from_static(b"key"),
            value: Bytes::from_static(b"value"),
        });
        let RaftRequest::Apply(operation) = request;
        state_machine.inner.lock().await.state.apply(operation);

        let snapshot = state_machine
            .build_snapshot()
            .await
            .expect("snapshot should succeed");
        let mut restored = MemStateMachine::default();
        restored
            .install_snapshot(&snapshot.meta, snapshot.snapshot)
            .await
            .expect("snapshot install should succeed");
        assert_eq!(restored.state().await, state_machine.state().await);
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
}
