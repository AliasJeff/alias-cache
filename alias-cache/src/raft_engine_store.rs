//! TiKV raft-engine backed [`RaftLogStorage`], replacing [`crate::raft::FileLogStore`].
//!
//! FileLogStore's fsync cost grows linearly with the WAL file size (2.3ms
//! average / 54ms peak at ~700MB, vs 0.1-0.3ms right after a truncation),
//! which is the measured root cause of the write-side throughput decay.
//! raft-engine segments the log internally, batches writes through
//! [`raft_engine::LogBatch`], and coalesces fsyncs, so the per-append cost no
//! longer depends on the log length.

use crate::error::{Error, Result as CacheResult};
use crate::raft::{Entry, LogId, LogStoreMetrics, TypeConfig, Vote};
use openraft::LogState;
use openraft::OptionalSend;
use openraft::RaftLogReader;
use openraft::alias::EntryOf;
use openraft::entry::RaftEntry;
use openraft::storage::IOFlushed;
use openraft::storage::RaftLogStorage;
use raft_engine::{Config, Engine, LogBatch, MessageExt, ReadableSize, ValueCodec};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Debug};
use std::io;
use std::ops::{Bound, RangeBounds};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use meta::StoreMeta;

/// Every alias-cache node runs a single raft group.
const RAFT_GROUP_ID: u64 = 1;

/// Create the raft-engine instance rooted at `path` (a directory owned by the
/// engine). `purge_threshold` keeps closed segments compacted before they grow
/// into the multi-hundred-MB WAL ranges that made FileLogStore's fsyncs
/// collapse; compression stays off because its CPU cost is measurable at the
/// throughputs this node targets.
pub fn create_raft_engine(path: impl AsRef<Path>) -> CacheResult<Arc<Engine>> {
    let path = path.as_ref();
    std::fs::create_dir_all(path)
        .map_err(|source| Error::io("create raft engine directory", source))?;
    let config = Config {
        dir: path.to_string_lossy().into_owned(),
        purge_threshold: ReadableSize::gb(2),
        batch_compression_threshold: ReadableSize::kb(0),
        ..Default::default()
    };
    let engine = Engine::open(config).map_err(|error| {
        Error::server(format!("open raft engine at {}: {error}", path.display()))
    })?;
    Ok(Arc::new(engine))
}

#[derive(Clone, Copy, Debug, Default)]
pub struct Bincode2Codec;

impl<T> ValueCodec<T> for Bincode2Codec
where
    T: Serialize + DeserializeOwned,
{
    #[inline]
    fn encode_to(v: &T, buf: &mut Vec<u8>) -> raft_engine::Result<()> {
        // Append-only: one LogBatch shares a single buffer across records.
        bincode2::serialize_into(&mut *buf, v)
            .map_err(|e| raft_engine::Error::Other(format!("bincode2 encode: {e}").into()))
    }

    #[inline]
    fn decode(bytes: &[u8]) -> raft_engine::Result<T> {
        bincode2::deserialize(bytes)
            .map_err(|e| raft_engine::Error::Corruption(format!("bincode2 decode: {e}")))
    }
}

/// The log entry as stored inside raft-engine.
///
/// raft-engine requires `MessageExt::Entry: PartialEq`, and openraft's `Entry`
/// only implements that when its payload does — so wrap it and compare by
/// `log_id` (raft's log-matching property makes that sufficient). The
/// `serde(transparent)` attribute keeps the on-disk bytes identical to a bare
/// `Entry`.
#[derive(Clone, Serialize, Deserialize)]
#[serde(transparent)]
pub struct StoredEntry(pub Entry);

impl PartialEq for StoredEntry {
    fn eq(&self, other: &Self) -> bool {
        self.0.log_id == other.0.log_id
    }
}

pub struct MessageExtTyped;

impl MessageExt<Bincode2Codec> for MessageExtTyped {
    type Entry = StoredEntry;

    fn index(e: &Self::Entry) -> u64 {
        e.0.log_id.index
    }
}

pub struct RaftEngineLogStore {
    engine: Arc<Engine>,
    committed_path: std::path::PathBuf,
    committed: Option<LogId>,
    metrics: Arc<LogStoreMetrics>,
    /// Debounce clock for the committed sidecar rewrite: the sidecar is a
    /// recovery hint (openraft re-confirms commits after a restart), so it
    /// must not sit on the quorum-acknowledgment path.
    last_sidecar_flush: Option<Instant>,
}

impl fmt::Debug for RaftEngineLogStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RaftEngineLogStore")
            .field("committed_path", &self.committed_path)
            .field("committed", &self.committed)
            .finish()
    }
}

impl RaftEngineLogStore {
    pub fn open(path: impl AsRef<Path>) -> CacheResult<Self> {
        let path = path.as_ref();
        let engine = create_raft_engine(path)?;
        let committed_path = path.with_extension("committed");
        // The sidecar only accelerates recovery; a missing or corrupt sidecar
        // simply means openraft starts with no remembered commit pointer.
        let committed = load_committed_sidecar(&committed_path).unwrap_or(None);
        Ok(Self {
            engine,
            committed_path,
            committed,
            metrics: Arc::new(LogStoreMetrics::default()),
            last_sidecar_flush: None,
        })
    }

    pub fn metrics(&self) -> Arc<LogStoreMetrics> {
        Arc::clone(&self.metrics)
    }

    /// Sync everything written so far on the blocking pool, recording the
    /// fsync latency for the `raftlog` INFO surface.
    async fn sync_engine(engine: Arc<Engine>, metrics: Arc<LogStoreMetrics>) -> io::Result<()> {
        let started = Instant::now();
        let result = tokio::task::spawn_blocking(move || {
            engine.sync().map_err(|e| io::Error::other(e.to_string()))
        })
        .await
        .map_err(|error| io::Error::other(format!("raft engine sync task: {error}")))?;
        metrics.record_sync_data(started.elapsed());
        result
    }

    fn get_meta<M: StoreMeta<TypeConfig>>(&self) -> Result<Option<M::Value>, io::Error> {
        let value = self
            .engine
            .get_value::<M::Value, Bincode2Codec>(RAFT_GROUP_ID, M::KEY.as_bytes())
            .map_err(|e| io::Error::other(e.to_string()))?;
        Ok(value)
    }

    /// Write a metadata value without syncing; callers persist explicitly.
    fn put_meta<M: StoreMeta<TypeConfig>>(&self, value: &M::Value) -> Result<(), io::Error> {
        let mut batch = LogBatch::with_capacity(256);
        batch
            .put_value::<Bincode2Codec, _>(RAFT_GROUP_ID, M::KEY.as_bytes().to_vec(), value)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        self.engine
            .write(&mut batch, false)
            .map_err(|e| io::Error::other(e.to_string()))?;
        Ok(())
    }
}

impl RaftLogReader<TypeConfig> for RaftEngineLogStore {
    async fn try_get_log_entries<RB>(&mut self, range: RB) -> Result<Vec<Entry>, io::Error>
    where
        RB: RangeBounds<u64> + Clone + Debug + OptionalSend,
    {
        let start = match range.start_bound() {
            Bound::Included(&n) => n,
            Bound::Excluded(&n) => n + 1,
            Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            Bound::Included(&n) => n + 1,
            Bound::Excluded(&n) => n,
            Bound::Unbounded => u64::MAX,
        };

        let mut res: Vec<StoredEntry> = Vec::new();

        // openraft tolerates (and expects) a short read at both ends of the
        // range: entries below `first_index` were removed by `purge`, entries
        // above `last_index` are not appended yet. raft-engine, however,
        // returns `EntryCompacted` / `EntryNotFound` for such ranges, and a
        // storage error here is fatal for the raft core (the replication
        // task reports it as `StorageError`). Clamp to what the engine holds.
        // A purge can still land between the clamp and the fetch, so retry a
        // few times before giving up.
        const ATTEMPTS: usize = 3;
        for attempt in 1..=ATTEMPTS {
            let (Some(first), Some(last)) = (
                self.engine.first_index(RAFT_GROUP_ID),
                self.engine.last_index(RAFT_GROUP_ID),
            ) else {
                return Ok(Vec::new());
            };
            let clamped_start = start.max(first);
            let clamped_end = end.min(last + 1);
            if clamped_start >= clamped_end {
                return Ok(Vec::new());
            }
            match self
                .engine
                .fetch_entries_to_with::<MessageExtTyped, Bincode2Codec>(
                    RAFT_GROUP_ID,
                    clamped_start,
                    clamped_end,
                    None,
                    &mut res,
                ) {
                Ok(_) => return Ok(res.into_iter().map(|stored| stored.0).collect()),
                Err(raft_engine::Error::EntryCompacted) if attempt < ATTEMPTS => {
                    res.clear();
                    continue;
                }
                Err(e) => return Err(io::Error::new(io::ErrorKind::InvalidData, e)),
            }
        }
        Ok(Vec::new())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote>, io::Error> {
        self.get_meta::<meta::Vote>()
    }
}

impl RaftLogStorage<TypeConfig> for RaftEngineLogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, io::Error> {
        let last_log_id = match self.engine.last_index(RAFT_GROUP_ID) {
            None => None,
            Some(index) => self
                .engine
                .get_entry_with::<MessageExtTyped, Bincode2Codec>(RAFT_GROUP_ID, index)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?
                .map(|entry| entry.0.log_id()),
        };
        let last_purged_log_id = self.get_meta::<meta::LastPurged>()?;
        let last_log_id = last_log_id.or(last_purged_log_id);
        Ok(LogState {
            last_purged_log_id,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        // RaftEngineLogStore is a cheap handle over the shared engine; the
        // committed fields are not part of the reader surface.
        Self {
            engine: Arc::clone(&self.engine),
            committed_path: self.committed_path.clone(),
            committed: self.committed,
            metrics: Arc::clone(&self.metrics),
            last_sidecar_flush: self.last_sidecar_flush,
        }
    }

    async fn save_vote(&mut self, vote: &Vote) -> Result<(), io::Error> {
        self.put_meta::<meta::Vote>(vote)?;
        // The vote must be durable before this returns (openraft requires it
        // for leader-campaign safety).
        Self::sync_engine(Arc::clone(&self.engine), Arc::clone(&self.metrics)).await
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: IOFlushed<TypeConfig>,
    ) -> Result<(), io::Error>
    where
        I: IntoIterator<Item = EntryOf<TypeConfig>> + Send,
    {
        let entries: Vec<StoredEntry> = entries.into_iter().map(StoredEntry).collect();
        let entry_count = entries.len();
        if entry_count == 0 {
            callback.io_completed(Ok(()));
            return Ok(());
        }
        // One LogBatch per append: a single write plus one fsync amortized
        // over the whole batch. Durability is only required before the
        // callback fires, so the sync runs off the async worker threads.
        let mut batch = LogBatch::with_capacity(256);
        batch
            .add_entries_with::<MessageExtTyped, Bincode2Codec>(RAFT_GROUP_ID, &entries)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        self.engine
            .write(&mut batch, false)
            .map_err(|e| io::Error::other(e.to_string()))?;
        self.metrics.record_append(entry_count);

        let engine = Arc::clone(&self.engine);
        let metrics = Arc::clone(&self.metrics);
        let _handle = tokio::task::spawn_blocking(move || {
            let started = Instant::now();
            let result = engine.sync().map_err(|e| io::Error::other(e.to_string()));
            metrics.record_sync_data(started.elapsed());
            callback.io_completed(result);
        });
        Ok(())
    }

    // raft-engine has no "drop the tail" primitive: it only knows `compact_to`
    // (drop a prefix) and overwrite-on-append (appending at an index that is
    // already present drops everything from that index on). Leaving this a
    // no-op is fine as long as the conflicting entries get overwritten by the
    // very next append, which is what happens on the normal log-matching
    // path. It is *not* fine on the two paths where openraft truncates without
    // appending afterwards:
    //
    // - `install_full_snapshot` truncates to `committed` and then purges up to
    //   the snapshot's last log id; a stale tail (from an older leader) beyond
    //   that index stays on disk, and after a restart `get_log_state` reports
    //   `last_log_id < last_purged_log_id` (older term), which openraft treats
    //   as a corrupted store and refuses to start.
    // - a `prev_log_id` mismatch truncates and replies "conflict" without
    //   appending anything.
    //
    // So implement it with the primitives raft-engine does have: re-append the
    // boundary entry (same content) so the engine drops the tail behind it, or
    // compact everything away when the boundary is not in the engine any more.
    async fn truncate_after(&mut self, last_log_id: Option<LogId>) -> Result<(), io::Error> {
        let (Some(first), Some(last)) = (
            self.engine.first_index(RAFT_GROUP_ID),
            self.engine.last_index(RAFT_GROUP_ID),
        ) else {
            // Nothing stored, nothing to truncate.
            return Ok(());
        };

        let keep_upto = last_log_id.as_ref().map(|id| id.index);
        if keep_upto.is_some_and(|index| index >= last) {
            // Nothing after `last_log_id`.
            return Ok(());
        }

        match keep_upto {
            Some(index) if index >= first => {
                let boundary = self
                    .engine
                    .get_entry_with::<MessageExtTyped, Bincode2Codec>(RAFT_GROUP_ID, index)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?
                    .ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("log entry {index} is within [{first}, {last}] but missing"),
                        )
                    })?;
                let mut batch = LogBatch::with_capacity(1);
                // Writing the boundary entry back makes raft-engine drop
                // everything after it, which is exactly the truncation.
                batch
                    .add_entries_with::<MessageExtTyped, Bincode2Codec>(RAFT_GROUP_ID, &[boundary])
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                self.engine
                    .write(&mut batch, false)
                    .map_err(|e| io::Error::other(e.to_string()))?;
            }
            _ => {
                // Either everything must go (`None`) or the boundary was
                // already purged: every stored entry is behind it, drop them
                // all. The memtable becomes empty, and raft-engine accepts an
                // append at any index on an empty memtable, so a later append
                // at `last_log_id + 1` (or at 1) works.
                self.engine.compact_to(RAFT_GROUP_ID, last + 1);
            }
        }

        // A snapshot installed right after this truncation is fsync'ed; if the
        // truncation were not, a crash could resurrect the stale tail next to
        // the new snapshot and fail openraft's startup consistency check.
        // Truncation is rare (conflicts only), so the extra fsync is cheap.
        Self::sync_engine(Arc::clone(&self.engine), Arc::clone(&self.metrics)).await
    }

    async fn purge(&mut self, log_id: LogId) -> Result<(), io::Error> {
        // Record the purged boundary first: openraft ignores logs at or below
        // it, and `get_log_state` reports it after a restart.
        self.put_meta::<meta::LastPurged>(&log_id)?;
        self.engine.compact_to(RAFT_GROUP_ID, log_id.index + 1);
        // Purging does not need to be durable.
        Ok(())
    }

    async fn save_committed(&mut self, committed: Option<LogId>) -> Result<(), io::Error> {
        // The commit pointer is recovery acceleration, not a correctness
        // record: openraft tolerates a stale value because a restarting node
        // re-confirms a cluster commit before serving reads again, and the
        // log replay covers whatever the stale sidecar missed. The rewrite is
        // therefore debounced (at most one every 200ms) so the per-commit
        // tmp-write+rename never sits on the quorum-acknowledgment path.
        self.committed = committed;
        let due = self
            .last_sidecar_flush
            .is_none_or(|flushed| flushed.elapsed() >= Duration::from_millis(200));
        if due {
            save_committed_sidecar(&self.committed_path, &self.committed)
                .map_err(|error| io::Error::other(error.to_string()))?;
            self.last_sidecar_flush = Some(Instant::now());
        }
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId>, io::Error> {
        Ok(self.committed)
    }
}

/// Load the recovery-acceleration sidecar that carries the last observed
/// cluster commit.
///
/// The sidecar is published atomically and never fsynced, so any failure to
/// parse it simply means recovery starts without a remembered commit; it can
/// never make the node serve wrong data.
fn load_committed_sidecar(path: &Path) -> io::Result<Option<LogId>> {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    if bytes.len() < RAFT_LOG_CHECKSUM_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "commit sidecar is truncated",
        ));
    }
    let payload_end = bytes.len() - RAFT_LOG_CHECKSUM_LEN;
    let expected = u32::from_le_bytes(bytes[payload_end..].try_into().unwrap());
    if checksum(&bytes[..payload_end]) != expected {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "commit sidecar checksum mismatch",
        ));
    }
    bincode::deserialize(&bytes[..payload_end])
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}

fn save_committed_sidecar(path: &Path, committed: &Option<LogId>) -> io::Result<()> {
    let payload = bincode::serialize(committed)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    let mut bytes = Vec::with_capacity(payload.len() + RAFT_LOG_CHECKSUM_LEN);
    bytes.extend_from_slice(&payload);
    let file_checksum = checksum(&bytes);
    bytes.extend_from_slice(&file_checksum.to_le_bytes());

    let temporary_path = path.with_extension("committed.tmp");
    std::fs::write(&temporary_path, &bytes)?;
    std::fs::rename(&temporary_path, path)
}

const RAFT_LOG_CHECKSUM_LEN: usize = 4;

fn checksum(bytes: &[u8]) -> u32 {
    let mut hash = 2_166_136_261_u32;
    for byte in bytes {
        hash ^= u32::from(*byte);
        hash = hash.wrapping_mul(16_777_619);
    }
    hash
}

/// Metadata of a raft store: everything openraft persists besides entries and
/// the state machine, keyed inside the engine's meta column.
mod meta {
    use openraft::RaftTypeConfig;
    use openraft::alias::LogIdOf;
    use openraft::alias::VoteOf;

    pub(crate) trait StoreMeta<C>
    where
        C: RaftTypeConfig,
    {
        const KEY: &'static str;
        type Value: serde::Serialize + serde::de::DeserializeOwned;
    }

    pub(crate) struct LastPurged {}
    pub(crate) struct Vote {}

    impl<C> StoreMeta<C> for LastPurged
    where
        C: RaftTypeConfig,
    {
        const KEY: &'static str = "last_purged_log_id";
        type Value = LogIdOf<C>;
    }

    impl<C> StoreMeta<C> for Vote
    where
        C: RaftTypeConfig,
    {
        const KEY: &'static str = "vote";
        type Value = VoteOf<C>;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use openraft::RaftTypeConfig;
    use openraft::entry::RaftEntry;
    use openraft::storage::IOFlushed;
    use openraft::vote::RaftLeaderId;

    fn log_id(term: u64, index: u64) -> LogId {
        LogId::new(
            <TypeConfig as RaftTypeConfig>::LeaderId::new(term, 1),
            index,
        )
    }

    fn blank(term: u64, index: u64) -> Entry {
        Entry::new_blank(log_id(term, index))
    }

    async fn append(store: &mut RaftEngineLogStore, entries: Vec<Entry>) {
        store.append(entries, IOFlushed::noop()).await.unwrap();
    }

    async fn indexes(store: &mut RaftEngineLogStore) -> Vec<u64> {
        store
            .try_get_log_entries(..)
            .await
            .unwrap()
            .into_iter()
            .map(|entry| entry.log_id().index)
            .collect()
    }

    async fn new_store() -> (tempfile::TempDir, RaftEngineLogStore) {
        let dir = tempfile::tempdir().unwrap();
        let store = RaftEngineLogStore::open(dir.path().join("raft-engine")).unwrap();
        (dir, store)
    }

    #[tokio::test]
    async fn append_read_roundtrip_and_vote_persistence() {
        let (_dir, mut store) = new_store().await;
        append(&mut store, vec![blank(1, 1), blank(1, 2), blank(1, 3)]).await;

        let entries = store.try_get_log_entries(2..4).await.unwrap();
        assert_eq!(
            entries.iter().map(|e| e.log_id().index).collect::<Vec<_>>(),
            vec![2, 3]
        );

        let vote = Vote::new_committed(3, 1);
        store.save_vote(&vote).await.unwrap();
        assert_eq!(store.read_vote().await.unwrap(), Some(vote));

        let state = store.get_log_state().await.unwrap();
        assert_eq!(state.last_log_id, Some(log_id(1, 3)));
        assert_eq!(state.last_purged_log_id, None);
    }

    #[tokio::test]
    async fn truncate_after_drops_the_tail_and_allows_reappend() {
        let (_dir, mut store) = new_store().await;
        append(&mut store, (1..=5).map(|i| blank(1, i)).collect()).await;
        assert_eq!(indexes(&mut store).await, vec![1, 2, 3, 4, 5]);

        // Conflict at 4: the leader's log continues with a different entry.
        store.truncate_after(Some(log_id(1, 3))).await.unwrap();
        assert_eq!(indexes(&mut store).await, vec![1, 2, 3]);
        assert_eq!(
            store.get_log_state().await.unwrap().last_log_id,
            Some(log_id(1, 3))
        );

        // The leader's version of 4.. is appended afterwards.
        append(&mut store, vec![blank(2, 4), blank(2, 5), blank(2, 6)]).await;
        let entries = store.try_get_log_entries(4..).await.unwrap();
        assert_eq!(
            entries.iter().map(|e| e.log_id()).collect::<Vec<_>>(),
            vec![log_id(2, 4), log_id(2, 5), log_id(2, 6)]
        );

        // Truncating at (or beyond) the end is a no-op.
        store.truncate_after(Some(log_id(2, 6))).await.unwrap();
        assert_eq!(indexes(&mut store).await, vec![1, 2, 3, 4, 5, 6]);
    }

    #[tokio::test]
    async fn truncate_after_none_and_after_purged_boundary_empty_the_log() {
        let (_dir, mut store) = new_store().await;
        append(&mut store, (1..=4).map(|i| blank(1, i)).collect()).await;

        store.truncate_after(None).await.unwrap();
        assert!(indexes(&mut store).await.is_empty());
        assert_eq!(store.get_log_state().await.unwrap().last_log_id, None);

        // The log can be rebuilt from index 1 afterwards.
        append(&mut store, (1..=6).map(|i| blank(2, i)).collect()).await;
        assert_eq!(indexes(&mut store).await, vec![1, 2, 3, 4, 5, 6]);

        // install_full_snapshot pattern: purge a prefix, then truncate to a
        // boundary that is no longer in the engine -> the (stale) rest goes.
        store.purge(log_id(2, 3)).await.unwrap();
        assert_eq!(indexes(&mut store).await, vec![4, 5, 6]);
        store.truncate_after(Some(log_id(2, 3))).await.unwrap();
        assert!(indexes(&mut store).await.is_empty());
        let state = store.get_log_state().await.unwrap();
        assert_eq!(state.last_purged_log_id, Some(log_id(2, 3)));
        assert_eq!(state.last_log_id, Some(log_id(2, 3)));

        // ...and the snapshot's successor entries can be appended at 4.
        append(&mut store, vec![blank(3, 4)]).await;
        assert_eq!(indexes(&mut store).await, vec![4]);
    }

    #[tokio::test]
    async fn reads_below_the_purged_prefix_are_clamped_not_errors() {
        let (_dir, mut store) = new_store().await;
        append(&mut store, (1..=6).map(|i| blank(1, i)).collect()).await;
        store.purge(log_id(1, 3)).await.unwrap();
        // openraft tolerates a short read at the purged end of the range.
        assert_eq!(indexes(&mut store).await, vec![4, 5, 6]);
        let partial = store.try_get_log_entries(2..5).await.unwrap();
        assert_eq!(
            partial.iter().map(|e| e.log_id().index).collect::<Vec<_>>(),
            vec![4]
        );
    }

    #[tokio::test]
    async fn committed_sidecar_survives_a_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let log_id = log_id(4, 9);
        {
            let mut store = RaftEngineLogStore::open(dir.path().join("raft-engine")).unwrap();
            store.save_committed(Some(log_id)).await.unwrap();
        }
        let mut reopened = RaftEngineLogStore::open(dir.path().join("raft-engine")).unwrap();
        assert_eq!(reopened.read_committed().await.unwrap(), Some(log_id));
    }
}
