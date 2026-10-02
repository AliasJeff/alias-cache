//! OpenRaft type configuration and an in-memory storage vertical slice.

use crate::state_machine::{Operation, StateMachine};
use openraft::storage::{
    EntryResponder, IOFlushed, LogState, RaftLogStorage, RaftSnapshotBuilder, RaftStateMachine,
};
use openraft::{EntryPayload, OptionalSend, RaftLogReader, RaftTypeConfig};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::io;
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;
use tokio::sync::Mutex;

pub type NodeId = u64;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Node {
    pub addr: String,
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

#[derive(Debug, Clone, Default)]
pub struct MemLogStore {
    entries: Vec<Entry>,
    vote: Option<Vote>,
    last_purged_log_id: Option<LogId>,
    committed: Option<LogId>,
}

impl MemLogStore {
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

impl RaftLogReader<TypeConfig> for MemLogStore {
    async fn try_get_log_entries<RB>(&mut self, range: RB) -> Result<Vec<Entry>, io::Error>
    where
        RB: RangeBounds<u64> + Clone + Debug + OptionalSend,
    {
        let start = match range.start_bound() {
            Bound::Included(index) => *index,
            Bound::Excluded(index) => index.saturating_add(1),
            Bound::Unbounded => self.first_index(),
        };
        let end = match range.end_bound() {
            Bound::Included(index) => index.saturating_add(1),
            Bound::Excluded(index) => *index,
            Bound::Unbounded => u64::MAX,
        };

        Ok(self
            .entries
            .iter()
            .filter(|entry| entry.log_id.index >= start && entry.log_id.index < end)
            .cloned()
            .collect())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote>, io::Error> {
        Ok(self.vote)
    }
}

impl RaftLogStorage<TypeConfig> for MemLogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, io::Error> {
        Ok(LogState {
            last_purged_log_id: self.last_purged_log_id,
            last_log_id: self.last_log_id(),
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote) -> Result<(), io::Error> {
        self.vote = Some(*vote);
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
        self.entries.extend(entries);
        callback.io_completed(Ok(()));
        Ok(())
    }

    async fn truncate_after(&mut self, last_log_id: Option<LogId>) -> Result<(), io::Error> {
        if let Some(last_log_id) = last_log_id {
            self.entries
                .retain(|entry| entry.log_id.index <= last_log_id.index);
        } else {
            self.entries.clear();
        }
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId) -> Result<(), io::Error> {
        self.entries
            .retain(|entry| entry.log_id.index > log_id.index);
        self.last_purged_log_id = Some(log_id);
        Ok(())
    }

    async fn save_committed(&mut self, committed: Option<LogId>) -> Result<(), io::Error> {
        self.committed = committed;
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId>, io::Error> {
        Ok(self.committed)
    }
}

#[derive(Debug, Clone, Default)]
pub struct MemStateMachine {
    state: StateMachine,
    last_applied: Option<LogId>,
    membership: StoredMembership,
    snapshot: Option<Vec<u8>>,
}

impl MemStateMachine {
    pub fn state(&self) -> &StateMachine {
        &self.state
    }
}

impl RaftStateMachine<TypeConfig> for MemStateMachine {
    type SnapshotData = std::io::Cursor<Vec<u8>>;
    type SnapshotBuilder = Self;

    async fn applied_state(&mut self) -> Result<(Option<LogId>, StoredMembership), io::Error> {
        Ok((self.last_applied, self.membership.clone()))
    }

    async fn apply<Strm>(&mut self, mut entries: Strm) -> Result<(), io::Error>
    where
        Strm: tokio_stream::Stream<Item = Result<EntryResponder<TypeConfig>, io::Error>>
            + Unpin
            + OptionalSend,
    {
        while let Some((entry, responder)) = tokio_stream::StreamExt::try_next(&mut entries).await?
        {
            self.last_applied = Some(entry.log_id);
            let response = match entry.payload {
                EntryPayload::Blank => RaftResponse::Noop,
                EntryPayload::Normal(RaftRequest::Apply(operation)) => {
                    let _ = self.state.apply(operation);
                    RaftResponse::Applied
                }
                EntryPayload::Membership(membership) => {
                    self.membership = StoredMembership::new(Some(entry.log_id), membership);
                    RaftResponse::Noop
                }
            };
            if let Some(responder) = responder {
                responder.send(response);
            }
        }
        Ok(())
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn install_snapshot(
        &mut self,
        meta: &openraft::SnapshotMeta<<TypeConfig as RaftTypeConfig>::LeaderId, NodeId, Node>,
        mut snapshot: Self::SnapshotData,
    ) -> Result<(), io::Error> {
        use std::io::Read;

        let mut data = Vec::new();
        snapshot.read_to_end(&mut data)?;
        self.state = bincode::deserialize(&data).map_err(io::Error::other)?;
        self.last_applied = meta.last_log_id;
        self.membership = meta.last_membership.clone();
        self.snapshot = Some(data);
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
        Ok(self.snapshot.clone().map(|data| openraft::Snapshot {
            meta: openraft::SnapshotMeta {
                last_log_id: self.last_applied,
                last_membership: self.membership.clone(),
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
        let data = bincode::serialize(&self.state).map_err(io::Error::other)?;
        self.snapshot = Some(data.clone());
        Ok(openraft::Snapshot {
            meta: openraft::SnapshotMeta {
                last_log_id: self.last_applied,
                last_membership: self.membership.clone(),
            },
            snapshot: std::io::Cursor::new(data),
        })
    }
}

pub type SharedMemStateMachine = Arc<Mutex<MemStateMachine>>;

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use openraft::vote::RaftLeaderId;

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
        assert_eq!(
            store
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
        assert_eq!(store.last_log_id().expect("entry should remain").index, 1);
    }

    #[tokio::test]
    async fn memory_state_machine_applies_operations_and_snapshots() {
        let mut state_machine = MemStateMachine::default();
        let request = RaftRequest::Apply(Operation::Set {
            key: Bytes::from_static(b"key"),
            value: Bytes::from_static(b"value"),
        });
        let RaftRequest::Apply(operation) = request;
        state_machine.state.apply(operation);

        let snapshot = state_machine
            .build_snapshot()
            .await
            .expect("snapshot should succeed");
        let mut restored = MemStateMachine::default();
        restored
            .install_snapshot(&snapshot.meta, snapshot.snapshot)
            .await
            .expect("snapshot install should succeed");
        assert_eq!(restored.state(), state_machine.state());
    }
}
