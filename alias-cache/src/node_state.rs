use std::path::Path;
use std::sync::Arc;

use tokio::sync::Mutex;

use crate::error::Result;
use crate::protocol::{Frame, Request};
use crate::snapshot::SnapshotStore;
use crate::state_machine::{Operation, StateMachine};
use crate::wal::Wal;

#[derive(Debug)]
pub struct NodeState {
    state_machine: StateMachine,
    wal: Wal,
    snapshot: SnapshotStore,
}

impl NodeState {
    pub fn open(data_dir: impl AsRef<Path>) -> Result<Self> {
        let data_dir = data_dir.as_ref();
        let snapshot = SnapshotStore::new(data_dir)?;
        let mut state_machine = snapshot.load()?.unwrap_or_default();
        let wal_path = data_dir.join("raft.log");
        let mut replay_wal = Wal::open(&wal_path)?;
        replay_wal.replay(&mut state_machine)?;
        let wal = Wal::open(wal_path)?;
        Ok(Self {
            state_machine,
            wal,
            snapshot,
        })
    }

    pub fn execute(&mut self, request: Request) -> Result<Frame> {
        let (operation, response) = self.state_machine.execute_request(request)?;
        if let Some(operation) = operation {
            self.append_and_apply(operation)?;
        }
        Ok(response)
    }

    fn append_and_apply(&mut self, operation: Operation) -> Result<()> {
        self.wal.append(&operation)?;
        let _ = self.state_machine.apply(operation);
        Ok(())
    }

    pub fn save_snapshot(&self) -> Result<()> {
        self.snapshot.save(&self.state_machine)
    }
}

pub type SharedNodeState = Arc<Mutex<NodeState>>;
