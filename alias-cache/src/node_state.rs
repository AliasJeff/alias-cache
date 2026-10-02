use std::path::Path;
use std::sync::Arc;

use tokio::sync::Mutex;

use crate::error::Result;
use crate::protocol::{Frame, Request};
use crate::state_machine::{Operation, StateMachine};
use crate::wal::Wal;

#[derive(Debug)]
pub struct NodeState {
    state_machine: StateMachine,
    wal: Wal,
}

impl NodeState {
    pub fn open(data_dir: impl AsRef<Path>) -> Result<Self> {
        let wal_path = data_dir.as_ref().join("raft.log");
        let mut state_machine = StateMachine::default();
        let mut replay_wal = Wal::open(&wal_path)?;
        replay_wal.replay(&mut state_machine)?;
        let wal = Wal::open(wal_path)?;
        Ok(Self { state_machine, wal })
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
}

pub type SharedNodeState = Arc<Mutex<NodeState>>;
