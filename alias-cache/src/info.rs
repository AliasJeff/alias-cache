//! Server observability commands.
//!
//! `INFO` exposes raft state, replication progress, and cluster membership as
//! Redis-style `key:value` lines. Everything is derived from openraft metrics
//! so the report can never disagree with what the consensus layer believes.

use std::fmt::Write as _;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use bytes::Bytes;
use openraft::rt::WatchReceiver;

use crate::protocol::{Frame, Request};
use crate::raft::{NodeId, RaftHandle};

/// Runtime counters and file locations backing the `INFO stats` section.
#[derive(Clone, Debug)]
pub struct StatsContext {
    /// Live client connections, maintained by the accept loop.
    pub live_connections: Arc<AtomicUsize>,
    /// Connection cap from configuration; 0 means unlimited.
    pub max_connections: u32,
    /// OOM write threshold from configuration; `None` means unlimited.
    pub max_memory: Option<u64>,
    /// Node data directory: raft log and snapshot live under `raft/`.
    pub data_dir: PathBuf,
    /// Total client commands handled since startup.
    pub requests_served: Arc<AtomicU64>,
}

impl StatsContext {
    pub fn new(
        data_dir: impl Into<PathBuf>,
        max_connections: u32,
        max_memory: Option<u64>,
    ) -> Self {
        Self {
            live_connections: Arc::new(AtomicUsize::new(0)),
            max_connections,
            max_memory,
            data_dir: data_dir.into(),
            requests_served: Arc::new(AtomicU64::new(0)),
        }
    }

    fn file_bytes(&self, relative: &str) -> u64 {
        std::fs::metadata(self.data_dir.join(relative))
            .map(|metadata| metadata.len())
            .unwrap_or(0)
    }
}

/// Handle an `INFO [section]` request; returns `None` for any other command.
///
/// Unknown sections render an empty body, matching Redis, and every field is
/// read from one metrics snapshot so the report cannot mix two raft states.
pub async fn execute(raft: &RaftHandle, stats: &StatsContext, request: &Request) -> Option<Frame> {
    let Request::Command(arguments) = request;
    if !arguments
        .first()
        .is_some_and(|command| command.eq_ignore_ascii_case(b"INFO"))
    {
        return None;
    }
    if arguments.len() > 2 {
        return Some(Frame::error(
            "ERR wrong number of arguments for 'info' command",
        ));
    }
    let wants = |section: &str| {
        arguments
            .get(1)
            .is_none_or(|requested| requested.eq_ignore_ascii_case(section.as_bytes()))
    };
    let wants_any = |sections: &[&str]| sections.iter().any(|section| wants(section));

    let metrics = raft.metrics().borrow_watched().clone();
    let leader_id = metrics.current_leader;
    let role = if leader_id == Some(metrics.id) {
        "leader"
    } else {
        "follower"
    };
    let voters: Vec<NodeId> = metrics.membership_config.voter_ids().collect();
    let learners: Vec<NodeId> = metrics
        .membership_config
        .membership()
        .nodes()
        .map(|(node_id, _)| *node_id)
        .filter(|node_id| !voters.contains(node_id))
        .collect();

    // Health levels, from liveness to write readiness. A follower is
    // readable only while some leader exists (reads forward); writability
    // additionally requires this node to be that leader.
    let mut output = String::new();
    if wants_any(&["health", "server"]) {
        let _ = writeln!(output, "# Health");
        let _ = writeln!(output, "alive:true");
        let raft_recovered = metrics.running_state.is_ok();
        let _ = writeln!(output, "raft_recovered:{raft_recovered}");
        let readable = leader_id.is_some();
        let _ = writeln!(output, "readable:{readable}");
        let writable = leader_id == Some(metrics.id);
        let _ = writeln!(output, "writable:{writable}");
    }
    if wants("server") {
        let _ = writeln!(output, "# Server");
        let alias_cache_version = env!("CARGO_PKG_VERSION");
        let _ = writeln!(output, "alias_cache_version:{alias_cache_version}");
        let node_id = metrics.id;
        let _ = writeln!(output, "node_id:{node_id}");
        let _ = writeln!(output, "role:{role}");
        let running = metrics.running_state.is_ok();
        let _ = writeln!(output, "running:{running}");
    }
    if wants("replication") {
        let _ = writeln!(output, "# Replication");
        let _ = writeln!(output, "state:{:?}", metrics.state);
        let term = metrics.current_term;
        let _ = writeln!(output, "term:{term}");
        let _ = writeln!(
            output,
            "leader_id:{}",
            leader_id.map_or(String::new(), |id| id.to_string())
        );
        let _ = writeln!(
            output,
            "last_log_index:{}",
            metrics.last_log_index.map_or(-1, |index| index as i64)
        );
        let _ = writeln!(
            output,
            "local_committed_index:{}",
            metrics
                .local_committed
                .map_or(-1, |log_id| log_id.index as i64)
        );
        let _ = writeln!(
            output,
            "cluster_committed_index:{}",
            metrics
                .cluster_committed
                .map_or(-1, |log_id| log_id.index as i64)
        );
        let _ = writeln!(
            output,
            "last_applied_index:{}",
            metrics
                .last_applied
                .map_or(-1, |log_id| log_id.index as i64)
        );
        let _ = writeln!(
            output,
            "snapshot_index:{}",
            metrics.snapshot.map_or(-1, |log_id| log_id.index as i64)
        );
        let _ = writeln!(
            output,
            "purged_index:{}",
            metrics.purged.map_or(-1, |log_id| log_id.index as i64)
        );
        // Replication lag is meaningful only on the leader: openraft reports
        // per-follower match points there and leaves them empty elsewhere.
        if role == "leader"
            && let Some(replication) = &metrics.replication
        {
            for (node_id, matched) in replication {
                let lag = match (metrics.last_log_index, matched.as_ref().map(|id| id.index)) {
                    (Some(head), Some(matched_index)) => (head - matched_index) as i64,
                    _ => -1,
                };
                let _ = writeln!(output, "replication_lag_node_{node_id}:{lag}");
            }
        }
    }
    if wants("stats") {
        let _ = writeln!(output, "# Stats");
        let _ = writeln!(
            output,
            "live_connections:{}",
            stats.live_connections.load(Ordering::SeqCst)
        );
        let max_connections = stats.max_connections;
        let _ = writeln!(output, "max_connections:{max_connections}");
        let memory = raft
            .with_state_machine(|state_machine| {
                Box::pin(async move { state_machine.state().await.memory_usage() })
            })
            .await
            .unwrap_or(0);
        let _ = writeln!(output, "memory_usage_bytes:{memory}");
        let max_memory_bytes = stats.max_memory.unwrap_or(0);
        let _ = writeln!(output, "max_memory_bytes:{max_memory_bytes}");
        let raft_log_bytes = stats.file_bytes("raft/log");
        let _ = writeln!(output, "raft_log_bytes:{raft_log_bytes}");
        let raft_snapshot_bytes = stats.file_bytes("raft/snapshot");
        let _ = writeln!(output, "raft_snapshot_bytes:{raft_snapshot_bytes}");
        let _ = writeln!(
            output,
            "client_requests:{}",
            stats.requests_served.load(Ordering::SeqCst)
        );
    }
    if wants("cluster") {
        let _ = writeln!(output, "# Cluster");
        let cluster_size = voters.len() + learners.len();
        let _ = writeln!(output, "cluster_size:{cluster_size}");
        let voters_list = join_ids(&voters);
        let _ = writeln!(output, "voters:{voters_list}");
        let learners_list = join_ids(&learners);
        let _ = writeln!(output, "learners:{learners_list}");
    }
    Some(Frame::VerbatimString {
        format: Bytes::from_static(b"txt"),
        data: Bytes::from(output),
    })
}

fn join_ids(node_ids: &[NodeId]) -> String {
    node_ids
        .iter()
        .map(|node_id| node_id.to_string())
        .collect::<Vec<_>>()
        .join(",")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::Request;
    use crate::raft::open_single_node;

    fn request(parts: &[&[u8]]) -> Request {
        Request::Command(
            parts
                .iter()
                .map(|part| Bytes::copy_from_slice(part))
                .collect(),
        )
    }

    fn temp_dir(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("alias-cache-info-{name}-{}", std::process::id()))
    }

    #[tokio::test]
    async fn single_node_info_reports_leader_and_membership() {
        let data_dir = temp_dir("single");
        let raft = open_single_node(7, &data_dir, "127.0.0.1:1", None)
            .await
            .unwrap();
        let stats = StatsContext::new(&data_dir, 0, None);
        let frame = execute(&raft, &stats, &request(&[b"INFO"])).await.unwrap();

        let Frame::VerbatimString { format, data } = frame else {
            panic!("INFO must return a verbatim string");
        };
        assert_eq!(format.as_ref(), b"txt");
        let text = String::from_utf8(data.to_vec()).unwrap();
        assert!(text.contains("# Health"));
        assert!(text.contains("alive:true"));
        assert!(text.contains("raft_recovered:true"));
        assert!(text.contains("readable:true"));
        assert!(text.contains("writable:true"));
        assert!(text.contains("# Server"));
        assert!(text.contains("node_id:7"));
        assert!(text.contains("role:leader"));
        assert!(text.contains("# Replication"));
        assert!(text.contains("state:Leader"));
        assert!(text.contains("leader_id:7"));
        assert!(text.contains("# Cluster"));
        assert!(text.contains("voters:7"));
        assert!(text.contains("learners:"));
        raft.shutdown().await.unwrap();
        let _ = std::fs::remove_dir_all(data_dir);
    }

    #[tokio::test]
    async fn stats_section_reports_memory_files_and_requests() {
        let data_dir = temp_dir("stats");
        let raft = open_single_node(9, &data_dir, "127.0.0.1:1", Some(1_048_576))
            .await
            .unwrap();
        let stats = StatsContext::new(&data_dir, 12, Some(1_048_576));

        // Serve a few writes through the state machine first so the raft log
        // file and memory counter have content.
        for index in 0..3 {
            raft.client_write(crate::raft::RaftRequest::Apply(
                crate::state_machine::Operation::Set {
                    key: Bytes::from(format!("key-{index}")),
                    value: Bytes::from_static(b"value"),
                },
            ))
            .await
            .unwrap();
        }
        // Mirror what a connection handler would count.
        stats
            .requests_served
            .fetch_add(3, std::sync::atomic::Ordering::SeqCst);

        let frame = execute(&raft, &stats, &request(&[b"INFO", b"stats"]))
            .await
            .unwrap();
        let Frame::VerbatimString { data, .. } = frame else {
            panic!("INFO stats must return a verbatim string");
        };
        let text = String::from_utf8(data.to_vec()).unwrap();
        assert!(text.starts_with("# Stats"), "got: {text}");
        assert!(text.contains("live_connections:0"));
        assert!(text.contains("max_connections:12"));
        assert!(
            text.contains("memory_usage_bytes:"),
            "memory counter must be present: {text}"
        );
        assert!(text.contains("max_memory_bytes:1048576"));
        assert!(
            text.contains("raft_log_bytes:"),
            "raft log size must be present: {text}"
        );
        assert!(text.contains("client_requests:3"));

        // The three writes each hold "key-N" (6-7 bytes) plus "value" (5):
        // at least 30 bytes of live payload must be counted.
        let memory: u64 = text
            .lines()
            .find_map(|line| line.strip_prefix("memory_usage_bytes:"))
            .expect("memory_usage_bytes line")
            .parse()
            .unwrap();
        assert!(
            memory >= 30,
            "memory counter must track writes, got {memory}"
        );

        raft.shutdown().await.unwrap();
        let _ = std::fs::remove_dir_all(data_dir);
    }

    #[tokio::test]
    async fn info_accepts_section_filters_and_unknown_sections_render_empty() {
        let data_dir = temp_dir("sections");
        let raft = open_single_node(8, &data_dir, "127.0.0.1:1", None)
            .await
            .unwrap();
        let stats = StatsContext::new(&data_dir, 0, None);

        let replication = execute(&raft, &stats, &request(&[b"INFO", b"replication"]))
            .await
            .unwrap();
        let Frame::VerbatimString { data, .. } = replication else {
            panic!("INFO replication must return a verbatim string");
        };
        let text = String::from_utf8(data.to_vec()).unwrap();
        assert!(text.starts_with("# Replication"));
        assert!(!text.contains("# Server"));

        let unknown = execute(&raft, &stats, &request(&[b"INFO", b"bogus"]))
            .await
            .unwrap();
        let Frame::VerbatimString { data, .. } = unknown else {
            panic!("INFO bogus must return a verbatim string");
        };
        assert!(data.is_empty());

        let arity = execute(&raft, &stats, &request(&[b"INFO", b"a", b"b"]))
            .await
            .unwrap();
        assert!(arity.to_string().contains("wrong number of arguments"));
        raft.shutdown().await.unwrap();
        let _ = std::fs::remove_dir_all(data_dir);
    }
}
