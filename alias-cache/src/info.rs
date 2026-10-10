//! Server observability commands.
//!
//! `INFO` exposes raft state, replication progress, and cluster membership as
//! Redis-style `key:value` lines. Everything is derived from openraft metrics
//! so the report can never disagree with what the consensus layer believes.

use std::fmt::Write as _;

use bytes::Bytes;
use openraft::rt::WatchReceiver;

use crate::protocol::{Frame, Request};
use crate::raft::{NodeId, RaftHandle};

/// Handle an `INFO [section]` request; returns `None` for any other command.
///
/// Unknown sections render an empty body, matching Redis, and every field is
/// read from one metrics snapshot so the report cannot mix two raft states.
pub async fn execute(raft: &RaftHandle, request: &Request) -> Option<Frame> {
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

    let mut output = String::new();
    if wants("server") {
        let _ = writeln!(output, "# Server");
        let _ = writeln!(output, "alias_cache_version:{}", env!("CARGO_PKG_VERSION"));
        let _ = writeln!(output, "node_id:{}", metrics.id);
        let _ = writeln!(output, "role:{role}");
        let _ = writeln!(output, "running:{}", metrics.running_state.is_ok());
    }
    if wants("replication") {
        let _ = writeln!(output, "# Replication");
        let _ = writeln!(output, "state:{:?}", metrics.state);
        let _ = writeln!(output, "term:{}", metrics.current_term);
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
    if wants("cluster") {
        let _ = writeln!(output, "# Cluster");
        let _ = writeln!(output, "cluster_size:{}", voters.len() + learners.len());
        let _ = writeln!(output, "voters:{}", join_ids(&voters));
        let _ = writeln!(output, "learners:{}", join_ids(&learners));
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
        let raft = open_single_node(7, &data_dir, "127.0.0.1:1").await.unwrap();
        let frame = execute(&raft, &request(&[b"INFO"])).await.unwrap();

        let Frame::VerbatimString { format, data } = frame else {
            panic!("INFO must return a verbatim string");
        };
        assert_eq!(format.as_ref(), b"txt");
        let text = String::from_utf8(data.to_vec()).unwrap();
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
    async fn info_accepts_section_filters_and_unknown_sections_render_empty() {
        let data_dir = temp_dir("sections");
        let raft = open_single_node(8, &data_dir, "127.0.0.1:1").await.unwrap();

        let replication = execute(&raft, &request(&[b"INFO", b"replication"]))
            .await
            .unwrap();
        let Frame::VerbatimString { data, .. } = replication else {
            panic!("INFO replication must return a verbatim string");
        };
        let text = String::from_utf8(data.to_vec()).unwrap();
        assert!(text.starts_with("# Replication"));
        assert!(!text.contains("# Server"));

        let unknown = execute(&raft, &request(&[b"INFO", b"bogus"]))
            .await
            .unwrap();
        let Frame::VerbatimString { data, .. } = unknown else {
            panic!("INFO bogus must return a verbatim string");
        };
        assert!(data.is_empty());

        let arity = execute(&raft, &request(&[b"INFO", b"a", b"b"]))
            .await
            .unwrap();
        assert!(arity.to_string().contains("wrong number of arguments"));
        raft.shutdown().await.unwrap();
        let _ = std::fs::remove_dir_all(data_dir);
    }
}
