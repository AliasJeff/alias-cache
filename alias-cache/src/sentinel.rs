use std::collections::BTreeMap;
use std::net::SocketAddr;

use bytes::Bytes;
use openraft::rt::WatchReceiver;

use crate::config::Config;
use crate::protocol::{Frame, Request};
use crate::raft::{Node, NodeId, RaftHandle};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SentinelContext {
    pub master_name: String,
    pub local_node_id: NodeId,
    pub local_client_addr: SocketAddr,
    pub cluster_client_addrs: BTreeMap<NodeId, SocketAddr>,
}

impl SentinelContext {
    pub fn from_config(config: &Config) -> Self {
        Self {
            master_name: config.sentinel_master_name.clone(),
            local_node_id: config.node_id,
            local_client_addr: config.client_addr,
            cluster_client_addrs: config.cluster_client_addrs.clone(),
        }
    }

    fn client_addr(&self, node_id: NodeId) -> Option<SocketAddr> {
        if node_id == self.local_node_id {
            Some(self.local_client_addr)
        } else {
            self.cluster_client_addrs.get(&node_id).copied()
        }
    }
}

pub async fn execute(
    raft: &RaftHandle,
    context: &SentinelContext,
    request: &Request,
) -> Option<Frame> {
    let Request::Command(arguments) = request;
    if !arguments
        .first()
        .is_some_and(|command| command.eq_ignore_ascii_case(b"SENTINEL"))
    {
        return None;
    }

    let response = match arguments.get(1).map(|command| command.as_ref()) {
        Some(command) if command.eq_ignore_ascii_case(b"MASTERS") => {
            masters(raft, context, arguments.len()).await
        }
        Some(command) if command.eq_ignore_ascii_case(b"GET-MASTER-ADDR-BY-NAME") => {
            get_master_addr(raft, context, arguments).await
        }
        Some(command) if command.eq_ignore_ascii_case(b"SLAVES") => {
            slaves(raft, context, arguments).await
        }
        Some(command) if command.eq_ignore_ascii_case(b"SENTINELS") => {
            sentinels(raft, context, arguments).await
        }
        Some(command) => Frame::error(format!(
            "ERR unknown SENTINEL subcommand '{}'",
            String::from_utf8_lossy(command)
        )),
        None => Frame::error("ERR wrong number of arguments for 'SENTINEL' command"),
    };
    Some(response)
}

async fn masters(raft: &RaftHandle, context: &SentinelContext, argument_count: usize) -> Frame {
    if argument_count != 2 {
        return Frame::error("ERR wrong number of arguments for 'SENTINEL MASTERS' command");
    }
    let leader = raft.current_leader().await;
    let nodes = membership_nodes(raft);
    let endpoint = leader.and_then(|node_id| context.client_addr(node_id));
    let flags = if leader.is_some() {
        "master"
    } else {
        "master,disconnected,o_down"
    };
    let mut fields = vec![
        "name".to_string(),
        context.master_name.clone(),
        "ip".to_string(),
        endpoint
            .map(|address| address.ip().to_string())
            .unwrap_or_default(),
        "port".to_string(),
        endpoint
            .map(|address| address.port().to_string())
            .unwrap_or_default(),
        "flags".to_string(),
        flags.to_string(),
        "num-other-sentinels".to_string(),
        nodes.len().saturating_sub(1).to_string(),
    ];
    if endpoint.is_none() {
        fields.extend(["state".to_string(), "client-address-unknown".to_string()]);
    }
    Frame::Array(vec![flat_fields(fields)])
}

async fn get_master_addr(
    raft: &RaftHandle,
    context: &SentinelContext,
    arguments: &[Bytes],
) -> Frame {
    if arguments.len() != 3 {
        return Frame::error(
            "ERR wrong number of arguments for 'SENTINEL GET-MASTER-ADDR-BY-NAME' command",
        );
    }
    if arguments[2] != context.master_name.as_bytes() {
        return Frame::Null;
    }
    let Some(address) = raft
        .current_leader()
        .await
        .and_then(|node_id| context.client_addr(node_id))
    else {
        return Frame::Null;
    };
    Frame::Array(vec![
        Frame::bulk_string(address.ip().to_string()),
        Frame::bulk_string(address.port().to_string()),
    ])
}

async fn slaves(raft: &RaftHandle, context: &SentinelContext, arguments: &[Bytes]) -> Frame {
    if arguments.len() != 3 {
        return Frame::error("ERR wrong number of arguments for 'SENTINEL SLAVES' command");
    }
    if arguments[2] != context.master_name.as_bytes() {
        return Frame::Array(Vec::new());
    }
    let leader = raft.current_leader().await;
    let master_endpoint = leader.and_then(|node_id| context.client_addr(node_id));
    let result = membership_nodes(raft)
        .into_iter()
        .filter(|(node_id, _)| Some(*node_id) != leader)
        .map(|(node_id, _)| {
            let endpoint = context.client_addr(node_id);
            let flags = if leader.is_some() && endpoint.is_some() {
                "slave"
            } else {
                "slave,disconnected"
            };
            flat_fields([
                "name".to_string(),
                node_id.to_string(),
                "ip".to_string(),
                endpoint
                    .map(|address| address.ip().to_string())
                    .unwrap_or_default(),
                "port".to_string(),
                endpoint
                    .map(|address| address.port().to_string())
                    .unwrap_or_default(),
                "runid".to_string(),
                node_id.to_string(),
                "flags".to_string(),
                flags.to_string(),
                "master-link-status".to_string(),
                if master_endpoint.is_some() {
                    "ok".to_string()
                } else {
                    "err".to_string()
                },
                "master-host".to_string(),
                master_endpoint
                    .map(|address| address.ip().to_string())
                    .unwrap_or_default(),
                "master-port".to_string(),
                master_endpoint
                    .map(|address| address.port().to_string())
                    .unwrap_or_default(),
                "slave-repl-offset".to_string(),
                "0".to_string(),
                "slave-priority".to_string(),
                "100".to_string(),
            ])
        })
        .collect();
    Frame::Array(result)
}

async fn sentinels(raft: &RaftHandle, context: &SentinelContext, arguments: &[Bytes]) -> Frame {
    if arguments.len() != 3 {
        return Frame::error("ERR wrong number of arguments for 'SENTINEL SENTINELS' command");
    }
    if arguments[2] != context.master_name.as_bytes() {
        return Frame::Array(Vec::new());
    }
    let result = membership_nodes(raft)
        .into_iter()
        .filter(|(node_id, _)| *node_id != context.local_node_id)
        .map(|(node_id, _)| {
            let endpoint = context.client_addr(node_id);
            flat_fields([
                "name".to_string(),
                node_id.to_string(),
                "ip".to_string(),
                endpoint
                    .map(|address| address.ip().to_string())
                    .unwrap_or_default(),
                "port".to_string(),
                endpoint
                    .map(|address| address.port().to_string())
                    .unwrap_or_default(),
                "runid".to_string(),
                node_id.to_string(),
                "flags".to_string(),
                if endpoint.is_some() {
                    "sentinel".to_string()
                } else {
                    "sentinel,disconnected".to_string()
                },
            ])
        })
        .collect();
    Frame::Array(result)
}

fn membership_nodes(raft: &RaftHandle) -> Vec<(NodeId, Node)> {
    let metrics = raft.metrics();
    metrics
        .borrow_watched()
        .membership_config
        .nodes()
        .map(|(node_id, node)| (*node_id, node.clone()))
        .collect()
}

fn flat_fields(fields: impl IntoIterator<Item = String>) -> Frame {
    Frame::Array(fields.into_iter().map(Frame::bulk_string).collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::Request;
    use crate::raft::{Node, PeerMap, enable_raft_runtime, new_mem_raft, open_single_node};
    use std::sync::Arc;
    use tokio::sync::Mutex;

    fn request(parts: &[&[u8]]) -> Request {
        Request::Command(
            parts
                .iter()
                .map(|part| Bytes::copy_from_slice(part))
                .collect(),
        )
    }

    #[test]
    fn context_uses_local_and_remote_client_addresses() {
        let config = Config::default()
            .with_cluster_client_addrs(BTreeMap::from([(2, "127.0.0.1:6380".parse().unwrap())]));
        let context = SentinelContext::from_config(&config);
        assert_eq!(context.client_addr(1), Some(config.client_addr));
        assert_eq!(
            context.client_addr(2),
            Some("127.0.0.1:6380".parse().unwrap())
        );
        assert!(context.client_addr(3).is_none());
    }

    #[test]
    fn non_sentinel_requests_are_not_claimed() {
        let request = request(&[b"GET", b"key"]);
        assert!(matches!(request, Request::Command(_)));
    }

    #[tokio::test]
    async fn read_only_commands_report_single_node_leader() {
        let data_dir = std::env::temp_dir().join(format!(
            "alias-cache-sentinel-{}-{}",
            std::process::id(),
            unique_test_id()
        ));
        let raft = open_single_node(1, &data_dir, "127.0.0.1:16379", None)
            .await
            .unwrap();
        let context = SentinelContext {
            master_name: "cache".to_string(),
            local_node_id: 1,
            local_client_addr: "127.0.0.1:6379".parse().unwrap(),
            cluster_client_addrs: BTreeMap::new(),
        };

        let address = execute(
            &raft,
            &context,
            &request(&[b"SENTINEL", b"GET-MASTER-ADDR-BY-NAME", b"cache"]),
        )
        .await
        .unwrap();
        assert_eq!(
            address,
            Frame::Array(vec![
                Frame::bulk_string("127.0.0.1"),
                Frame::bulk_string("6379"),
            ])
        );

        let masters = execute(&raft, &context, &request(&[b"SENTINEL", b"MASTERS"]))
            .await
            .unwrap();
        assert!(matches!(
            masters,
            Frame::Array(values)
                if values.len() == 1
                    && matches!(&values[0], Frame::Array(fields)
                        if fields.contains(&Frame::bulk_string("name"))
                            && fields.contains(&Frame::bulk_string("cache"))
                            && fields.contains(&Frame::bulk_string("flags"))
                            && fields.contains(&Frame::bulk_string("master")))
        ));

        let mismatch = execute(
            &raft,
            &context,
            &request(&[b"SENTINEL", b"SLAVES", b"other"]),
        )
        .await
        .unwrap();
        assert_eq!(mismatch, Frame::Array(Vec::new()));

        raft.shutdown().await.unwrap();
        let _ = std::fs::remove_dir_all(data_dir);
    }

    #[tokio::test]
    async fn replica_and_sentinel_lists_use_flat_redis_fields() {
        let peers: PeerMap = Arc::new(Mutex::new(BTreeMap::new()));
        let nodes = BTreeMap::from([
            (
                1,
                Node {
                    addr: "mem://node-1".to_string(),
                },
            ),
            (
                2,
                Node {
                    addr: "mem://node-2".to_string(),
                },
            ),
            (
                3,
                Node {
                    addr: "mem://node-3".to_string(),
                },
            ),
        ]);
        let mut rafts = Vec::new();
        for node_id in 1..=3 {
            let raft = new_mem_raft(node_id, Arc::clone(&peers)).await.unwrap();
            rafts.push(raft);
        }
        rafts[0].initialize(nodes).await.unwrap();
        for raft in &rafts {
            enable_raft_runtime(raft);
        }
        rafts[0]
            .wait(None)
            .current_leader(1, "node 1 should lead")
            .await
            .unwrap();

        let context = SentinelContext {
            master_name: "cache".to_string(),
            local_node_id: 1,
            local_client_addr: "127.0.0.1:6379".parse().unwrap(),
            cluster_client_addrs: BTreeMap::from([
                (2, "127.0.0.1:6380".parse().unwrap()),
                (3, "127.0.0.1:6381".parse().unwrap()),
            ]),
        };
        let slaves = execute(
            &rafts[0],
            &context,
            &request(&[b"SENTINEL", b"SLAVES", b"cache"]),
        )
        .await
        .unwrap();
        assert!(matches!(
            slaves,
            Frame::Array(values)
                if values.len() == 2
                    && values.iter().all(|value| matches!(
                        value,
                        Frame::Array(fields) if fields.len() == 20
                    ))
        ));

        let sentinels = execute(
            &rafts[0],
            &context,
            &request(&[b"SENTINEL", b"SENTINELS", b"cache"]),
        )
        .await
        .unwrap();
        assert!(matches!(
            sentinels,
            Frame::Array(values)
                if values.len() == 2
                    && values.iter().all(|value| matches!(
                        value,
                        Frame::Array(fields) if fields.len() == 10
                    ))
        ));

        for raft in rafts {
            raft.shutdown().await.unwrap();
        }
    }

    fn unique_test_id() -> u128 {
        use std::time::{SystemTime, UNIX_EPOCH};

        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    }
}
