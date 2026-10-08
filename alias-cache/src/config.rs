use std::collections::{BTreeMap, BTreeSet};
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};

use crate::error::{Error, Result};
use crate::protocol::ProtocolLimits;
use crate::raft::{Node, NodeId};

/// Peer specification parsed from a command-line cluster entry.
///
/// The raft address is required; the optional client address is only used for
/// Sentinel-style discovery replies and never for raft replication.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PeerSpec {
    pub node_id: NodeId,
    pub raft_addr: SocketAddr,
    pub client_addr: Option<SocketAddr>,
}

impl PeerSpec {
    /// Parse a `<node-id>=<raft-addr>[/<client-addr>]` specification.
    ///
    /// Addresses must be literal `IP:PORT` values; name resolution is
    /// intentionally not performed so cluster membership stays stable.
    pub fn parse(spec: &str) -> Result<Self> {
        let invalid = || {
            Error::invalid_config(format!(
                "peer '{spec}' must look like <node-id>=<raft-addr>[/<client-addr>]"
            ))
        };
        let (node_id, addresses) = spec.split_once('=').ok_or_else(invalid)?;
        let node_id: NodeId = node_id.trim().parse().map_err(|_| invalid())?;
        if node_id == 0 {
            return Err(Error::invalid_config(format!(
                "peer '{spec}' has node id zero; node ids must be greater than zero"
            )));
        }
        let (raft_addr, client_addr) = match addresses.split_once('/') {
            Some((raft_addr, client_addr)) => (raft_addr, Some(client_addr)),
            None => (addresses, None),
        };
        let raft_addr: SocketAddr = raft_addr.trim().parse().map_err(|_| invalid())?;
        if raft_addr.ip().is_unspecified() {
            return Err(Error::invalid_config(format!(
                "peer '{spec}' must not use an unspecified raft address"
            )));
        }
        let client_addr = match client_addr {
            Some(client_addr) => {
                let parsed: SocketAddr = client_addr.trim().parse().map_err(|_| invalid())?;
                Some(parsed)
            }
            None => None,
        };
        Ok(Self {
            node_id,
            raft_addr,
            client_addr,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Config {
    pub client_addr: SocketAddr,
    pub raft_addr: SocketAddr,
    pub node_id: u64,
    pub bootstrap: bool,
    pub sentinel_master_name: String,
    pub cluster_nodes: BTreeMap<NodeId, Node>,
    pub cluster_client_addrs: BTreeMap<NodeId, SocketAddr>,
    pub data_dir: PathBuf,
    pub max_frame_size: usize,
    pub max_array_len: usize,
    pub max_nesting_depth: usize,
    pub max_pipeline_len: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            client_addr: SocketAddr::from(([127, 0, 0, 1], 6379)),
            raft_addr: SocketAddr::from(([127, 0, 0, 1], 16379)),
            node_id: 1,
            bootstrap: false,
            sentinel_master_name: "alias-cache".to_string(),
            cluster_nodes: BTreeMap::new(),
            cluster_client_addrs: BTreeMap::new(),
            data_dir: PathBuf::from("data/node-1"),
            max_frame_size: 512 * 1024 * 1024,
            max_array_len: 1_024,
            max_nesting_depth: 32,
            max_pipeline_len: 1_024,
        }
    }
}

impl Config {
    pub fn validate(&self) -> Result<()> {
        if self.node_id == 0 {
            return Err(Error::invalid_config("node_id must be greater than zero"));
        }
        if self.sentinel_master_name.trim().is_empty() {
            return Err(Error::invalid_config(
                "sentinel_master_name must not be empty",
            ));
        }
        if !self.cluster_nodes.is_empty() && !self.cluster_nodes.contains_key(&self.node_id) {
            return Err(Error::invalid_config(
                "cluster_nodes must contain the local node_id",
            ));
        }
        if self.client_addr.ip().is_unspecified() {
            return Err(Error::invalid_config(
                "client_addr must not use an unspecified address",
            ));
        }
        if self.raft_addr.ip().is_unspecified() {
            return Err(Error::invalid_config(
                "raft_addr must not use an unspecified address",
            ));
        }
        if self.client_addr == self.raft_addr {
            return Err(Error::invalid_config(
                "client_addr and raft_addr must use different ports",
            ));
        }
        if self.data_dir.as_os_str().is_empty() {
            return Err(Error::invalid_config("data_dir must not be empty"));
        }
        if self.max_frame_size == 0 {
            return Err(Error::invalid_config(
                "max_frame_size must be greater than zero",
            ));
        }
        if self.max_array_len == 0 {
            return Err(Error::invalid_config(
                "max_array_len must be greater than zero",
            ));
        }
        if self.max_nesting_depth == 0 {
            return Err(Error::invalid_config(
                "max_nesting_depth must be greater than zero",
            ));
        }
        if self.max_pipeline_len == 0 {
            return Err(Error::invalid_config(
                "max_pipeline_len must be greater than zero",
            ));
        }
        Ok(())
    }

    pub fn with_client_addr(mut self, ip: IpAddr, port: u16) -> Self {
        self.client_addr = SocketAddr::new(ip, port);
        self
    }

    pub fn with_cluster_nodes(mut self, nodes: BTreeMap<NodeId, Node>) -> Self {
        self.cluster_nodes = nodes;
        self
    }

    pub fn with_sentinel_master_name(mut self, name: impl Into<String>) -> Self {
        self.sentinel_master_name = name.into();
        self
    }

    pub fn with_cluster_client_addrs(mut self, addresses: BTreeMap<NodeId, SocketAddr>) -> Self {
        self.cluster_client_addrs = addresses;
        self
    }

    /// Fill the cluster topology from parsed peer specifications.
    ///
    /// Every voter, including the local node, must be listed. An empty peer
    /// list selects single-node operation instead of a cluster.
    pub fn with_peers(mut self, peers: &[PeerSpec]) -> Result<Self> {
        if !peers.is_empty() {
            let mut cluster_nodes = BTreeMap::new();
            let mut cluster_client_addrs = BTreeMap::new();
            let mut raft_addrs = BTreeSet::new();
            for peer in peers {
                if peer.node_id == self.node_id && peer.raft_addr != self.raft_addr {
                    return Err(Error::invalid_config(format!(
                        "peer entry for node {} must use the local raft address {}",
                        peer.node_id, self.raft_addr
                    )));
                }
                if peer.node_id != self.node_id && peer.raft_addr == self.raft_addr {
                    return Err(Error::invalid_config(format!(
                        "peer entry for node {} reuses the local raft address {}",
                        peer.node_id, self.raft_addr
                    )));
                }
                if !raft_addrs.insert(peer.raft_addr) {
                    return Err(Error::invalid_config(format!(
                        "raft address {} is assigned to more than one peer",
                        peer.raft_addr
                    )));
                }
                cluster_nodes.insert(
                    peer.node_id,
                    Node {
                        addr: peer.raft_addr.to_string(),
                    },
                );
                if let Some(client_addr) = peer.client_addr {
                    cluster_client_addrs.insert(peer.node_id, client_addr);
                }
            }
            self.cluster_nodes = cluster_nodes;
            self.cluster_client_addrs = cluster_client_addrs;
        }
        self.validate()?;
        if self.bootstrap && self.cluster_nodes.is_empty() {
            return Err(Error::invalid_config(
                "bootstrap requires at least one --peer entry to seed membership",
            ));
        }
        Ok(self)
    }

    pub fn ensure_data_dir(&self) -> Result<()> {
        std::fs::create_dir_all(Path::new(&self.data_dir))
            .map_err(|source| Error::io("create data directory", source))
    }

    pub fn protocol_limits(&self) -> ProtocolLimits {
        ProtocolLimits {
            max_frame_size: self.max_frame_size,
            max_array_len: self.max_array_len,
            max_nesting_depth: self.max_nesting_depth,
            max_pipeline_len: self.max_pipeline_len,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn peer(spec: &str) -> PeerSpec {
        PeerSpec::parse(spec).expect("peer spec should parse")
    }

    #[test]
    fn default_config_is_valid() {
        assert!(Config::default().validate().is_ok());
    }

    #[test]
    fn peer_spec_parses_raft_and_client_addresses() {
        assert_eq!(
            peer("2=127.0.0.1:16380/127.0.0.1:6380"),
            PeerSpec {
                node_id: 2,
                raft_addr: "127.0.0.1:16380".parse().unwrap(),
                client_addr: Some("127.0.0.1:6380".parse().unwrap()),
            }
        );
        assert_eq!(
            peer("3=127.0.0.1:16381"),
            PeerSpec {
                node_id: 3,
                raft_addr: "127.0.0.1:16381".parse().unwrap(),
                client_addr: None,
            }
        );
    }

    #[test]
    fn peer_spec_rejects_malformed_input() {
        for spec in [
            "",
            "2",
            "2=",
            "=127.0.0.1:16380",
            "x=127.0.0.1:16380",
            "0=127.0.0.1:16380",
            "2=localhost:16380",
            "2=127.0.0.1",
            "2=0.0.0.0:16380",
            "2=127.0.0.1:16380/",
            "2=127.0.0.1:16380/nope",
        ] {
            assert!(
                PeerSpec::parse(spec).is_err(),
                "peer spec {spec:?} should be rejected"
            );
        }
    }

    #[test]
    fn with_peers_builds_cluster_topology_and_keeps_single_node_mode() {
        let config = Config {
            node_id: 1,
            raft_addr: "127.0.0.1:16379".parse().unwrap(),
            ..Config::default()
        }
        .with_peers(&[
            peer("1=127.0.0.1:16379/127.0.0.1:6379"),
            peer("2=127.0.0.1:16380/127.0.0.1:6380"),
            peer("3=127.0.0.1:16381"),
        ])
        .expect("peer list should build a cluster config");

        assert_eq!(config.cluster_nodes.len(), 3);
        assert_eq!(
            config.cluster_nodes.get(&2).map(|node| node.addr.as_str()),
            Some("127.0.0.1:16380")
        );
        assert_eq!(config.cluster_client_addrs.len(), 2);

        let single = Config::default()
            .with_peers(&[])
            .expect("empty peer list should keep single-node mode");
        assert!(single.cluster_nodes.is_empty());
    }

    #[test]
    fn with_peers_rejects_inconsistent_local_entry() {
        let config = Config {
            node_id: 1,
            raft_addr: "127.0.0.1:16379".parse().unwrap(),
            ..Config::default()
        };
        let mismatched = config
            .clone()
            .with_peers(&[peer("1=127.0.0.1:19999")])
            .expect_err("local peer entry must match the local raft address");
        assert!(matches!(mismatched, Error::InvalidConfig(_)));

        let duplicated = config
            .with_peers(&[peer("1=127.0.0.1:16379"), peer("2=127.0.0.1:16379")])
            .expect_err("a raft address must belong to exactly one peer");
        assert!(matches!(duplicated, Error::InvalidConfig(_)));
    }

    #[test]
    fn with_peers_requires_self_in_cluster_membership() {
        let error = Config {
            node_id: 1,
            raft_addr: "127.0.0.1:16379".parse().unwrap(),
            ..Config::default()
        }
        .with_peers(&[peer("2=127.0.0.1:16380")])
        .expect_err("peer list without the local node must be rejected");
        assert!(matches!(error, Error::InvalidConfig(_)));
    }

    #[test]
    fn bootstrap_without_peers_is_rejected() {
        let error = Config {
            bootstrap: true,
            ..Config::default()
        }
        .with_peers(&[])
        .expect_err("bootstrap requires peers to seed membership");
        assert!(matches!(error, Error::InvalidConfig(_)));
    }

    #[test]
    fn zero_node_id_is_rejected() {
        let config = Config {
            node_id: 0,
            ..Config::default()
        };
        assert!(matches!(config.validate(), Err(Error::InvalidConfig(_))));
    }

    #[test]
    fn identical_addresses_are_rejected() {
        let config = Config {
            raft_addr: Config::default().client_addr,
            ..Config::default()
        };
        assert!(matches!(config.validate(), Err(Error::InvalidConfig(_))));
    }

    #[test]
    fn empty_sentinel_master_name_is_rejected() {
        let config = Config {
            sentinel_master_name: "  ".to_string(),
            ..Config::default()
        };
        assert!(matches!(config.validate(), Err(Error::InvalidConfig(_))));
    }
}
