use std::collections::BTreeMap;
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};

use crate::error::{Error, Result};
use crate::protocol::ProtocolLimits;
use crate::raft::{Node, NodeId};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Config {
    pub client_addr: SocketAddr,
    pub raft_addr: SocketAddr,
    pub node_id: u64,
    pub bootstrap: bool,
    pub cluster_nodes: BTreeMap<NodeId, Node>,
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
            cluster_nodes: BTreeMap::new(),
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

    #[test]
    fn default_config_is_valid() {
        assert!(Config::default().validate().is_ok());
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
}
