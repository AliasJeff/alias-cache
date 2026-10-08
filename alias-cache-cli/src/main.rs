use std::net::SocketAddr;
use std::path::PathBuf;

use alias_cache::config::{Config, PeerSpec};
use alias_cache::server::Server;
use alias_cache::{Error, Result};
use clap::Parser;
use tokio::sync::watch;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

/// Redis-compatible, strongly consistent in-memory KV store backed by Raft.
#[derive(Debug, Parser)]
#[command(name = "alias-cache", version, about)]
struct Args {
    /// Unique non-zero node ID of this instance.
    #[arg(long, value_name = "ID")]
    node_id: u64,

    /// Address that accepts RESP client connections.
    #[arg(long, default_value = "127.0.0.1:6379")]
    client_addr: SocketAddr,

    /// Address that accepts raft RPC traffic from cluster peers.
    #[arg(long, default_value = "127.0.0.1:16379")]
    raft_addr: SocketAddr,

    /// Directory for the raft log and snapshots. Defaults to data/node-<ID>.
    #[arg(long, value_name = "DIR")]
    data_dir: Option<PathBuf>,

    /// Master name reported by Sentinel-style discovery commands.
    #[arg(long, default_value = "alias-cache")]
    sentinel_master_name: String,

    /// Cluster member as <NODE_ID=RAFT_ADDR[/CLIENT_ADDR]>. Repeat for every
    /// voter including this node. Omit all peers to run a single-node store.
    #[arg(long = "peer", value_name = "SPEC")]
    peers: Vec<String>,

    /// Seed cluster membership on first start. Set on exactly one node.
    #[arg(long)]
    bootstrap: bool,
}

fn build_config(args: &Args) -> Result<Config> {
    let peers = args
        .peers
        .iter()
        .map(|spec| PeerSpec::parse(spec))
        .collect::<Result<Vec<_>>>()?;
    let data_dir = args
        .data_dir
        .clone()
        .unwrap_or_else(|| PathBuf::from(format!("data/node-{}", args.node_id)));
    Config {
        node_id: args.node_id,
        client_addr: args.client_addr,
        raft_addr: args.raft_addr,
        sentinel_master_name: args.sentinel_master_name.clone(),
        bootstrap: args.bootstrap,
        data_dir,
        ..Config::default()
    }
    .with_peers(&peers)
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();
    let args = Args::parse();
    let config = build_config(&args).map_err(|error| {
        error!("{error}");
        error
    })?;
    info!(
        node_id = config.node_id,
        client_addr = %config.client_addr,
        raft_addr = %config.raft_addr,
        peers = config.cluster_nodes.len(),
        bootstrap = config.bootstrap,
        "starting alias-cache"
    );

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let server = Server::new(config, shutdown_rx)?;
    let server_task = tokio::spawn(server.run());

    tokio::signal::ctrl_c()
        .await
        .map_err(|source| Error::io("wait for ctrl-c", source))?;
    info!("received ctrl-c, shutting down");
    shutdown_tx
        .send(true)
        .map_err(|_| Error::server("server shutdown receiver dropped"))?;
    server_task
        .await
        .map_err(|error| Error::server(format!("join server task: {error}")))?
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(arguments: &[&str]) -> std::result::Result<Args, clap::Error> {
        Args::try_parse_from(std::iter::once("alias-cache").chain(arguments.iter().copied()))
    }

    #[test]
    fn single_node_config_uses_address_defaults() {
        let config = build_config(&args(&["--node-id", "1"]).unwrap()).unwrap();
        assert_eq!(config.node_id, 1);
        assert_eq!(config.client_addr.port(), 6379);
        assert_eq!(config.raft_addr.port(), 16379);
        assert_eq!(config.data_dir, PathBuf::from("data/node-1"));
        assert!(config.cluster_nodes.is_empty());
        assert!(!config.bootstrap);
    }

    #[test]
    fn cluster_config_collects_peer_topology() {
        let config = build_config(
            &args(&[
                "--node-id",
                "2",
                "--client-addr",
                "127.0.0.1:6380",
                "--raft-addr",
                "127.0.0.1:16380",
                "--peer",
                "1=127.0.0.1:16379/127.0.0.1:6379",
                "--peer",
                "2=127.0.0.1:16380/127.0.0.1:6380",
                "--peer",
                "3=127.0.0.1:16381/127.0.0.1:6381",
            ])
            .unwrap(),
        )
        .unwrap();
        assert_eq!(config.cluster_nodes.len(), 3);
        assert_eq!(config.cluster_client_addrs.len(), 3);
        assert_eq!(config.data_dir, PathBuf::from("data/node-2"));
    }

    #[test]
    fn invalid_peer_spec_fails_config_build() {
        let config = build_config(&args(&["--node-id", "1", "--peer", "bogus"]).unwrap());
        assert!(config.is_err());
    }

    #[test]
    fn node_id_is_required() {
        assert!(args(&[]).is_err());
    }
}
