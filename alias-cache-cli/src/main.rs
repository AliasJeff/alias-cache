use std::net::SocketAddr;
use std::path::PathBuf;

use alias_cache::config::{Config, PeerSpec};
use alias_cache::server::Server;
use alias_cache::{Error, Result};
use bytes::Bytes;
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

    /// Password required by client connections. ALIAS_CACHE_PASSWORD is also
    /// accepted so credentials do not need to appear in the process command line.
    #[arg(long, env = "ALIAS_CACHE_PASSWORD", hide_env_values = true)]
    password: Option<String>,

    /// PEM certificate chain for the client-facing TLS listener.
    #[arg(long, value_name = "FILE")]
    tls_cert_file: Option<PathBuf>,

    /// PEM private key for the client-facing TLS listener.
    #[arg(long, value_name = "FILE")]
    tls_key_file: Option<PathBuf>,

    /// PEM CA bundle used to verify client certificates.
    #[arg(long, value_name = "FILE")]
    tls_ca_file: Option<PathBuf>,

    /// Require and verify a client certificate against --tls-ca-file.
    #[arg(long)]
    tls_auth_clients: bool,

    /// Encrypt and authenticate Raft RPC and snapshot traffic with TLS.
    #[arg(long)]
    tls_replication: bool,

    /// Cluster member as <NODE_ID=RAFT_ADDR[/CLIENT_ADDR]>. Repeat for every
    /// voter including this node. Omit all peers to run a single-node store.
    #[arg(long = "peer", value_name = "SPEC")]
    peers: Vec<String>,

    /// Seed cluster membership on first start. Set on exactly one node.
    #[arg(long)]
    bootstrap: bool,

    /// Reject writes whose payload would push live data past this many
    /// bytes. 0 disables the limit.
    #[arg(long, default_value_t = 0)]
    max_memory: u64,

    /// Refuse new client connections past this count. 0 disables the limit.
    #[arg(long, default_value_t = 0)]
    max_connections: u32,

    /// Create a raft snapshot after this many appended log entries.
    #[arg(long, default_value_t = 4096)]
    snapshot_policy: u64,
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
        password: args
            .password
            .as_ref()
            .map(|value| Bytes::copy_from_slice(value.as_bytes())),
        tls_cert_file: args.tls_cert_file.clone(),
        tls_key_file: args.tls_key_file.clone(),
        tls_ca_file: args.tls_ca_file.clone(),
        tls_auth_clients: args.tls_auth_clients,
        tls_replication: args.tls_replication,
        bootstrap: args.bootstrap,
        data_dir,
        max_memory_bytes: args.max_memory,
        max_connections: args.max_connections,
        snapshot_policy_logs: args.snapshot_policy,
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
    let mut server_task = tokio::spawn(server.run());

    tokio::select! {
        result = &mut server_task => {
            result.map_err(|error| Error::server(format!("join server task: {error}")))?
        }
        signal = tokio::signal::ctrl_c() => {
            signal.map_err(|source| Error::io("wait for ctrl-c", source))?;
            info!("received ctrl-c, shutting down");
            shutdown_tx
                .send(true)
                .map_err(|_| Error::server("server shutdown receiver dropped"))?;
            server_task
                .await
                .map_err(|error| Error::server(format!("join server task: {error}")))?
        }
    }
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

    #[test]
    fn resource_limits_default_to_unlimited() {
        let parsed = args(&["--node-id", "1"]).unwrap();
        assert_eq!(parsed.max_memory, 0);
        assert_eq!(parsed.max_connections, 0);
        let config = build_config(&parsed).unwrap();
        assert_eq!(config.max_memory_bytes, 0);
        assert_eq!(config.max_connections, 0);
        assert_eq!(config.max_memory_non_zero(), None);
    }

    #[test]
    fn snapshot_policy_defaults_and_zero_falls_back() {
        let parsed = args(&["--node-id", "1"]).unwrap();
        assert_eq!(parsed.snapshot_policy, 4096);
        let config = build_config(&parsed).unwrap();
        assert_eq!(config.snapshot_policy(), 4096);
        let zero =
            build_config(&args(&["--node-id", "1", "--snapshot-policy", "0"]).unwrap()).unwrap();
        assert_eq!(
            zero.snapshot_policy(),
            4096,
            "zero falls back to the default"
        );
        let custom =
            build_config(&args(&["--node-id", "1", "--snapshot-policy", "128"]).unwrap()).unwrap();
        assert_eq!(custom.snapshot_policy(), 128);
    }

    #[test]
    fn resource_limits_parse_from_cli() {
        let parsed = args(&[
            "--node-id",
            "1",
            "--max-memory",
            "1048576",
            "--max-connections",
            "64",
        ])
        .unwrap();
        let config = build_config(&parsed).unwrap();
        assert_eq!(config.max_memory_bytes, 1_048_576);
        assert_eq!(config.max_connections, 64);
        assert_eq!(config.max_memory_non_zero(), Some(1_048_576));
    }

    #[test]
    fn password_can_be_loaded_from_cli() {
        let config =
            build_config(&args(&["--node-id", "1", "--password", "secret"]).unwrap()).unwrap();
        assert_eq!(config.password, Some(Bytes::from_static(b"secret")));
    }

    #[test]
    fn tls_options_can_be_loaded_from_cli() {
        let config = build_config(
            &args(&[
                "--node-id",
                "1",
                "--tls-cert-file",
                "server.crt",
                "--tls-key-file",
                "server.key",
                "--tls-ca-file",
                "clients.crt",
                "--tls-auth-clients",
                "--tls-replication",
            ])
            .unwrap(),
        )
        .unwrap();
        assert_eq!(config.tls_cert_file, Some(PathBuf::from("server.crt")));
        assert_eq!(config.tls_key_file, Some(PathBuf::from("server.key")));
        assert!(config.tls_auth_clients);
        assert!(config.tls_replication);
    }
}
