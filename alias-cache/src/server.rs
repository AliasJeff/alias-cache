use std::sync::Arc;

use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tracing::{info, warn};

use crate::config::Config;
use crate::error::{Error, Result};
#[cfg(test)]
use crate::node_state::{NodeState, SharedNodeState};
use crate::protocol::{Decoder, Frame, command_from_frame};
use crate::raft::{
    ClientRequestOutcome, RaftHandle, enable_raft_runtime, execute_client_request_with_id,
    execute_transaction_request_with_id, new_client_request_id, new_tcp_raft_for_server,
    open_single_node, run_expiration_worker, run_leader_time_worker, send_client_request_with_id,
    send_transaction_request_with_id, serve_raft_rpc, wait_for_any_leader,
};
use crate::sentinel::{self, SentinelContext};

pub struct Server {
    config: Config,
    shutdown: watch::Receiver<bool>,
}

impl Server {
    pub fn new(config: Config, shutdown: watch::Receiver<bool>) -> Result<Self> {
        config.validate()?;
        Ok(Self { config, shutdown })
    }

    /// Open the local Raft node for the configured topology.
    ///
    /// A single-node deployment initializes itself and waits for its own
    /// leader. In a cluster, exactly one seed node bootstraps membership;
    /// every other voter joins passively and must have its election runtime
    /// enabled so it can win leadership after it catches up.
    async fn open_raft(&self) -> Result<Arc<RaftHandle>> {
        if self.config.cluster_nodes.is_empty() {
            let raft = open_single_node(
                self.config.node_id,
                &self.config.data_dir,
                self.config.raft_addr.to_string(),
            )
            .await?;
            return Ok(Arc::new(raft));
        }
        let raft = new_tcp_raft_for_server(
            self.config.node_id,
            &self.config.data_dir,
            self.config.cluster_nodes.clone(),
            self.config.bootstrap,
        )
        .await?;
        if !self.config.bootstrap {
            enable_raft_runtime(&raft);
        }
        Ok(Arc::new(raft))
    }

    pub async fn run(self) -> Result<()> {
        self.config.ensure_data_dir()?;
        let listener = TcpListener::bind(self.config.client_addr)
            .await
            .map_err(|source| Error::io("bind client listener", source))?;
        info!(addr = %self.config.client_addr, node_id = self.config.node_id, "client listener started");

        let raft_listener = TcpListener::bind(self.config.raft_addr)
            .await
            .map_err(|source| Error::io("bind raft listener", source))?;
        let raft = self.open_raft().await?;
        let shutdown = Arc::new(self.shutdown);
        let raft_shutdown = shutdown.as_ref().clone();
        let rpc_raft = raft.clone();
        tokio::spawn(async move {
            if let Err(error) =
                serve_raft_rpc(raft_listener, (*rpc_raft).clone(), raft_shutdown).await
            {
                warn!(%error, "raft rpc listener failed");
            }
        });
        let time_worker_raft = raft.clone();
        let time_worker_shutdown = shutdown.as_ref().clone();
        let node_id = self.config.node_id;
        tokio::spawn(async move {
            run_leader_time_worker(
                time_worker_raft,
                node_id,
                time_worker_shutdown,
                std::time::Duration::from_millis(100),
            )
            .await;
        });
        let expiration_worker_raft = raft.clone();
        let expiration_worker_shutdown = shutdown.as_ref().clone();
        let node_id = self.config.node_id;
        let sentinel_context = SentinelContext::from_config(&self.config);
        tokio::spawn(async move {
            run_expiration_worker(
                expiration_worker_raft,
                node_id,
                expiration_worker_shutdown,
                std::time::Duration::from_millis(100),
            )
            .await;
        });
        if !self.config.cluster_nodes.is_empty() {
            wait_for_any_leader(&raft).await?;
        }
        loop {
            tokio::select! {
                result = listener.accept() => {
                    let (stream, peer) = result
                        .map_err(|source| Error::io("accept client connection", source))?;
                    let child_shutdown = shutdown.clone();
                    let limits = self.config.protocol_limits();
                    let child_raft = raft.clone();
                    let sentinel_context = sentinel_context.clone();
                    tokio::spawn(async move {
                        if let Err(error) =
                            handle_raft_connection(
                                stream,
                                child_shutdown,
                                child_raft,
                                limits,
                                sentinel_context.clone(),
                            )
                            .await
                        {
                            warn!(%peer, %error, "client connection failed");
                        }
                    });
                }
                result = wait_for_shutdown(shutdown.clone()) => {
                    result?;
                    info!("client listener stopped");
                    raft.shutdown()
                        .await
                        .map_err(|error| Error::server(format!("shutdown raft node: {error}")))?;
                    return Ok(());
                }
            }
        }
    }
}

async fn wait_for_shutdown(shutdown: Arc<watch::Receiver<bool>>) -> Result<()> {
    let mut receiver = shutdown.as_ref().clone();
    receiver
        .wait_for(|value| *value)
        .await
        .map(|_| ())
        .map_err(|_| Error::server("shutdown signal was closed"))
}

#[cfg(test)]
async fn handle_connection(
    mut stream: TcpStream,
    shutdown: Arc<watch::Receiver<bool>>,
    node_state: SharedNodeState,
    limits: crate::protocol::ProtocolLimits,
) -> Result<()> {
    let mut decoder = Decoder::new(limits);
    let mut input = [0_u8; 8 * 1024];
    loop {
        tokio::select! {
            result = stream.read(&mut input) => {
                let read = result.map_err(|source| Error::io("read client request", source))?;
                if read == 0 {
                    return Ok(());
                }
                let frames = match decoder.push(&input[..read]) {
                    Ok(frames) => frames,
                    Err(error) => {
                        write_frame(&mut stream, Frame::error(format!("ERR {error}"))).await?;
                        return Err(error);
                    }
                };
                for frame in frames {
                    let response = match command_from_frame(frame) {
                        Ok(request) => {
                            let mut state = node_state.lock().await;
                            match state.execute(request) {
                                Ok(response) => response,
                                Err(error) => Frame::error(format!("ERR {error}")),
                            }
                        }
                        Err(error) => Frame::error(format!("ERR {error}")),
                    };
                    write_frame(&mut stream, response).await?;
                }
            }
            result = wait_for_shutdown(shutdown.clone()) => {
                result?;
                stream.shutdown().await
                    .map_err(|source| Error::io("close client connection", source))?;
                return Ok(());
            }
        }
    }
}

async fn handle_raft_connection(
    mut stream: TcpStream,
    shutdown: Arc<watch::Receiver<bool>>,
    raft: Arc<RaftHandle>,
    limits: crate::protocol::ProtocolLimits,
    sentinel_context: SentinelContext,
) -> Result<()> {
    let mut decoder = Decoder::new(limits);
    let mut input = [0_u8; 8 * 1024];
    let mut transaction = None;
    loop {
        tokio::select! {
            result = stream.read(&mut input) => {
                let read = result.map_err(|source| Error::io("read client request", source))?;
                if read == 0 {
                    return Ok(());
                }
                let frames = match decoder.push(&input[..read]) {
                    Ok(frames) => frames,
                    Err(error) => {
                        write_frame(&mut stream, Frame::error(format!("ERR {error}"))).await?;
                        return Err(error);
                    }
                };
                for frame in frames {
                    let response = match command_from_frame(frame) {
                        Ok(request) => {
                            handle_transaction_command(
                                &raft,
                                &mut transaction,
                                request,
                                &sentinel_context,
                            )
                            .await
                        }
                        Err(error) => Frame::error(format!("ERR {error}")),
                    };
                    write_frame(&mut stream, response).await?;
                }
            }
            result = wait_for_shutdown(shutdown.clone()) => {
                result?;
                stream.shutdown().await
                    .map_err(|source| Error::io("close client connection", source))?;
                return Ok(());
            }
        }
    }
}

async fn handle_transaction_command(
    raft: &RaftHandle,
    transaction: &mut Option<Vec<crate::protocol::Request>>,
    request: crate::protocol::Request,
    sentinel_context: &SentinelContext,
) -> Frame {
    if transaction.is_none()
        && let Some(response) = sentinel::execute(raft, sentinel_context, &request).await
    {
        return response;
    }
    let crate::protocol::Request::Command(arguments) = &request;
    let command = arguments.first().map(|argument| argument.as_ref());
    if command.is_some_and(|command| command.eq_ignore_ascii_case(b"MULTI")) {
        if transaction.is_some() {
            return Frame::error("ERR MULTI calls can not be nested");
        }
        *transaction = Some(Vec::new());
        return Frame::simple_string("OK");
    }
    if command.is_some_and(|command| command.eq_ignore_ascii_case(b"DISCARD")) {
        if transaction.take().is_none() {
            return Frame::error("ERR DISCARD without MULTI");
        }
        return Frame::simple_string("OK");
    }
    if command.is_some_and(|command| command.eq_ignore_ascii_case(b"EXEC")) {
        let Some(requests) = transaction.take() else {
            return Frame::error("ERR EXEC without MULTI");
        };
        return execute_transaction(raft, requests).await;
    }
    if let Some(requests) = transaction {
        if command.is_some_and(|command| {
            command.eq_ignore_ascii_case(b"DISCARD")
                || command.eq_ignore_ascii_case(b"MULTI")
                || command.eq_ignore_ascii_case(b"EXEC")
        }) {
            return Frame::error("ERR transaction control command is invalid here");
        }
        if requests.len() >= 1_024 {
            return Frame::error("ERR transaction command limit exceeded");
        }
        requests.push(request);
        return Frame::simple_string("QUEUED");
    }
    execute_client_command(raft, request).await
}

async fn execute_transaction(raft: &RaftHandle, requests: Vec<crate::protocol::Request>) -> Frame {
    let request_id = new_client_request_id();
    match execute_transaction_request_with_id(raft, request_id, requests.clone()).await {
        Ok(ClientRequestOutcome::Applied(response)) => response,
        Ok(ClientRequestOutcome::Redirect(address)) => match send_transaction_request_with_id(
            &address,
            request_id,
            requests,
            &openraft::network::RPCOption::new(std::time::Duration::from_secs(1)),
        )
        .await
        {
            Ok(response) => response,
            Err(error) => Frame::error(format!("ERR {error}")),
        },
        Err(error) => Frame::error(format!("ERR {error}")),
    }
}

async fn execute_client_command(raft: &RaftHandle, request: crate::protocol::Request) -> Frame {
    let request_id = new_client_request_id();
    match execute_client_request_with_id(raft, request_id, request.clone()).await {
        Ok(ClientRequestOutcome::Applied(response)) => response,
        Ok(ClientRequestOutcome::Redirect(address)) => {
            match send_client_request_with_id(
                &address,
                request_id,
                request,
                &openraft::network::RPCOption::new(std::time::Duration::from_secs(1)),
            )
            .await
            {
                Ok(response) => response,
                Err(error) => Frame::error(format!("ERR {error}")),
            }
        }
        Err(error) => Frame::error(format!("ERR {error}")),
    }
}

async fn write_frame(stream: &mut TcpStream, frame: Frame) -> Result<()> {
    let mut output = BytesMut::new();
    frame.encode(&mut output)?;
    stream
        .write_all(&output)
        .await
        .map_err(|source| Error::io("write client response", source))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::net::SocketAddr;
    use std::time::Duration;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;
    use tokio::time::timeout;

    use super::*;

    fn unique_server_dir(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "alias-cache-server-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
        ))
    }

    #[tokio::test]
    async fn foundation_server_preserves_pipeline_response_order() {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let config = Config {
            client_addr: SocketAddr::from(([127, 0, 0, 1], 0)),
            data_dir: std::env::temp_dir()
                .join(format!("alias-cache-server-test-{}", std::process::id())),
            ..Config::default()
        };
        let listener = TcpListener::bind(config.client_addr).await.unwrap();
        let addr = listener.local_addr().unwrap();
        config.ensure_data_dir().unwrap();
        let shutdown = Arc::new(shutdown_rx);
        let limits = config.protocol_limits();
        let task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let state = Arc::new(tokio::sync::Mutex::new(
                NodeState::open(&config.data_dir).unwrap(),
            ));
            handle_connection(stream, shutdown, state, limits).await
        });

        let mut client = TcpStream::connect(addr).await.unwrap();
        client
            .write_all(
                b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n",
            )
            .await
            .unwrap();
        let expected = b"+OK\r\n$5\r\nvalue\r\n";
        let mut output = Vec::with_capacity(expected.len());
        let mut buffer = [0_u8; 96];
        while output.len() < expected.len() {
            let read = tokio::time::timeout(Duration::from_secs(1), client.read(&mut buffer))
                .await
                .unwrap()
                .unwrap();
            assert_ne!(read, 0);
            output.extend_from_slice(&buffer[..read]);
        }
        assert_eq!(&output[..expected.len()], expected);

        shutdown_tx.send(true).unwrap();
        let _ = task.await;
    }

    #[tokio::test]
    async fn foundation_server_preserves_ttl_pipeline_responses() {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let config = Config {
            client_addr: SocketAddr::from(([127, 0, 0, 1], 0)),
            data_dir: std::env::temp_dir().join(format!(
                "alias-cache-server-ttl-pipeline-{}",
                std::process::id()
            )),
            ..Config::default()
        };
        let listener = TcpListener::bind(config.client_addr).await.unwrap();
        let addr = listener.local_addr().unwrap();
        config.ensure_data_dir().unwrap();
        let data_dir = config.data_dir.clone();
        let shutdown = Arc::new(shutdown_rx);
        let limits = config.protocol_limits();
        let task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let state = Arc::new(tokio::sync::Mutex::new(
                NodeState::open(&config.data_dir).unwrap(),
            ));
            handle_connection(stream, shutdown, state, limits).await
        });

        let mut client = TcpStream::connect(addr).await.unwrap();
        client
            .write_all(
                b"*5\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n$2\r\nPX\r\n$3\r\n500\r\n\
*2\r\n$4\r\nPTTL\r\n$3\r\nkey\r\n\
*2\r\n$6\r\nEXISTS\r\n$3\r\nkey\r\n",
            )
            .await
            .unwrap();
        let expected_prefix = b"+OK\r\n:500\r\n:1\r\n";
        let mut output = Vec::with_capacity(expected_prefix.len());
        let mut buffer = [0_u8; 96];
        while output.len() < expected_prefix.len() {
            let read = tokio::time::timeout(Duration::from_secs(1), client.read(&mut buffer))
                .await
                .unwrap()
                .unwrap();
            assert_ne!(read, 0);
            output.extend_from_slice(&buffer[..read]);
        }
        assert_eq!(&output[..expected_prefix.len()], expected_prefix);

        shutdown_tx.send(true).unwrap();
        let _ = task.await;
        let _ = std::fs::remove_dir_all(data_dir);
    }

    #[tokio::test]
    async fn single_connection_executes_ttl_transaction_atomically() {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let config = Config {
            client_addr: SocketAddr::from(([127, 0, 0, 1], 0)),
            raft_addr: SocketAddr::from(([127, 0, 0, 1], 0)),
            data_dir: std::env::temp_dir().join(format!(
                "alias-cache-server-transaction-{}",
                std::process::id()
            )),
            ..Config::default()
        };
        let listener = TcpListener::bind(config.client_addr).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let data_dir = config.data_dir.clone();
        let shutdown = Arc::new(shutdown_rx);
        let raft = Arc::new(
            open_single_node(
                config.node_id,
                &config.data_dir,
                config.raft_addr.to_string(),
            )
            .await
            .unwrap(),
        );
        let task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            handle_raft_connection(
                stream,
                shutdown,
                raft,
                config.protocol_limits(),
                SentinelContext::from_config(&config),
            )
            .await
        });

        let mut client = TcpStream::connect(addr).await.unwrap();
        client
            .write_all(
                b"*1\r\n$5\r\nMULTI\r\n\
*5\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n$2\r\nPX\r\n$3\r\n500\r\n\
*3\r\n$7\r\nPEXPIRE\r\n$3\r\nkey\r\n$3\r\n250\r\n\
*2\r\n$7\r\nPERSIST\r\n$3\r\nkey\r\n\
*1\r\n$4\r\nEXEC\r\n",
            )
            .await
            .unwrap();
        let mut decoder = crate::protocol::Decoder::new(crate::protocol::ProtocolLimits::default());
        let mut frames = Vec::new();
        let mut buffer = [0_u8; 128];
        while frames.len() < 5 {
            let read = tokio::time::timeout(Duration::from_secs(2), client.read(&mut buffer))
                .await
                .unwrap()
                .unwrap();
            assert_ne!(read, 0);
            frames.extend(decoder.push(&buffer[..read]).unwrap());
        }
        assert_eq!(
            frames,
            vec![
                Frame::simple_string("OK"),
                Frame::simple_string("QUEUED"),
                Frame::simple_string("QUEUED"),
                Frame::simple_string("QUEUED"),
                Frame::Array(vec![
                    Frame::simple_string("OK"),
                    Frame::Integer(1),
                    Frame::Integer(1),
                ]),
            ]
        );

        client
            .write_all(b"*2\r\n$4\r\nPTTL\r\n$3\r\nkey\r\n")
            .await
            .unwrap();
        let mut frames = Vec::new();
        while frames.is_empty() {
            let read = tokio::time::timeout(Duration::from_secs(2), client.read(&mut buffer))
                .await
                .unwrap()
                .unwrap();
            assert_ne!(read, 0);
            frames.extend(decoder.push(&buffer[..read]).unwrap());
        }
        assert_eq!(frames, vec![Frame::Integer(-1)]);

        shutdown_tx.send(true).unwrap();
        let _ = task.await;
        let _ = std::fs::remove_dir_all(data_dir);
    }

    #[tokio::test]
    async fn single_node_server_serves_raft_backed_commands() {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let data_dir = std::env::temp_dir().join(format!(
            "alias-cache-raft-server-test-{}",
            std::process::id()
        ));
        let config = Config {
            client_addr: SocketAddr::from(([127, 0, 0, 1], 0)),
            raft_addr: SocketAddr::from(([127, 0, 0, 1], 0)),
            data_dir: data_dir.clone(),
            ..Config::default()
        };
        let listener = TcpListener::bind(config.client_addr).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let shutdown = Arc::new(shutdown_rx);
        let raft = Arc::new(
            open_single_node(
                config.node_id,
                &config.data_dir,
                config.raft_addr.to_string(),
            )
            .await
            .unwrap(),
        );
        let task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            handle_raft_connection(
                stream,
                shutdown,
                raft,
                config.protocol_limits(),
                SentinelContext::from_config(&config),
            )
            .await
        });

        let mut client = TcpStream::connect(addr).await.unwrap();
        client
            .write_all(
                b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n",
            )
            .await
            .unwrap();
        let expected = b"+OK\r\n$5\r\nvalue\r\n";
        let mut output = Vec::with_capacity(expected.len());
        let mut buffer = [0_u8; 96];
        while output.len() < expected.len() {
            let read = tokio::time::timeout(Duration::from_secs(2), client.read(&mut buffer))
                .await
                .unwrap()
                .unwrap();
            assert_ne!(read, 0);
            output.extend_from_slice(&buffer[..read]);
        }
        assert_eq!(&output[..expected.len()], expected);

        shutdown_tx.send(true).unwrap();
        let _ = task.await;
        let _ = std::fs::remove_dir_all(data_dir);
    }

    #[tokio::test]
    async fn single_node_server_serves_read_only_sentinel_discovery() {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let data_dir = std::env::temp_dir().join(format!(
            "alias-cache-sentinel-server-test-{}",
            std::process::id()
        ));
        let config = Config {
            client_addr: SocketAddr::from(([127, 0, 0, 1], 0)),
            raft_addr: SocketAddr::from(([127, 0, 0, 1], 0)),
            data_dir: data_dir.clone(),
            sentinel_master_name: "cache".to_string(),
            ..Config::default()
        };
        let listener = TcpListener::bind(config.client_addr).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let shutdown = Arc::new(shutdown_rx);
        let raft = Arc::new(
            open_single_node(
                config.node_id,
                &config.data_dir,
                config.raft_addr.to_string(),
            )
            .await
            .unwrap(),
        );
        let mut sentinel_context = SentinelContext::from_config(&config);
        sentinel_context.local_client_addr = addr;
        let task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            handle_raft_connection(
                stream,
                shutdown,
                raft,
                config.protocol_limits(),
                sentinel_context,
            )
            .await
        });

        let mut client = TcpStream::connect(addr).await.unwrap();
        client
            .write_all(b"*3\r\n$8\r\nSENTINEL\r\n$23\r\nGET-MASTER-ADDR-BY-NAME\r\n$5\r\ncache\r\n")
            .await
            .unwrap();
        let mut decoder = crate::protocol::Decoder::new(crate::protocol::ProtocolLimits::default());
        let mut buffer = [0_u8; 128];
        let response = loop {
            let read = tokio::time::timeout(Duration::from_secs(2), client.read(&mut buffer))
                .await
                .unwrap()
                .unwrap();
            assert_ne!(read, 0);
            let frames = decoder.push(&buffer[..read]).unwrap();
            if let Some(frame) = frames.into_iter().next() {
                break frame;
            }
        };
        assert_eq!(
            response,
            Frame::Array(vec![
                Frame::bulk_string("127.0.0.1"),
                Frame::bulk_string(addr.port().to_string()),
            ])
        );

        shutdown_tx.send(true).unwrap();
        let _ = task.await;
        let _ = std::fs::remove_dir_all(data_dir);
    }

    #[tokio::test]
    async fn cluster_servers_form_and_fail_over_without_seed() {
        use crate::raft::{Node, RaftRequest};
        use crate::state_machine::Operation;
        use bytes::Bytes;

        let root = unique_server_dir("cluster-failover");

        // Bind raft listeners first so the seeded membership can reference
        // concrete ports.
        let mut listeners = Vec::new();
        let mut nodes = BTreeMap::new();
        for index in 1..=3_u64 {
            let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
                .await
                .unwrap();
            let address = listener.local_addr().unwrap();
            nodes.insert(
                index,
                Node {
                    addr: address.to_string(),
                },
            );
            listeners.push(listener);
        }

        let mut rafts = Vec::new();
        let mut rpc_tasks = Vec::new();
        for (index, listener) in listeners.into_iter().enumerate() {
            let node_id = u64::try_from(index + 1).unwrap();
            let config = Config {
                node_id,
                bootstrap: index == 0,
                cluster_nodes: nodes.clone(),
                data_dir: root.join(format!("node-{node_id}")),
                ..Config::default()
            };
            let server = Server::new(config, watch::channel(false).1).unwrap();
            let raft = server.open_raft().await.unwrap();
            let (shutdown_tx, shutdown_rx) = watch::channel(false);
            let rpc_raft = raft.clone();
            rpc_tasks.push((
                shutdown_tx,
                tokio::spawn(async move {
                    serve_raft_rpc(listener, (*rpc_raft).clone(), shutdown_rx)
                        .await
                        .expect("rpc server should stop");
                }),
            ));
            rafts.push(raft);
        }

        // Non-seed voters must get past startup by waiting for any cluster
        // leader instead of blocking until they lead themselves.
        timeout(Duration::from_secs(10), async {
            for raft in &rafts {
                wait_for_any_leader(raft)
                    .await
                    .expect("leader wait should succeed");
            }
        })
        .await
        .expect("cluster servers should observe a leader");

        rafts.remove(0).shutdown().await.unwrap();
        let (leader_shutdown, leader_task) = rpc_tasks.remove(0);
        leader_shutdown.send(true).unwrap();
        leader_task.await.unwrap();

        let new_leader = timeout(Duration::from_secs(8), async {
            loop {
                match rafts[0].current_leader().await {
                    Some(node_id) if node_id != 1 => break node_id,
                    _ => tokio::time::sleep(Duration::from_millis(20)).await,
                }
            }
        })
        .await
        .expect("remaining voters should elect a new leader");
        let leader_raft = &rafts[usize::try_from(new_leader - 2).unwrap()];
        timeout(Duration::from_secs(5), async {
            loop {
                match leader_raft
                    .client_write(RaftRequest::Apply(Operation::Set {
                        key: Bytes::from_static(b"failover-key"),
                        value: Bytes::from_static(b"failover-value"),
                    }))
                    .await
                {
                    Ok(_) => break,
                    Err(openraft::error::RaftError::APIError(
                        openraft::error::ClientWriteError::ForwardToLeader(_),
                    )) => tokio::time::sleep(Duration::from_millis(25)).await,
                    Err(error) => panic!("failover write should succeed: {error}"),
                }
            }
        })
        .await
        .expect("failover write should finish");

        for raft in &rafts {
            raft.shutdown().await.unwrap();
        }
        for (shutdown_tx, task) in rpc_tasks {
            shutdown_tx.send(true).unwrap();
            task.await.unwrap();
        }
        let _ = std::fs::remove_dir_all(root);
    }
}
