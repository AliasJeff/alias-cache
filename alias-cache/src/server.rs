use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio_rustls::TlsConnector;
use tracing::{info, warn};

use crate::config::Config;
use crate::error::{Error, Result};
#[cfg(test)]
use crate::node_state::{NodeState, SharedNodeState};
use crate::protocol::{Decoder, Frame, RespVersion, command_from_frame};
use crate::raft::{
    ClientRequestOutcome, RaftHandle, enable_raft_runtime, execute_client_request_with_id,
    execute_transaction_request_with_id, new_client_request_id, new_tcp_raft_for_server,
    open_single_node, run_expiration_worker, run_leader_time_worker, send_client_request_with_id,
    send_transaction_request_with_id, serve_raft_rpc_with_tls, wait_for_any_leader,
};
use crate::sentinel::{self, SentinelContext};
use crate::tls::TlsContext;

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
    async fn open_raft(
        &self,
        tls_connector: Option<tokio_rustls::TlsConnector>,
    ) -> Result<Arc<RaftHandle>> {
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
            tls_connector,
        )
        .await?;
        if !self.config.bootstrap {
            enable_raft_runtime(&raft);
        }
        Ok(Arc::new(raft))
    }

    pub async fn run(self) -> Result<()> {
        self.config.ensure_data_dir()?;
        let tls_context = TlsContext::load(&self.config)?;
        let client_tls_acceptor = tls_context.client_acceptor();
        let raft_tls_acceptor = tls_context.replication_acceptor();
        let tls_connector = tls_context.replication_connector();
        let listener = TcpListener::bind(self.config.client_addr)
            .await
            .map_err(|source| Error::io("bind client listener", source))?;
        info!(addr = %self.config.client_addr, node_id = self.config.node_id, "client listener started");

        let raft_listener = TcpListener::bind(self.config.raft_addr)
            .await
            .map_err(|source| Error::io("bind raft listener", source))?;
        let raft = self.open_raft(tls_connector.clone()).await?;
        let shutdown = Arc::new(self.shutdown);
        let raft_shutdown = shutdown.as_ref().clone();
        let rpc_raft = raft.clone();
        tokio::spawn(async move {
            if let Err(error) = serve_raft_rpc_with_tls(
                raft_listener,
                (*rpc_raft).clone(),
                raft_shutdown,
                raft_tls_acceptor,
            )
            .await
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
                    let password = self.config.password.clone();
                    let tls_acceptor = client_tls_acceptor.clone();
                    let tls_connector = tls_connector.clone();
                    tokio::spawn(async move {
                        let result = match tls_acceptor {
                            Some(acceptor) => match acceptor.accept(stream).await {
                                Ok(stream) => {
                                    handle_raft_connection(
                                        stream,
                                        child_shutdown,
                                        child_raft,
                                        limits,
                                        sentinel_context,
                                        password,
                                        tls_connector.clone(),
                                    )
                                    .await
                                }
                                Err(error) => Err(Error::server(format!(
                                    "TLS client handshake failed: {error}"
                                ))),
                            },
                            None => {
                                handle_raft_connection(
                                    stream,
                                    child_shutdown,
                                    child_raft,
                                    limits,
                                    sentinel_context,
                                    password,
                                    tls_connector.clone(),
                                )
                                .await
                            }
                        };
                        if let Err(error) = result {
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
    mut stream: impl tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send,
    shutdown: Arc<watch::Receiver<bool>>,
    node_state: SharedNodeState,
    limits: crate::protocol::ProtocolLimits,
    password: Option<Bytes>,
) -> Result<()> {
    let mut decoder = Decoder::new(limits);
    let mut resp_version = RespVersion::Resp2;
    let mut authenticated = password.is_none();
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
                        write_frame(
                            &mut stream,
                            Frame::error(format!("ERR {error}")),
                            resp_version,
                        )
                        .await?;
                        return Err(error);
                    }
                };
                for frame in frames {
                    let response = match command_from_frame(frame) {
                        Ok(request) => {
                            if let Some(response) = connection_command(
                                &request,
                                &mut resp_version,
                                &mut authenticated,
                                password.as_deref(),
                            ) {
                                response
                            } else {
                                let mut state = node_state.lock().await;
                                match state.execute(request) {
                                    Ok(response) => response,
                                    Err(error) => Frame::error(format!("ERR {error}")),
                                }
                            }
                        }
                        Err(error) => Frame::error(format!("ERR {error}")),
                    };
                    write_frame(&mut stream, response, resp_version).await?;
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
    mut stream: impl tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send,
    shutdown: Arc<watch::Receiver<bool>>,
    raft: Arc<RaftHandle>,
    limits: crate::protocol::ProtocolLimits,
    sentinel_context: SentinelContext,
    password: Option<Bytes>,
    tls_connector: Option<TlsConnector>,
) -> Result<()> {
    let mut decoder = Decoder::new(limits);
    let mut input = [0_u8; 8 * 1024];
    let mut transaction = None;
    let mut resp_version = RespVersion::Resp2;
    let mut authenticated = password.is_none();
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
                        write_frame(
                            &mut stream,
                            Frame::error(format!("ERR {error}")),
                            resp_version,
                        )
                        .await?;
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
                                &mut resp_version,
                                &mut authenticated,
                                password.as_deref(),
                                tls_connector.clone(),
                            )
                            .await
                        }
                        Err(error) => Frame::error(format!("ERR {error}")),
                    };
                    write_frame(&mut stream, response, resp_version).await?;
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

/// Connection-level commands that never touch the replicated state machine.
///
/// PING and ECHO observe no state, so any node answers them locally without
/// leader redirection; a follower must stay usable as a liveness endpoint.
fn connection_command(
    request: &crate::protocol::Request,
    resp_version: &mut RespVersion,
    authenticated: &mut bool,
    configured_password: Option<&[u8]>,
) -> Option<Frame> {
    let crate::protocol::Request::Command(arguments) = request;
    let command = arguments.first()?;
    if command.eq_ignore_ascii_case(b"HELLO") {
        return Some(hello_command(
            arguments,
            resp_version,
            authenticated,
            configured_password,
        ));
    }
    if command.eq_ignore_ascii_case(b"AUTH") {
        return Some(auth_command(arguments, authenticated, configured_password));
    }
    if command.eq_ignore_ascii_case(b"QUIT") {
        return Some(Frame::simple_string("OK"));
    }
    if configured_password.is_some() && !*authenticated {
        return Some(Frame::error("NOAUTH Authentication required."));
    }
    if command.eq_ignore_ascii_case(b"PING") {
        return Some(match arguments.len() {
            1 => Frame::simple_string("PONG"),
            2 => Frame::bulk_string(arguments[1].clone()),
            _ => Frame::error("ERR wrong number of arguments for 'ping' command"),
        });
    }
    if command.eq_ignore_ascii_case(b"ECHO") {
        return Some(match arguments.len() {
            2 => Frame::bulk_string(arguments[1].clone()),
            _ => Frame::error("ERR wrong number of arguments for 'echo' command"),
        });
    }
    None
}

fn auth_command(
    arguments: &[Bytes],
    authenticated: &mut bool,
    configured_password: Option<&[u8]>,
) -> Frame {
    let password = match arguments {
        [command, password] if command.eq_ignore_ascii_case(b"AUTH") => password.as_ref(),
        [command, username, password]
            if command.eq_ignore_ascii_case(b"AUTH")
                && username.eq_ignore_ascii_case(b"default") =>
        {
            password.as_ref()
        }
        [command, _, _] if command.eq_ignore_ascii_case(b"AUTH") => {
            return Frame::error("WRONGPASS invalid username-password pair");
        }
        _ => return Frame::error("ERR wrong number of arguments for 'auth' command"),
    };
    let Some(configured_password) = configured_password else {
        return Frame::error(
            "ERR AUTH <password> called without any password configured for the default user.",
        );
    };
    if password == configured_password {
        *authenticated = true;
        Frame::simple_string("OK")
    } else {
        Frame::error("WRONGPASS invalid username-password pair")
    }
}

fn hello_command(
    arguments: &[Bytes],
    resp_version: &mut RespVersion,
    authenticated: &mut bool,
    configured_password: Option<&[u8]>,
) -> Frame {
    let requested = match arguments.get(1) {
        None => *resp_version,
        Some(value) => match value.as_ref() {
            b"2" => RespVersion::Resp2,
            b"3" => RespVersion::Resp3,
            _ => return Frame::error("NOPROTO unsupported protocol version"),
        },
    };
    let mut index = usize::from(arguments.len() > 1) + 1;
    while index < arguments.len() {
        if arguments[index].eq_ignore_ascii_case(b"SETNAME") {
            if index + 1 >= arguments.len() {
                return Frame::error("ERR syntax error");
            }
            index += 2;
        } else if arguments[index].eq_ignore_ascii_case(b"AUTH") {
            if index + 2 >= arguments.len()
                || !arguments[index + 1].eq_ignore_ascii_case(b"default")
            {
                return Frame::error("WRONGPASS invalid username-password pair");
            }
            let Some(configured_password) = configured_password else {
                return Frame::error("ERR AUTH is not configured");
            };
            if arguments[index + 2].as_ref() != configured_password {
                return Frame::error("WRONGPASS invalid username-password pair");
            }
            *authenticated = true;
            index += 3;
        } else {
            return Frame::error("ERR Syntax error");
        }
    }
    if configured_password.is_some() && !*authenticated {
        return Frame::error(
            "NOAUTH HELLO must be called with the client already authenticated, otherwise the HELLO AUTH <user> <pass> option can be used",
        );
    }
    *resp_version = requested;
    Frame::Map(vec![
        (
            Frame::bulk_string("server"),
            Frame::bulk_string("alias-cache"),
        ),
        (
            Frame::bulk_string("version"),
            Frame::bulk_string(env!("CARGO_PKG_VERSION")),
        ),
        (
            Frame::bulk_string("proto"),
            Frame::Integer(if requested == RespVersion::Resp3 {
                3
            } else {
                2
            }),
        ),
        (Frame::bulk_string("id"), Frame::Integer(0)),
        (Frame::bulk_string("mode"), Frame::bulk_string("standalone")),
        (Frame::bulk_string("role"), Frame::bulk_string("master")),
        (Frame::bulk_string("modules"), Frame::Array(Vec::new())),
    ])
}

#[allow(clippy::too_many_arguments)]
async fn handle_transaction_command(
    raft: &RaftHandle,
    transaction: &mut Option<Vec<crate::protocol::Request>>,
    request: crate::protocol::Request,
    sentinel_context: &SentinelContext,
    resp_version: &mut RespVersion,
    authenticated: &mut bool,
    configured_password: Option<&[u8]>,
    tls_connector: Option<TlsConnector>,
) -> Frame {
    let command = match &request {
        crate::protocol::Request::Command(arguments) => {
            arguments.first().map(|argument| argument.as_ref())
        }
    };
    if configured_password.is_some()
        && !*authenticated
        && !command.is_some_and(|command| {
            command.eq_ignore_ascii_case(b"AUTH")
                || command.eq_ignore_ascii_case(b"HELLO")
                || command.eq_ignore_ascii_case(b"QUIT")
        })
    {
        return Frame::error("NOAUTH Authentication required.");
    }
    if transaction.is_none() {
        if let Some(response) =
            connection_command(&request, resp_version, authenticated, configured_password)
        {
            return response;
        }
        if let Some(response) = sentinel::execute(raft, sentinel_context, &request).await {
            return response;
        }
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
        return execute_transaction_with_tls(raft, requests, tls_connector).await;
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
    execute_client_command_with_tls(raft, request, tls_connector).await
}

async fn execute_transaction_with_tls(
    raft: &RaftHandle,
    requests: Vec<crate::protocol::Request>,
    tls_connector: Option<TlsConnector>,
) -> Frame {
    let request_id = new_client_request_id();
    match execute_transaction_request_with_id(raft, request_id, requests.clone()).await {
        Ok(ClientRequestOutcome::Applied(response)) => response,
        Ok(ClientRequestOutcome::Redirect(address)) => match send_transaction_request_with_id(
            &address,
            request_id,
            requests,
            &openraft::network::RPCOption::new(std::time::Duration::from_secs(1)),
            tls_connector,
        )
        .await
        {
            Ok(response) => response,
            Err(error) => Frame::error(format!("ERR {error}")),
        },
        Err(error) => Frame::error(format!("ERR {error}")),
    }
}

async fn execute_client_command_with_tls(
    raft: &RaftHandle,
    request: crate::protocol::Request,
    tls_connector: Option<TlsConnector>,
) -> Frame {
    let request_id = new_client_request_id();
    match execute_client_request_with_id(raft, request_id, request.clone()).await {
        Ok(ClientRequestOutcome::Applied(response)) => response,
        Ok(ClientRequestOutcome::Redirect(address)) => {
            match send_client_request_with_id(
                &address,
                request_id,
                request,
                &openraft::network::RPCOption::new(std::time::Duration::from_secs(1)),
                tls_connector,
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

async fn write_frame(
    stream: &mut (impl tokio::io::AsyncWrite + Unpin),
    frame: Frame,
    resp_version: RespVersion,
) -> Result<()> {
    let mut output = BytesMut::new();
    frame.encode_for(resp_version, &mut output)?;
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
    use crate::raft::serve_raft_rpc;

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

    /// Buffered RESP frame reader that keeps decoder state across reads, so
    /// multiple responses arriving in one TCP segment are not lost.
    struct FrameReader {
        decoder: crate::protocol::Decoder,
        pending: std::collections::VecDeque<Frame>,
        buffer: [u8; 256],
    }

    impl FrameReader {
        fn new() -> Self {
            Self {
                decoder: crate::protocol::Decoder::new(crate::protocol::ProtocolLimits::default()),
                pending: std::collections::VecDeque::new(),
                buffer: [0_u8; 256],
            }
        }

        async fn next_frame(&mut self, stream: &mut TcpStream) -> Frame {
            loop {
                if let Some(frame) = self.pending.pop_front() {
                    return frame;
                }
                let read = timeout(Duration::from_secs(5), stream.read(&mut self.buffer))
                    .await
                    .unwrap()
                    .unwrap();
                assert_ne!(read, 0);
                self.pending
                    .extend(self.decoder.push(&self.buffer[..read]).unwrap());
            }
        }
    }

    /// A real three-node cluster serving RESP connections through the
    /// production server path, used by cluster-level connection tests.
    struct TestCluster {
        client_addrs: Vec<SocketAddr>,
        rafts: Vec<Arc<RaftHandle>>,
        connection_tasks: Vec<tokio::task::JoinHandle<()>>,
        rpc_tasks: Vec<(watch::Sender<bool>, tokio::task::JoinHandle<()>)>,
        shutdown: watch::Sender<bool>,
        root: std::path::PathBuf,
    }

    async fn spawn_three_node_cluster(name: &str) -> TestCluster {
        use crate::raft::Node;

        let root = unique_server_dir(name);
        let mut raft_listeners = Vec::new();
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
            raft_listeners.push(listener);
        }

        let (shutdown_tx, _) = watch::channel(false);
        let mut client_addrs = Vec::new();
        let mut rafts = Vec::new();
        let mut rpc_tasks = Vec::new();
        let mut connection_tasks = Vec::new();
        for (index, raft_listener) in raft_listeners.into_iter().enumerate() {
            let node_id = u64::try_from(index + 1).unwrap();
            let client_listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
                .await
                .unwrap();
            client_addrs.push(client_listener.local_addr().unwrap());
            let config = Config {
                node_id,
                bootstrap: index == 0,
                cluster_nodes: nodes.clone(),
                data_dir: root.join(format!("node-{node_id}")),
                client_addr: client_listener.local_addr().unwrap(),
                ..Config::default()
            };
            let server = Server::new(config.clone(), watch::channel(false).1).unwrap();
            let raft = server.open_raft(None).await.unwrap();
            let rpc_raft = raft.clone();
            let rpc_shutdown = shutdown_tx.subscribe();
            rpc_tasks.push((
                shutdown_tx.clone(),
                tokio::spawn(async move {
                    serve_raft_rpc(raft_listener, (*rpc_raft).clone(), rpc_shutdown)
                        .await
                        .expect("rpc server should stop");
                }),
            ));
            let connection_raft = raft.clone();
            let connection_shutdown = Arc::new(shutdown_tx.subscribe());
            let limits = config.protocol_limits();
            let sentinel_context = SentinelContext::from_config(&config);
            connection_tasks.push(tokio::spawn(async move {
                loop {
                    tokio::select! {
                        accepted = client_listener.accept() => {
                            let (stream, _) = accepted.unwrap();
                            let raft = connection_raft.clone();
                            let shutdown = connection_shutdown.clone();
                            let sentinel_context = sentinel_context.clone();
                            let password = config.password.clone();
                            tokio::spawn(async move {
                                let _ = handle_raft_connection(
                                    stream,
                                    shutdown,
                                    raft,
                                    limits,
                                    sentinel_context,
                                    password,
                                    None,
                                )
                                .await;
                            });
                        }
                        result = wait_for_shutdown(connection_shutdown.clone()) => {
                            result.unwrap();
                            break;
                        }
                    }
                }
            }));
            rafts.push(raft);
        }
        TestCluster {
            client_addrs,
            rafts,
            connection_tasks,
            rpc_tasks,
            shutdown: shutdown_tx,
            root,
        }
    }

    impl TestCluster {
        async fn leader_index(&self) -> usize {
            timeout(Duration::from_secs(10), async {
                loop {
                    if let Some(node_id) = self.rafts[0].current_leader().await {
                        return usize::try_from(node_id - 1).unwrap();
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .expect("cluster should elect a leader")
        }

        async fn shutdown(self) {
            // One broadcast stops the connection loops and every rpc task.
            self.shutdown.send(true).unwrap();
            for task in self.connection_tasks {
                task.await.unwrap();
            }
            for raft in &self.rafts {
                raft.shutdown().await.unwrap();
            }
            for (_, task) in self.rpc_tasks {
                task.await.unwrap();
            }
            let _ = std::fs::remove_dir_all(self.root);
        }
    }

    #[tokio::test]
    async fn follower_connection_serves_reads_through_leader_redirect() {
        let cluster = spawn_three_node_cluster("follower-read").await;
        let leader = cluster.leader_index().await;
        let follower = if leader == 0 { 1 } else { 0 };

        let mut writer = TcpStream::connect(cluster.client_addrs[leader])
            .await
            .unwrap();
        let mut writer_frames = FrameReader::new();
        writer
            .write_all(b"*3\r\n$3\r\nSET\r\n$8\r\nread-key\r\n$10\r\nread-value\r\n")
            .await
            .unwrap();
        assert_eq!(
            writer_frames.next_frame(&mut writer).await,
            Frame::simple_string("OK")
        );

        // The follower cannot pass a linearizable read barrier, so the read
        // must be redirected to the leader and answered with the same value
        // a directly connected client would observe.
        let mut reader = TcpStream::connect(cluster.client_addrs[follower])
            .await
            .unwrap();
        let mut reader_frames = FrameReader::new();
        reader
            .write_all(b"*2\r\n$3\r\nGET\r\n$8\r\nread-key\r\n")
            .await
            .unwrap();
        assert_eq!(
            reader_frames.next_frame(&mut reader).await,
            Frame::bulk_string("read-value")
        );

        cluster.shutdown().await;
    }

    /// Encode one RESP command as a bulk-array frame so tests cannot desync
    /// the declared length from the payload.
    fn encode_command(parts: &[&[u8]]) -> Vec<u8> {
        let mut frame = format!("*{}\r\n", parts.len()).into_bytes();
        for part in parts {
            frame.extend(format!("${}\r\n", part.len()).into_bytes());
            frame.extend_from_slice(part);
            frame.extend_from_slice(b"\r\n");
        }
        frame
    }

    #[tokio::test]
    async fn cluster_replicates_every_data_structure_and_serves_follower_reads() {
        let cluster = spawn_three_node_cluster("structures").await;
        let leader = cluster.leader_index().await;
        let follower = if leader == 0 { 1 } else { 0 };

        let mut leader_conn = TcpStream::connect(cluster.client_addrs[leader])
            .await
            .unwrap();
        let mut leader_frames = FrameReader::new();
        // Hash, list, set, sorted set, and string counters: one write round
        // through the leader, then the same reads through a follower to prove
        // replication plus read forwarding agree on every structure.
        for command in [
            encode_command(&[b"HSET", b"h", b"b", b"two", b"a", b"one"]),
            encode_command(&[b"LPUSH", b"l", b"one"]),
            encode_command(&[b"LPUSH", b"l", b"two"]),
            encode_command(&[b"SADD", b"s", b"x"]),
            encode_command(&[b"SADD", b"s", b"y"]),
            encode_command(&[b"ZADD", b"z", b"2", b"beta"]),
            encode_command(&[b"ZADD", b"z", b"1", b"alpha"]),
            encode_command(&[b"INCRBY", b"hits", b"7"]),
        ] {
            leader_conn.write_all(&command).await.unwrap();
        }
        for expected in [
            Frame::Integer(2), // HSET: two new fields
            Frame::Integer(1), // LPUSH: list length after one element
            Frame::Integer(2), // LPUSH: list length after two elements
            Frame::Integer(1), // SADD
            Frame::Integer(1), // SADD
            Frame::Integer(1), // ZADD
            Frame::Integer(1), // ZADD
            Frame::Integer(7), // INCRBY from zero
        ] {
            assert_eq!(
                leader_frames.next_frame(&mut leader_conn).await,
                expected,
                "leader write round must return canonical counts"
            );
        }

        let mut follower_conn = TcpStream::connect(cluster.client_addrs[follower])
            .await
            .unwrap();
        let mut follower_frames = FrameReader::new();
        for command in [
            encode_command(&[b"HGETALL", b"h"]),
            encode_command(&[b"LRANGE", b"l", b"0", b"-1"]),
            encode_command(&[b"SMEMBERS", b"s"]),
            encode_command(&[b"ZRANGE", b"z", b"0", b"-1", b"WITHSCORES"]),
            encode_command(&[b"GET", b"hits"]),
            encode_command(&[b"TYPE", b"z"]),
        ] {
            follower_conn.write_all(&command).await.unwrap();
        }
        assert_eq!(
            follower_frames.next_frame(&mut follower_conn).await,
            Frame::Array(vec![
                Frame::bulk_string("a"),
                Frame::bulk_string("one"),
                Frame::bulk_string("b"),
                Frame::bulk_string("two"),
            ]),
            "HGETALL returns fields in deterministic byte order"
        );
        assert_eq!(
            follower_frames.next_frame(&mut follower_conn).await,
            Frame::Array(vec![Frame::bulk_string("two"), Frame::bulk_string("one"),]),
            "LRANGE reflects the last push first"
        );
        assert_eq!(
            follower_frames.next_frame(&mut follower_conn).await,
            Frame::Array(vec![Frame::bulk_string("x"), Frame::bulk_string("y"),]),
            "SMEMBERS returns members in byte order"
        );
        assert_eq!(
            follower_frames.next_frame(&mut follower_conn).await,
            Frame::Array(vec![
                Frame::bulk_string("alpha"),
                Frame::bulk_string("1"),
                Frame::bulk_string("beta"),
                Frame::bulk_string("2"),
            ]),
            "ZRANGE sorts by score then member"
        );
        assert_eq!(
            follower_frames.next_frame(&mut follower_conn).await,
            Frame::bulk_string("7"),
            "INCRBY result must be visible through a follower read"
        );
        assert_eq!(
            follower_frames.next_frame(&mut follower_conn).await,
            Frame::simple_string("zset"),
            "TYPE must agree with the replicated value type"
        );

        cluster.shutdown().await;
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
            handle_connection(stream, shutdown, state, limits, None).await
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
            handle_connection(stream, shutdown, state, limits, None).await
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
    async fn connection_commands_answer_ping_and_echo_without_raft() {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let config = Config {
            client_addr: SocketAddr::from(([127, 0, 0, 1], 0)),
            raft_addr: SocketAddr::from(([127, 0, 0, 1], 0)),
            data_dir: unique_server_dir("connection-commands"),
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
        let limits = config.protocol_limits();
        let sentinel_context = SentinelContext::from_config(&config);
        let task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            handle_raft_connection(stream, shutdown, raft, limits, sentinel_context, None, None)
                .await
        });

        let mut client = TcpStream::connect(addr).await.unwrap();
        let mut frames = FrameReader::new();
        client
            .write_all(
                b"*1\r\n$4\r\nPING\r\n\
                *2\r\n$4\r\nPING\r\n$5\r\nhello\r\n\
                *2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n\
                *1\r\n$4\r\nECHO\r\n\
                *3\r\n$4\r\nPING\r\n$1\r\na\r\n$1\r\nb\r\n",
            )
            .await
            .unwrap();
        assert_eq!(
            frames.next_frame(&mut client).await,
            Frame::simple_string("PONG")
        );
        assert_eq!(
            frames.next_frame(&mut client).await,
            Frame::bulk_string("hello")
        );
        assert_eq!(
            frames.next_frame(&mut client).await,
            Frame::bulk_string("hello")
        );
        assert_eq!(
            frames.next_frame(&mut client).await,
            Frame::error("ERR wrong number of arguments for 'echo' command")
        );
        assert_eq!(
            frames.next_frame(&mut client).await,
            Frame::error("ERR wrong number of arguments for 'ping' command")
        );

        shutdown_tx.send(true).unwrap();
        let _ = task.await;
        let _ = std::fs::remove_dir_all(data_dir);
    }

    #[test]
    fn hello_negotiates_resp3_and_can_switch_back_to_resp2() {
        let mut version = RespVersion::Resp2;
        let mut authenticated = true;
        let response = hello_command(
            &[
                bytes::Bytes::from_static(b"HELLO"),
                bytes::Bytes::from_static(b"3"),
            ],
            &mut version,
            &mut authenticated,
            None,
        );
        assert_eq!(version, RespVersion::Resp3);
        assert!(matches!(response, Frame::Map(_)));

        let mut output = BytesMut::new();
        response.encode_for(version, &mut output).unwrap();
        assert!(output.starts_with(b"%7\r\n"));

        let response = hello_command(
            &[
                bytes::Bytes::from_static(b"HELLO"),
                bytes::Bytes::from_static(b"2"),
            ],
            &mut version,
            &mut authenticated,
            None,
        );
        assert_eq!(version, RespVersion::Resp2);
        assert!(matches!(response, Frame::Map(_)));
        let mut output = BytesMut::new();
        response.encode_for(version, &mut output).unwrap();
        assert!(output.starts_with(b"*14\r\n"));
    }

    #[test]
    fn hello_rejects_unsupported_versions_and_auth_without_configuration() {
        let mut version = RespVersion::Resp2;
        let mut authenticated = true;
        assert_eq!(
            hello_command(
                &[
                    bytes::Bytes::from_static(b"HELLO"),
                    bytes::Bytes::from_static(b"4"),
                ],
                &mut version,
                &mut authenticated,
                None,
            ),
            Frame::error("NOPROTO unsupported protocol version")
        );
        assert_eq!(
            hello_command(
                &[
                    bytes::Bytes::from_static(b"HELLO"),
                    bytes::Bytes::from_static(b"3"),
                    bytes::Bytes::from_static(b"AUTH"),
                    bytes::Bytes::from_static(b"default"),
                    bytes::Bytes::from_static(b"secret"),
                ],
                &mut version,
                &mut authenticated,
                None,
            ),
            Frame::error("ERR AUTH is not configured")
        );
        assert_eq!(version, RespVersion::Resp2);
    }

    #[tokio::test]
    async fn password_protects_client_commands_and_supports_hello_auth() {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let mut config = Config {
            client_addr: SocketAddr::from(([127, 0, 0, 1], 0)),
            raft_addr: SocketAddr::from(([127, 0, 0, 1], 0)),
            data_dir: unique_server_dir("password-auth"),
            ..Config::default()
        };
        config.password = Some(Bytes::from_static(b"secret"));
        let listener = TcpListener::bind(config.client_addr).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let data_dir = config.data_dir.clone();
        let password = config.password.clone();
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
                password,
                None,
            )
            .await
        });

        let mut client = TcpStream::connect(addr).await.unwrap();
        let mut frames = FrameReader::new();
        client
            .write_all(
                b"*1\r\n$5\r\nMULTI\r\n\
*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n\
*2\r\n$4\r\nAUTH\r\n$5\r\nwrong\r\n\
*2\r\n$4\r\nAUTH\r\n$6\r\nsecret\r\n\
*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n\
*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n",
            )
            .await
            .unwrap();
        assert_eq!(
            frames.next_frame(&mut client).await,
            Frame::error("NOAUTH Authentication required.")
        );
        assert_eq!(
            frames.next_frame(&mut client).await,
            Frame::error("NOAUTH Authentication required.")
        );
        assert_eq!(
            frames.next_frame(&mut client).await,
            Frame::error("WRONGPASS invalid username-password pair")
        );
        assert_eq!(
            frames.next_frame(&mut client).await,
            Frame::simple_string("OK")
        );
        assert_eq!(
            frames.next_frame(&mut client).await,
            Frame::simple_string("OK")
        );
        assert_eq!(
            frames.next_frame(&mut client).await,
            Frame::bulk_string("value")
        );

        shutdown_tx.send(true).unwrap();
        let _ = task.await;
        let _ = std::fs::remove_dir_all(data_dir);

        let mut version = RespVersion::Resp2;
        let response = hello_command(
            &[
                Bytes::from_static(b"HELLO"),
                Bytes::from_static(b"3"),
                Bytes::from_static(b"AUTH"),
                Bytes::from_static(b"default"),
                Bytes::from_static(b"secret"),
            ],
            &mut version,
            &mut false,
            Some(b"secret"),
        );
        assert_eq!(version, RespVersion::Resp3);
        assert!(matches!(response, Frame::Map(_)));
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
                None,
                None,
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
                None,
                None,
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
                None,
                None,
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
            let raft = server.open_raft(None).await.unwrap();
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
