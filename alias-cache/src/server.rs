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
    ClientRequestOutcome, RaftHandle, execute_client_request, new_tcp_raft_for_server,
    open_single_node, send_client_request, serve_raft_rpc, wait_for_leader,
};

pub struct Server {
    config: Config,
    shutdown: watch::Receiver<bool>,
}

impl Server {
    pub fn new(config: Config, shutdown: watch::Receiver<bool>) -> Result<Self> {
        config.validate()?;
        Ok(Self { config, shutdown })
    }

    pub async fn run(self) -> Result<()> {
        self.config.ensure_data_dir()?;
        let listener = TcpListener::bind(self.config.client_addr)
            .await
            .map_err(|source| Error::io("bind client listener", source))?;
        info!(addr = %self.config.client_addr, node_id = self.config.node_id, "client listener started");

        let shutdown = Arc::new(self.shutdown);
        let raft_listener = TcpListener::bind(self.config.raft_addr)
            .await
            .map_err(|source| Error::io("bind raft listener", source))?;
        let raft = Arc::new(if self.config.cluster_nodes.is_empty() {
            open_single_node(
                self.config.node_id,
                &self.config.data_dir,
                self.config.raft_addr.to_string(),
            )
            .await?
        } else {
            new_tcp_raft_for_server(
                self.config.node_id,
                &self.config.data_dir,
                self.config.cluster_nodes.clone(),
                self.config.bootstrap,
            )
            .await?
        });
        let raft_shutdown = shutdown.as_ref().clone();
        let rpc_raft = raft.clone();
        tokio::spawn(async move {
            if let Err(error) =
                serve_raft_rpc(raft_listener, (*rpc_raft).clone(), raft_shutdown).await
            {
                warn!(%error, "raft rpc listener failed");
            }
        });
        if !self.config.cluster_nodes.is_empty() && self.config.bootstrap {
            wait_for_leader(&raft, self.config.node_id).await?;
        }
        loop {
            tokio::select! {
                result = listener.accept() => {
                    let (stream, peer) = result
                        .map_err(|source| Error::io("accept client connection", source))?;
                    let child_shutdown = shutdown.clone();
                    let limits = self.config.protocol_limits();
                    let child_raft = raft.clone();
                    tokio::spawn(async move {
                        if let Err(error) =
                            handle_raft_connection(stream, child_shutdown, child_raft, limits).await
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
                        Ok(request) => execute_client_command(&raft, request).await,
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

async fn execute_client_command(raft: &RaftHandle, request: crate::protocol::Request) -> Frame {
    match execute_client_request(raft, request.clone()).await {
        Ok(ClientRequestOutcome::Applied(response)) => response,
        Ok(ClientRequestOutcome::Redirect(address)) => {
            match send_client_request(
                &address,
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
    use std::net::SocketAddr;
    use std::time::Duration;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    use super::*;

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
            handle_raft_connection(stream, shutdown, raft, config.protocol_limits()).await
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
}
