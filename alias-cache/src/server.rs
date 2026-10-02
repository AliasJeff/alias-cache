use std::sync::Arc;

use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tracing::{info, warn};

use crate::config::Config;
use crate::error::{Error, Result};

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
        loop {
            tokio::select! {
                result = listener.accept() => {
                    let (stream, peer) = result
                        .map_err(|source| Error::io("accept client connection", source))?;
                    let child_shutdown = shutdown.clone();
                    tokio::spawn(async move {
                        if let Err(error) = handle_connection(stream, child_shutdown).await {
                            warn!(%peer, %error, "client connection failed");
                        }
                    });
                }
                result = wait_for_shutdown(shutdown.clone()) => {
                    result?;
                    info!("client listener stopped");
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

async fn handle_connection(
    mut stream: TcpStream,
    shutdown: Arc<watch::Receiver<bool>>,
) -> Result<()> {
    stream
        .write_all(b"-ERR alias-cache foundation server is not ready\r\n")
        .await
        .map_err(|source| Error::io("write foundation response", source))?;
    let _ = shutdown;
    stream
        .shutdown()
        .await
        .map_err(|source| Error::io("close client connection", source))?;
    Ok(())
}
