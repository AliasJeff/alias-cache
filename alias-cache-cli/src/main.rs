use alias_cache::{Config, Result};
use tokio::sync::watch;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let config = Config::default();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let server = alias_cache::server::Server::new(config, shutdown_rx)?;

    tokio::select! {
        result = server.run() => result,
        result = tokio::signal::ctrl_c() => {
            result.map_err(|source| alias_cache::Error::io("wait for ctrl-c", source))?;
            shutdown_tx.send(true).map_err(|_| alias_cache::Error::server("server shutdown receiver dropped"))?;
            Ok(())
        }
    }
}
