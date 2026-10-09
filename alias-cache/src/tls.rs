use std::io::Cursor;
use std::path::Path;
use std::sync::Arc;

use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::{ClientConfig, RootCertStore, ServerConfig};
use tokio_rustls::{TlsAcceptor, TlsConnector};

use crate::config::Config;
use crate::error::{Error, Result};

fn load_certificates(path: &Path) -> Result<Vec<CertificateDer<'static>>> {
    let data =
        std::fs::read(path).map_err(|source| Error::io("read TLS certificate file", source))?;
    let mut reader = Cursor::new(data);
    let certificates = rustls_pemfile::certs(&mut reader)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| Error::invalid_config(format!("parse TLS certificate file: {error}")))?;
    if certificates.is_empty() {
        return Err(Error::invalid_config(format!(
            "TLS certificate file '{}' contains no certificates",
            path.display()
        )));
    }
    Ok(certificates)
}

fn load_private_key(path: &Path) -> Result<PrivateKeyDer<'static>> {
    let data =
        std::fs::read(path).map_err(|source| Error::io("read TLS private key file", source))?;

    let mut reader = Cursor::new(data.clone());
    if let Some(key) = rustls_pemfile::pkcs8_private_keys(&mut reader).next() {
        return key
            .map(PrivateKeyDer::from)
            .map_err(|error| Error::invalid_config(format!("parse PKCS#8 private key: {error}")));
    }

    let mut reader = Cursor::new(data.clone());
    if let Some(key) = rustls_pemfile::rsa_private_keys(&mut reader).next() {
        return key
            .map(PrivateKeyDer::from)
            .map_err(|error| Error::invalid_config(format!("parse RSA private key: {error}")));
    }

    let mut reader = Cursor::new(data);
    if let Some(key) = rustls_pemfile::ec_private_keys(&mut reader).next() {
        return key
            .map(PrivateKeyDer::from)
            .map_err(|error| Error::invalid_config(format!("parse EC private key: {error}")));
    }

    Err(Error::invalid_config(format!(
        "TLS private key file '{}' contains no supported private key",
        path.display()
    )))
}

fn load_root_store(path: &Path) -> Result<RootCertStore> {
    let mut roots = RootCertStore::empty();
    for certificate in load_certificates(path)? {
        roots
            .add(certificate)
            .map_err(|error| Error::invalid_config(format!("add TLS CA certificate: {error}")))?;
    }
    Ok(roots)
}

fn build_server_config(config: &Config, require_client_auth: bool) -> Result<Option<ServerConfig>> {
    let (Some(cert_file), Some(key_file)) = (&config.tls_cert_file, &config.tls_key_file) else {
        return Ok(None);
    };
    let certificates = load_certificates(cert_file)?;
    let private_key = load_private_key(key_file)?;
    let server = if require_client_auth {
        let ca_file = config.tls_ca_file.as_ref().ok_or_else(|| {
            Error::invalid_config("TLS mutual authentication requires a CA certificate")
        })?;
        let verifier =
            rustls::server::WebPkiClientVerifier::builder(load_root_store(ca_file)?.into())
                .build()
                .map_err(|error| {
                    Error::invalid_config(format!("build TLS client verifier: {error}"))
                })?;
        ServerConfig::builder()
            .with_client_cert_verifier(verifier)
            .with_single_cert(certificates, private_key)
            .map_err(|error| Error::invalid_config(format!("build TLS server config: {error}")))?
    } else {
        ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certificates, private_key)
            .map_err(|error| Error::invalid_config(format!("build TLS server config: {error}")))?
    };
    Ok(Some(server))
}

/// Build the client-facing TLS acceptor from validated server configuration.
pub fn client_acceptor(config: &Config) -> Result<Option<TlsAcceptor>> {
    Ok(build_server_config(config, config.tls_auth_clients)?
        .map(|server| TlsAcceptor::from(Arc::new(server))))
}

#[derive(Clone, Default)]
pub struct TlsContext {
    client_acceptor: Option<TlsAcceptor>,
    replication_acceptor: Option<TlsAcceptor>,
    replication_connector: Option<TlsConnector>,
}

impl TlsContext {
    pub fn load(config: &Config) -> Result<Self> {
        let (Some(cert_file), Some(key_file)) = (&config.tls_cert_file, &config.tls_key_file)
        else {
            return Ok(Self::default());
        };

        let certificates = load_certificates(cert_file)?;
        let private_key = load_private_key(key_file)?;
        let client_acceptor = client_acceptor(config)?;
        let replication_acceptor = if config.tls_replication {
            let server = build_server_config(config, true)?.ok_or_else(|| {
                Error::invalid_config("TLS replication requires a server certificate")
            })?;
            Some(TlsAcceptor::from(Arc::new(server)))
        } else {
            None
        };
        let replication_connector = if config.tls_replication {
            let ca_file = config.tls_ca_file.as_ref().ok_or_else(|| {
                Error::invalid_config("TLS replication requires a CA certificate")
            })?;
            let client = ClientConfig::builder()
                .with_root_certificates(load_root_store(ca_file)?)
                .with_client_auth_cert(certificates, private_key)
                .map_err(|error| {
                    Error::invalid_config(format!("build TLS replication client config: {error}"))
                })?;
            Some(TlsConnector::from(Arc::new(client)))
        } else {
            None
        };

        Ok(Self {
            client_acceptor,
            replication_acceptor,
            replication_connector,
        })
    }

    pub fn client_acceptor(&self) -> Option<TlsAcceptor> {
        self.client_acceptor.clone()
    }

    pub fn replication_connector(&self) -> Option<TlsConnector> {
        self.replication_connector.clone()
    }

    pub fn replication_acceptor(&self) -> Option<TlsAcceptor> {
        self.replication_acceptor.clone()
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::time::{SystemTime, UNIX_EPOCH};

    use rcgen::generate_simple_self_signed;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    use super::*;

    #[tokio::test]
    async fn replication_context_performs_mutual_tls_handshake() {
        let certificate = generate_simple_self_signed(vec!["localhost".to_string()])
            .expect("test certificate should generate");
        let directory = std::env::temp_dir().join(format!(
            "alias-cache-tls-test-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock should be after unix epoch")
                .as_nanos()
        ));
        std::fs::create_dir_all(&directory).expect("TLS test directory should be created");
        let cert_file = directory.join("identity.crt");
        let key_file = directory.join("identity.key");
        let ca_file = directory.join("ca.crt");
        std::fs::write(&cert_file, certificate.cert.pem()).expect("certificate should be written");
        std::fs::write(&ca_file, certificate.cert.pem()).expect("CA should be written");
        std::fs::write(&key_file, certificate.signing_key.serialize_pem())
            .expect("private key should be written");

        let config = Config {
            tls_cert_file: Some(cert_file),
            tls_key_file: Some(key_file),
            tls_ca_file: Some(ca_file),
            tls_replication: true,
            ..Config::default()
        };
        let context = TlsContext::load(&config).expect("TLS context should load");
        let acceptor = context
            .replication_acceptor()
            .expect("replication acceptor should be configured");
        let connector = context
            .replication_connector()
            .expect("replication connector should be configured");

        let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .await
            .expect("TLS test listener should bind");
        let address = listener
            .local_addr()
            .expect("listener address should resolve");
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("client should connect");
            let mut stream = acceptor.accept(stream).await.expect("mTLS should succeed");
            stream.write_all(b"ok").await.expect("server should write");
        });

        let stream = TcpStream::connect(address)
            .await
            .expect("client TCP connection should succeed");
        let server_name = rustls::pki_types::ServerName::try_from("localhost")
            .expect("test server name should parse");
        let mut stream = connector
            .connect(server_name, stream)
            .await
            .expect("mTLS client handshake should succeed");
        let mut response = [0_u8; 2];
        stream
            .read_exact(&mut response)
            .await
            .expect("client should read TLS response");
        assert_eq!(&response, b"ok");
        server.await.expect("TLS server task should finish");

        let _ = std::fs::remove_dir_all(directory);
    }
}
