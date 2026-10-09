use std::io::Cursor;
use std::path::Path;
use std::sync::Arc;

use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::{RootCertStore, ServerConfig};
use tokio_rustls::TlsAcceptor;

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

/// Build the client-facing TLS acceptor from validated server configuration.
pub fn client_acceptor(config: &Config) -> Result<Option<TlsAcceptor>> {
    let (Some(cert_file), Some(key_file)) = (&config.tls_cert_file, &config.tls_key_file) else {
        return Ok(None);
    };
    let certificates = load_certificates(cert_file)?;
    let private_key = load_private_key(key_file)?;
    let server = if config.tls_auth_clients {
        let ca_file = config.tls_ca_file.as_ref().ok_or_else(|| {
            Error::invalid_config("TLS client authentication requires a CA certificate")
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
    Ok(Some(TlsAcceptor::from(Arc::new(server))))
}
