use std::sync::Arc;
use std::time::Duration;

use axum_server::tls_rustls::RustlsConfig;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::WebPkiClientVerifier;
use rustls::{RootCertStore, ServerConfig};
use tokio::time::sleep;
use tracing::{error, info};

use crate::config::Config;
use crate::error::{Error, Result};

/// Build a complete rustls ServerConfig (with or without mTLS).
async fn build_server_config(cfg: &Config) -> Result<ServerConfig> {
    let cert_path = cfg.cert_path();
    let key_path = cfg.key_path();

    let cert_pem = tokio::fs::read(&cert_path).await.map_err(|e| {
        Error::Tls(format!(
            "failed to read server cert {}: {e}",
            cert_path.display()
        ))
    })?;
    let key_pem = tokio::fs::read(&key_path).await.map_err(|e| {
        Error::Tls(format!(
            "failed to read server key {}: {e}",
            key_path.display()
        ))
    })?;

    let certs = load_certs(&cert_pem)?;
    let key = load_private_key(&key_pem)?;

    let mut server_config = if cfg.mtls_enabled {
        let ca_path = cfg.ca_path();
        let ca_pem = tokio::fs::read(&ca_path).await.map_err(|e| {
            Error::Tls(format!("failed to read CA cert {}: {e}", ca_path.display()))
        })?;

        let mut roots = RootCertStore::empty();
        for cert in load_certs(&ca_pem)? {
            roots
                .add(cert)
                .map_err(|e| Error::Tls(format!("failed to add CA certificate: {e}")))?;
        }

        let verifier = WebPkiClientVerifier::builder(Arc::new(roots))
            .build()
            .map_err(|e| Error::Tls(format!("failed to build client cert verifier: {e}")))?;

        ServerConfig::builder()
            .with_client_cert_verifier(verifier)
            .with_single_cert(certs, key)
            .map_err(|e| Error::Tls(format!("failed to build ServerConfig with mTLS: {e}")))?
    } else {
        ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .map_err(|e| Error::Tls(format!("failed to build ServerConfig: {e}")))?
    };

    // Required by axum / hyper
    server_config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];

    Ok(server_config)
}

/// Create the initial RustlsConfig used by axum-server.
pub async fn build_rustls_config(cfg: &Config) -> Result<RustlsConfig> {
    let server_config = build_server_config(cfg).await?;
    let rustls_config = RustlsConfig::from_config(Arc::new(server_config));

    info!(
        cert = %cfg.cert_path().display(),
        key = %cfg.key_path().display(),
        mtls = cfg.mtls_enabled,
        "TLS configuration loaded"
    );

    Ok(rustls_config)
}

/// Background task that periodically reloads certificates (and CA when mTLS is enabled).
pub fn spawn_tls_reloader(config: RustlsConfig, cfg: Config) {
    tokio::spawn(async move {
        let interval = Duration::from_secs(30);

        loop {
            sleep(interval).await;

            match build_server_config(&cfg).await {
                Ok(new_server_config) => {
                    config.reload_from_config(Arc::new(new_server_config));
                    info!(
                        cert = %cfg.cert_path().display(),
                        key = %cfg.key_path().display(),
                        mtls = cfg.mtls_enabled,
                        "TLS certificates reloaded successfully"
                    );
                }
                Err(e) => {
                    error!(
                        error = %e,
                        "failed to reload TLS certificates – keeping previous config"
                    );
                }
            }
        }
    });
}

/// Convenience helper used by main.
pub async fn setup_tls(cfg: &Config) -> Result<Option<RustlsConfig>> {
    if !cfg.tls_enabled {
        return Ok(None);
    }

    // Basic existence checks
    if !cfg.cert_path().exists() {
        return Err(Error::Tls(format!(
            "server certificate not found: {}",
            cfg.cert_path().display()
        )));
    }
    if !cfg.key_path().exists() {
        return Err(Error::Tls(format!(
            "server private key not found: {}",
            cfg.key_path().display()
        )));
    }
    if cfg.mtls_enabled && !cfg.ca_path().exists() {
        return Err(Error::Tls(format!(
            "mTLS enabled but CA certificate not found: {}",
            cfg.ca_path().display()
        )));
    }

    let rustls_config = build_rustls_config(cfg).await?;

    // Start automatic reload (server cert + key + CA)
    spawn_tls_reloader(rustls_config.clone(), cfg.clone());

    Ok(Some(rustls_config))
}

// ===== PEM helpers =====

fn load_certs(pem: &[u8]) -> Result<Vec<CertificateDer<'static>>> {
    let mut reader = std::io::Cursor::new(pem);
    let certs = rustls_pemfile::certs(&mut reader)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| Error::Tls(format!("failed to parse certificates: {e}")))?;

    if certs.is_empty() {
        return Err(Error::Tls("no certificates found in PEM data".into()));
    }
    Ok(certs)
}

fn load_private_key(pem: &[u8]) -> Result<PrivateKeyDer<'static>> {
    let mut reader = std::io::Cursor::new(pem);
    let mut keys = rustls_pemfile::read_all(&mut reader)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| Error::Tls(format!("failed to parse private key: {e}")))?;

    match keys.pop() {
        Some(rustls_pemfile::Item::Pkcs8Key(key)) => Ok(PrivateKeyDer::Pkcs8(key)),
        Some(rustls_pemfile::Item::Pkcs1Key(key)) => Ok(PrivateKeyDer::Pkcs1(key)),
        Some(rustls_pemfile::Item::Sec1Key(key)) => Ok(PrivateKeyDer::Sec1(key)),
        _ => Err(Error::Tls("no valid private key found in PEM data".into())),
    }
}
