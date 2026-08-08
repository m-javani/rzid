use clap::Parser;
use std::net::SocketAddr;
use std::path::PathBuf;

use crate::error::{Error, Result};

/// RZID - Roomzin Identity Directory
#[derive(Debug, Clone, Parser)]
#[command(
    name = "rzid",
    version,
    about = "Roomzin Identity Directory (control plane)"
)]
pub struct Config {
    /// Listen address
    #[arg(long, default_value = "0.0.0.0")]
    pub addr: String,

    /// Listen port
    #[arg(long, short = 'p', default_value = "8080")]
    pub port: u16,

    /// Path to the state JSON file
    #[arg(long, default_value = "state.json")]
    pub state_file: PathBuf,

    /// Enable TLS
    #[arg(long, default_value_t = false)]
    pub tls_enabled: bool,

    /// Enable mutual TLS (requires --tls-enabled and ca.crt)
    #[arg(long, default_value_t = false)]
    pub mtls_enabled: bool,

    /// Directory containing TLS certificates.
    /// Expected files:
    ///   - server.crt  (server certificate, may include chain)
    ///   - server.key  (private key)
    ///   - ca.crt      (CA certificate, required only when --mtls-enabled)
    #[arg(long, default_value = "certs")]
    pub cert_dir: PathBuf,

    /// Persistence buffer time in milliseconds (how long to wait before writing state to disk)
    #[arg(long, default_value_t = 1000)]
    pub buffer_ms: u64,

    /// Timeout in seconds after which a component is deregistered if no heartbeat is received
    #[arg(long, default_value_t = 60)]
    pub heartbeat_timeout_secs: u64,
}

impl Config {
    pub fn socket_addr(&self) -> Result<SocketAddr> {
        let addr = format!("{}:{}", self.addr, self.port);
        addr.parse()
            .map_err(|e| Error::Config(format!("invalid listen address '{addr}': {e}")))
    }

    pub fn cert_path(&self) -> PathBuf {
        self.cert_dir.join("server.crt")
    }

    pub fn key_path(&self) -> PathBuf {
        self.cert_dir.join("server.key")
    }

    pub fn ca_path(&self) -> PathBuf {
        self.cert_dir.join("ca.crt")
    }

    /// Ensure required directories exist
    pub fn ensure_directories(&self) -> Result<()> {
        if let Some(parent) = self.state_file.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }

        if self.tls_enabled {
            std::fs::create_dir_all(&self.cert_dir)?;
        }

        Ok(())
    }
}
