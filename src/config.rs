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

    /// Ensure required directories exist
    pub fn ensure_directories(&self) -> Result<()> {
        if let Some(parent) = self.state_file.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }

        Ok(())
    }
}
