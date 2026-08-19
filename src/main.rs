// // SPDX-License-Identifier: BUSL-1.1
// // Copyright (c) 2026 M. Javani
// //
// // This file is part of rzid.
// //
// // Use of this software is governed by the Business Source License 1.1
// // included in the LICENSE file in the root of this repository.

mod api;
mod codecs;
mod config;
mod error;
mod metrics;
mod state;

use axum_server::Handle;
use clap::Parser;
use tracing::info;

use crate::codecs::load_codecs_yaml;
use crate::config::Config;
use crate::error::Result;
use crate::metrics::init_global_metrics;
use crate::state::AppState;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_target(false)
        .with_thread_ids(false)
        .with_file(false)
        .with_line_number(false)
        .init();

    let cfg = Config::parse();
    cfg.ensure_directories()?;

    info!(
        addr = %cfg.addr,
        port = cfg.port,
        state_file = %cfg.state_file.display(),
        "starting RZID"
    );

    init_global_metrics();

    // Validate codecs file exists
    cfg.validate_codecs_file()?;

    let codecs = load_codecs_yaml(&cfg.codecs_path)
        .map_err(|e| crate::error::RzError::Config(format!("Failed to load codecs: {}", e)))?;

    // Create state (loads from disk if present)
    let state = AppState::new(
        cfg.state_file.clone(),
        cfg.heartbeat_timeout_secs,
        cfg.buffer_ms,
        codecs,
    )
    .await?;

    // Background tasks (sweep + flusher)
    state.spawn_background_tasks();

    // Build router with state
    let app = api::build_router(state.clone());

    let handle = Handle::new();

    // Graceful shutdown
    let shutdown_handle = handle.clone();
    let shutdown_state = state.clone();
    tokio::spawn(async move {
        shutdown_signal().await;
        info!("shutdown signal received – flushing state");
        if let Err(e) = shutdown_state.persist().await {
            tracing::error!(error = %e, "failed to flush state on shutdown");
        }
        shutdown_handle.graceful_shutdown(Some(std::time::Duration::from_secs(10)));
    });

    let addr = cfg.socket_addr()?;

    info!(%addr, "listening (plain HTTP)");
    axum_server::bind(addr)
        .handle(handle)
        .serve(app.into_make_service())
        .await?;

    info!("server stopped");
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}
