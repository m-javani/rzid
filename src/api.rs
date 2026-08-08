use std::sync::Arc;

use axum::{
    Json, Router,
    extract::{Path, State},
    http::{StatusCode, header},
    middleware,
    response::IntoResponse,
    routing::{get, post},
};
use axum::{
    body::Body,
    extract::MatchedPath,
    http::{Request, Response},
    middleware::Next,
};
use prometheus::Encoder;
use serde::{Deserialize, Serialize};
use std::time::Instant;
use tracing::info;

use crate::metrics::get_global_metrics;
use crate::state::AppState;

// =============================================================================
// Router
// =============================================================================

pub fn build_router(state: Arc<AppState>) -> Router {
    Router::new()
        // Operational
        .route("/health", get(health_handler))
        .route("/metrics", get(metrics_handler))
        // Registration
        .route("/register", post(register_handler))
        // Segment ownership (leader → RZID)
        .route(
            "/shards/{shard_id}/segments/version",
            get(shard_segments_version_handler),
        )
        .route("/shards/{shard_id}/segments", post(update_segments_handler))
        // Edge Router queries
        .route("/zones/routers", get(zones_routers_handler))
        .route("/zones/{zone_id}/segments", get(zone_segments_handler))
        .route(
            "/zones/{zone_id}/segments/version",
            get(zone_segments_version_handler),
        )
        // Zone Router queries
        .route("/zones/{zone_id}", get(zone_handler))
        .route("/zones/{zone_id}/version", get(zone_version_handler))
        // RzBridge queries
        .route("/shards/{shard_id}/nodes", get(shard_nodes_handler))
        .with_state(state)
        .layer(middleware::from_fn(track_metrics))
}

// =============================================================================
// Operational handlers
// =============================================================================

async fn health_handler() -> &'static str {
    "OK"
}

async fn metrics_handler() -> impl IntoResponse {
    let metrics = get_global_metrics();
    let encoder = prometheus::TextEncoder::new();
    let metric_families = metrics.registry.gather();

    let mut buffer = Vec::new();
    if let Err(e) = encoder.encode(&metric_families, &mut buffer) {
        tracing::error!(error = %e, "failed to encode prometheus metrics");
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to encode metrics",
        )
            .into_response();
    }

    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, encoder.format_type())],
        buffer,
    )
        .into_response()
}

// =============================================================================
// Registration
// =============================================================================

#[derive(Debug, Deserialize)]
struct RegisterRequest {
    kind: String, // "router" | "bridge" | "node"
    id: String,
    zone: String,
    shard: Option<String>,
}

async fn register_handler(
    State(state): State<Arc<AppState>>,
    Json(req): Json<RegisterRequest>,
) -> impl IntoResponse {
    match req.kind.as_str() {
        "router" => {
            state
                .register_router(req.id.clone(), req.zone.clone())
                .await;
            info!(id = %req.id, zone = %req.zone, "registered router");
            StatusCode::OK.into_response()
        }
        "bridge" => {
            let Some(shard) = req.shard else {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({"error": "shard is required for bridge"})),
                )
                    .into_response();
            };
            state
                .register_bridge(req.id.clone(), shard.clone(), req.zone.clone())
                .await;
            info!(id = %req.id, shard = %shard, zone = %req.zone, "registered bridge");
            StatusCode::OK.into_response()
        }
        "node" => {
            let Some(shard) = req.shard else {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({"error": "shard is required for node"})),
                )
                    .into_response();
            };
            state
                .register_node(req.id.clone(), shard.clone(), req.zone.clone())
                .await;
            info!(id = %req.id, shard = %shard, zone = %req.zone, "registered node");
            StatusCode::OK.into_response()
        }
        other => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": format!("unknown kind: {other}")})),
        )
            .into_response(),
    }
}

// =============================================================================
// Segment ownership
// =============================================================================

#[derive(Debug, Serialize)]
struct ChecksumResponse {
    checksum: String,
}

async fn shard_segments_version_handler(
    State(state): State<Arc<AppState>>,
    Path(shard_id): Path<String>,
) -> impl IntoResponse {
    match state.segment_checksum(&shard_id).await {
        Some(checksum) => Json(ChecksumResponse { checksum }).into_response(),
        None => Json(ChecksumResponse {
            checksum: String::new(),
        })
        .into_response(),
    }
}

#[derive(Debug, Deserialize)]
struct UpdateSegmentsRequest {
    zone: String,
    segments: Vec<String>,
}

async fn update_segments_handler(
    State(state): State<Arc<AppState>>,
    Path(shard_id): Path<String>,
    Json(req): Json<UpdateSegmentsRequest>,
) -> impl IntoResponse {
    let changed = state
        .update_segments(shard_id.clone(), req.zone, req.segments)
        .await;

    if changed {
        info!(shard = %shard_id, "segment list updated");
    }

    StatusCode::OK
}

// =============================================================================
// Edge Router queries
// =============================================================================

#[derive(Debug, Serialize)]
struct ZonesRoutersResponse {
    zones: std::collections::HashMap<String, ZoneRouters>,
}

#[derive(Debug, Serialize)]
struct ZoneRouters {
    routers: Vec<String>,
}

async fn zones_routers_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let map = state.zones_routers().await;
    let zones = map
        .into_iter()
        .map(|(zone, routers)| (zone, ZoneRouters { routers }))
        .collect();
    Json(ZonesRoutersResponse { zones })
}

#[derive(Debug, Serialize)]
struct SegmentsResponse {
    segments: Vec<String>,
}

async fn zone_segments_handler(
    State(state): State<Arc<AppState>>,
    Path(zone_id): Path<String>,
) -> impl IntoResponse {
    let (segments, _) = state.zone_segments(&zone_id).await;
    Json(SegmentsResponse { segments })
}

async fn zone_segments_version_handler(
    State(state): State<Arc<AppState>>,
    Path(zone_id): Path<String>,
) -> impl IntoResponse {
    let (_, checksum) = state.zone_segments(&zone_id).await;
    Json(ChecksumResponse { checksum })
}

// =============================================================================
// Zone Router queries
// =============================================================================

#[derive(Debug, Serialize)]
struct ZoneResponse {
    shards: std::collections::HashMap<String, ShardBridge>,
    version: String,
}

#[derive(Debug, Serialize)]
struct ShardBridge {
    bridge_id: String,
}

async fn zone_handler(
    State(state): State<Arc<AppState>>,
    Path(zone_id): Path<String>,
) -> impl IntoResponse {
    let (shards_map, version) = state.zone_shards(&zone_id).await;
    let shards = shards_map
        .into_iter()
        .map(|(shard, bridge_id)| (shard, ShardBridge { bridge_id }))
        .collect();
    Json(ZoneResponse { shards, version })
}

async fn zone_version_handler(
    State(state): State<Arc<AppState>>,
    Path(zone_id): Path<String>,
) -> impl IntoResponse {
    let (_, checksum) = state.zone_shards(&zone_id).await;
    Json(ChecksumResponse { checksum })
}

// =============================================================================
// RzBridge queries
// =============================================================================

#[derive(Debug, Serialize)]
struct NodesResponse {
    nodes: Vec<String>,
}

async fn shard_nodes_handler(
    State(state): State<Arc<AppState>>,
    Path(shard_id): Path<String>,
) -> impl IntoResponse {
    let nodes = state.shard_nodes(&shard_id).await;
    Json(NodesResponse { nodes })
}

async fn track_metrics(req: Request<Body>, next: Next) -> Response<Body> {
    let start = Instant::now();
    let path = req
        .extensions()
        .get::<MatchedPath>()
        .map(|m| m.as_str().to_owned())
        .unwrap_or_else(|| req.uri().path().to_owned());

    let response = next.run(req).await;

    let status = response.status().as_u16();
    let metrics = get_global_metrics();

    metrics.requests_total.with_label_values(&[&path]).inc();

    metrics
        .request_duration_seconds
        .with_label_values(&[&path])
        .observe(start.elapsed().as_secs_f64());

    if status >= 400 {
        let error = if status >= 500 {
            "server_error"
        } else {
            "client_error"
        };
        metrics
            .request_errors_total
            .with_label_values(&[&path, error])
            .inc();
    }

    response
}
