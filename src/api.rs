use std::sync::Arc;
use std::time::Instant;

use axum::{
    Json, Router,
    body::Body,
    extract::{MatchedPath, Path, State},
    http::{Request, StatusCode, header},
    middleware::{self, Next},
    response::IntoResponse,
    routing::{get, post},
};
use prometheus::Encoder;
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::metrics::get_global_metrics;
use crate::state::{AppState, VersionManifest};

// =============================================================================
// Router
// =============================================================================

/// Builds the complete RzID HTTP API.
pub fn build_router(state: Arc<AppState>) -> Router {
    Router::new()
        // ---------------------------------------------------------------------
        // Operational endpoints
        // ---------------------------------------------------------------------
        .route("/health", get(health_handler))
        .route("/metrics", get(metrics_handler))
        // ---------------------------------------------------------------------
        // Registration / heartbeat
        // ---------------------------------------------------------------------
        .route("/register", post(register_handler))
        // ---------------------------------------------------------------------
        // Version manifest
        //
        // Routers call this once and receive all dataset versions. They then
        // decide locally which data endpoints need to be fetched.
        // ---------------------------------------------------------------------
        .route("/versions", get(versions_handler))
        // ---------------------------------------------------------------------
        // Segment ownership
        //
        // A shard/leader publishes its authoritative segment list here.
        // ---------------------------------------------------------------------
        .route("/shards/{shard_id}/segments", post(update_segments_handler))
        // ---------------------------------------------------------------------
        // Edge Router views
        //
        // Edge routers need:
        //   1. routers in each zone
        //   2. all segments in each zone
        // ---------------------------------------------------------------------
        .route("/zones/{zone_id}/routers", get(zone_routers_handler))
        .route("/zones/{zone_id}/segments", get(zone_segments_handler))
        // ---------------------------------------------------------------------
        // Zone Router views
        //
        // Zone routers need:
        //   1. shards in their zone + all bridge instances
        //   2. segments owned by each shard
        // ---------------------------------------------------------------------
        .route("/zones/{zone_id}/shards", get(zone_shards_handler))
        .route("/shards/{shard_id}/segments", get(shard_segments_handler))
        // ---------------------------------------------------------------------
        // RzBridge view
        //
        // A bridge only needs the nodes belonging to its shard.
        // ---------------------------------------------------------------------
        .route("/shards/{shard_id}/nodes", get(shard_nodes_handler))
        .with_state(state)
        .layer(middleware::from_fn(track_metrics))
}

// =============================================================================
// Operational
// =============================================================================

/// Returns a simple health response.
async fn health_handler() -> &'static str {
    "OK"
}

/// Returns Prometheus metrics.
async fn metrics_handler() -> impl IntoResponse {
    let metrics = get_global_metrics();
    let encoder = prometheus::TextEncoder::new();
    let metric_families = metrics.registry.gather();

    let mut buffer = Vec::new();

    if let Err(error) = encoder.encode(&metric_families, &mut buffer) {
        tracing::error!(
            error = %error,
            "failed to encode metrics"
        );

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

/// Describes a component registration or heartbeat.
#[derive(Debug, Deserialize)]
struct RegisterRequest {
    /// "router", "bridge", or "node".
    kind: String,

    /// Component ID.
    id: String,

    /// Zone containing the component.
    zone: String,

    /// Required for bridges and nodes.
    shard: Option<String>,
}

/// Registers a router, bridge, or node.
async fn register_handler(
    State(state): State<Arc<AppState>>,
    Json(req): Json<RegisterRequest>,
) -> impl IntoResponse {
    match req.kind.as_str() {
        "router" => {
            state
                .register_router(req.id.clone(), req.zone.clone())
                .await;

            info!(
                id = %req.id,
                zone = %req.zone,
                "registered router"
            );

            StatusCode::OK.into_response()
        }

        "bridge" => {
            let Some(shard) = req.shard else {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({
                        "error": "shard is required for bridge"
                    })),
                )
                    .into_response();
            };

            state
                .register_bridge(req.id.clone(), shard.clone(), req.zone.clone())
                .await;

            info!(
                id = %req.id,
                shard = %shard,
                zone = %req.zone,
                "registered bridge"
            );

            StatusCode::OK.into_response()
        }

        "node" => {
            let Some(shard) = req.shard else {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({
                        "error": "shard is required for node"
                    })),
                )
                    .into_response();
            };

            state
                .register_node(req.id.clone(), shard.clone(), req.zone.clone())
                .await;

            info!(
                id = %req.id,
                shard = %shard,
                zone = %req.zone,
                "registered node"
            );

            StatusCode::OK.into_response()
        }

        other => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": format!(
                    "unknown registration kind: {other}"
                )
            })),
        )
            .into_response(),
    }
}

// =============================================================================
// Version manifest
// =============================================================================

/// Returns every dataset version in one request.
async fn versions_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let manifest: VersionManifest = state.version_manifest().await;

    Json(manifest)
}

// =============================================================================
// Segment ownership
// =============================================================================

/// Describes a shard's segment update.
#[derive(Debug, Deserialize)]
struct UpdateSegmentsRequest {
    /// Zone containing the shard.
    zone: String,

    /// Complete authoritative segment set for the shard.
    segments: Vec<String>,
}

/// Replaces the complete segment ownership of a shard.
async fn update_segments_handler(
    State(state): State<Arc<AppState>>,
    Path(shard_id): Path<String>,
    Json(req): Json<UpdateSegmentsRequest>,
) -> impl IntoResponse {
    let changed = state
        .update_segments(shard_id.clone(), req.zone, req.segments)
        .await;

    if changed {
        info!(
            shard = %shard_id,
            "shard segment ownership updated"
        );
    }

    StatusCode::OK
}

// =============================================================================
// Edge Router
// =============================================================================

/// Returns all routers belonging to one zone together with its version.
#[derive(Debug, Serialize)]
struct ZoneRoutersResponse {
    version: u64,
    routers: Vec<String>,
}

/// Returns the routers used by an edge router for a zone.
async fn zone_routers_handler(
    State(state): State<Arc<AppState>>,
    Path(zone_id): Path<String>,
) -> impl IntoResponse {
    let (version, routers) = state.zone_routers(&zone_id).await;

    Json(ZoneRoutersResponse { version, routers })
}

/// Returns all unique segments currently owned in a zone.
#[derive(Debug, Serialize)]
struct ZoneSegmentsResponse {
    version: u64,
    segments: Vec<String>,
}

/// Returns the complete zone segment set.
async fn zone_segments_handler(
    State(state): State<Arc<AppState>>,
    Path(zone_id): Path<String>,
) -> impl IntoResponse {
    let (version, segments) = state.zone_segments(&zone_id).await;

    Json(ZoneSegmentsResponse { version, segments })
}

// =============================================================================
// Zone Router
// =============================================================================

/// Describes all bridges serving one shard.
#[derive(Debug, Serialize)]
struct ShardBridges {
    bridges: Vec<String>,
}

/// Returns all shards in a zone and all bridge instances serving each shard.
#[derive(Debug, Serialize)]
struct ZoneShardsResponse {
    version: u64,
    shards: std::collections::HashMap<String, ShardBridges>,
}

/// Returns the shard/bridge topology for a zone.
async fn zone_shards_handler(
    State(state): State<Arc<AppState>>,
    Path(zone_id): Path<String>,
) -> impl IntoResponse {
    let (version, shards) = state.zone_shards(&zone_id).await;

    let shards = shards
        .into_iter()
        .map(|(shard_id, bridges)| (shard_id, ShardBridges { bridges }))
        .collect();

    Json(ZoneShardsResponse { version, shards })
}

/// Returns the complete segment set owned by one shard.
#[derive(Debug, Serialize)]
struct ShardSegmentsResponse {
    version: u64,
    zone: String,
    segments: Vec<String>,
}

/// Returns one shard's authoritative segments.
async fn shard_segments_handler(
    State(state): State<Arc<AppState>>,
    Path(shard_id): Path<String>,
) -> impl IntoResponse {
    match state.shard_segments(&shard_id).await {
        Some((version, zone, segments)) => Json(ShardSegmentsResponse {
            version,
            zone,
            segments,
        })
        .into_response(),

        None => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({
                "error": "unknown shard"
            })),
        )
            .into_response(),
    }
}

// =============================================================================
// RzBridge
// =============================================================================

/// Returns all nodes belonging to a shard.
#[derive(Debug, Serialize)]
struct NodesResponse {
    version: u64,
    nodes: Vec<String>,
}

/// Returns the node membership for a shard.
async fn shard_nodes_handler(
    State(state): State<Arc<AppState>>,
    Path(shard_id): Path<String>,
) -> impl IntoResponse {
    match state.shard_nodes(&shard_id).await {
        Some((version, nodes)) => Json(NodesResponse { version, nodes }).into_response(),

        None => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({
                "error": "unknown shard"
            })),
        )
            .into_response(),
    }
}

// =============================================================================
// Request metrics
// =============================================================================

/// Records request count, latency, and HTTP errors.
async fn track_metrics(req: Request<Body>, next: Next) -> axum::response::Response {
    let start = Instant::now();

    let path = req
        .extensions()
        .get::<MatchedPath>()
        .map(|matched| matched.as_str().to_owned())
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
        let error_type = if status >= 500 {
            "server_error"
        } else {
            "client_error"
        };

        metrics
            .request_errors_total
            .with_label_values(&[&path, error_type])
            .inc();
    }

    response
}
