use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tracing::{info, warn};

use crate::error::Result;
use crate::metrics::get_global_metrics;

// ===== Public ID types =====

pub type RouterId = String;
pub type BridgeId = String;
pub type NodeId = String;
pub type ZoneId = String;
pub type ShardId = String;
pub type Segment = String;

fn instant_now() -> Instant {
    Instant::now()
}

// ===== Records =====
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RouterInfo {
    pub zone_id: ZoneId,
    #[serde(skip, default = "instant_now")]
    pub last_seen: Instant,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BridgeInfo {
    pub shard_id: ShardId,
    pub zone_id: ZoneId,
    #[serde(skip, default = "instant_now")]
    pub last_seen: Instant,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    pub shard_id: ShardId,
    pub zone_id: ZoneId,
    #[serde(skip, default = "instant_now")]
    pub last_seen: Instant,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardSegments {
    pub zone_id: ZoneId,
    pub segments: Vec<Segment>,
    pub checksum: String,
}

// ===== Internal stores =====

#[derive(Debug, Default, Serialize, Deserialize)]
struct Components {
    routers: HashMap<RouterId, RouterInfo>,
    bridges: HashMap<BridgeId, BridgeInfo>,
    nodes: HashMap<NodeId, NodeInfo>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct SegmentStore {
    by_shard: HashMap<ShardId, ShardSegments>,
}

// ===== Top-level State =====

#[derive(Debug)]
pub struct AppState {
    components: RwLock<Components>,
    segments: RwLock<SegmentStore>,

    heartbeat_timeout: Duration,
    state_file: std::path::PathBuf,
    buffer_ms: u64,

    dirty: RwLock<bool>,
}

impl AppState {
    pub async fn new(
        state_file: impl Into<std::path::PathBuf>,
        heartbeat_timeout_secs: u64,
        buffer_ms: u64,
    ) -> Result<Arc<Self>> {
        let state_file = state_file.into();
        let heartbeat_timeout = Duration::from_secs(heartbeat_timeout_secs);

        let (mut components, segments) = if state_file.exists() {
            match load_from_disk(&state_file).await {
                Ok(data) => {
                    info!(path = %state_file.display(), "loaded state from disk");
                    data
                }
                Err(e) => {
                    warn!(
                        path = %state_file.display(),
                        error = %e,
                        "failed to load state file – starting with empty state"
                    );
                    (Components::default(), SegmentStore::default())
                }
            }
        } else {
            info!(path = %state_file.display(), "no state file found – starting empty");
            (Components::default(), SegmentStore::default())
        };

        // After loading from disk, last_seen is Instant::default() (zero).
        // Treat every loaded component as "just seen" so they are not immediately swept.
        let now = Instant::now();
        for info in components.routers.values_mut() {
            info.last_seen = now;
        }
        for info in components.bridges.values_mut() {
            info.last_seen = now;
        }
        for info in components.nodes.values_mut() {
            info.last_seen = now;
        }

        let state = Arc::new(Self {
            components: RwLock::new(components),
            segments: RwLock::new(segments),
            heartbeat_timeout,
            state_file,
            buffer_ms,
            dirty: RwLock::new(false),
        });

        state.update_gauges().await;

        Ok(state)
    }

    // -------------------------------------------------------------------------
    // Registration / Heartbeat
    // -------------------------------------------------------------------------

    pub async fn register_router(&self, id: RouterId, zone_id: ZoneId) {
        let mut guard = self.components.write().await;
        guard.routers.insert(
            id,
            RouterInfo {
                zone_id,
                last_seen: Instant::now(),
            },
        );
        self.mark_dirty().await;
        self.update_gauges_locked(&guard).await;
    }

    pub async fn register_bridge(&self, id: BridgeId, shard_id: ShardId, zone_id: ZoneId) {
        let mut guard = self.components.write().await;
        guard.bridges.insert(
            id,
            BridgeInfo {
                shard_id,
                zone_id,
                last_seen: Instant::now(),
            },
        );
        self.mark_dirty().await;
        self.update_gauges_locked(&guard).await;
    }

    pub async fn register_node(&self, id: NodeId, shard_id: ShardId, zone_id: ZoneId) {
        let mut guard = self.components.write().await;
        guard.nodes.insert(
            id,
            NodeInfo {
                shard_id,
                zone_id,
                last_seen: Instant::now(),
            },
        );
        self.mark_dirty().await;
        self.update_gauges_locked(&guard).await;
    }

    // -------------------------------------------------------------------------
    // Segment ownership
    // -------------------------------------------------------------------------

    pub async fn segment_checksum(&self, shard_id: &str) -> Option<String> {
        let guard = self.segments.read().await;
        guard.by_shard.get(shard_id).map(|s| s.checksum.clone())
    }

    pub async fn update_segments(
        &self,
        shard_id: ShardId,
        zone_id: ZoneId,
        mut segments: Vec<Segment>,
    ) -> bool {
        segments.sort();
        segments.dedup();
        let checksum = compute_checksum(&segments);

        let mut guard = self.segments.write().await;

        let changed = match guard.by_shard.get(&shard_id) {
            Some(existing) => existing.checksum != checksum,
            None => true,
        };

        if changed {
            guard.by_shard.insert(
                shard_id.clone(),
                ShardSegments {
                    zone_id,
                    segments,
                    checksum: checksum.clone(),
                },
            );
            self.mark_dirty().await;

            let metrics = get_global_metrics();
            metrics
                .segment_updates_total
                .with_label_values(&[&shard_id])
                .inc();
            metrics
                .segment_update_last_timestamp
                .set(now_unix_secs() as f64);
        }

        changed
    }

    // -------------------------------------------------------------------------
    // Query helpers
    // -------------------------------------------------------------------------

    pub async fn zones_routers(&self) -> HashMap<ZoneId, Vec<RouterId>> {
        let guard = self.components.read().await;
        let mut result: HashMap<ZoneId, Vec<RouterId>> = HashMap::new();

        for (router_id, info) in &guard.routers {
            result
                .entry(info.zone_id.clone())
                .or_default()
                .push(router_id.clone());
        }

        for list in result.values_mut() {
            list.sort();
        }
        result
    }

    pub async fn zone_shards(&self, zone_id: &str) -> (HashMap<ShardId, BridgeId>, String) {
        let guard = self.components.read().await;
        let mut shards: HashMap<ShardId, BridgeId> = HashMap::new();

        for (bridge_id, info) in &guard.bridges {
            if info.zone_id == zone_id {
                shards.insert(info.shard_id.clone(), bridge_id.clone());
            }
        }

        let checksum = compute_map_checksum(&shards);
        (shards, checksum)
    }

    pub async fn zone_segments(&self, zone_id: &str) -> (Vec<Segment>, String) {
        let guard = self.segments.read().await;
        let mut segments = Vec::new();

        for ss in guard.by_shard.values() {
            if ss.zone_id == zone_id {
                segments.extend(ss.segments.iter().cloned());
            }
        }

        segments.sort();
        segments.dedup();
        let checksum = compute_checksum(&segments);
        (segments, checksum)
    }

    pub async fn shard_nodes(&self, shard_id: &str) -> Vec<NodeId> {
        let guard = self.components.read().await;
        let mut nodes: Vec<NodeId> = guard
            .nodes
            .iter()
            .filter(|(_, info)| info.shard_id == shard_id)
            .map(|(id, _)| id.clone())
            .collect();
        nodes.sort();
        nodes
    }

    // -------------------------------------------------------------------------
    // Heartbeat sweep
    // -------------------------------------------------------------------------

    pub async fn sweep_expired_components(&self) {
        let now = Instant::now();
        let mut guard = self.components.write().await;

        let before_routers = guard.routers.len();
        let before_bridges = guard.bridges.len();
        let before_nodes = guard.nodes.len();

        guard
            .routers
            .retain(|_, info| now.duration_since(info.last_seen) <= self.heartbeat_timeout);
        guard
            .bridges
            .retain(|_, info| now.duration_since(info.last_seen) <= self.heartbeat_timeout);
        guard
            .nodes
            .retain(|_, info| now.duration_since(info.last_seen) <= self.heartbeat_timeout);

        let removed = (before_routers - guard.routers.len())
            + (before_bridges - guard.bridges.len())
            + (before_nodes - guard.nodes.len());

        if removed > 0 {
            // Prometheus Counter::inc_by takes f64
            get_global_metrics()
                .heartbeat_timeout_total
                .inc_by(removed as f64);
            self.mark_dirty().await;
        }

        self.update_gauges_locked(&guard).await;
    }

    // -------------------------------------------------------------------------
    // Persistence
    // -------------------------------------------------------------------------

    async fn mark_dirty(&self) {
        *self.dirty.write().await = true;
    }

    pub async fn is_dirty(&self) -> bool {
        *self.dirty.read().await
    }

    pub async fn persist(&self) -> Result<()> {
        let components = self.components.read().await;
        let segments = self.segments.read().await;

        let snapshot = PersistSnapshot {
            components: &*components,
            segments: &*segments,
        };

        atomic_write(&self.state_file, &snapshot).await?;

        *self.dirty.write().await = false;

        let metrics = get_global_metrics();
        metrics.json_write_total.inc();
        metrics.last_persist_timestamp.set(now_unix_secs() as f64);

        Ok(())
    }

    // -------------------------------------------------------------------------
    // Metrics
    // -------------------------------------------------------------------------

    async fn update_gauges(&self) {
        let guard = self.components.read().await;
        self.update_gauges_locked(&guard).await;
    }

    async fn update_gauges_locked(&self, components: &Components) {
        let zones: std::collections::HashSet<&str> = components
            .routers
            .values()
            .map(|r| r.zone_id.as_str())
            .chain(components.bridges.values().map(|b| b.zone_id.as_str()))
            .chain(components.nodes.values().map(|n| n.zone_id.as_str()))
            .collect();

        get_global_metrics().update_component_gauges(
            components.routers.len(),
            components.bridges.len(),
            components.nodes.len(),
            zones.len(),
        );
    }
}

// ===== Persistence types =====

#[derive(Serialize)]
struct PersistSnapshot<'a> {
    components: &'a Components,
    segments: &'a SegmentStore,
}

#[derive(Deserialize)]
struct PersistSnapshotOwned {
    components: Components,
    segments: SegmentStore,
}

async fn load_from_disk(path: &Path) -> Result<(Components, SegmentStore)> {
    let data = tokio::fs::read(path).await?;
    let snapshot: PersistSnapshotOwned = serde_json::from_slice(&data)?;
    Ok((snapshot.components, snapshot.segments))
}

async fn atomic_write<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    let tmp = path.with_extension("json.tmp");
    let json = serde_json::to_vec_pretty(value)?;
    tokio::fs::write(&tmp, &json).await?;
    tokio::fs::rename(&tmp, path).await?;
    Ok(())
}

// ===== Checksum helpers =====

fn compute_checksum(segments: &[Segment]) -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut hasher = DefaultHasher::new();
    for s in segments {
        s.hash(&mut hasher);
    }
    format!("{:016x}", hasher.finish())
}

fn compute_map_checksum(map: &HashMap<ShardId, BridgeId>) -> String {
    let mut pairs: Vec<_> = map.iter().collect();
    pairs.sort_by(|(a, _), (b, _)| a.cmp(b));

    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut hasher = DefaultHasher::new();
    for (k, v) in pairs {
        k.hash(&mut hasher);
        v.hash(&mut hasher);
    }
    format!("{:016x}", hasher.finish())
}

fn now_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

impl AppState {
    /// Spawn the periodic heartbeat sweep + the buffered persistence task.
    pub fn spawn_background_tasks(self: &Arc<Self>) {
        // Heartbeat sweep every 5 seconds
        let state = Arc::clone(self);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(5));
            loop {
                interval.tick().await;
                state.sweep_expired_components().await;
            }
        });

        // Buffered persistence
        let state = Arc::clone(self);
        let buffer = Duration::from_millis(state.buffer_ms);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(buffer);
            loop {
                interval.tick().await;
                if state.is_dirty().await {
                    if let Err(e) = state.persist().await {
                        tracing::error!(error = %e, "failed to persist state");
                        get_global_metrics().json_write_errors_total.inc();
                    }
                }
            }
        });
    }
}
