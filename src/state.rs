use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tracing::{info, warn};

use crate::error::Result;
use crate::metrics::get_global_metrics;

// =============================================================================
// Public ID types
// =============================================================================

pub type RouterId = String;
pub type BridgeId = String;
pub type NodeId = String;
pub type ZoneId = String;
pub type ShardId = String;
pub type Segment = String;

pub type Version = u64;

// =============================================================================
// Version keys
// =============================================================================
//
// Each key represents one independently fetchable dataset.
//
// Consumers query /state/versions once and compare these versions with their
// local versions before deciding which data endpoints need to be fetched.
//

fn zone_routers_key(zone_id: &str) -> String {
    format!("zones/{zone_id}/routers")
}

fn zone_shards_key(zone_id: &str) -> String {
    format!("zones/{zone_id}/shards")
}

fn zone_segments_key(zone_id: &str) -> String {
    format!("zones/{zone_id}/segments")
}

fn shard_segments_key(shard_id: &str) -> String {
    format!("shards/{shard_id}/segments")
}

fn shard_nodes_key(shard_id: &str) -> String {
    format!("shards/{shard_id}/nodes")
}

// =============================================================================
// Component records
// =============================================================================

/// Stores the authoritative registration information for a router.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RouterInfo {
    pub zone_id: ZoneId,

    /// Runtime heartbeat timestamp.
    ///
    /// This value is reset after loading persisted state because wall-clock
    /// heartbeat age should not make all persisted components immediately
    /// expire after a restart.
    #[serde(default)]
    pub last_seen_ms: u64,
}

/// Stores the authoritative registration information for a bridge.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BridgeInfo {
    pub shard_id: ShardId,
    pub zone_id: ZoneId,

    #[serde(default)]
    pub last_seen_ms: u64,
}

/// Stores the authoritative registration information for a node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    pub shard_id: ShardId,
    pub zone_id: ZoneId,

    #[serde(default)]
    pub last_seen_ms: u64,
}

// =============================================================================
// Component stores
// =============================================================================

/// Stores all authoritative component registrations.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct Components {
    routers: HashMap<RouterId, RouterInfo>,
    bridges: HashMap<BridgeId, BridgeInfo>,
    nodes: HashMap<NodeId, NodeInfo>,
}

// =============================================================================
// Segment storage
// =============================================================================

/// Stores a shard's segments in both set and sorted-vector form.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct SegmentSet {
    /// Used for fast equality and membership checks.
    set: HashSet<Segment>,

    /// Used directly for deterministic API responses.
    sorted: Vec<Segment>,
}

/// Creates a normalized segment set.
impl SegmentSet {
    fn new(mut segments: Vec<Segment>) -> Self {
        segments.sort();
        segments.dedup();

        let set = segments.iter().cloned().collect();

        Self {
            set,
            sorted: segments,
        }
    }

    /// Checks whether two segment sets contain exactly the same segments.
    fn same_contents(&self, other: &Self) -> bool {
        self.set == other.set
    }
}

/// Stores the authoritative segment ownership of one shard.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ShardSegments {
    zone_id: ZoneId,
    segments: SegmentSet,
}

/// Stores segment ownership indexed by shard.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct SegmentStore {
    by_shard: HashMap<ShardId, ShardSegments>,
}

// =============================================================================
// Derived indexes
// =============================================================================
//
// These indexes are maintained on writes.
//
// Therefore normal API reads do not scan the authoritative stores or calculate
// anything expensive.
//

/// Stores all query-optimized derived indexes.
#[derive(Debug, Default)]
struct Indexes {
    /// zone -> sorted router IDs
    routers_by_zone: HashMap<ZoneId, Vec<RouterId>>,

    /// zone -> shard -> sorted bridge IDs
    bridges_by_zone_shard: HashMap<ZoneId, HashMap<ShardId, Vec<BridgeId>>>,

    /// shard -> sorted node IDs
    nodes_by_shard: HashMap<ShardId, Vec<NodeId>>,

    /// zone -> segment -> number of shards owning the segment
    segment_refs_by_zone: HashMap<ZoneId, HashMap<Segment, u32>>,

    /// zone -> sorted unique segments
    segments_by_zone: HashMap<ZoneId, Vec<Segment>>,
}

// =============================================================================
// Persisted state
// =============================================================================

/// Contains only authoritative state and version information.
///
/// Derived indexes are intentionally not persisted. They are rebuilt on
/// startup from the authoritative records.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedState {
    components: Components,
    segments: SegmentStore,

    /// Dataset key -> monotonically increasing version.
    versions: HashMap<String, Version>,

    /// Monotonically increasing revision of any externally visible change.
    global_version: Version,
}

// =============================================================================
// Version manifest
// =============================================================================

/// Contains every dataset version in one response.
#[derive(Debug, Clone, Serialize)]
pub struct VersionManifest {
    pub global_version: Version,

    /// Example:
    ///
    /// zones/eu/routers       -> 17
    /// zones/eu/shards        -> 31
    /// zones/eu/segments      -> 84
    /// shards/a/segments     -> 9
    /// shards/a/nodes        -> 12
    pub versions: HashMap<String, Version>,
}

// =============================================================================
// AppState
// =============================================================================

/// Owns the RzID source of truth and all query-optimized indexes.
#[derive(Debug)]
pub struct AppState {
    /// Protects authoritative state, indexes, and versions together.
    data: RwLock<AppData>,

    /// Components older than this are removed by the background sweep.
    heartbeat_timeout: Duration,

    /// Location of the persisted state file.
    state_file: PathBuf,

    /// Interval between buffered persistence attempts.
    buffer_ms: u64,

    /// Protects persistence dirty-state transitions.
    dirty: RwLock<bool>,
}

/// Contains all runtime state protected by AppState::data.
#[derive(Debug)]
struct AppData {
    components: Components,
    segments: SegmentStore,
    indexes: Indexes,

    versions: HashMap<String, Version>,
    global_version: Version,
}

/// Creates an empty runtime state.
impl Default for AppData {
    fn default() -> Self {
        Self {
            components: Components::default(),
            segments: SegmentStore::default(),
            indexes: Indexes::default(),
            versions: HashMap::new(),
            global_version: 0,
        }
    }
}

// =============================================================================
// Construction
// =============================================================================

impl AppState {
    /// Loads persisted authoritative state and rebuilds all derived indexes.
    pub async fn new(
        state_file: impl Into<PathBuf>,
        heartbeat_timeout_secs: u64,
        buffer_ms: u64,
    ) -> Result<Arc<Self>> {
        let state_file = state_file.into();

        let persisted = if state_file.exists() {
            match load_from_disk(&state_file).await {
                Ok(value) => {
                    info!(
                        path = %state_file.display(),
                        "loaded RzID state from disk"
                    );

                    value
                }

                Err(error) => {
                    warn!(
                        path = %state_file.display(),
                        error = %error,
                        "failed to load state; starting empty"
                    );

                    PersistedState {
                        components: Components::default(),
                        segments: SegmentStore::default(),
                        versions: HashMap::new(),
                        global_version: 0,
                    }
                }
            }
        } else {
            info!(
                path = %state_file.display(),
                "no state file found; starting empty"
            );

            PersistedState {
                components: Components::default(),
                segments: SegmentStore::default(),
                versions: HashMap::new(),
                global_version: 0,
            }
        };

        let mut data = AppData {
            components: persisted.components,
            segments: persisted.segments,
            indexes: Indexes::default(),
            versions: persisted.versions,
            global_version: persisted.global_version,
        };

        // Heartbeat timestamps are runtime state. Loaded components are
        // considered alive when the service starts.
        reset_heartbeat_times(&mut data.components);

        // Reconstruct all read indexes from authoritative state.
        rebuild_indexes(&mut data);

        // Update component gauges with current state
        let routers = data.components.routers.len();
        let bridges = data.components.bridges.len();
        let nodes = data.components.nodes.len();
        let zones = data.indexes.routers_by_zone.len(); // zones with routers
        get_global_metrics().update_component_gauges(routers, bridges, nodes, zones);

        Ok(Arc::new(Self {
            data: RwLock::new(data),

            heartbeat_timeout: Duration::from_secs(heartbeat_timeout_secs),

            state_file,

            buffer_ms,

            dirty: RwLock::new(false),
        }))
    }
}

// =============================================================================
// Router registration
// =============================================================================

impl AppState {
    /// Registers or refreshes a router and updates affected zone indexes.
    pub async fn register_router(&self, id: RouterId, zone_id: ZoneId) {
        let now = now_ms();

        let mut data = self.data.write().await;

        let old_zone = data
            .components
            .routers
            .get(&id)
            .map(|router| router.zone_id.clone());

        let topology_changed = old_zone.as_deref() != Some(zone_id.as_str());

        data.components.routers.insert(
            id.clone(),
            RouterInfo {
                zone_id: zone_id.clone(),
                last_seen_ms: now,
            },
        );

        if topology_changed {
            // Remove from old zone.
            if let Some(old_zone) = old_zone {
                rebuild_zone_routers(&mut data, &old_zone);

                bump_version(&mut data, zone_routers_key(&old_zone));
            }

            // Add to new zone.
            rebuild_zone_routers(&mut data, &zone_id);

            bump_version(&mut data, zone_routers_key(&zone_id));

            let routers = data.components.routers.len();
            let bridges = data.components.bridges.len();
            let nodes = data.components.nodes.len();
            let zones = data.indexes.routers_by_zone.len();
            get_global_metrics().update_component_gauges(routers, bridges, nodes, zones);

            drop(data);
            self.mark_dirty().await;
        }
    }

    /// Registers or refreshes a bridge and updates affected zone/shard indexes.
    pub async fn register_bridge(&self, id: BridgeId, shard_id: ShardId, zone_id: ZoneId) {
        let now = now_ms();

        let mut data = self.data.write().await;

        let old = data.components.bridges.get(&id).cloned();

        let topology_changed = match &old {
            Some(old) => old.shard_id != shard_id || old.zone_id != zone_id,
            None => true,
        };

        data.components.bridges.insert(
            id,
            BridgeInfo {
                shard_id: shard_id.clone(),
                zone_id: zone_id.clone(),
                last_seen_ms: now,
            },
        );

        if topology_changed {
            // Remove old location.
            if let Some(old) = old {
                rebuild_zone_shards(&mut data, &old.zone_id);

                bump_version(&mut data, zone_shards_key(&old.zone_id));
            }

            // Add new location.
            rebuild_zone_shards(&mut data, &zone_id);

            bump_version(&mut data, zone_shards_key(&zone_id));

            let routers = data.components.routers.len();
            let bridges = data.components.bridges.len();
            let nodes = data.components.nodes.len();
            let zones = data.indexes.routers_by_zone.len();
            get_global_metrics().update_component_gauges(routers, bridges, nodes, zones);

            drop(data);
            self.mark_dirty().await;
        }
    }

    /// Registers or refreshes a node and updates its shard index.
    pub async fn register_node(&self, id: NodeId, shard_id: ShardId, zone_id: ZoneId) {
        let now = now_ms();

        let mut data = self.data.write().await;

        let old = data.components.nodes.get(&id).cloned();

        let topology_changed = match &old {
            Some(old) => old.shard_id != shard_id,
            None => true,
        };

        data.components.nodes.insert(
            id,
            NodeInfo {
                shard_id: shard_id.clone(),
                zone_id,
                last_seen_ms: now,
            },
        );

        if topology_changed {
            // Remove from old shard.
            if let Some(old) = old {
                rebuild_shard_nodes(&mut data, &old.shard_id);

                bump_version(&mut data, shard_nodes_key(&old.shard_id));
            }

            // Add to new shard.
            rebuild_shard_nodes(&mut data, &shard_id);

            bump_version(&mut data, shard_nodes_key(&shard_id));

            let routers = data.components.routers.len();
            let bridges = data.components.bridges.len();
            let nodes = data.components.nodes.len();
            let zones = data.indexes.routers_by_zone.len();
            get_global_metrics().update_component_gauges(routers, bridges, nodes, zones);

            drop(data);
            self.mark_dirty().await;
        }
    }
}

// =============================================================================
// Segment ownership
// =============================================================================

impl AppState {
    /// Replaces a shard's segment ownership and updates only affected zone
    /// segment reference counts.
    pub async fn update_segments(
        &self,
        shard_id: ShardId,
        zone_id: ZoneId,
        segments: Vec<Segment>,
    ) -> bool {
        let new_segments = SegmentSet::new(segments);

        let mut data = self.data.write().await;

        let old = data.segments.by_shard.get(&shard_id).cloned();

        // Identical update: no index changes and no version changes.
        if let Some(old) = &old {
            if old.zone_id == zone_id && old.segments.same_contents(&new_segments) {
                return false;
            }
        }

        // Track which zone segment datasets are actually changed.
        let mut changed_zones = HashSet::new();

        // Remove old shard ownership.
        if let Some(old) = &old {
            let changed = remove_shard_segments_from_zone(&mut data, &old.zone_id, &old.segments);

            if changed {
                changed_zones.insert(old.zone_id.clone());
            }
        }

        // Add new shard ownership.
        let changed = add_shard_segments_to_zone(&mut data, &zone_id, &new_segments);

        if changed {
            changed_zones.insert(zone_id.clone());
        }

        // Replace authoritative shard ownership.
        data.segments.by_shard.insert(
            shard_id.clone(),
            ShardSegments {
                zone_id: zone_id.clone(),
                segments: new_segments,
            },
        );

        // The shard-level dataset definitely changed because either its
        // segments or its zone changed.
        bump_version(&mut data, shard_segments_key(&shard_id));

        // Only bump zone versions when the zone's observable unique segment
        // set actually changed.
        for changed_zone in changed_zones {
            bump_version(&mut data, zone_segments_key(&changed_zone));
        }

        get_global_metrics()
            .segment_update_last_timestamp
            .set(now_ms() as f64 / 1000.0);

        let routers = data.components.routers.len();
        let bridges = data.components.bridges.len();
        let nodes = data.components.nodes.len();
        let zones = data.indexes.routers_by_zone.len();
        get_global_metrics().update_component_gauges(routers, bridges, nodes, zones);

        drop(data);

        self.mark_dirty().await;

        get_global_metrics()
            .segment_updates_total
            .with_label_values(&[&shard_id])
            .inc();

        true
    }
}

// =============================================================================
// Version manifest
// =============================================================================

impl AppState {
    /// Returns every dataset version in one cheap read.
    pub async fn version_manifest(&self) -> VersionManifest {
        let data = self.data.read().await;

        VersionManifest {
            global_version: data.global_version,
            versions: data.versions.clone(),
        }
    }
}

// =============================================================================
// Edge Router queries
// =============================================================================

impl AppState {
    /// Returns the routers belonging to a zone.
    pub async fn zone_routers(&self, zone_id: &str) -> (Version, Vec<RouterId>) {
        let data = self.data.read().await;

        let version = get_version(&data, &zone_routers_key(zone_id));

        let routers = data
            .indexes
            .routers_by_zone
            .get(zone_id)
            .cloned()
            .unwrap_or_default();

        (version, routers)
    }

    /// Returns the unique segments currently owned anywhere in a zone.
    pub async fn zone_segments(&self, zone_id: &str) -> (Version, Vec<Segment>) {
        let data = self.data.read().await;

        let version = get_version(&data, &zone_segments_key(zone_id));

        let segments = data
            .indexes
            .segments_by_zone
            .get(zone_id)
            .cloned()
            .unwrap_or_default();

        (version, segments)
    }
}

// =============================================================================
// Zone Router queries
// =============================================================================

impl AppState {
    /// Returns every shard in a zone and all bridge instances serving it.
    pub async fn zone_shards(&self, zone_id: &str) -> (Version, HashMap<ShardId, Vec<BridgeId>>) {
        let data = self.data.read().await;

        let version = get_version(&data, &zone_shards_key(zone_id));

        let shards = data
            .indexes
            .bridges_by_zone_shard
            .get(zone_id)
            .cloned()
            .unwrap_or_default();

        (version, shards)
    }

    /// Returns the authoritative segment list for one shard.
    pub async fn shard_segments(&self, shard_id: &str) -> Option<(Version, ZoneId, Vec<Segment>)> {
        let data = self.data.read().await;

        let shard = data.segments.by_shard.get(shard_id)?;

        let version = get_version(&data, &shard_segments_key(shard_id));

        Some((
            version,
            shard.zone_id.clone(),
            shard.segments.sorted.clone(),
        ))
    }
}

// =============================================================================
// Bridge queries
// =============================================================================

impl AppState {
    /// Returns all currently registered nodes for a shard.
    pub async fn shard_nodes(&self, shard_id: &str) -> Option<(Version, Vec<NodeId>)> {
        let data = self.data.read().await;

        let nodes = data.indexes.nodes_by_shard.get(shard_id)?;

        let version = get_version(&data, &shard_nodes_key(shard_id));

        Some((version, nodes.clone()))
    }
}

// =============================================================================
// Heartbeat sweeping
// =============================================================================

impl AppState {
    /// Removes expired routers, bridges, and nodes and updates only affected
    /// indexes and versions.
    pub async fn sweep_expired_components(&self) {
        let now = now_ms();
        let timeout_ms = self.heartbeat_timeout.as_millis() as u64;

        let mut data = self.data.write().await;

        let mut changed_router_zones = HashSet::new();
        let mut changed_bridge_zones = HashSet::new();
        let mut changed_node_shards = HashSet::new();

        // ---------------------------------------------------------------------
        // Find expired routers.
        // ---------------------------------------------------------------------

        let expired_routers: Vec<(RouterId, ZoneId)> = data
            .components
            .routers
            .iter()
            .filter(|(_, info)| now.saturating_sub(info.last_seen_ms) > timeout_ms)
            .map(|(id, info)| (id.clone(), info.zone_id.clone()))
            .collect();

        // ---------------------------------------------------------------------
        // Remove expired routers.
        // ---------------------------------------------------------------------

        for (id, zone_id) in expired_routers {
            data.components.routers.remove(&id);

            changed_router_zones.insert(zone_id);
        }

        // ---------------------------------------------------------------------
        // Find expired bridges.
        // ---------------------------------------------------------------------

        let expired_bridges: Vec<(BridgeId, ZoneId)> = data
            .components
            .bridges
            .iter()
            .filter(|(_, info)| now.saturating_sub(info.last_seen_ms) > timeout_ms)
            .map(|(id, info)| (id.clone(), info.zone_id.clone()))
            .collect();

        // ---------------------------------------------------------------------
        // Remove expired bridges.
        // ---------------------------------------------------------------------

        for (id, zone_id) in expired_bridges {
            data.components.bridges.remove(&id);

            changed_bridge_zones.insert(zone_id);
        }

        // ---------------------------------------------------------------------
        // Find expired nodes.
        // ---------------------------------------------------------------------

        let expired_nodes: Vec<(NodeId, ShardId)> = data
            .components
            .nodes
            .iter()
            .filter(|(_, info)| now.saturating_sub(info.last_seen_ms) > timeout_ms)
            .map(|(id, info)| (id.clone(), info.shard_id.clone()))
            .collect();

        // ---------------------------------------------------------------------
        // Remove expired nodes.
        // ---------------------------------------------------------------------

        for (id, shard_id) in expired_nodes {
            data.components.nodes.remove(&id);

            changed_node_shards.insert(shard_id);
        }

        // ---------------------------------------------------------------------
        // Rebuild affected router indexes.
        // ---------------------------------------------------------------------

        for zone_id in &changed_router_zones {
            rebuild_zone_routers(&mut data, zone_id);

            bump_version(&mut data, zone_routers_key(zone_id));
        }

        // ---------------------------------------------------------------------
        // Rebuild affected bridge indexes.
        // ---------------------------------------------------------------------

        for zone_id in &changed_bridge_zones {
            rebuild_zone_shards(&mut data, zone_id);

            bump_version(&mut data, zone_shards_key(zone_id));
        }

        // ---------------------------------------------------------------------
        // Rebuild affected node indexes.
        // ---------------------------------------------------------------------

        for shard_id in &changed_node_shards {
            rebuild_shard_nodes(&mut data, shard_id);

            bump_version(&mut data, shard_nodes_key(shard_id));
        }

        let removed =
            changed_router_zones.len() + changed_bridge_zones.len() + changed_node_shards.len();

        // Update component gauges after sweeping
        let routers = data.components.routers.len();
        let bridges = data.components.bridges.len();
        let nodes = data.components.nodes.len();
        let zones = data.indexes.routers_by_zone.len();
        get_global_metrics().update_component_gauges(routers, bridges, nodes, zones);

        drop(data);

        if removed > 0 {
            self.mark_dirty().await;

            get_global_metrics()
                .heartbeat_timeout_total
                .inc_by(removed as f64);
        }
    }
}

// =============================================================================
// Persistence
// =============================================================================

impl AppState {
    /// Marks state dirty so the background persistence task will flush it.
    async fn mark_dirty(&self) {
        *self.dirty.write().await = true;
    }

    /// Returns whether persistence is currently required.
    pub async fn is_dirty(&self) -> bool {
        *self.dirty.read().await
    }

    /// Persists authoritative state atomically and clears dirty state only
    /// after a successful write.
    pub async fn persist(&self) -> Result<()> {
        //
        // Hold the dirty lock during the snapshot/write/clear sequence.
        //
        // A concurrent mutation can still update data, but its subsequent
        // mark_dirty() waits until this operation finishes, preventing a
        // mutation from being accidentally hidden by dirty=false.
        //
        let mut dirty = self.dirty.write().await;

        if !*dirty {
            return Ok(());
        }

        let snapshot = {
            let data = self.data.read().await;

            PersistedState {
                components: data.components.clone(),
                segments: data.segments.clone(),
                versions: data.versions.clone(),
                global_version: data.global_version,
            }
        };

        atomic_write(&self.state_file, &snapshot).await?;

        *dirty = false;

        get_global_metrics()
            .last_persist_timestamp
            .set(now_ms() as f64 / 1000.0);

        let metrics = get_global_metrics();

        metrics.json_write_total.inc();

        Ok(())
    }
}

// =============================================================================
// Index maintenance
// =============================================================================

/// Rebuilds the router index for one zone.
fn rebuild_zone_routers(data: &mut AppData, zone_id: &str) {
    let mut routers = data
        .components
        .routers
        .iter()
        .filter(|(_, info)| info.zone_id == zone_id)
        .map(|(id, _)| id.clone())
        .collect::<Vec<_>>();

    routers.sort();

    if routers.is_empty() {
        data.indexes.routers_by_zone.remove(zone_id);
    } else {
        data.indexes
            .routers_by_zone
            .insert(zone_id.to_owned(), routers);
    }
}

/// Rebuilds the bridge index for one zone.
fn rebuild_zone_shards(data: &mut AppData, zone_id: &str) {
    let mut shards: HashMap<ShardId, Vec<BridgeId>> = HashMap::new();

    for (bridge_id, bridge) in &data.components.bridges {
        if bridge.zone_id == zone_id {
            shards
                .entry(bridge.shard_id.clone())
                .or_default()
                .push(bridge_id.clone());
        }
    }

    for bridges in shards.values_mut() {
        bridges.sort();
    }

    if shards.is_empty() {
        data.indexes.bridges_by_zone_shard.remove(zone_id);
    } else {
        data.indexes
            .bridges_by_zone_shard
            .insert(zone_id.to_owned(), shards);
    }
}

/// Rebuilds the node index for one shard.
fn rebuild_shard_nodes(data: &mut AppData, shard_id: &str) {
    let mut nodes = data
        .components
        .nodes
        .iter()
        .filter(|(_, info)| info.shard_id == shard_id)
        .map(|(id, _)| id.clone())
        .collect::<Vec<_>>();

    nodes.sort();

    if nodes.is_empty() {
        data.indexes.nodes_by_shard.remove(shard_id);
    } else {
        data.indexes
            .nodes_by_shard
            .insert(shard_id.to_owned(), nodes);
    }
}

// =============================================================================
// Segment reference-count index
// =============================================================================

/// Adds a shard's segments to a zone and returns whether the zone's unique
/// segment set changed.
fn add_shard_segments_to_zone(data: &mut AppData, zone_id: &str, segments: &SegmentSet) -> bool {
    let refs = data
        .indexes
        .segment_refs_by_zone
        .entry(zone_id.to_owned())
        .or_default();

    let mut observable_change = false;

    for segment in &segments.set {
        let count = refs.entry(segment.clone()).or_insert(0);

        if *count == 0 {
            observable_change = true;
        }

        *count += 1;
    }

    if observable_change {
        rebuild_zone_segment_vector(data, zone_id);
    }

    observable_change
}

/// Removes a shard's segments from a zone and returns whether the zone's
/// unique segment set changed.
fn remove_shard_segments_from_zone(
    data: &mut AppData,
    zone_id: &str,
    segments: &SegmentSet,
) -> bool {
    let Some(refs) = data.indexes.segment_refs_by_zone.get_mut(zone_id) else {
        return false;
    };

    let mut observable_change = false;

    for segment in &segments.set {
        let Some(count) = refs.get_mut(segment) else {
            continue;
        };

        *count -= 1;

        if *count == 0 {
            refs.remove(segment);
            observable_change = true;
        }
    }

    if refs.is_empty() {
        data.indexes.segment_refs_by_zone.remove(zone_id);

        data.indexes.segments_by_zone.remove(zone_id);
    } else if observable_change {
        rebuild_zone_segment_vector(data, zone_id);
    }

    observable_change
}

/// Rebuilds only the sorted API representation of one zone's segment set.
fn rebuild_zone_segment_vector(data: &mut AppData, zone_id: &str) {
    let Some(refs) = data.indexes.segment_refs_by_zone.get(zone_id) else {
        data.indexes.segments_by_zone.remove(zone_id);

        return;
    };

    let mut segments = refs.keys().cloned().collect::<Vec<_>>();

    segments.sort();

    data.indexes
        .segments_by_zone
        .insert(zone_id.to_owned(), segments);
}

// =============================================================================
// Version helpers
// =============================================================================

/// Increments one dataset version and the global revision.
fn bump_version(data: &mut AppData, key: String) {
    let version = data.versions.entry(key).or_insert(0);

    *version += 1;

    data.global_version += 1;
}

/// Returns a dataset version or zero when the dataset has never existed.
fn get_version(data: &AppData, key: &str) -> Version {
    data.versions.get(key).copied().unwrap_or(0)
}

// =============================================================================
// Index reconstruction
// =============================================================================

/// Rebuilds every derived index from authoritative state after startup.
fn rebuild_indexes(data: &mut AppData) {
    data.indexes = Indexes::default();

    // -------------------------------------------------------------------------
    // Routers.
    // -------------------------------------------------------------------------

    for (router_id, router) in &data.components.routers {
        data.indexes
            .routers_by_zone
            .entry(router.zone_id.clone())
            .or_default()
            .push(router_id.clone());
    }

    for routers in data.indexes.routers_by_zone.values_mut() {
        routers.sort();
    }

    // -------------------------------------------------------------------------
    // Bridges.
    // -------------------------------------------------------------------------

    for (bridge_id, bridge) in &data.components.bridges {
        data.indexes
            .bridges_by_zone_shard
            .entry(bridge.zone_id.clone())
            .or_default()
            .entry(bridge.shard_id.clone())
            .or_default()
            .push(bridge_id.clone());
    }

    for shards in data.indexes.bridges_by_zone_shard.values_mut() {
        for bridges in shards.values_mut() {
            bridges.sort();
        }
    }

    // -------------------------------------------------------------------------
    // Nodes.
    // -------------------------------------------------------------------------

    for (node_id, node) in &data.components.nodes {
        data.indexes
            .nodes_by_shard
            .entry(node.shard_id.clone())
            .or_default()
            .push(node_id.clone());
    }

    for nodes in data.indexes.nodes_by_shard.values_mut() {
        nodes.sort();
    }

    // -------------------------------------------------------------------------
    // Zone segment reference counts.
    // -------------------------------------------------------------------------

    for shard in data.segments.by_shard.values() {
        let refs = data
            .indexes
            .segment_refs_by_zone
            .entry(shard.zone_id.clone())
            .or_default();

        for segment in &shard.segments.set {
            *refs.entry(segment.clone()).or_insert(0) += 1;
        }
    }

    // -------------------------------------------------------------------------
    // Zone segment response vectors.
    // -------------------------------------------------------------------------

    let zones = data
        .indexes
        .segment_refs_by_zone
        .keys()
        .cloned()
        .collect::<Vec<_>>();

    for zone_id in zones {
        rebuild_zone_segment_vector(data, &zone_id);
    }
}

// =============================================================================
// Persistence helpers
// =============================================================================

/// Loads authoritative state from disk.
async fn load_from_disk(path: &Path) -> Result<PersistedState> {
    let bytes = tokio::fs::read(path).await?;

    let state = serde_json::from_slice(&bytes)?;

    Ok(state)
}

/// Atomically replaces the previous state file with a new snapshot.
async fn atomic_write<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    let tmp = path.with_extension("json.tmp");

    let json = serde_json::to_vec_pretty(value)?;

    tokio::fs::write(&tmp, json).await?;

    tokio::fs::rename(&tmp, path).await?;

    Ok(())
}

// =============================================================================
// Runtime helpers
// =============================================================================

/// Returns the current wall-clock time in milliseconds.
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

/// Marks all loaded components as freshly seen after process startup.
fn reset_heartbeat_times(components: &mut Components) {
    let now = now_ms();

    for router in components.routers.values_mut() {
        router.last_seen_ms = now;
    }

    for bridge in components.bridges.values_mut() {
        bridge.last_seen_ms = now;
    }

    for node in components.nodes.values_mut() {
        node.last_seen_ms = now;
    }
}

// =============================================================================
// Background tasks
// =============================================================================

impl AppState {
    /// Starts heartbeat expiration and buffered persistence tasks.
    pub fn spawn_background_tasks(self: &Arc<Self>) {
        // ---------------------------------------------------------------------
        // Heartbeat sweep.
        // ---------------------------------------------------------------------

        let state = Arc::clone(self);

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(5));

            loop {
                interval.tick().await;

                state.sweep_expired_components().await;
            }
        });

        // ---------------------------------------------------------------------
        // Buffered persistence.
        // ---------------------------------------------------------------------

        let state = Arc::clone(self);

        let interval_duration = Duration::from_millis(state.buffer_ms);

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(interval_duration);

            loop {
                interval.tick().await;

                if state.is_dirty().await {
                    if let Err(error) = state.persist().await {
                        tracing::error!(
                            error = %error,
                            "failed to persist RzID state"
                        );

                        get_global_metrics().json_write_errors_total.inc();
                    }
                }
            }
        });
    }
}
